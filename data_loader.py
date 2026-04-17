"""
Capa de descarga de datos. Patrón Adaptador con fallback:
- BaseSpotSource / BaseFuturesSource: interfaces abstractas
- YahooSpotSource: spot VIX + VIX9D/3M/6M via yfinance (muy fiable)
- CBOEFuturesSource: futuros VX por contrato desde CDN de CBOE (fuente primaria,
  pero algunas IPs de datacenter pueden recibir 403)
- StooqFuturesSource: fuente de respaldo para series continuas (menos granular
  pero no bloqueada por datacenters)
- FuturesSourceWithFallback: combina CBOE + Stooq

Esto permite cambiar la fuente sin tocar storage, analytics ni UI.
"""
from __future__ import annotations

import datetime as dt
import io
import logging
from abc import ABC, abstractmethod

import pandas as pd
import requests

from config import (
    CBOE_URL_PATTERNS,
    HISTORY_START_DATE,
    YAHOO_TICKERS,
)
from utils import VXContract

logger = logging.getLogger(__name__)

# User-agent realista y headers completos para evitar 403 de CBOE CDN
_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/605.1.15 (KHTML, like Gecko) "
        "Version/17.0 Safari/605.1.15"
    ),
    "Accept": "text/csv,application/csv,application/octet-stream,*/*;q=0.9",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
    "Referer": "https://www.cboe.com/us/futures/market_statistics/historical_data/",
    "Origin": "https://www.cboe.com",
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "same-origin",
}
_REQUEST_TIMEOUT = 30


# -----------------------------------------------------------------------------
# INTERFACES
# -----------------------------------------------------------------------------
class BaseSpotSource(ABC):
    """Fuente de datos spot VIX e índices sintéticos."""

    @abstractmethod
    def fetch(self, start: dt.date, end: dt.date) -> pd.DataFrame:
        """Devuelve DataFrame con DatetimeIndex y columnas
        [vix, vix9d, vix3m, vix6m]."""
        ...


class BaseFuturesSource(ABC):
    """Fuente de datos de futuros VX individuales."""

    @abstractmethod
    def fetch_contract(self, contract: VXContract) -> pd.DataFrame:
        """Devuelve DataFrame con DatetimeIndex y columnas
        [open, high, low, settle, volume, open_interest]."""
        ...


# -----------------------------------------------------------------------------
# YAHOO — spot e índices sintéticos
# -----------------------------------------------------------------------------
class YahooSpotSource(BaseSpotSource):
    """Descarga VIX, VIX9D, VIX3M, VIX6M desde Yahoo Finance."""

    def fetch(self, start: dt.date, end: dt.date) -> pd.DataFrame:
        import yfinance as yf

        tickers = list(YAHOO_TICKERS.values())
        raw = yf.download(
            tickers=tickers,
            start=start.isoformat(),
            end=(end + dt.timedelta(days=1)).isoformat(),
            progress=False,
            auto_adjust=False,
            group_by="ticker",
            threads=False,
        )

        if raw is None or raw.empty:
            return pd.DataFrame()

        closes = {}
        for name, ticker in YAHOO_TICKERS.items():
            try:
                if isinstance(raw.columns, pd.MultiIndex):
                    if (ticker, "Close") in raw.columns:
                        s = raw[(ticker, "Close")]
                    elif ("Close", ticker) in raw.columns:
                        s = raw[("Close", ticker)]
                    else:
                        logger.warning("Ticker %s no encontrado", ticker)
                        continue
                else:
                    s = raw["Close"] if "Close" in raw.columns else raw
                closes[name] = s
            except Exception as e:
                logger.warning("Error extrayendo %s: %s", ticker, e)

        if not closes:
            return pd.DataFrame()

        df = pd.DataFrame(closes)
        df.index = pd.to_datetime(df.index).tz_localize(None).normalize()
        df.index.name = "date"
        df = df.dropna(how="all")
        for col in ["vix", "vix9d", "vix3m", "vix6m"]:
            if col not in df.columns:
                df[col] = pd.NA
        return df[["vix", "vix9d", "vix3m", "vix6m"]]


# -----------------------------------------------------------------------------
# CBOE — futuros VX por contrato
# -----------------------------------------------------------------------------
class CBOEFuturesSource(BaseFuturesSource):
    """
    Descarga CSVs individuales de CBOE por contrato.

    NOTA: CBOE bloquea con 403 algunas IPs de datacenter. Esto funciona
    fiable desde IPs residenciales (tu casa) pero puede fallar en algunos
    hosts cloud. Si falla, StooqFuturesSource actúa de fallback.
    """

    CSV_COLUMNS_MAP = {
        "Trade Date": "date",
        "Futures": "futures",
        "Open": "open",
        "High": "high",
        "Low": "low",
        "Close": "close",
        "Settle": "settle",
        "Change": "change",
        "Total Volume": "volume",
        "EFP": "efp",
        "Open Interest": "open_interest",
    }

    def fetch_contract(self, contract: VXContract) -> pd.DataFrame:
        """Intenta cada patrón de URL hasta que uno funcione."""
        # Expiración en formato YYYY-MM-DD (patrón moderno CBOE)
        expiry_str = contract.expiry.isoformat()

        urls = []
        for pattern in CBOE_URL_PATTERNS:
            try:
                url = pattern.format(
                    year=contract.year,
                    month_code=contract.month_code,
                    yy=contract.yy,
                    expiry=expiry_str,
                )
                urls.append(url)
            except KeyError:
                continue

        for url in urls:
            try:
                df = self._try_download(url)
                if df is not None and not df.empty:
                    logger.info("Descargado %s desde %s (%d filas)",
                                contract.code, url, len(df))
                    return df
            except Exception as e:
                logger.debug("Fallo %s: %s", url, e)
                continue

        logger.warning("No se pudo descargar %s desde CBOE", contract.code)
        return pd.DataFrame()

    def _try_download(self, url: str) -> pd.DataFrame | None:
        resp = requests.get(url, headers=_HEADERS, timeout=_REQUEST_TIMEOUT)
        if resp.status_code in (403, 404):
            return None
        resp.raise_for_status()

        text = resp.text
        lines = text.splitlines()
        header_idx = 0
        for i, ln in enumerate(lines):
            if ln.lower().startswith("trade date"):
                header_idx = i
                break
        clean_csv = "\n".join(lines[header_idx:])

        df = pd.read_csv(io.StringIO(clean_csv))
        if df.empty:
            return df

        df = df.rename(columns={k: v for k, v in self.CSV_COLUMNS_MAP.items()
                                if k in df.columns})
        if "date" not in df.columns:
            return pd.DataFrame()

        df["date"] = pd.to_datetime(df["date"], errors="coerce")
        df = df.dropna(subset=["date"]).set_index("date")
        df.index = df.index.tz_localize(None).normalize()

        for col in ["open", "high", "low", "settle", "volume", "open_interest"]:
            if col not in df.columns:
                df[col] = pd.NA
            df[col] = pd.to_numeric(df[col], errors="coerce")

        return df[["open", "high", "low", "settle", "volume", "open_interest"]]


# -----------------------------------------------------------------------------
# STOOQ — fallback para series continuas M1, M2
# -----------------------------------------------------------------------------
class StooqContinuousSource:
    """
    Descarga series continuas M1, M2 desde Stooq como fallback.

    No sustituye por completo a CBOE (Stooq no da histórico por contrato
    individual), pero cubre el caso crítico de tener algo con qué arrancar
    si CBOE falla.
    """

    @staticmethod
    def fetch_continuous(month: int = 1) -> pd.DataFrame:
        """Descarga serie continua VX{month}. Month: 1=M1, 2=M2."""
        symbol = f"vx{month}.f"
        url = f"https://stooq.com/q/d/l/?s={symbol}&i=d"
        try:
            resp = requests.get(url, headers={"User-Agent": "Mozilla/5.0"},
                                timeout=30)
            resp.raise_for_status()
            df = pd.read_csv(io.StringIO(resp.text))
            if "Date" not in df.columns:
                return pd.DataFrame()
            df["Date"] = pd.to_datetime(df["Date"])
            df = df.set_index("Date")
            df.index.name = "date"
            df = df.rename(columns={
                "Open": "open", "High": "high", "Low": "low",
                "Close": "settle", "Volume": "volume",
            })
            df["open_interest"] = pd.NA
            logger.info("Stooq M%d: %d filas descargadas", month, len(df))
            return df[["open", "high", "low", "settle", "volume", "open_interest"]]
        except Exception as e:
            logger.warning("Fallo Stooq M%d: %s", month, e)
            return pd.DataFrame()


# -----------------------------------------------------------------------------
# FACTORIES
# -----------------------------------------------------------------------------
def get_spot_source() -> BaseSpotSource:
    return YahooSpotSource()


def get_futures_source() -> BaseFuturesSource:
    return CBOEFuturesSource()


def get_stooq_fallback() -> StooqContinuousSource:
    return StooqContinuousSource()
