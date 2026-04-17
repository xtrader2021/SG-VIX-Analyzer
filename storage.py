"""
Capa de persistencia: lectura/escritura de parquet y SQLite.

Contiene validación de schemas y deduplicación. Nunca toca red.
Es el único módulo que sabe dónde y cómo están guardados los datos.
"""
from __future__ import annotations

import datetime as dt
import sqlite3
from contextlib import contextmanager
from pathlib import Path
from typing import Iterable

import pandas as pd

from config import (
    CONTINUOUS_DIR,
    DB_PATH,
    FUTURES_DIR,
    SPOT_DIR,
)
from utils import VXContract

# -----------------------------------------------------------------------------
# SCHEMAS ESPERADOS
# -----------------------------------------------------------------------------
SPOT_COLUMNS = ["vix", "vix9d", "vix3m", "vix6m"]
FUTURES_COLUMNS = ["open", "high", "low", "settle", "volume", "open_interest"]
CONTINUOUS_COLUMNS = [f"m{i}" for i in range(1, 9)]


# -----------------------------------------------------------------------------
# SQLITE
# -----------------------------------------------------------------------------
@contextmanager
def _db_conn():
    """Context manager para conexión SQLite."""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    try:
        yield conn
        conn.commit()
    finally:
        conn.close()


def init_db() -> None:
    """Crea tablas si no existen. Idempotente."""
    with _db_conn() as conn:
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS expirations (
                contract_code TEXT PRIMARY KEY,
                expiry_date   DATE NOT NULL,
                month         INTEGER NOT NULL,
                year          INTEGER NOT NULL
            );

            CREATE TABLE IF NOT EXISTS contracts_meta (
                contract_code TEXT PRIMARY KEY,
                first_trade   DATE,
                last_trade    DATE,
                is_active     INTEGER NOT NULL DEFAULT 1,
                n_rows        INTEGER DEFAULT 0
            );

            CREATE TABLE IF NOT EXISTS update_log (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                run_at      TIMESTAMP NOT NULL,
                source      TEXT NOT NULL,
                rows_added  INTEGER DEFAULT 0,
                status      TEXT NOT NULL,
                error       TEXT
            );
        """)


def register_contract(c: VXContract, is_active: bool) -> None:
    """Inserta o actualiza metadatos de un contrato."""
    with _db_conn() as conn:
        conn.execute(
            """INSERT OR REPLACE INTO expirations
               (contract_code, expiry_date, month, year)
               VALUES (?, ?, ?, ?)""",
            (c.code, c.expiry.isoformat(), c.month, c.year),
        )
        # Solo actualizar is_active sin pisar first/last trade si ya existen
        conn.execute(
            """INSERT INTO contracts_meta (contract_code, is_active)
               VALUES (?, ?)
               ON CONFLICT(contract_code) DO UPDATE SET is_active=excluded.is_active""",
            (c.code, 1 if is_active else 0),
        )


def update_contract_stats(code: str, df: pd.DataFrame) -> None:
    """Actualiza first_trade, last_trade y n_rows tras guardar."""
    if df.empty:
        return
    first = df.index.min().date().isoformat()
    last = df.index.max().date().isoformat()
    n = len(df)
    with _db_conn() as conn:
        conn.execute(
            """UPDATE contracts_meta
               SET first_trade=?, last_trade=?, n_rows=?
               WHERE contract_code=?""",
            (first, last, n, code),
        )


def log_update(source: str, rows_added: int, status: str,
               error: str | None = None) -> None:
    """Registra una ejecución de update en el log."""
    with _db_conn() as conn:
        conn.execute(
            """INSERT INTO update_log (run_at, source, rows_added, status, error)
               VALUES (?, ?, ?, ?, ?)""",
            (dt.datetime.now().isoformat(), source, rows_added, status, error),
        )


def get_last_update() -> dict | None:
    """Devuelve info de la última ejecución exitosa."""
    with _db_conn() as conn:
        row = conn.execute(
            """SELECT run_at, source, rows_added, status
               FROM update_log
               WHERE status='ok'
               ORDER BY id DESC LIMIT 1"""
        ).fetchone()
        return dict(row) if row else None


def get_update_log(limit: int = 20) -> pd.DataFrame:
    """Devuelve las últimas entradas del log como DataFrame."""
    with _db_conn() as conn:
        return pd.read_sql_query(
            "SELECT * FROM update_log ORDER BY id DESC LIMIT ?",
            conn, params=(limit,),
        )


def get_active_contract_codes() -> list[str]:
    """Devuelve los códigos de contratos marcados como activos."""
    with _db_conn() as conn:
        rows = conn.execute(
            "SELECT contract_code FROM contracts_meta WHERE is_active=1"
        ).fetchall()
        return [r["contract_code"] for r in rows]


def get_all_contracts_info() -> pd.DataFrame:
    """Devuelve DataFrame con toda la info de contratos registrados."""
    with _db_conn() as conn:
        return pd.read_sql_query(
            """SELECT e.contract_code, e.expiry_date, e.month, e.year,
                      m.first_trade, m.last_trade, m.is_active, m.n_rows
               FROM expirations e
               LEFT JOIN contracts_meta m ON e.contract_code = m.contract_code
               ORDER BY e.expiry_date""",
            conn,
        )


# -----------------------------------------------------------------------------
# PARQUET — SPOT
# -----------------------------------------------------------------------------
SPOT_PATH = SPOT_DIR / "vix_spot.parquet"


def read_spot() -> pd.DataFrame:
    """Lee el parquet de spot. Devuelve DataFrame vacío si no existe."""
    if not SPOT_PATH.exists():
        return pd.DataFrame(columns=SPOT_COLUMNS,
                            index=pd.DatetimeIndex([], name="date"))
    df = pd.read_parquet(SPOT_PATH)
    df.index.name = "date"
    return df


def write_spot(df: pd.DataFrame) -> None:
    """Guarda el parquet de spot tras validar schema."""
    _validate_spot(df)
    df.sort_index().to_parquet(SPOT_PATH)


def merge_spot(new_df: pd.DataFrame) -> int:
    """
    Merge incremental: combina new_df con el existente, deduplica por fecha
    (se queda con la versión más reciente = new_df) y guarda.

    Devuelve número de filas añadidas (netas).
    """
    if new_df.empty:
        return 0
    _validate_spot(new_df)
    existing = read_spot()
    n_before = len(existing)
    # concat con new_df al final, keep='last' → deduplicación prefiere nuevo
    combined = pd.concat([existing, new_df])
    combined = combined[~combined.index.duplicated(keep="last")]
    combined = combined.sort_index()
    write_spot(combined)
    return len(combined) - n_before


def _validate_spot(df: pd.DataFrame) -> None:
    """Lanza ValueError si el schema no coincide."""
    missing = set(SPOT_COLUMNS) - set(df.columns)
    if missing:
        raise ValueError(f"Spot DataFrame missing columns: {missing}")
    if not isinstance(df.index, pd.DatetimeIndex):
        raise ValueError("Spot DataFrame must have DatetimeIndex")


# -----------------------------------------------------------------------------
# PARQUET — FUTURES (un archivo por contrato)
# -----------------------------------------------------------------------------
def _contract_path(code: str) -> Path:
    return FUTURES_DIR / f"{code}.parquet"


def read_contract(code: str) -> pd.DataFrame:
    """Lee el parquet de un contrato. Vacío si no existe."""
    path = _contract_path(code)
    if not path.exists():
        return pd.DataFrame(columns=FUTURES_COLUMNS,
                            index=pd.DatetimeIndex([], name="date"))
    df = pd.read_parquet(path)
    df.index.name = "date"
    return df


def write_contract(code: str, df: pd.DataFrame) -> None:
    """Guarda el parquet de un contrato."""
    _validate_futures(df)
    df.sort_index().to_parquet(_contract_path(code))


def merge_contract(code: str, new_df: pd.DataFrame) -> int:
    """Merge incremental para un contrato. Devuelve filas netas añadidas."""
    if new_df.empty:
        return 0
    _validate_futures(new_df)
    existing = read_contract(code)
    n_before = len(existing)
    combined = pd.concat([existing, new_df])
    combined = combined[~combined.index.duplicated(keep="last")]
    combined = combined.sort_index()
    write_contract(code, combined)
    update_contract_stats(code, combined)
    return len(combined) - n_before


def read_all_contracts() -> pd.DataFrame:
    """
    Lee todos los contratos guardados y devuelve DataFrame en formato LARGO:
    columnas [date, contract_code, open, high, low, settle, volume,
              open_interest, expiry_date, dte].
    """
    frames = []
    info = get_all_contracts_info()
    if info.empty:
        return pd.DataFrame()

    expiry_map = dict(zip(info["contract_code"],
                          pd.to_datetime(info["expiry_date"]).dt.date))

    for code in info["contract_code"]:
        df = read_contract(code)
        if df.empty:
            continue
        df = df.reset_index()
        df["contract_code"] = code
        df["expiry_date"] = expiry_map[code]
        frames.append(df)

    if not frames:
        return pd.DataFrame()

    out = pd.concat(frames, ignore_index=True)
    out["date"] = pd.to_datetime(out["date"])
    out["expiry_date"] = pd.to_datetime(out["expiry_date"])
    # DTE: días naturales (más simple y coherente con métricas VIXCentral)
    out["dte"] = (out["expiry_date"] - out["date"]).dt.days
    # Filtrar filas sin sentido (dte negativo = después de expiración)
    out = out[out["dte"] >= 0]
    return out


def _validate_futures(df: pd.DataFrame) -> None:
    missing = set(["settle"]) - set(df.columns)  # settle es el mínimo crítico
    if missing:
        raise ValueError(f"Futures DataFrame missing required cols: {missing}")
    if not isinstance(df.index, pd.DatetimeIndex):
        raise ValueError("Futures DataFrame must have DatetimeIndex")


# -----------------------------------------------------------------------------
# PARQUET — CONTINUOUS
# -----------------------------------------------------------------------------
CONTINUOUS_PATH = CONTINUOUS_DIR / "vx_continuous.parquet"


def read_continuous() -> pd.DataFrame:
    """Lee las series continuas M1..M8."""
    if not CONTINUOUS_PATH.exists():
        return pd.DataFrame(columns=CONTINUOUS_COLUMNS,
                            index=pd.DatetimeIndex([], name="date"))
    df = pd.read_parquet(CONTINUOUS_PATH)
    df.index.name = "date"
    return df


def write_continuous(df: pd.DataFrame) -> None:
    """Guarda las series continuas. Sobrescribe (se reconstruye cada update)."""
    df.sort_index().to_parquet(CONTINUOUS_PATH)


# -----------------------------------------------------------------------------
# HELPERS DE CONSULTA
# -----------------------------------------------------------------------------
def get_last_date_spot() -> dt.date | None:
    """Última fecha disponible en el spot, o None si está vacío."""
    df = read_spot()
    if df.empty:
        return None
    return df.index.max().date()


def get_last_date_contract(code: str) -> dt.date | None:
    """Última fecha disponible para un contrato, o None si está vacío."""
    df = read_contract(code)
    if df.empty:
        return None
    return df.index.max().date()
