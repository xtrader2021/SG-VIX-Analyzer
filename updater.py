"""
Sistema de actualización incremental de datos.

Ejecutable como:
  - función `update_historical_data()` desde la app
  - script CLI: `python updater.py [--full] [--spot-only] [--futures-only]`

Lógica clave:
  1. Detecta última fecha local por serie.
  2. Descarga sólo desde (última_fecha - LOOKBACK_DAYS) hasta hoy.
  3. Deduplica y valida antes de guardar.
  4. Reconstruye series continuas M1..M8.
  5. Registra resultado en update_log.
"""
from __future__ import annotations

import argparse
import datetime as dt
import logging
from dataclasses import dataclass, field
from typing import Iterable

import pandas as pd

from config import (
    HISTORY_START_DATE,
    MAX_CONTINUOUS_MONTHS,
    UPDATE_LOOKBACK_DAYS,
)
from data_loader import get_futures_source, get_spot_source
from storage import (
    get_active_contract_codes,
    get_all_contracts_info,
    get_last_date_contract,
    get_last_date_spot,
    init_db,
    log_update,
    merge_contract,
    merge_spot,
    read_all_contracts,
    register_contract,
    write_continuous,
)
from utils import (
    VXContract,
    active_contracts_today,
    generate_contracts_between,
    last_business_day,
    parse_contract_code,
)

logger = logging.getLogger(__name__)


# -----------------------------------------------------------------------------
# RESULT DATACLASS
# -----------------------------------------------------------------------------
@dataclass
class UpdateResult:
    """Resultado estructurado de una ejecución de update."""
    started_at: dt.datetime
    finished_at: dt.datetime | None = None
    spot_rows_added: int = 0
    futures_rows_added: int = 0
    contracts_updated: int = 0
    contracts_failed: list[str] = field(default_factory=list)
    last_spot_date: dt.date | None = None
    continuous_rebuilt: bool = False
    errors: list[str] = field(default_factory=list)
    status: str = "running"

    @property
    def duration_seconds(self) -> float:
        if self.finished_at is None:
            return 0.0
        return (self.finished_at - self.started_at).total_seconds()

    def to_dict(self) -> dict:
        return {
            "started_at": self.started_at.isoformat(),
            "finished_at": (self.finished_at.isoformat()
                            if self.finished_at else None),
            "duration_s": round(self.duration_seconds, 2),
            "spot_rows_added": self.spot_rows_added,
            "futures_rows_added": self.futures_rows_added,
            "contracts_updated": self.contracts_updated,
            "contracts_failed": self.contracts_failed,
            "last_spot_date": (self.last_spot_date.isoformat()
                               if self.last_spot_date else None),
            "continuous_rebuilt": self.continuous_rebuilt,
            "errors": self.errors,
            "status": self.status,
        }


# -----------------------------------------------------------------------------
# SPOT
# -----------------------------------------------------------------------------
def update_spot(full_history: bool = False) -> int:
    """
    Actualiza el spot (VIX + VIX9D/3M/6M) incrementalmente.

    Si no hay datos o full_history=True, descarga desde HISTORY_START_DATE.
    Devuelve número de filas añadidas netas.
    """
    source = get_spot_source()
    today = last_business_day()

    last_local = get_last_date_spot() if not full_history else None
    if last_local is None:
        start = dt.date.fromisoformat(HISTORY_START_DATE)
    else:
        # Retroceder LOOKBACK_DAYS para capturar republicaciones
        start = last_local - dt.timedelta(days=UPDATE_LOOKBACK_DAYS)

    if start > today:
        logger.info("Spot ya está al día (last=%s, today=%s)", last_local, today)
        return 0

    logger.info("Descargando spot desde %s hasta %s", start, today)
    new_df = source.fetch(start, today)
    if new_df.empty:
        logger.warning("Fetch de spot vacío")
        return 0

    added = merge_spot(new_df)
    logger.info("Spot actualizado: %d filas netas añadidas", added)
    return added


# -----------------------------------------------------------------------------
# FUTURES
# -----------------------------------------------------------------------------
def _determine_contracts_to_update(full_history: bool) -> tuple[list[VXContract], list[VXContract]]:
    """
    Decide qué contratos descargar enteros (históricos faltantes) y cuáles
    actualizar incrementalmente (vivos).

    Devuelve (historicos_a_descargar, activos_a_actualizar).
    """
    today = last_business_day()
    start_date = dt.date.fromisoformat(HISTORY_START_DATE)

    # CBOE lista típicamente 9 vencimientos consecutivos a la vez (front
    # month + 8 siguientes). Generamos hasta ~10 meses por delante para
    # cubrir con margen sin intentar contratos que aún no existen.
    all_contracts = generate_contracts_between(
        start_date, today + dt.timedelta(days=300)
    )

    # Activos = expiración >= hoy
    active = [c for c in all_contracts if c.expiry >= today]
    expired = [c for c in all_contracts if c.expiry < today]

    # Activos: tomar los primeros 9 vencimientos (lo que CBOE realmente lista)
    to_update_incremental = active[:9]
    # Los que estén expirados pero sin datos locales hay que descargarlos
    # completos. Si full_history=True, descargar todos.
    info = get_all_contracts_info()
    known_codes = set(info["contract_code"]) if not info.empty else set()

    if full_history:
        to_download_full = expired
    else:
        to_download_full = [c for c in expired if c.code not in known_codes]

    return to_download_full, to_update_incremental


def update_futures(full_history: bool = False,
                   progress_callback=None) -> tuple[int, int, list[str]]:
    """
    Actualiza futuros VX.

    Devuelve (rows_added_total, contracts_updated_ok, contracts_failed).
    """
    import time
    source = get_futures_source()
    historicos, activos = _determine_contracts_to_update(full_history)
    all_contracts = historicos + activos
    total = len(all_contracts)
    logger.info("Contratos a procesar: %d históricos + %d activos = %d",
                len(historicos), len(activos), total)

    rows_total = 0
    ok_count = 0
    failed = []

    for i, contract in enumerate(all_contracts):
        if progress_callback:
            progress_callback(i, total, contract.code)

        is_active = contract in activos
        register_contract(contract, is_active=is_active)

        try:
            df = source.fetch_contract(contract)
            if df.empty:
                # Si es histórico que aún no cotiza, no es un fallo real
                if contract.expiry >= last_business_day():
                    logger.debug("Contrato %s aún sin datos", contract.code)
                    continue
                failed.append(contract.code)
                # Pequeña pausa adicional si ha fallado (puede ser rate-limit)
                time.sleep(0.5)
                continue

            # Para contratos activos: si ya tenemos datos, filtrar sólo nuevos
            # (con margen de lookback). Para históricos, guardar todo.
            if is_active:
                last = get_last_date_contract(contract.code)
                if last is not None:
                    cutoff = pd.Timestamp(
                        last - dt.timedelta(days=UPDATE_LOOKBACK_DAYS)
                    )
                    df = df[df.index >= cutoff]

            added = merge_contract(contract.code, df)
            rows_total += added
            ok_count += 1
        except Exception as e:
            logger.exception("Error procesando %s: %s", contract.code, e)
            failed.append(contract.code)

        # Rate-limit suave: 0.3s entre peticiones para no saturar CBOE.
        # En carga inicial de ~150 contratos son ~45s añadidos; merece la pena
        # para evitar bloqueos del CDN.
        time.sleep(0.3)

    if progress_callback:
        progress_callback(total, total, "done")

    return rows_total, ok_count, failed


# -----------------------------------------------------------------------------
# SERIES CONTINUAS M1..M8
# -----------------------------------------------------------------------------
def rebuild_continuous_series() -> bool:
    """
    Reconstruye series continuas M1..M8 a partir de todos los contratos.

    Para cada fecha t, ordena los contratos vivos en t por DTE ascendente
    y asigna M1 = front (DTE >= 0 más pequeño), M2 = siguiente, etc.

    Devuelve True si se reconstruyó OK.
    """
    df = read_all_contracts()
    if df.empty:
        logger.warning("No hay contratos para construir series continuas")
        return False

    # Para cada (date, contract) tenemos settle y dte. El M_i de cada fecha
    # es el i-ésimo contrato ordenado por dte ascendente entre los que tienen
    # dte >= 0 y settle no-NaN.
    df = df.dropna(subset=["settle"])
    df = df[df["settle"] > 0]  # filtra filas de settlement final con 0s

    # Ranking dentro de cada fecha
    df = df.sort_values(["date", "dte"])
    df["month_rank"] = df.groupby("date").cumcount() + 1

    # Pivot a wide
    wide = df[df["month_rank"] <= MAX_CONTINUOUS_MONTHS].pivot_table(
        index="date", columns="month_rank", values="settle", aggfunc="first"
    )
    wide.columns = [f"m{int(c)}" for c in wide.columns]
    # Asegurar todas las columnas M1..M8
    for i in range(1, MAX_CONTINUOUS_MONTHS + 1):
        col = f"m{i}"
        if col not in wide.columns:
            wide[col] = pd.NA
    wide = wide[[f"m{i}" for i in range(1, MAX_CONTINUOUS_MONTHS + 1)]]
    wide = wide.sort_index()
    write_continuous(wide)
    logger.info("Series continuas reconstruidas: %d filas, %d columnas",
                len(wide), len(wide.columns))
    return True


# -----------------------------------------------------------------------------
# ORQUESTADOR
# -----------------------------------------------------------------------------
def update_historical_data(
    full_history: bool = False,
    spot: bool = True,
    futures: bool = True,
    rebuild_continuous: bool = True,
    progress_callback=None,
) -> UpdateResult:
    """
    Punto de entrada principal para actualizar todos los datos.

    Args:
        full_history: si True, redescarga TODO el histórico (lento).
        spot: actualizar spot.
        futures: actualizar futuros.
        rebuild_continuous: reconstruir series M1..M8 al final.
        progress_callback: opcional, func(i, total, label) para UI.

    Returns:
        UpdateResult con métricas y estado.
    """
    init_db()
    result = UpdateResult(started_at=dt.datetime.now())

    # --- SPOT ---
    if spot:
        try:
            n = update_spot(full_history=full_history)
            result.spot_rows_added = n
            result.last_spot_date = get_last_date_spot()
        except Exception as e:
            logger.exception("Error en update_spot")
            result.errors.append(f"spot: {e}")

    # --- FUTURES ---
    if futures:
        try:
            rows, ok, failed = update_futures(
                full_history=full_history,
                progress_callback=progress_callback,
            )
            result.futures_rows_added = rows
            result.contracts_updated = ok
            result.contracts_failed = failed
        except Exception as e:
            logger.exception("Error en update_futures")
            result.errors.append(f"futures: {e}")

    # --- CONTINUOUS ---
    if rebuild_continuous:
        try:
            result.continuous_rebuilt = rebuild_continuous_series()
        except Exception as e:
            logger.exception("Error reconstruyendo continuas")
            result.errors.append(f"continuous: {e}")

    result.finished_at = dt.datetime.now()
    result.status = "ok" if not result.errors else "partial"

    # Registrar en log
    total_added = result.spot_rows_added + result.futures_rows_added
    err_msg = "; ".join(result.errors) if result.errors else None
    log_update(source="updater.py", rows_added=total_added,
               status=result.status, error=err_msg)

    return result


# -----------------------------------------------------------------------------
# CLI
# -----------------------------------------------------------------------------
def _cli_progress(i: int, total: int, label: str) -> None:
    bar_len = 30
    frac = i / total if total else 1
    filled = int(bar_len * frac)
    bar = "█" * filled + "░" * (bar_len - filled)
    print(f"\r[{bar}] {i}/{total} {label[:20]:<20}", end="", flush=True)
    if i == total:
        print()


def main():
    parser = argparse.ArgumentParser(description="VIX data updater")
    parser.add_argument("--full", action="store_true",
                        help="Redescargar todo el histórico")
    parser.add_argument("--spot-only", action="store_true")
    parser.add_argument("--futures-only", action="store_true")
    parser.add_argument("--no-continuous", action="store_true",
                        help="No reconstruir series M1..M8")
    parser.add_argument("-v", "--verbose", action="store_true")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )

    do_spot = not args.futures_only
    do_futures = not args.spot_only

    print(f"VIX data updater — full={args.full}, spot={do_spot}, futures={do_futures}")
    result = update_historical_data(
        full_history=args.full,
        spot=do_spot,
        futures=do_futures,
        rebuild_continuous=not args.no_continuous,
        progress_callback=_cli_progress,
    )

    print("\n=== RESULTADO ===")
    for k, v in result.to_dict().items():
        print(f"  {k}: {v}")


if __name__ == "__main__":
    main()
