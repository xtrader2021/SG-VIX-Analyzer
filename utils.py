"""
Utilidades: calendario de expiraciones de futuros VX, días hábiles,
conversión entre códigos de contrato y fechas, etc.

El cálculo de expiración del VX es específico: el VX expira el miércoles
30 días naturales antes del tercer viernes del mes SIGUIENTE al mes del
contrato. Si ese miércoles es festivo, se mueve al día hábil anterior.
"""
from __future__ import annotations

import datetime as dt
from dataclasses import dataclass
from typing import Iterable

import pandas as pd
from pandas.tseries.holiday import USFederalHolidayCalendar
from pandas.tseries.offsets import CustomBusinessDay

from config import FUTURES_MONTH_CODES, MONTH_CODE_TO_NUM

# Calendario de días hábiles NYSE aproximado (USFederalHolidayCalendar es
# suficientemente cercano para cálculo de expiración VX; el settlement
# real usa el calendario CFE pero coincide en >99% de los días)
_US_BDAY = CustomBusinessDay(calendar=USFederalHolidayCalendar())


# -----------------------------------------------------------------------------
# CONTRACT CODES
# -----------------------------------------------------------------------------
@dataclass(frozen=True)
class VXContract:
    """Representa un contrato VX individual."""
    year: int          # año completo, ej 2024
    month: int         # mes del contrato, 1-12
    month_code: str    # letra F/G/H/.../Z
    code: str          # código canónico, ej "VX_F24"
    expiry: dt.date

    @property
    def yy(self) -> str:
        """Año de dos dígitos."""
        return f"{self.year % 100:02d}"

    @property
    def filename(self) -> str:
        """Nombre de archivo parquet para este contrato."""
        return f"{self.code}.parquet"


def make_contract(year: int, month: int) -> VXContract:
    """Construye un VXContract dados año y mes."""
    month_code = FUTURES_MONTH_CODES[month]
    code = f"VX_{month_code}{year % 100:02d}"
    expiry = vx_expiry_date(year, month)
    return VXContract(year=year, month=month, month_code=month_code,
                      code=code, expiry=expiry)


def parse_contract_code(code: str) -> VXContract:
    """Parsea 'VX_F24' → VXContract. Lanza ValueError si inválido."""
    if not code.startswith("VX_") or len(code) != 6:
        raise ValueError(f"Código inválido: {code}")
    month_code = code[3]
    yy = int(code[4:6])
    # Heurística de siglo: yy<50 → 20xx, yy>=50 → 19xx (no aplica a VIX
    # futures que empiezan en 2004, pero es defensivo)
    year = 2000 + yy if yy < 50 else 1900 + yy
    month = MONTH_CODE_TO_NUM[month_code]
    return make_contract(year, month)


# -----------------------------------------------------------------------------
# EXPIRACIONES
# -----------------------------------------------------------------------------
def vx_expiry_date(year: int, month: int) -> dt.date:
    """
    Calcula la fecha de expiración de un contrato VX.

    Regla CBOE: el VX expira el miércoles que está exactamente 30 días
    naturales antes del tercer viernes del mes SIGUIENTE al mes del
    contrato. Si ese miércoles es festivo, se mueve al día hábil anterior.
    """
    # Mes siguiente
    if month == 12:
        next_month, next_year = 1, year + 1
    else:
        next_month, next_year = month + 1, year

    # Tercer viernes del mes siguiente
    first_of_next = dt.date(next_year, next_month, 1)
    # weekday(): lunes=0 ... viernes=4
    days_until_first_friday = (4 - first_of_next.weekday()) % 7
    first_friday = first_of_next + dt.timedelta(days=days_until_first_friday)
    third_friday = first_friday + dt.timedelta(days=14)

    # 30 días naturales antes
    expiry = third_friday - dt.timedelta(days=30)

    # Ajuste por festivo: si expiry no es día hábil, mover al anterior
    expiry_ts = pd.Timestamp(expiry)
    holidays = USFederalHolidayCalendar().holidays(
        start=expiry_ts - pd.Timedelta(days=10),
        end=expiry_ts + pd.Timedelta(days=10),
    )
    while expiry_ts.weekday() >= 5 or expiry_ts in holidays:
        expiry_ts -= pd.Timedelta(days=1)

    return expiry_ts.date()


def generate_contracts_between(
    start_date: dt.date, end_date: dt.date
) -> list[VXContract]:
    """
    Genera todos los contratos VX con expiración dentro del rango dado.

    Incluye un buffer de ±3 meses por seguridad (contratos que empiezan
    a cotizar antes o se liquidan después del rango estricto).
    """
    contracts = []
    # Expandir rango para cubrir contratos en los bordes
    year_start = start_date.year - 1
    year_end = end_date.year + 2
    for year in range(year_start, year_end + 1):
        for month in range(1, 13):
            c = make_contract(year, month)
            if start_date <= c.expiry <= end_date + dt.timedelta(days=90):
                contracts.append(c)
    return sorted(contracts, key=lambda x: x.expiry)


def active_contracts_today(today: dt.date | None = None,
                           n_months: int = 8) -> list[VXContract]:
    """Devuelve los n_months contratos con expiración >= hoy."""
    if today is None:
        today = dt.date.today()
    all_c = generate_contracts_between(
        today - dt.timedelta(days=30),
        today + dt.timedelta(days=400),
    )
    alive = [c for c in all_c if c.expiry >= today]
    return alive[:n_months]


# -----------------------------------------------------------------------------
# BUSINESS DAYS
# -----------------------------------------------------------------------------
def business_days_between(start: dt.date, end: dt.date) -> int:
    """Número de días hábiles US entre dos fechas (inclusivo de start)."""
    if start > end:
        return 0
    rng = pd.bdate_range(start=start, end=end, freq=_US_BDAY)
    return len(rng)


def last_business_day(d: dt.date | None = None) -> dt.date:
    """Último día hábil US anterior o igual a d (hoy por defecto)."""
    if d is None:
        d = dt.date.today()
    ts = pd.Timestamp(d)
    # Si es fin de semana o festivo, rebobinar
    holidays = USFederalHolidayCalendar().holidays(
        start=ts - pd.Timedelta(days=15), end=ts + pd.Timedelta(days=1)
    )
    while ts.weekday() >= 5 or ts in holidays:
        ts -= pd.Timedelta(days=1)
    return ts.date()


# -----------------------------------------------------------------------------
# FORMATO
# -----------------------------------------------------------------------------
def format_pct(value: float, decimals: int = 2) -> str:
    """Formatea un ratio como porcentaje."""
    if pd.isna(value):
        return "N/A"
    return f"{value * 100:.{decimals}f}%"


def format_number(value: float, decimals: int = 2) -> str:
    """Formatea un número con separador de miles."""
    if pd.isna(value):
        return "N/A"
    return f"{value:,.{decimals}f}"
