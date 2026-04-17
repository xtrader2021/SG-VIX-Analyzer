"""
Análisis estacional del VIX spot y spreads.

Tres granularidades: mes del año, semana del año, día del año.
Metodología: sobre RETURNS (pct_change), no sobre niveles — los niveles
del VIX no son estacionarios y sesgan la media estacional.

Para cada bucket devolvemos mean, median, P25, P75 y overlay del año
actual para comparación visual.
"""
from __future__ import annotations

import datetime as dt

import numpy as np
import pandas as pd


def _ensure_returns(series: pd.Series, use_returns: bool) -> pd.Series:
    """Convierte a returns si procede, mantiene niveles si no."""
    if use_returns:
        return series.pct_change().dropna()
    return series.dropna()


# -----------------------------------------------------------------------------
# MES DEL AÑO
# -----------------------------------------------------------------------------
def seasonality_by_month(series: pd.Series,
                         use_returns: bool = True) -> pd.DataFrame:
    """
    Estadísticos por mes del año (1-12).

    Columnas: month, count, mean, median, p25, p75, std
    """
    s = _ensure_returns(series, use_returns)
    if s.empty:
        return pd.DataFrame()
    df = pd.DataFrame({"value": s})
    df["month"] = df.index.month
    agg = df.groupby("month")["value"].agg(
        count="count",
        mean="mean",
        median="median",
        p25=lambda x: x.quantile(0.25),
        p75=lambda x: x.quantile(0.75),
        std="std",
    ).reset_index()
    return agg


# -----------------------------------------------------------------------------
# SEMANA DEL AÑO
# -----------------------------------------------------------------------------
def seasonality_by_week(series: pd.Series,
                        use_returns: bool = True) -> pd.DataFrame:
    """Estadísticos por semana ISO del año (1-53)."""
    s = _ensure_returns(series, use_returns)
    if s.empty:
        return pd.DataFrame()
    df = pd.DataFrame({"value": s})
    iso = df.index.isocalendar()
    df["week"] = iso.week.values
    agg = df.groupby("week")["value"].agg(
        count="count",
        mean="mean",
        median="median",
        p25=lambda x: x.quantile(0.25),
        p75=lambda x: x.quantile(0.75),
        std="std",
    ).reset_index()
    return agg


# -----------------------------------------------------------------------------
# DÍA DEL AÑO
# -----------------------------------------------------------------------------
def seasonality_by_dayofyear(series: pd.Series,
                             use_returns: bool = True,
                             smooth_window: int = 5) -> pd.DataFrame:
    """
    Estadísticos por día del año (1-366). Con suavizado rolling opcional
    porque el ruido diario es enorme en series cortas.

    Columnas: doy, count, mean, median, p25, p75, mean_smooth
    """
    s = _ensure_returns(series, use_returns)
    if s.empty:
        return pd.DataFrame()
    df = pd.DataFrame({"value": s})
    df["doy"] = df.index.dayofyear
    agg = df.groupby("doy")["value"].agg(
        count="count",
        mean="mean",
        median="median",
        p25=lambda x: x.quantile(0.25),
        p75=lambda x: x.quantile(0.75),
    ).reset_index()
    if smooth_window > 1:
        agg["mean_smooth"] = agg["mean"].rolling(
            smooth_window, center=True, min_periods=1).mean()
        agg["median_smooth"] = agg["median"].rolling(
            smooth_window, center=True, min_periods=1).mean()
    return agg


# -----------------------------------------------------------------------------
# OVERLAY DEL AÑO ACTUAL
# -----------------------------------------------------------------------------
def current_year_overlay(series: pd.Series,
                         granularity: str = "month",
                         use_returns: bool = True) -> pd.DataFrame:
    """
    Devuelve los valores del año en curso agregados por la granularidad
    indicada para dibujar sobre el baseline histórico.

    granularity: 'month', 'week', 'doy'
    """
    s = _ensure_returns(series, use_returns)
    if s.empty:
        return pd.DataFrame()
    current_year = dt.date.today().year
    s_y = s[s.index.year == current_year]
    if s_y.empty:
        return pd.DataFrame()

    df = pd.DataFrame({"value": s_y})
    if granularity == "month":
        df["key"] = df.index.month
    elif granularity == "week":
        df["key"] = df.index.isocalendar().week.values
    elif granularity == "doy":
        df["key"] = df.index.dayofyear
    else:
        raise ValueError(f"Granularidad inválida: {granularity}")

    if granularity == "doy":
        # Para DOY no agregamos, devolvemos los valores crudos (ruidosos)
        return df.reset_index().rename(columns={"date": "date"})

    agg = df.groupby("key")["value"].agg(
        mean="mean",
        median="median",
    ).reset_index()
    return agg


# -----------------------------------------------------------------------------
# CONTEXTO DE CUÁL ES "AHORA"
# -----------------------------------------------------------------------------
def current_season_position() -> dict:
    """Devuelve mes, semana y doy actuales."""
    today = dt.date.today()
    iso = pd.Timestamp(today).isocalendar()
    return {
        "month": today.month,
        "week": int(iso.week),
        "doy": int(pd.Timestamp(today).dayofyear),
        "date": today,
    }
