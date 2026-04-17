"""
Funciones analíticas puras. Todas reciben DataFrames y devuelven
DataFrames/Series/escalares. Sin estado, sin I/O.

Cubren:
  - construcción de curva actual
  - spreads (M1-M2, M2-M3, etc.) con histórico, percentil, z-score
  - contango/backwardation por múltiples criterios
  - valoración relativa (futuro vs spot): diff, ratio, percentil, z-score
  - slope (regresión lineal de la curva)
  - clasificación cuantitativa caro/barato
"""
from __future__ import annotations

import numpy as np
import pandas as pd

from config import (
    PERCENTILE_CHEAP_THRESHOLD,
    PERCENTILE_EXPENSIVE_THRESHOLD,
    ROLLING_PERCENTILE_WINDOW_DAYS,
    ROLLING_ZSCORE_WINDOW_DAYS,
    ZSCORE_CHEAP_THRESHOLD,
    ZSCORE_EXPENSIVE_THRESHOLD,
)


# -----------------------------------------------------------------------------
# CURVA ACTUAL
# -----------------------------------------------------------------------------
def current_curve(df_futures_long: pd.DataFrame,
                  as_of: pd.Timestamp | None = None) -> pd.DataFrame:
    """
    Devuelve la curva de futuros para una fecha dada (default: última
    disponible). Ordenada por expiry, con DTE e índice de mes.

    Columnas: [contract_code, expiry_date, dte, settle, month_index]
    """
    if df_futures_long.empty:
        return pd.DataFrame()

    if as_of is None:
        as_of = df_futures_long["date"].max()
    else:
        as_of = pd.Timestamp(as_of).normalize()

    snap = df_futures_long[df_futures_long["date"] == as_of].copy()
    if snap.empty:
        # Fallback: día hábil anterior más cercano
        available = df_futures_long["date"].unique()
        available = sorted(d for d in available if d <= as_of)
        if not available:
            return pd.DataFrame()
        as_of = available[-1]
        snap = df_futures_long[df_futures_long["date"] == as_of].copy()

    snap = snap[snap["dte"] >= 0]
    snap = snap.dropna(subset=["settle"])
    snap = snap[snap["settle"] > 0]
    snap = snap.sort_values("expiry_date").reset_index(drop=True)
    snap["month_index"] = np.arange(1, len(snap) + 1)
    return snap[["contract_code", "date", "expiry_date", "dte",
                 "settle", "month_index"]]


def curve_on_date(df_futures_long: pd.DataFrame,
                  date: pd.Timestamp) -> pd.DataFrame:
    """Alias semánticamente claro para comparaciones históricas de curva."""
    return current_curve(df_futures_long, as_of=date)


# -----------------------------------------------------------------------------
# SLOPE (pendiente de la curva)
# -----------------------------------------------------------------------------
def curve_slope(curve: pd.DataFrame) -> float:
    """
    Pendiente de la curva de futuros por regresión lineal de settle ~ dte.

    Positivo = contango; negativo = backwardation.
    Unidades: puntos VIX por día.
    """
    if curve is None or curve.empty or len(curve) < 2:
        return float("nan")
    x = curve["dte"].to_numpy(dtype=float)
    y = curve["settle"].to_numpy(dtype=float)
    if np.any(np.isnan(x)) or np.any(np.isnan(y)):
        return float("nan")
    slope, _intercept = np.polyfit(x, y, 1)
    return float(slope)


def historical_slopes(df_futures_long: pd.DataFrame) -> pd.Series:
    """Serie temporal de slope diario de la curva."""
    if df_futures_long.empty:
        return pd.Series(dtype=float)
    out = {}
    for date, group in df_futures_long.groupby("date"):
        snap = group[(group["dte"] >= 0) & (group["settle"] > 0)]
        snap = snap.dropna(subset=["settle"])
        if len(snap) >= 2:
            x = snap["dte"].to_numpy(dtype=float)
            y = snap["settle"].to_numpy(dtype=float)
            s, _ = np.polyfit(x, y, 1)
            out[date] = s
    return pd.Series(out).sort_index()


# -----------------------------------------------------------------------------
# SPREADS ENTRE CONTINUAS
# -----------------------------------------------------------------------------
def compute_spread(df_continuous: pd.DataFrame,
                   leg_near: str, leg_far: str) -> pd.Series:
    """
    Spread absoluto (leg_far - leg_near). Ej: compute_spread(df, 'm1', 'm2')
    → M2 - M1. Positivo en contango típico.
    """
    if df_continuous.empty or leg_near not in df_continuous or leg_far not in df_continuous:
        return pd.Series(dtype=float)
    s = df_continuous[leg_far] - df_continuous[leg_near]
    s.name = f"{leg_far}_minus_{leg_near}"
    return s.dropna()


def spread_ratio(df_continuous: pd.DataFrame,
                 leg_near: str, leg_far: str) -> pd.Series:
    """Ratio leg_far / leg_near - 1. Normaliza por nivel de volatilidad."""
    if df_continuous.empty or leg_near not in df_continuous or leg_far not in df_continuous:
        return pd.Series(dtype=float)
    s = df_continuous[leg_far] / df_continuous[leg_near] - 1
    s.name = f"{leg_far}_over_{leg_near}_ratio"
    return s.dropna()


# -----------------------------------------------------------------------------
# PERCENTILES Y Z-SCORE
# -----------------------------------------------------------------------------
def rolling_percentile(series: pd.Series,
                       window: int = ROLLING_PERCENTILE_WINDOW_DAYS) -> pd.Series:
    """
    Percentil rolling del último valor dentro de la ventana.

    Para cada t, calcula qué percentil ocupa series[t] en la ventana
    [t-window+1, t]. Resultado en [0, 100].
    """
    if series.empty:
        return series
    # rank pct devuelve [0,1]; *100 para escala estándar
    def _pct(x):
        if len(x) < 2:
            return np.nan
        return (x.rank(pct=True).iloc[-1]) * 100
    return series.rolling(window=window, min_periods=max(20, window // 10)).apply(
        _pct, raw=False
    )


def full_sample_percentile(series: pd.Series) -> pd.Series:
    """Percentil usando toda la muestra hasta cada t (expanding)."""
    if series.empty:
        return series
    return series.expanding(min_periods=20).apply(
        lambda x: (x.rank(pct=True).iloc[-1]) * 100, raw=False
    )


def rolling_zscore(series: pd.Series,
                   window: int = ROLLING_ZSCORE_WINDOW_DAYS) -> pd.Series:
    """Z-score rolling: (x - mean) / std dentro de la ventana."""
    if series.empty:
        return series
    mean = series.rolling(window=window, min_periods=max(20, window // 10)).mean()
    std = series.rolling(window=window, min_periods=max(20, window // 10)).std()
    return (series - mean) / std


def compute_stats_bundle(series: pd.Series,
                         pct_window: int = ROLLING_PERCENTILE_WINDOW_DAYS,
                         z_window: int = ROLLING_ZSCORE_WINDOW_DAYS) -> pd.DataFrame:
    """
    Devuelve DataFrame con columnas:
      value, pct_rolling, pct_full, zscore, mean_window, std_window
    """
    if series.empty:
        return pd.DataFrame()
    df = pd.DataFrame({"value": series})
    df["pct_rolling"] = rolling_percentile(series, pct_window)
    df["pct_full"] = full_sample_percentile(series)
    df["zscore"] = rolling_zscore(series, z_window)
    df["mean_window"] = series.rolling(z_window, min_periods=20).mean()
    df["std_window"] = series.rolling(z_window, min_periods=20).std()
    return df


# -----------------------------------------------------------------------------
# CONTANGO / BACKWARDATION
# -----------------------------------------------------------------------------
def contango_metrics(df_spot: pd.DataFrame,
                     df_continuous: pd.DataFrame) -> pd.DataFrame:
    """
    Calcula múltiples métricas de contango/backwardation alineadas en fechas.

    Columnas resultado:
      - spot, m1..m8
      - spot_vs_m1_ratio: (m1/spot - 1)
      - m1_vs_m2_ratio:  (m2/m1 - 1)   [la métrica clásica de VIXCentral]
      - m2_vs_m3_ratio:  (m3/m2 - 1)
      - m4_vs_m7_ratio:  (m7/m4 - 1)
      - slope: pendiente M1..M4 (pts por día)

    Convención: valor > 0 → contango; < 0 → backwardation.
    """
    if df_continuous.empty:
        return pd.DataFrame()

    df = df_continuous.copy()
    if not df_spot.empty and "vix" in df_spot.columns:
        df = df.join(df_spot["vix"].rename("spot"), how="left")
    else:
        df["spot"] = np.nan

    # Ratios (sólo donde denominador > 0)
    def safe_ratio(num, den):
        return (num / den).where((den > 0) & num.notna(), np.nan) - 1

    if "spot" in df:
        df["spot_vs_m1_ratio"] = safe_ratio(df["m1"], df["spot"])
    df["m1_vs_m2_ratio"] = safe_ratio(df["m2"], df["m1"])
    df["m2_vs_m3_ratio"] = safe_ratio(df["m3"], df["m2"])
    if "m4" in df and "m7" in df:
        df["m4_vs_m7_ratio"] = safe_ratio(df["m7"], df["m4"])

    # Slope M1..M4 (asume DTE aprox 30, 60, 90, 120)
    # Aquí usamos simplemente pendiente de la regresión y = a + b*i en i=1..4
    def _slope_row(row):
        vals = [row.get(f"m{i}") for i in range(1, 5)]
        vals = [v for v in vals if pd.notna(v) and v > 0]
        if len(vals) < 2:
            return np.nan
        x = np.arange(1, len(vals) + 1, dtype=float)
        return float(np.polyfit(x, vals, 1)[0])

    df["slope_m1_m4"] = df.apply(_slope_row, axis=1)
    return df


# -----------------------------------------------------------------------------
# VALORACIÓN RELATIVA (futuro vs spot)
# -----------------------------------------------------------------------------
def premium_spot_vs_future(df_spot: pd.DataFrame,
                           df_continuous: pd.DataFrame,
                           month: int = 1) -> pd.DataFrame:
    """
    Métricas de prima del futuro M{month} sobre el spot VIX.

    Devuelve DataFrame con columnas:
      spot, future, diff, ratio, diff_pct_rolling, diff_pct_full,
      diff_zscore, ratio_pct_rolling, ratio_zscore
    """
    col = f"m{month}"
    if df_spot.empty or df_continuous.empty or col not in df_continuous:
        return pd.DataFrame()

    df = pd.DataFrame({
        "spot": df_spot["vix"],
        "future": df_continuous[col],
    }).dropna()
    if df.empty:
        return df

    df["diff"] = df["future"] - df["spot"]
    df["ratio"] = df["future"] / df["spot"] - 1

    stats_diff = compute_stats_bundle(df["diff"])
    stats_ratio = compute_stats_bundle(df["ratio"])

    df["diff_pct_rolling"] = stats_diff["pct_rolling"]
    df["diff_pct_full"] = stats_diff["pct_full"]
    df["diff_zscore"] = stats_diff["zscore"]
    df["ratio_pct_rolling"] = stats_ratio["pct_rolling"]
    df["ratio_zscore"] = stats_ratio["zscore"]
    return df


# -----------------------------------------------------------------------------
# CLASIFICACIÓN CUANTITATIVA
# -----------------------------------------------------------------------------
def classify_by_percentile(pct: float) -> str:
    """Devuelve 'Barato' / 'Neutral' / 'Caro' según percentil."""
    if pd.isna(pct):
        return "N/D"
    if pct < PERCENTILE_CHEAP_THRESHOLD:
        return "Barato"
    if pct > PERCENTILE_EXPENSIVE_THRESHOLD:
        return "Caro"
    return "Neutral"


def classify_by_zscore(z: float) -> str:
    """Devuelve 'Barato' / 'Neutral' / 'Caro' según z-score."""
    if pd.isna(z):
        return "N/D"
    if z < ZSCORE_CHEAP_THRESHOLD:
        return "Barato"
    if z > ZSCORE_EXPENSIVE_THRESHOLD:
        return "Caro"
    return "Neutral"


def classify_contango(ratio_m1_m2: float) -> str:
    """
    Clasifica estructura temporal según ratio M2/M1 - 1.

    Umbrales habituales en la literatura:
      < -5%: backwardation fuerte
      -5% a -1%: backwardation
      -1% a +1%: flat
      +1% a +5%: contango
      > +5%: contango fuerte
    """
    if pd.isna(ratio_m1_m2):
        return "N/D"
    if ratio_m1_m2 < -0.05:
        return "Backwardation fuerte"
    if ratio_m1_m2 < -0.01:
        return "Backwardation"
    if ratio_m1_m2 < 0.01:
        return "Flat"
    if ratio_m1_m2 < 0.05:
        return "Contango"
    return "Contango fuerte"


# -----------------------------------------------------------------------------
# INTERPRETACIÓN AUTOMÁTICA
# -----------------------------------------------------------------------------
def auto_interpretation(df_spot: pd.DataFrame,
                        df_continuous: pd.DataFrame) -> list[str]:
    """
    Genera frases descriptivas basadas en métricas cuantitativas actuales.

    No hace opiniones ni predicciones — sólo describe el estado.
    """
    lines = []
    if df_continuous.empty:
        return ["Sin datos suficientes para interpretación."]

    last = df_continuous.iloc[-1]
    last_date = df_continuous.index[-1].date()
    lines.append(f"Datos al cierre de {last_date}.")

    # Contango
    m1, m2 = last.get("m1"), last.get("m2")
    if pd.notna(m1) and pd.notna(m2) and m1 > 0:
        ratio = m2 / m1 - 1
        cls = classify_contango(ratio)
        lines.append(
            f"Estructura M1/M2: {cls} ({ratio*100:+.2f}%) — "
            f"M1={m1:.2f}, M2={m2:.2f}."
        )

    # Spread M1-M2 en contexto histórico
    spread = (df_continuous["m2"] - df_continuous["m1"]).dropna()
    if len(spread) > 30:
        cur = spread.iloc[-1]
        pct = (spread.rank(pct=True).iloc[-1]) * 100
        lines.append(
            f"Spread M2-M1 = {cur:+.2f}, percentil histórico = {pct:.0f} "
            f"→ {classify_by_percentile(pct).lower()} "
            f"relativamente a su histórico."
        )

    # Prima M1 vs spot
    if not df_spot.empty and "vix" in df_spot.columns:
        spot = df_spot["vix"].reindex(df_continuous.index).iloc[-1]
        if pd.notna(spot) and pd.notna(m1) and spot > 0:
            prem = m1 / spot - 1
            lines.append(
                f"M1 cotiza un {prem*100:+.2f}% sobre el spot "
                f"(spot={spot:.2f}, M1={m1:.2f})."
            )

    # Slope
    slope_series = (df_continuous[["m1", "m2", "m3", "m4"]]
                    .apply(lambda r: np.polyfit(
                        np.arange(1, len(r.dropna()) + 1, dtype=float),
                        r.dropna().to_numpy(dtype=float), 1
                    )[0] if r.dropna().size >= 2 else np.nan,
                    axis=1))
    if not slope_series.empty:
        cur_slope = slope_series.iloc[-1]
        slope_pct = (slope_series.rank(pct=True).iloc[-1]) * 100 if len(slope_series.dropna()) > 30 else np.nan
        if pd.notna(cur_slope):
            lines.append(
                f"Pendiente M1-M4 = {cur_slope:+.3f} pts/mes"
                + (f", percentil {slope_pct:.0f}." if pd.notna(slope_pct) else ".")
            )

    return lines


# -----------------------------------------------------------------------------
# SPREADS PERSONALIZADOS (multi-pata, con pesos)
# -----------------------------------------------------------------------------
def compute_custom_spread(df_futures_long: pd.DataFrame,
                          df_spot: pd.DataFrame,
                          legs: list[dict]) -> pd.DataFrame:
    """
    Calcula un spread/butterfly personalizado con múltiples patas y pesos.

    Args:
        df_futures_long: formato largo con columnas [date, settle, dte, ...]
        df_spot: spot con 'vix'
        legs: lista de {'month': int (1-8), 'weight': float}
              Ej. M1-M2 → [{'month':1,'weight':1}, {'month':2,'weight':-1}]
              Ej. Fly 1-2-3 → [{'month':1,'weight':1}, {'month':2,'weight':-2}, {'month':3,'weight':1}]

    Devuelve DataFrame con [date, spread, dte_front, vix, year, monthDay].
    """
    if df_futures_long is None or df_futures_long.empty or not legs:
        return pd.DataFrame()

    df = df_futures_long.copy()
    df = df.dropna(subset=["settle"])
    df = df[df["settle"] > 0]
    # month_rank: M1 = contrato con menor DTE positivo ese día, M2 siguiente, etc.
    df = df.sort_values(["date", "dte"])
    df["month_rank"] = df.groupby("date").cumcount() + 1

    # Para cada pata, extraer la serie correspondiente al month_rank
    leg_series = []
    for i, leg in enumerate(legs):
        m = leg["month"]
        w = leg["weight"]
        leg_df = df[df["month_rank"] == m][["date", "settle", "dte"]].rename(
            columns={"settle": f"settle_{i}", "dte": f"dte_{i}"})
        leg_series.append((leg_df, w, i))

    # Merge inner: solo fechas donde están todas las patas
    result = leg_series[0][0]
    for leg_df, _, _ in leg_series[1:]:
        result = result.merge(leg_df, on="date", how="inner")

    if result.empty:
        return pd.DataFrame()

    # Calcular spread ponderado
    result["spread"] = sum(
        legs[i]["weight"] * result[f"settle_{i}"]
        for i in range(len(legs))
    )

    # DTE del front (pata más cercana)
    dte_cols = [f"dte_{i}" for i in range(len(legs))]
    result["dte_front"] = result[dte_cols].min(axis=1)

    # Merge con spot
    if df_spot is not None and not df_spot.empty and "vix" in df_spot.columns:
        spot = df_spot[["vix"]].reset_index()
        spot["date"] = pd.to_datetime(spot["date"])
        result = result.merge(spot, on="date", how="left")
    else:
        result["vix"] = np.nan

    result["date"] = pd.to_datetime(result["date"])
    result["year"] = result["date"].dt.year
    result["monthDay"] = result["date"].dt.strftime("%m-%d")

    return result[["date", "spread", "dte_front", "vix", "year", "monthDay"]]


def formula_string(legs: list[dict]) -> str:
    """Construye cadena 'M1 − M2' o '+M1 −2M2 +M3' etc."""
    parts = []
    for leg in legs:
        w = leg["weight"]
        m = f"M{leg['month']}"
        if w == 1:
            parts.append(f"+{m}")
        elif w == -1:
            parts.append(f"−{m}")
        elif w > 0:
            parts.append(f"+{w:g}{m}")
        else:
            parts.append(f"{w:g}{m}")
    return " ".join(parts).lstrip("+").strip()


def spread_valuation(spread_series: pd.Series,
                     current_value: float | None = None,
                     similar_mask: pd.Series | None = None) -> dict:
    """
    Devuelve métricas de valoración de un spread:
      - valor actual
      - percentil full-sample
      - percentil rolling 3Y
      - z-score 252d
      - percentil condicional (si similar_mask se pasa)
      - clasificación textual (señal)

    Args:
        spread_series: serie temporal del spread (indexed by date)
        current_value: valor a evaluar (default: último)
        similar_mask: bool mask para calcular percentil condicional
    """
    if spread_series.empty:
        return {}

    s = spread_series.dropna()
    if s.empty:
        return {}

    if current_value is None:
        current_value = s.iloc[-1]

    # Percentil full sample
    pct_full = (s < current_value).mean() * 100

    # Percentil rolling 3Y (756 días)
    pct_rolling = np.nan
    if len(s) >= 60:
        window = min(len(s), ROLLING_PERCENTILE_WINDOW_DAYS)
        recent = s.iloc[-window:]
        pct_rolling = (recent < current_value).mean() * 100

    # Z-score 252d
    zscore = np.nan
    if len(s) >= 60:
        window = min(len(s), ROLLING_ZSCORE_WINDOW_DAYS)
        recent = s.iloc[-window:]
        mu = recent.mean()
        sd = recent.std()
        if sd > 0:
            zscore = (current_value - mu) / sd

    # Percentil condicional (ej: días con VIX similar)
    pct_conditional = np.nan
    if similar_mask is not None:
        cond = s[similar_mask.reindex(s.index, fill_value=False)]
        if len(cond) >= 20:
            pct_conditional = (cond < current_value).mean() * 100

    # Clasificación (5 niveles)
    ref_pct = pct_rolling if pd.notna(pct_rolling) else pct_full
    if ref_pct < 10:
        signal = "EXTREMO BARATO"
    elif ref_pct < 30:
        signal = "BARATO"
    elif ref_pct < 70:
        signal = "NEUTRAL"
    elif ref_pct < 90:
        signal = "CARO"
    else:
        signal = "EXTREMO CARO"

    return {
        "current": float(current_value),
        "pct_full": float(pct_full),
        "pct_rolling": float(pct_rolling) if pd.notna(pct_rolling) else None,
        "zscore": float(zscore) if pd.notna(zscore) else None,
        "pct_conditional": float(pct_conditional) if pd.notna(pct_conditional) else None,
        "signal": signal,
        "n_observations": len(s),
        "mean": float(s.mean()),
        "std": float(s.std()) if len(s) > 1 else 0.0,
        "min": float(s.min()),
        "max": float(s.max()),
    }
