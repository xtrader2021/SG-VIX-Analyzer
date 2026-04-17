"""
Funciones de visualización con Plotly. Todas reciben DataFrames ya
procesados y devuelven objetos `go.Figure` listos para `st.plotly_chart`.

Sin cálculos analíticos aquí — eso vive en analytics.py.
"""
from __future__ import annotations

import numpy as np
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots

from config import DEFAULT_PLOT_HEIGHT


# -----------------------------------------------------------------------------
# CURVA ACTUAL / HISTÓRICA
# -----------------------------------------------------------------------------
def plot_curve(curve: pd.DataFrame, spot_value: float | None = None,
               title: str = "Curva de futuros VX") -> go.Figure:
    """Gráfico de la curva actual: settle vs DTE, con spot opcional."""
    fig = go.Figure()
    if curve is None or curve.empty:
        fig.add_annotation(text="Sin datos", showarrow=False,
                           x=0.5, y=0.5, xref="paper", yref="paper")
        return fig

    fig.add_trace(go.Scatter(
        x=curve["dte"], y=curve["settle"],
        mode="lines+markers+text",
        name="Curva VX",
        text=curve["contract_code"].str.replace("VX_", "M"),
        textposition="top center",
        marker=dict(size=10, color="#1f77b4"),
        line=dict(width=2),
        hovertemplate="<b>%{text}</b><br>DTE: %{x}<br>Settle: %{y:.2f}<extra></extra>",
    ))

    if spot_value is not None and not pd.isna(spot_value):
        fig.add_trace(go.Scatter(
            x=[0], y=[spot_value],
            mode="markers+text",
            name="VIX spot",
            text=["Spot"], textposition="top center",
            marker=dict(size=12, color="#d62728", symbol="diamond"),
            hovertemplate="<b>VIX spot</b><br>Valor: %{y:.2f}<extra></extra>",
        ))

    fig.update_layout(
        title=title,
        xaxis_title="Días hasta expiración (DTE)",
        yaxis_title="Settle (puntos VIX)",
        hovermode="closest",
        height=DEFAULT_PLOT_HEIGHT,
        template="plotly_white",
    )
    return fig


def plot_historical_curves(curves: dict[str, pd.DataFrame]) -> go.Figure:
    """
    Compara múltiples curvas históricas en un mismo gráfico.

    curves: dict {label: curve_df}. Cada curve_df con columnas dte, settle.
    """
    fig = go.Figure()
    if not curves:
        return fig

    colors = ["#1f77b4", "#ff7f0e", "#2ca02c", "#d62728", "#9467bd",
              "#8c564b", "#e377c2", "#7f7f7f", "#bcbd22", "#17becf"]

    for i, (label, df) in enumerate(curves.items()):
        if df.empty:
            continue
        fig.add_trace(go.Scatter(
            x=df["dte"], y=df["settle"],
            mode="lines+markers", name=label,
            line=dict(color=colors[i % len(colors)], width=2),
            marker=dict(size=7),
        ))

    fig.update_layout(
        title="Comparación histórica de curvas",
        xaxis_title="Días hasta expiración (DTE)",
        yaxis_title="Settle",
        height=DEFAULT_PLOT_HEIGHT,
        template="plotly_white",
        hovermode="x unified",
    )
    return fig


# -----------------------------------------------------------------------------
# SERIES DE SPREADS
# -----------------------------------------------------------------------------
def plot_spread_history(series: pd.Series,
                        title: str = "Spread histórico",
                        percentile_bands: dict | None = None) -> go.Figure:
    """
    Serie temporal de un spread con bandas de percentiles opcionales.

    percentile_bands: dict {label: value} — ej {"P10": 0.5, "P90": 3.2}
    """
    fig = go.Figure()
    if series.empty:
        return fig

    fig.add_trace(go.Scatter(
        x=series.index, y=series.values,
        mode="lines", name="Spread",
        line=dict(color="#1f77b4", width=1.5),
    ))

    if percentile_bands:
        colors_p = {"P10": "#2ca02c", "P25": "#98df8a",
                    "P50": "#ff7f0e", "P75": "#ff9896", "P90": "#d62728"}
        for label, val in percentile_bands.items():
            fig.add_hline(y=val, line_dash="dash",
                          line_color=colors_p.get(label, "#888"),
                          annotation_text=f"{label}: {val:.2f}",
                          annotation_position="right")

    fig.add_hline(y=0, line_color="black", line_width=1)

    fig.update_layout(
        title=title,
        xaxis_title="Fecha",
        yaxis_title="Spread",
        height=DEFAULT_PLOT_HEIGHT,
        template="plotly_white",
        hovermode="x unified",
    )
    return fig


def plot_spread_distribution(series: pd.Series,
                             current_value: float | None = None,
                             title: str = "Distribución del spread") -> go.Figure:
    """Histograma + boxplot del spread, con línea del valor actual."""
    if series.empty:
        return go.Figure()

    fig = make_subplots(
        rows=2, cols=1, row_heights=[0.75, 0.25],
        shared_xaxes=True, vertical_spacing=0.05,
    )

    fig.add_trace(go.Histogram(
        x=series.values, nbinsx=60, name="Frecuencia",
        marker_color="#1f77b4", opacity=0.75,
    ), row=1, col=1)

    fig.add_trace(go.Box(
        x=series.values, name="Boxplot",
        marker_color="#1f77b4", boxmean=True,
    ), row=2, col=1)

    if current_value is not None and not pd.isna(current_value):
        fig.add_vline(x=current_value, line_color="red", line_width=2,
                      line_dash="dash",
                      annotation_text=f"Actual: {current_value:.2f}",
                      annotation_position="top")

    fig.update_layout(
        title=title,
        height=DEFAULT_PLOT_HEIGHT,
        template="plotly_white",
        showlegend=False,
    )
    return fig


# -----------------------------------------------------------------------------
# CONTANGO / BACKWARDATION
# -----------------------------------------------------------------------------
def plot_contango_metrics(df: pd.DataFrame,
                          metrics: list[str] | None = None) -> go.Figure:
    """Serie temporal de múltiples métricas de contango."""
    if df.empty:
        return go.Figure()
    if metrics is None:
        metrics = [c for c in
                   ["spot_vs_m1_ratio", "m1_vs_m2_ratio",
                    "m2_vs_m3_ratio", "m4_vs_m7_ratio"]
                   if c in df.columns]

    fig = go.Figure()
    for m in metrics:
        if m not in df.columns:
            continue
        fig.add_trace(go.Scatter(
            x=df.index, y=df[m] * 100,
            mode="lines", name=m.replace("_", " "),
            line=dict(width=1.5),
        ))

    fig.add_hline(y=0, line_color="black", line_width=1)
    fig.update_layout(
        title="Contango/Backwardation a través del tiempo (% ratio)",
        xaxis_title="Fecha",
        yaxis_title="Ratio (%)",
        height=DEFAULT_PLOT_HEIGHT,
        template="plotly_white",
        hovermode="x unified",
    )
    return fig


def plot_contango_colored_area(ratio_series: pd.Series,
                               title: str = "M1/M2 Ratio") -> go.Figure:
    """
    Área con colores condicionales: verde cuando contango, rojo cuando
    backwardation.
    """
    if ratio_series.empty:
        return go.Figure()

    s = ratio_series * 100
    pos = s.where(s >= 0, 0)
    neg = s.where(s < 0, 0)

    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=s.index, y=pos, fill="tozeroy", mode="none",
        fillcolor="rgba(44,160,44,0.5)", name="Contango",
    ))
    fig.add_trace(go.Scatter(
        x=s.index, y=neg, fill="tozeroy", mode="none",
        fillcolor="rgba(214,39,40,0.5)", name="Backwardation",
    ))
    fig.add_trace(go.Scatter(
        x=s.index, y=s, mode="lines", name="Ratio",
        line=dict(color="black", width=1),
    ))
    fig.update_layout(
        title=title,
        xaxis_title="Fecha",
        yaxis_title="Ratio (%)",
        height=DEFAULT_PLOT_HEIGHT,
        template="plotly_white",
    )
    return fig


# -----------------------------------------------------------------------------
# VALORACIÓN RELATIVA
# -----------------------------------------------------------------------------
def plot_premium_over_time(df: pd.DataFrame,
                           title: str = "Prima M1 sobre spot") -> go.Figure:
    """Diff y ratio de la prima futuro vs spot con percentil en sub-panel."""
    if df.empty:
        return go.Figure()

    fig = make_subplots(
        rows=3, cols=1, shared_xaxes=True,
        row_heights=[0.5, 0.25, 0.25], vertical_spacing=0.05,
        subplot_titles=("Prima (diff)", "Percentil rolling 3Y", "Z-score"),
    )
    fig.add_trace(go.Scatter(
        x=df.index, y=df["diff"], mode="lines", name="M1 - Spot",
        line=dict(color="#1f77b4"),
    ), row=1, col=1)
    fig.add_hline(y=0, line_color="black", line_width=1, row=1, col=1)

    fig.add_trace(go.Scatter(
        x=df.index, y=df["diff_pct_rolling"],
        mode="lines", name="Pct rolling",
        line=dict(color="#ff7f0e"),
    ), row=2, col=1)
    fig.add_hrect(y0=80, y1=100, fillcolor="red", opacity=0.1,
                  line_width=0, row=2, col=1)
    fig.add_hrect(y0=0, y1=20, fillcolor="green", opacity=0.1,
                  line_width=0, row=2, col=1)

    fig.add_trace(go.Scatter(
        x=df.index, y=df["diff_zscore"], mode="lines", name="Z-score",
        line=dict(color="#2ca02c"),
    ), row=3, col=1)
    for z in (-1.5, 0, 1.5):
        fig.add_hline(y=z, line_color="black",
                      line_width=1, line_dash="dot", row=3, col=1)

    fig.update_layout(title=title, height=700, template="plotly_white",
                      showlegend=False)
    return fig


# -----------------------------------------------------------------------------
# ESTACIONALIDAD
# -----------------------------------------------------------------------------
def plot_seasonality(agg: pd.DataFrame, overlay: pd.DataFrame | None,
                     x_col: str, x_label: str,
                     title: str = "Estacionalidad") -> go.Figure:
    """
    Dibuja mediana histórica + banda IQR + overlay año actual.

    agg: DataFrame con columnas [x_col, mean, median, p25, p75]
    overlay: DataFrame con [x_col o key, median o value] del año actual
    """
    fig = go.Figure()
    if agg.empty:
        return fig

    # Banda IQR
    fig.add_trace(go.Scatter(
        x=agg[x_col], y=agg["p75"] * 100,
        mode="lines", line=dict(width=0), showlegend=False,
        hoverinfo="skip",
    ))
    fig.add_trace(go.Scatter(
        x=agg[x_col], y=agg["p25"] * 100,
        mode="lines", fill="tonexty",
        fillcolor="rgba(31,119,180,0.2)", line=dict(width=0),
        name="IQR (P25-P75)",
    ))

    # Mediana histórica
    y_med = agg.get("median_smooth", agg["median"]) * 100
    fig.add_trace(go.Scatter(
        x=agg[x_col], y=y_med, mode="lines",
        line=dict(color="#1f77b4", width=2.5), name="Mediana histórica",
    ))

    # Media histórica
    y_mean = agg.get("mean_smooth", agg["mean"]) * 100
    fig.add_trace(go.Scatter(
        x=agg[x_col], y=y_mean, mode="lines",
        line=dict(color="#ff7f0e", width=1.5, dash="dash"),
        name="Media histórica",
    ))

    # Overlay año actual
    if overlay is not None and not overlay.empty:
        key_col = "key" if "key" in overlay.columns else x_col
        if "median" in overlay.columns:
            y_ov = overlay["median"] * 100
        elif "value" in overlay.columns:
            y_ov = overlay["value"] * 100
        else:
            y_ov = None
        if y_ov is not None:
            fig.add_trace(go.Scatter(
                x=overlay[key_col], y=y_ov,
                mode="lines+markers", name="Año actual",
                line=dict(color="#d62728", width=2.5),
                marker=dict(size=6),
            ))

    fig.add_hline(y=0, line_color="black", line_width=1)
    fig.update_layout(
        title=title,
        xaxis_title=x_label,
        yaxis_title="Return (%)",
        height=DEFAULT_PLOT_HEIGHT,
        template="plotly_white",
        hovermode="x unified",
    )
    return fig


# -----------------------------------------------------------------------------
# VIX SPOT
# -----------------------------------------------------------------------------
def plot_vix_spot(df_spot: pd.DataFrame,
                  include: list[str] | None = None) -> go.Figure:
    """Plot básico del VIX spot y variantes sintéticas."""
    fig = go.Figure()
    if df_spot.empty:
        return fig
    if include is None:
        include = ["vix", "vix9d", "vix3m", "vix6m"]
    colors = {"vix": "#1f77b4", "vix9d": "#ff7f0e",
              "vix3m": "#2ca02c", "vix6m": "#d62728"}
    for col in include:
        if col in df_spot.columns:
            fig.add_trace(go.Scatter(
                x=df_spot.index, y=df_spot[col],
                mode="lines", name=col.upper(),
                line=dict(color=colors.get(col, "#888"), width=1.3),
            ))
    fig.update_layout(
        title="VIX spot e índices sintéticos",
        xaxis_title="Fecha",
        yaxis_title="Nivel",
        height=DEFAULT_PLOT_HEIGHT,
        template="plotly_white",
        hovermode="x unified",
    )
    return fig
