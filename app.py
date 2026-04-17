"""
VIX Analytics Platform — Streamlit App
=======================================
Análisis de estructura temporal del VIX (spot + futuros) y spreads.

Secciones:
  - Resumen (dashboard rápido)
  - VIX Central (curva estilo vixcentral.com)
  - Spread Analyzer (constructor de spreads + valoración por percentiles)
  - Actualización de datos

Estética oscura, fuente monospace.

Ejecutar:
    streamlit run app.py
"""
from __future__ import annotations

import datetime as dt
import logging

import numpy as np
import pandas as pd
import plotly.graph_objects as go
import streamlit as st
from plotly.subplots import make_subplots

import analytics
from storage import (
    get_all_contracts_info,
    get_last_date_spot,
    get_last_update,
    get_update_log,
    init_db,
    read_all_contracts,
    read_continuous,
    read_spot,
)
from updater import update_historical_data

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s [%(levelname)s] %(message)s")

# ═══════════════════════════════════════════════════════════════════════════
# CONFIG PÁGINA
# ═══════════════════════════════════════════════════════════════════════════
st.set_page_config(
    page_title="VIX Analytics Platform",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded",
)

init_db()

# ═══════════════════════════════════════════════════════════════════════════
# TEMA — COLORES Y ESTILO PLOTLY
# ═══════════════════════════════════════════════════════════════════════════
COLORS = {
    "bg": "#0a0e17",
    "panel": "#111827",
    "panel_border": "#1e293b",
    "text": "#e2e8f0",
    "dim": "#94a3b8",
    "accent": "#3b82f6",
    "green": "#10b981",
    "red": "#ef4444",
    "orange": "#f59e0b",
    "purple": "#a855f7",
    "cyan": "#06b6d4",
    "yellow": "#fbbf24",
    "curve_colors": [
        "#3b82f6", "#06b6d4", "#10b981", "#84cc16",
        "#fbbf24", "#f97316", "#ef4444", "#ec4899",
    ],
}

PLOTLY_LAYOUT = dict(
    template="plotly_dark",
    paper_bgcolor="rgba(0,0,0,0)",
    plot_bgcolor="#111827",
    font=dict(family="JetBrains Mono, Fira Code, monospace",
              color=COLORS["text"]),
    margin=dict(l=50, r=30, t=50, b=50),
    xaxis=dict(gridcolor="#1e293b", gridwidth=1, zerolinecolor="#334155"),
    yaxis=dict(gridcolor="#1e293b", gridwidth=1, zerolinecolor="#334155"),
    legend=dict(bgcolor="rgba(0,0,0,0)"),
)

# ═══════════════════════════════════════════════════════════════════════════
# CSS GLOBAL
# ═══════════════════════════════════════════════════════════════════════════
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=JetBrains+Mono:wght@300;400;500;600;700;800&display=swap');

.stApp {
    background: #0a0e17;
    font-family: 'JetBrains Mono', monospace;
}
section[data-testid="stSidebar"] {
    background: #111827;
    border-right: 1px solid #1e293b;
}
h1, h2, h3, h4, h5 { color: #e2e8f0 !important; }
p, label, span, div { color: #e2e8f0; }

.metric-card {
    background: #111827;
    border: 1px solid #1e293b;
    border-radius: 10px;
    padding: 14px 20px;
    text-align: center;
    height: 100%;
}
.metric-value {
    font-size: 24px;
    font-weight: 800;
    margin: 4px 0;
    font-family: 'JetBrains Mono', monospace;
}
.metric-label {
    font-size: 10px;
    color: #94a3b8;
    text-transform: uppercase;
    letter-spacing: 0.12em;
}
.metric-subtext {
    font-size: 11px;
    color: #64748b;
}

.signal-badge {
    display: inline-block;
    padding: 6px 14px;
    border-radius: 6px;
    font-size: 12px;
    font-weight: 700;
    letter-spacing: 0.05em;
}
.signal-extremo-barato { background: #10b98125; color: #10b981; border: 1px solid #10b98140; }
.signal-barato { background: #10b98115; color: #6ee7b7; border: 1px solid #10b98130; }
.signal-neutral { background: #94a3b815; color: #94a3b8; border: 1px solid #94a3b830; }
.signal-caro { background: #f59e0b15; color: #f59e0b; border: 1px solid #f59e0b30; }
.signal-extremo-caro { background: #ef444425; color: #ef4444; border: 1px solid #ef444440; }
.signal-contango { background: #10b98115; color: #10b981; border: 1px solid #10b98130; }
.signal-backwardation { background: #ef444415; color: #ef4444; border: 1px solid #ef444430; }
.signal-flat { background: #94a3b815; color: #94a3b8; border: 1px solid #94a3b830; }

.header-bar {
    background: linear-gradient(135deg, #0f172a 0%, #1e1b4b 50%, #0f172a 100%);
    border-bottom: 1px solid #1e293b;
    padding: 18px 24px;
    border-radius: 10px;
    margin-bottom: 20px;
    display: flex;
    align-items: center;
    gap: 14px;
}
.header-title {
    font-size: 22px;
    font-weight: 800;
    color: white;
    letter-spacing: 0.05em;
}
.header-accent { color: #3b82f6; }
.header-badge {
    font-size: 10px;
    padding: 3px 10px;
    border-radius: 4px;
    background: rgba(59,130,246,0.15);
    color: #3b82f6;
    font-weight: 600;
    letter-spacing: 0.12em;
}

.stDataFrame {
    background: #111827 !important;
    border: 1px solid #1e293b;
    border-radius: 8px;
}

.formula-box {
    background: #1e293b;
    border: 1px solid #334155;
    border-radius: 8px;
    padding: 12px 16px;
    text-align: center;
    margin: 12px 0;
}
.formula-label {
    color: #94a3b8;
    font-size: 10px;
    text-transform: uppercase;
    letter-spacing: 0.1em;
}
.formula-value {
    color: #fff;
    font-size: 18px;
    font-weight: 700;
    margin-top: 4px;
}

hr { border-color: #1e293b !important; }
</style>
""", unsafe_allow_html=True)


# ═══════════════════════════════════════════════════════════════════════════
# CACHE DE DATOS
# ═══════════════════════════════════════════════════════════════════════════
@st.cache_data(ttl=3600, show_spinner=False)
def load_spot():
    return read_spot()


@st.cache_data(ttl=3600, show_spinner=False)
def load_continuous():
    return read_continuous()


@st.cache_data(ttl=3600, show_spinner=False)
def load_futures_long():
    return read_all_contracts()


@st.cache_data(ttl=3600, show_spinner=False)
def load_contracts_info():
    return get_all_contracts_info()


def invalidate_all_caches():
    load_spot.clear()
    load_continuous.clear()
    load_futures_long.clear()
    load_contracts_info.clear()


# ═══════════════════════════════════════════════════════════════════════════
# HELPERS DE FORMATO
# ═══════════════════════════════════════════════════════════════════════════
def metric_card(label: str, value: str, color: str = None,
                subtext: str = None) -> str:
    col = color or COLORS["text"]
    sub_html = f'<div class="metric-subtext">{subtext}</div>' if subtext else ''
    return f"""
    <div class="metric-card">
        <div class="metric-label">{label}</div>
        <div class="metric-value" style="color:{col}">{value}</div>
        {sub_html}
    </div>
    """


def signal_badge(signal: str) -> str:
    key = (signal.lower()
                 .replace(" ", "-")
                 .replace("á", "a").replace("é", "e")
                 .replace("í", "i").replace("ó", "o").replace("ú", "u"))
    return f'<span class="signal-badge signal-{key}">{signal}</span>'


def contango_badge(value: float) -> str:
    if pd.isna(value):
        return '<span class="signal-badge signal-flat">N/D</span>'
    if abs(value) < 0.005:
        return f'<span class="signal-badge signal-flat">FLAT ({value*100:+.2f}%)</span>'
    if value > 0:
        return f'<span class="signal-badge signal-contango">CONTANGO {value*100:+.2f}%</span>'
    return f'<span class="signal-badge signal-backwardation">BACKWARDATION {value*100:+.2f}%</span>'


def _pct_color(pct) -> str:
    if pct is None or pd.isna(pct):
        return COLORS["dim"]
    if pct < 10:
        return COLORS["green"]
    if pct < 30:
        return "#6ee7b7"
    if pct > 90:
        return COLORS["red"]
    if pct > 70:
        return COLORS["orange"]
    return COLORS["dim"]


def _zscore_color(z) -> str:
    if z is None or pd.isna(z):
        return COLORS["dim"]
    if z < -1.5:
        return COLORS["green"]
    if z > 1.5:
        return COLORS["red"]
    return COLORS["dim"]


# ═══════════════════════════════════════════════════════════════════════════
# SIDEBAR — NAVEGACIÓN
# ═══════════════════════════════════════════════════════════════════════════
def render_sidebar() -> str:
    st.sidebar.markdown("""
    <div style='text-align:center; padding: 8px 0 16px 0;'>
        <span style='font-size:22px; font-weight:800; letter-spacing:0.05em;'>
            <span style='color:#3b82f6;'>VIX</span> ANALYTICS
        </span>
        <br>
        <span style='font-size:9px; color:#94a3b8; letter-spacing:0.15em;'>
            TERM STRUCTURE · v2.0
        </span>
    </div>
    """, unsafe_allow_html=True)
    st.sidebar.markdown("---")

    section = st.sidebar.radio(
        "Navegación",
        ["📊 Resumen", "🎯 VIX Central", "⚡ Spread Analyzer",
         "🔄 Actualización"],
        label_visibility="collapsed",
    )

    st.sidebar.markdown("---")

    last_spot = get_last_date_spot()
    info = load_contracts_info()
    n_active = int(info["is_active"].sum()) if not info.empty else 0

    st.sidebar.markdown(f"""
    <div style='font-size:10px; color:#94a3b8; letter-spacing:0.1em;
                text-transform:uppercase; margin-bottom:8px;'>
        Estado del sistema
    </div>
    <div style='color:#e2e8f0; font-size:12px; line-height:1.7;'>
        📅 Último spot: <span style='color:#3b82f6;'>{last_spot.isoformat() if last_spot else '—'}</span><br>
        📦 Contratos: <span style='color:#3b82f6;'>{len(info) if not info.empty else 0}</span><br>
        🔵 Activos: <span style='color:#10b981;'>{n_active}</span>
    </div>
    """, unsafe_allow_html=True)

    last_update = get_last_update()
    if last_update:
        st.sidebar.caption(f"Última actualización: {last_update['run_at'][:19]}")

    return section


# ═══════════════════════════════════════════════════════════════════════════
# SECCIÓN: RESUMEN
# ═══════════════════════════════════════════════════════════════════════════
def page_resumen():
    st.markdown("""
    <div class="header-bar">
        <span class="header-title">
            <span class="header-accent">VIX</span> ANALYTICS
        </span>
        <span class="header-badge">RESUMEN</span>
    </div>
    """, unsafe_allow_html=True)

    df_spot = load_spot()
    df_cont = load_continuous()
    df_long = load_futures_long()

    if df_spot.empty and df_cont.empty:
        st.warning("⚠️ No hay datos locales. Ve a **Actualización** y "
                   "ejecuta una primera descarga.")
        return

    cols = st.columns(5)
    if not df_spot.empty and "vix" in df_spot.columns:
        last_spot = df_spot["vix"].dropna().iloc[-1]
        prev_spot = (df_spot["vix"].dropna().iloc[-2]
                     if len(df_spot["vix"].dropna()) > 1 else last_spot)
        diff = last_spot - prev_spot
        subtext = f"{diff:+.2f}"
        cols[0].markdown(
            metric_card("VIX SPOT", f"{last_spot:.2f}",
                        color=COLORS["orange"], subtext=subtext),
            unsafe_allow_html=True,
        )

    if not df_cont.empty:
        last = df_cont.iloc[-1]
        for idx, m in enumerate(["m1", "m2", "m3", "m4"]):
            if pd.notna(last.get(m)):
                cols[idx + 1].markdown(
                    metric_card(m.upper(), f"{last[m]:.2f}",
                                color=COLORS["accent"]),
                    unsafe_allow_html=True,
                )

    st.markdown("<br>", unsafe_allow_html=True)

    st.markdown("### 📝 Interpretación cuantitativa")
    lines = analytics.auto_interpretation(df_spot, df_cont)
    for line in lines:
        st.markdown(f"<div style='color:#94a3b8; font-size:13px; "
                    f"margin-bottom:6px;'>▸ {line}</div>",
                    unsafe_allow_html=True)

    st.markdown("<br>", unsafe_allow_html=True)

    col1, col2 = st.columns([1, 1])
    with col1:
        st.markdown("### 📈 Curva actual")
        if not df_long.empty:
            curve = analytics.current_curve(df_long)
            spot_val = (df_spot["vix"].dropna().iloc[-1]
                        if not df_spot.empty else None)
            fig = _plot_curve_dark(curve, spot_val)
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("Sin datos de curva.")

    with col2:
        st.markdown("### 📊 VIX Spot (últimos 12 meses)")
        if not df_spot.empty:
            fig = _plot_spot_dark(df_spot.tail(252))
            st.plotly_chart(fig, use_container_width=True)


def _plot_curve_dark(curve, spot_value=None):
    fig = go.Figure()
    if curve is None or curve.empty:
        fig.update_layout(**PLOTLY_LAYOUT, height=350)
        return fig

    fig.add_trace(go.Scatter(
        x=curve["dte"], y=curve["settle"],
        mode="lines+markers+text",
        text=[f"M{i}" for i in curve["month_index"]],
        textposition="top center",
        textfont=dict(size=10, color=COLORS["dim"]),
        marker=dict(size=10, color=COLORS["accent"],
                    line=dict(width=1, color=COLORS["text"])),
        line=dict(color=COLORS["accent"], width=2.5),
        name="VX Curve",
        hovertemplate="<b>M%{text}</b><br>DTE: %{x}<br>Settle: %{y:.2f}<extra></extra>",
    ))

    if spot_value is not None and pd.notna(spot_value):
        fig.add_trace(go.Scatter(
            x=[0], y=[spot_value],
            mode="markers+text",
            text=["SPOT"], textposition="top center",
            textfont=dict(size=10, color=COLORS["orange"]),
            marker=dict(size=14, color=COLORS["orange"], symbol="diamond"),
            name="VIX Spot",
        ))

    layout = dict(PLOTLY_LAYOUT)
    layout["height"] = 350
    layout["xaxis"] = dict(PLOTLY_LAYOUT["xaxis"], title="DTE (días)")
    layout["yaxis"] = dict(PLOTLY_LAYOUT["yaxis"], title="Settle")
    layout["showlegend"] = False
    fig.update_layout(**layout)
    return fig


def _plot_spot_dark(df_spot):
    fig = go.Figure()
    if df_spot.empty or "vix" not in df_spot.columns:
        fig.update_layout(**PLOTLY_LAYOUT, height=350)
        return fig
    fig.add_trace(go.Scatter(
        x=df_spot.index, y=df_spot["vix"],
        mode="lines", fill="tozeroy",
        fillcolor="rgba(245,158,11,0.1)",
        line=dict(color=COLORS["orange"], width=1.5),
        name="VIX",
        hovertemplate="%{x|%Y-%m-%d}<br>%{y:.2f}<extra></extra>",
    ))
    layout = dict(PLOTLY_LAYOUT)
    layout["height"] = 350
    layout["showlegend"] = False
    fig.update_layout(**layout)
    return fig


# ═══════════════════════════════════════════════════════════════════════════
# SECCIÓN: VIX CENTRAL
# ═══════════════════════════════════════════════════════════════════════════
def page_vix_central():
    st.markdown("""
    <div class="header-bar">
        <span class="header-title">
            <span class="header-accent">VIX</span> CENTRAL
        </span>
        <span class="header-badge">TERM STRUCTURE</span>
    </div>
    """, unsafe_allow_html=True)

    df_spot = load_spot()
    df_cont = load_continuous()
    df_long = load_futures_long()

    if df_long.empty or df_cont.empty:
        st.warning("⚠️ Sin datos de futuros. Ejecuta actualización primero.")
        return

    available_dates = sorted(df_long["date"].dt.date.unique(), reverse=True)
    col_a, col_b = st.columns([2, 5])
    with col_a:
        as_of = st.selectbox(
            "📅 Fecha de referencia",
            options=available_dates[:500],
            index=0,
            key="vc_date",
        )
    as_of_ts = pd.Timestamp(as_of)

    curve = analytics.current_curve(df_long, as_of=as_of_ts)
    if curve.empty:
        st.error("No hay datos en esa fecha.")
        return

    spot_val = None
    if not df_spot.empty and "vix" in df_spot.columns:
        spot_series = df_spot["vix"].dropna()
        spot_at = spot_series[spot_series.index <= as_of_ts]
        if not spot_at.empty:
            spot_val = float(spot_at.iloc[-1])

    # ── Gráfico de la curva grande ──
    fig = _plot_vix_central_curve(curve, spot_val)
    st.plotly_chart(fig, use_container_width=True)

    st.markdown("<br>", unsafe_allow_html=True)

    # ── Tabla VIX Central ──
    st.markdown("### 📋 Estructura temporal completa")
    table_df, total_contango = _build_vixcentral_table(curve, spot_val)
    if not table_df.empty:
        _render_vixcentral_table(table_df, total_contango)

    st.markdown("<br>", unsafe_allow_html=True)

    # ── Contango histórico ──
    col_left, col_right = st.columns([3, 2])

    with col_left:
        st.markdown("### 📈 Contango M1→M2 histórico")
        df_metrics = analytics.contango_metrics(df_spot, df_cont)
        if not df_metrics.empty:
            fig = _plot_contango_history(df_metrics["m1_vs_m2_ratio"])
            st.plotly_chart(fig, use_container_width=True)

    with col_right:
        st.markdown("### 🎯 Estado actual")
        m1 = curve["settle"].iloc[0] if len(curve) >= 1 else np.nan
        m2 = curve["settle"].iloc[1] if len(curve) >= 2 else np.nan
        ratio_m1_m2 = ((m2 / m1 - 1) if pd.notna(m1) and pd.notna(m2)
                       and m1 > 0 else np.nan)

        st.markdown(f"""
        <div style='background:#111827; border:1px solid #1e293b;
                    border-radius:10px; padding:16px; margin-bottom:12px;'>
            <div style='color:#94a3b8; font-size:10px; letter-spacing:0.1em;
                        text-transform:uppercase; margin-bottom:8px;'>
                Estructura M1 → M2
            </div>
            {contango_badge(ratio_m1_m2)}
        </div>
        """, unsafe_allow_html=True)

        if not df_metrics.empty:
            ratio_series = df_metrics["m1_vs_m2_ratio"].dropna()
            if len(ratio_series) > 30 and pd.notna(ratio_m1_m2):
                pct = (ratio_series < ratio_m1_m2).mean() * 100
                color = (COLORS["green"] if pct < 30
                         else COLORS["orange"] if pct > 70
                         else COLORS["dim"])
                st.markdown(f"""
                <div style='background:#111827; border:1px solid #1e293b;
                            border-radius:10px; padding:16px;
                            margin-bottom:12px;'>
                    <div style='color:#94a3b8; font-size:10px;
                                letter-spacing:0.1em;
                                text-transform:uppercase;
                                margin-bottom:8px;'>
                        Percentil histórico del ratio
                    </div>
                    <div style='font-size:28px; font-weight:800; color:{color};'>
                        {pct:.0f}%
                    </div>
                    <div style='color:#64748b; font-size:11px;'>
                        sobre {len(ratio_series):,} observaciones
                    </div>
                </div>
                """, unsafe_allow_html=True)

        slope = analytics.curve_slope(curve)
        st.markdown(f"""
        <div style='background:#111827; border:1px solid #1e293b;
                    border-radius:10px; padding:16px;'>
            <div style='color:#94a3b8; font-size:10px; letter-spacing:0.1em;
                        text-transform:uppercase; margin-bottom:8px;'>
                Pendiente (puntos/día)
            </div>
            <div style='font-size:28px; font-weight:800; color:#06b6d4;'>
                {slope:+.4f}
            </div>
            <div style='color:#64748b; font-size:11px;'>
                Regresión lineal settle ~ DTE
            </div>
        </div>
        """, unsafe_allow_html=True)


def _plot_vix_central_curve(curve, spot_value):
    fig = go.Figure()
    if curve.empty:
        fig.update_layout(**PLOTLY_LAYOUT, height=500)
        return fig

    for i, row in curve.iterrows():
        color = COLORS["curve_colors"][i % len(COLORS["curve_colors"])]
        fig.add_trace(go.Scatter(
            x=[row["dte"]], y=[row["settle"]],
            mode="markers+text",
            text=[f"M{row['month_index']}<br>{row['settle']:.2f}"],
            textposition="top center",
            textfont=dict(size=11, color=color, family="JetBrains Mono"),
            marker=dict(size=16, color=color,
                        line=dict(width=2, color="white")),
            name=f"M{row['month_index']}",
            showlegend=False,
            hovertemplate=(
                f"<b>M{row['month_index']}</b><br>"
                f"Contract: {row['contract_code']}<br>"
                f"Expiry: {row['expiry_date'].strftime('%Y-%m-%d') if pd.notna(row['expiry_date']) else '—'}<br>"
                f"DTE: {row['dte']}<br>"
                f"Settle: {row['settle']:.3f}<extra></extra>"
            ),
        ))

    fig.add_trace(go.Scatter(
        x=curve["dte"], y=curve["settle"],
        mode="lines",
        line=dict(color="#334155", width=2),
        showlegend=False, hoverinfo="skip",
    ))

    if spot_value is not None:
        fig.add_trace(go.Scatter(
            x=[0], y=[spot_value],
            mode="markers+text",
            text=[f"SPOT<br>{spot_value:.2f}"],
            textposition="top center",
            textfont=dict(size=11, color=COLORS["orange"],
                          family="JetBrains Mono"),
            marker=dict(size=18, color=COLORS["orange"], symbol="diamond",
                        line=dict(width=2, color="white")),
            name="Spot", showlegend=False,
        ))

    layout = dict(PLOTLY_LAYOUT)
    layout["height"] = 500
    layout["xaxis"] = dict(PLOTLY_LAYOUT["xaxis"],
                           title="Días hasta expiración (DTE)")
    layout["yaxis"] = dict(PLOTLY_LAYOUT["yaxis"],
                           title="Settle (puntos VIX)")
    layout["title"] = dict(text="VIX Futures Term Structure",
                           font=dict(size=16, color=COLORS["text"]))
    fig.update_layout(**layout)
    return fig


def _build_vixcentral_table(curve, spot):
    if curve.empty:
        return pd.DataFrame(), float("nan")

    rows = []
    if spot is not None:
        rows.append({
            "Mes": "Spot (VIX)",
            "Contrato": "—",
            "Settle": spot,
            "DTE": 0,
            "Ratio vs anterior": "—",
            "_ratio_num": np.nan,
        })

    prev_settle = spot if spot is not None else None
    for _, row in curve.iterrows():
        ratio = ((row["settle"] / prev_settle - 1)
                 if prev_settle and prev_settle > 0 else np.nan)
        rows.append({
            "Mes": f"M{row['month_index']}",
            "Contrato": row["contract_code"].replace("VX_", ""),
            "Settle": row["settle"],
            "DTE": int(row["dte"]) if pd.notna(row["dte"]) else 0,
            "Ratio vs anterior": (f"{ratio*100:+.2f}%"
                                  if pd.notna(ratio) else "—"),
            "_ratio_num": ratio,
        })
        prev_settle = row["settle"]

    df = pd.DataFrame(rows)

    if len(curve) >= 2:
        m1, m_last = curve["settle"].iloc[0], curve["settle"].iloc[-1]
        total_contango = (m_last / m1 - 1) if m1 > 0 else np.nan
    else:
        total_contango = float("nan")

    return df, total_contango


def _render_vixcentral_table(df, total_contango):
    html = """
    <div style='background:#111827; border:1px solid #1e293b;
                border-radius:10px; padding:16px;'>
    <table style='width:100%; border-collapse:collapse;
                  font-family:JetBrains Mono;'>
    <thead>
        <tr style='border-bottom:1px solid #334155; color:#94a3b8;
                   font-size:11px; text-transform:uppercase;
                   letter-spacing:0.1em;'>
            <th style='text-align:left; padding:10px 8px;'>Mes</th>
            <th style='text-align:left; padding:10px 8px;'>Contrato</th>
            <th style='text-align:right; padding:10px 8px;'>Settle</th>
            <th style='text-align:right; padding:10px 8px;'>DTE</th>
            <th style='text-align:right; padding:10px 8px;'>Ratio vs anterior</th>
        </tr>
    </thead>
    <tbody>
    """
    for _, row in df.iterrows():
        is_spot = row["Mes"] == "Spot (VIX)"
        name_color = "#f59e0b" if is_spot else "#3b82f6"
        ratio_num = row.get("_ratio_num", np.nan)
        if pd.isna(ratio_num):
            ratio_color = "#94a3b8"
        elif ratio_num > 0:
            ratio_color = "#10b981"
        elif ratio_num < 0:
            ratio_color = "#ef4444"
        else:
            ratio_color = "#94a3b8"

        html += f"""
        <tr style='border-bottom:1px solid #1e293b; color:#e2e8f0;'>
            <td style='padding:10px 8px; color:{name_color}; font-weight:700;'>{row['Mes']}</td>
            <td style='padding:10px 8px; color:#94a3b8;'>{row['Contrato']}</td>
            <td style='padding:10px 8px; text-align:right; font-weight:600;'>{row['Settle']:.3f}</td>
            <td style='padding:10px 8px; text-align:right; color:#94a3b8;'>{row['DTE']}</td>
            <td style='padding:10px 8px; text-align:right; color:{ratio_color}; font-weight:600;'>{row['Ratio vs anterior']}</td>
        </tr>
        """

    html += "</tbody></table>"

    if pd.notna(total_contango):
        total_color = "#10b981" if total_contango > 0 else "#ef4444"
        label = ("CONTANGO TOTAL" if total_contango > 0
                 else "BACKWARDATION TOTAL")
        html += f"""
        <div style='margin-top:16px; padding-top:12px;
                    border-top:1px solid #334155;
                    display:flex; justify-content:space-between;
                    align-items:center;'>
            <span style='color:#94a3b8; font-size:11px;
                         letter-spacing:0.1em; text-transform:uppercase;'>
                {label} (M_last / M1)
            </span>
            <span style='color:{total_color}; font-size:20px; font-weight:800;'>
                {total_contango*100:+.2f}%
            </span>
        </div>
        """

    html += "</div>"
    st.markdown(html, unsafe_allow_html=True)


def _plot_contango_history(ratio_series):
    fig = go.Figure()
    if ratio_series.empty:
        fig.update_layout(**PLOTLY_LAYOUT, height=400)
        return fig

    s = ratio_series.dropna() * 100
    pos = s.where(s >= 0, 0)
    neg = s.where(s < 0, 0)

    fig.add_trace(go.Scatter(
        x=s.index, y=pos, fill="tozeroy", mode="none",
        fillcolor="rgba(16,185,129,0.3)", name="Contango",
    ))
    fig.add_trace(go.Scatter(
        x=s.index, y=neg, fill="tozeroy", mode="none",
        fillcolor="rgba(239,68,68,0.3)", name="Backwardation",
    ))
    fig.add_trace(go.Scatter(
        x=s.index, y=s, mode="lines", name="M2/M1 ratio",
        line=dict(color="#e2e8f0", width=1.2),
        hovertemplate="%{x|%Y-%m-%d}<br>%{y:+.2f}%<extra></extra>",
    ))
    fig.add_hline(y=0, line_dash="dash", line_color="#475569",
                  line_width=0.5)

    layout = dict(PLOTLY_LAYOUT)
    layout["height"] = 400
    layout["xaxis"] = dict(PLOTLY_LAYOUT["xaxis"], title="")
    layout["yaxis"] = dict(PLOTLY_LAYOUT["yaxis"],
                           title="Ratio M2/M1 (%)")
    fig.update_layout(**layout)
    return fig


# ═══════════════════════════════════════════════════════════════════════════
# SECCIÓN: SPREAD ANALYZER
# ═══════════════════════════════════════════════════════════════════════════
SPREAD_PRESETS = {
    "M1 − M2": [{"month": 1, "weight": 1.0}, {"month": 2, "weight": -1.0}],
    "M2 − M3": [{"month": 2, "weight": 1.0}, {"month": 3, "weight": -1.0}],
    "M3 − M4": [{"month": 3, "weight": 1.0}, {"month": 4, "weight": -1.0}],
    "M4 − M5": [{"month": 4, "weight": 1.0}, {"month": 5, "weight": -1.0}],
    "M1 − M3": [{"month": 1, "weight": 1.0}, {"month": 3, "weight": -1.0}],
    "M2 − M4": [{"month": 2, "weight": 1.0}, {"month": 4, "weight": -1.0}],
    "M1 − M4": [{"month": 1, "weight": 1.0}, {"month": 4, "weight": -1.0}],
    "M4 − M7": [{"month": 4, "weight": 1.0}, {"month": 7, "weight": -1.0}],
    "Fly 1-2-3": [{"month": 1, "weight": 1.0}, {"month": 2, "weight": -2.0},
                  {"month": 3, "weight": 1.0}],
    "Fly 2-3-4": [{"month": 2, "weight": 1.0}, {"month": 3, "weight": -2.0},
                  {"month": 4, "weight": 1.0}],
    "Fly 3-4-5": [{"month": 3, "weight": 1.0}, {"month": 4, "weight": -2.0},
                  {"month": 5, "weight": 1.0}],
    "Condor 1-2-3-4": [{"month": 1, "weight": 1.0}, {"month": 2, "weight": -1.0},
                       {"month": 3, "weight": -1.0}, {"month": 4, "weight": 1.0}],
}


def page_spread_analyzer():
    st.markdown("""
    <div class="header-bar">
        <span class="header-title">
            <span class="header-accent">SPREAD</span> ANALYZER
        </span>
        <span class="header-badge">VALORACIÓN HISTÓRICA</span>
    </div>
    """, unsafe_allow_html=True)

    df_spot = load_spot()
    df_long = load_futures_long()

    if df_long.empty:
        st.warning("⚠️ Sin datos de futuros. Ejecuta actualización primero.")
        return

    # ── Constructor ──
    st.markdown("### 🔧 Constructor de spread")

    col1, col2 = st.columns([1, 1])
    with col1:
        preset_name = st.selectbox(
            "Preset rápido",
            options=list(SPREAD_PRESETS.keys()),
            index=0,
        )
        if ("custom_legs" not in st.session_state or
                st.session_state.get("last_preset") != preset_name):
            st.session_state["custom_legs"] = [
                dict(leg) for leg in SPREAD_PRESETS[preset_name]
            ]
            st.session_state["last_preset"] = preset_name

    with col2:
        use_custom = st.checkbox("Personalizar patas", value=False)

    legs = st.session_state["custom_legs"]

    if use_custom:
        n_legs = st.radio("Nº patas", [2, 3, 4], horizontal=True,
                          index={2: 0, 3: 1, 4: 2}.get(len(legs), 0))
        while len(legs) < n_legs:
            legs.append({"month": len(legs) + 1, "weight": 1.0})
        while len(legs) > n_legs:
            legs.pop()

        leg_cols = st.columns(n_legs)
        new_legs = []
        for i in range(n_legs):
            with leg_cols[i]:
                st.markdown(f"**Pata {i + 1}**")
                m = st.selectbox(
                    "Mes", options=list(range(1, 9)),
                    format_func=lambda x: f"M{x}",
                    index=min(legs[i]["month"] - 1, 7),
                    key=f"spread_m_{i}",
                )
                w = st.number_input(
                    "Peso", value=float(legs[i]["weight"]),
                    step=0.5, min_value=-5.0, max_value=5.0,
                    key=f"spread_w_{i}",
                )
                new_legs.append({"month": m, "weight": w})
        legs = new_legs
        st.session_state["custom_legs"] = legs

    formula = analytics.formula_string(legs)
    st.markdown(f"""
    <div class='formula-box'>
        <div class='formula-label'>Fórmula</div>
        <div class='formula-value'>{formula}</div>
    </div>
    """, unsafe_allow_html=True)

    # ── Calcular ──
    spread_df = analytics.compute_custom_spread(df_long, df_spot, legs)
    if spread_df.empty:
        st.error("No se pudo calcular el spread (faltan datos para esos meses).")
        return

    latest = spread_df.iloc[-1]
    current_val = float(latest["spread"])
    series = spread_df.set_index("date")["spread"].sort_index()

    # VIX similar para percentil condicional
    similar_mask = None
    if pd.notna(latest.get("vix")):
        vix_latest = latest["vix"]
        df_vix_indexed = spread_df.dropna(subset=["vix"]).set_index("date")
        similar_mask = ((df_vix_indexed["vix"] >= vix_latest - 3) &
                        (df_vix_indexed["vix"] <= vix_latest + 3))

    val = analytics.spread_valuation(series, current_value=current_val,
                                     similar_mask=similar_mask)

    st.markdown("<br>", unsafe_allow_html=True)

    # ── KPIs ──
    k_cols = st.columns(6)
    spread_color = COLORS["green"] if current_val >= 0 else COLORS["red"]
    k_cols[0].markdown(
        metric_card("SPREAD ACTUAL", f"{current_val:+.3f}",
                    color=spread_color),
        unsafe_allow_html=True,
    )
    k_cols[1].markdown(
        metric_card("VIX SPOT",
                    f"{latest['vix']:.2f}" if pd.notna(latest.get("vix")) else "—",
                    color=COLORS["orange"]),
        unsafe_allow_html=True,
    )
    k_cols[2].markdown(
        metric_card("DTE FRONT",
                    f"{int(latest['dte_front'])}d" if pd.notna(latest.get("dte_front")) else "—",
                    color=COLORS["cyan"]),
        unsafe_allow_html=True,
    )
    k_cols[3].markdown(
        metric_card("PCT ROLLING 3Y",
                    f"{val.get('pct_rolling', 0):.0f}%" if val.get("pct_rolling") is not None else "—",
                    color=_pct_color(val.get("pct_rolling"))),
        unsafe_allow_html=True,
    )
    k_cols[4].markdown(
        metric_card("Z-SCORE 1Y",
                    f"{val.get('zscore', 0):+.2f}σ" if val.get("zscore") is not None else "—",
                    color=_zscore_color(val.get("zscore"))),
        unsafe_allow_html=True,
    )
    k_cols[5].markdown(
        metric_card("OBSERVACIONES", f"{val['n_observations']:,}",
                    color=COLORS["dim"]),
        unsafe_allow_html=True,
    )

    st.markdown("<br>", unsafe_allow_html=True)

    # ── Señal ──
    signal = val["signal"]
    cond_str = ""
    if val.get("pct_conditional") is not None:
        cond_str = (f" · Percentil con VIX similar: "
                    f"<span style='color:#a855f7;'>"
                    f"{val['pct_conditional']:.1f}%</span>")

    st.markdown(f"""
    <div style='background:#111827; border:1px solid #1e293b;
                border-radius:10px; padding:20px; text-align:center;
                margin-bottom:20px;'>
        <div style='color:#94a3b8; font-size:11px; letter-spacing:0.15em;
                    text-transform:uppercase; margin-bottom:10px;'>
            Valoración del spread frente a su histórico
        </div>
        <div style='margin-bottom:12px;'>
            {signal_badge(signal)}
        </div>
        <div style='color:#64748b; font-size:12px;'>
            Percentil rolling 3Y: <span style='color:#3b82f6;'>{val.get('pct_rolling', 0):.1f}%</span> ·
            Percentil full-sample: <span style='color:#3b82f6;'>{val['pct_full']:.1f}%</span>{cond_str}
        </div>
    </div>
    """, unsafe_allow_html=True)

    # ── Gráficos ──
    col_l, col_r = st.columns([3, 2])

    with col_l:
        st.markdown("### 📈 Histórico del spread")
        fig_hist = _plot_spread_history(series, current_val)
        st.plotly_chart(fig_hist, use_container_width=True)

    with col_r:
        st.markdown("### 📊 Distribución")
        fig_dist = _plot_spread_distribution(series, current_val)
        st.plotly_chart(fig_dist, use_container_width=True)

    # ── Scanner ──
    st.markdown("<br>", unsafe_allow_html=True)
    st.markdown("### 🔍 Scanner — todos los spreads")
    scanner_df = _compute_scanner(df_long, df_spot)
    if scanner_df is not None and not scanner_df.empty:
        _render_scanner_table(scanner_df)
    else:
        st.info("No hay datos suficientes para el scanner.")


def _plot_spread_history(series, current):
    fig = go.Figure()
    if series.empty:
        fig.update_layout(**PLOTLY_LAYOUT, height=400)
        return fig

    pos = series.where(series >= 0, 0)
    neg = series.where(series < 0, 0)

    fig.add_trace(go.Scatter(
        x=series.index, y=pos, fill="tozeroy", mode="none",
        fillcolor="rgba(16,185,129,0.25)", showlegend=False,
    ))
    fig.add_trace(go.Scatter(
        x=series.index, y=neg, fill="tozeroy", mode="none",
        fillcolor="rgba(239,68,68,0.25)", showlegend=False,
    ))
    fig.add_trace(go.Scatter(
        x=series.index, y=series.values,
        mode="lines", line=dict(color="#e2e8f0", width=1.0),
        name="Spread",
        hovertemplate="%{x|%Y-%m-%d}<br>%{y:+.3f}<extra></extra>",
    ))

    p10 = series.quantile(0.10)
    p90 = series.quantile(0.90)
    fig.add_hline(y=p10, line_dash="dot", line_color="#10b981",
                  annotation_text=f"P10: {p10:+.3f}",
                  annotation_position="right",
                  annotation_font_color="#10b981")
    fig.add_hline(y=p90, line_dash="dot", line_color="#ef4444",
                  annotation_text=f"P90: {p90:+.3f}",
                  annotation_position="right",
                  annotation_font_color="#ef4444")
    fig.add_hline(y=current, line_color="#3b82f6", line_width=2,
                  annotation_text=f"Actual: {current:+.3f}",
                  annotation_position="left",
                  annotation_font_color="#3b82f6")
    fig.add_hline(y=0, line_color="#475569", line_width=0.5,
                  line_dash="dash")

    layout = dict(PLOTLY_LAYOUT)
    layout["height"] = 400
    layout["showlegend"] = False
    fig.update_layout(**layout)
    return fig


def _plot_spread_distribution(series, current):
    fig = go.Figure()
    if series.empty:
        fig.update_layout(**PLOTLY_LAYOUT, height=400)
        return fig

    fig.add_trace(go.Histogram(
        x=series.values, nbinsx=40,
        marker=dict(color="#334155", line=dict(color="#475569", width=1)),
        name="Distribución",
        hovertemplate="Rango: %{x:.2f}<br>Freq: %{y}<extra></extra>",
    ))

    fig.add_vline(x=current, line_color=COLORS["accent"], line_width=2.5,
                  annotation_text=f"Actual: {current:+.3f}",
                  annotation_position="top",
                  annotation_font_color=COLORS["accent"])

    median = float(series.median())
    fig.add_vline(x=median, line_color="#94a3b8", line_width=1,
                  line_dash="dash",
                  annotation_text=f"Med: {median:+.3f}",
                  annotation_position="bottom",
                  annotation_font_color="#94a3b8")

    layout = dict(PLOTLY_LAYOUT)
    layout["height"] = 400
    layout["showlegend"] = False
    layout["xaxis"] = dict(PLOTLY_LAYOUT["xaxis"], title="Spread")
    layout["yaxis"] = dict(PLOTLY_LAYOUT["yaxis"], title="Frecuencia")
    fig.update_layout(**layout)
    return fig


def _compute_scanner(df_long, df_spot):
    results = []
    for name, legs in SPREAD_PRESETS.items():
        try:
            df = analytics.compute_custom_spread(df_long, df_spot, legs)
            if df.empty:
                continue
            latest = df.iloc[-1]
            current = float(latest["spread"])
            series = df.set_index("date")["spread"].sort_index()
            val = analytics.spread_valuation(series, current_value=current)
            results.append({
                "Spread": name,
                "Actual": current,
                "Media": val["mean"],
                "P10": series.quantile(0.10),
                "P90": series.quantile(0.90),
                "Pct rolling": val.get("pct_rolling"),
                "Pct full": val.get("pct_full"),
                "Z-score": val.get("zscore"),
                "Señal": val["signal"],
                "N": val["n_observations"],
            })
        except Exception:
            continue
    if not results:
        return pd.DataFrame()
    return pd.DataFrame(results)


def _render_scanner_table(df):
    df_sorted = df.sort_values(
        "Pct rolling", ascending=False, na_position="last"
    ).copy()

    html = """
    <div style='background:#111827; border:1px solid #1e293b;
                border-radius:10px; padding:16px; overflow-x:auto;'>
    <table style='width:100%; border-collapse:collapse;
                  font-family:JetBrains Mono; font-size:12px;'>
    <thead>
        <tr style='border-bottom:2px solid #334155; color:#94a3b8;
                   font-size:10px; text-transform:uppercase;
                   letter-spacing:0.1em;'>
            <th style='text-align:left; padding:10px 8px;'>Spread</th>
            <th style='text-align:right; padding:10px 8px;'>Actual</th>
            <th style='text-align:right; padding:10px 8px;'>Media</th>
            <th style='text-align:right; padding:10px 8px;'>P10</th>
            <th style='text-align:right; padding:10px 8px;'>P90</th>
            <th style='text-align:right; padding:10px 8px;'>Pct 3Y</th>
            <th style='text-align:right; padding:10px 8px;'>Z-score</th>
            <th style='text-align:center; padding:10px 8px;'>Señal</th>
        </tr>
    </thead>
    <tbody>
    """
    for _, row in df_sorted.iterrows():
        current_color = "#10b981" if row["Actual"] >= 0 else "#ef4444"
        pct = row.get("Pct rolling")
        pct_color = _pct_color(pct)
        z = row.get("Z-score")
        z_color = _zscore_color(z)

        signal_html = signal_badge(row["Señal"])

        pct_str = (f"{pct:.0f}%"
                   if pct is not None and not pd.isna(pct) else "—")
        z_str = (f"{z:+.2f}σ"
                 if z is not None and not pd.isna(z) else "—")

        html += f"""
        <tr style='border-bottom:1px solid #1e293b; color:#e2e8f0;'>
            <td style='padding:10px 8px; color:#3b82f6; font-weight:700;'>{row['Spread']}</td>
            <td style='padding:10px 8px; text-align:right; color:{current_color}; font-weight:600;'>{row['Actual']:+.3f}</td>
            <td style='padding:10px 8px; text-align:right; color:#94a3b8;'>{row['Media']:+.3f}</td>
            <td style='padding:10px 8px; text-align:right; color:#6ee7b7;'>{row['P10']:+.3f}</td>
            <td style='padding:10px 8px; text-align:right; color:#fca5a5;'>{row['P90']:+.3f}</td>
            <td style='padding:10px 8px; text-align:right; color:{pct_color}; font-weight:600;'>{pct_str}</td>
            <td style='padding:10px 8px; text-align:right; color:{z_color};'>{z_str}</td>
            <td style='padding:10px 8px; text-align:center;'>{signal_html}</td>
        </tr>
        """

    html += "</tbody></table></div>"
    st.markdown(html, unsafe_allow_html=True)


# ═══════════════════════════════════════════════════════════════════════════
# SECCIÓN: ACTUALIZACIÓN
# ═══════════════════════════════════════════════════════════════════════════
def page_update():
    st.markdown("""
    <div class="header-bar">
        <span class="header-title">
            <span class="header-accent">DATA</span> UPDATE
        </span>
        <span class="header-badge">PIPELINE</span>
    </div>
    """, unsafe_allow_html=True)

    info = load_contracts_info()
    last_spot_date = get_last_date_spot()
    last_update = get_last_update()

    cols = st.columns(3)
    cols[0].markdown(
        metric_card("ÚLTIMA FECHA SPOT",
                    last_spot_date.isoformat() if last_spot_date else "—",
                    color=COLORS["orange"]),
        unsafe_allow_html=True,
    )
    cols[1].markdown(
        metric_card("CONTRATOS EN BD",
                    str(len(info)) if not info.empty else "0",
                    color=COLORS["accent"]),
        unsafe_allow_html=True,
    )
    cols[2].markdown(
        metric_card("ÚLTIMA EJECUCIÓN OK",
                    last_update["run_at"][:19] if last_update else "—",
                    color=COLORS["green"]),
        unsafe_allow_html=True,
    )

    st.markdown("<br>", unsafe_allow_html=True)
    st.markdown("### ⚡ Ejecutar actualización")

    col1, col2 = st.columns(2)
    with col1:
        full = st.checkbox("Histórico completo (lento, primera vez)",
                           value=(info.empty if info is not None else True))
    with col2:
        only = st.radio("Alcance",
                        ["Todo", "Sólo spot", "Sólo futuros"],
                        horizontal=True)

    if info is not None and not info.empty:
        last_log = get_update_log(1)
        if not last_log.empty and last_log.iloc[0].get("error"):
            st.info(
                "💡 **Para reintentar contratos fallidos**: desmarca "
                "'Histórico completo' y pulsa Ejecutar. Sólo se "
                "descargarán los contratos que faltan. El sistema prueba "
                "±7 días alrededor de la expiración teórica."
            )

    if st.button("▶ Ejecutar actualización", type="primary"):
        do_spot = only in ("Todo", "Sólo spot")
        do_fut = only in ("Todo", "Sólo futuros")
        progress = st.progress(0.0)
        status = st.empty()

        def _cb(i, total, label):
            if total > 0:
                progress.progress(min(i / total, 1.0))
            status.text(f"Procesando: {label} ({i}/{total})")

        with st.spinner("Descargando datos..."):
            result = update_historical_data(
                full_history=full,
                spot=do_spot,
                futures=do_fut,
                rebuild_continuous=True,
                progress_callback=_cb,
            )
        progress.progress(1.0)
        status.empty()

        invalidate_all_caches()

        n_failed = len(result.contracts_failed)
        n_ok = result.contracts_updated

        if do_fut and n_failed > 0 and n_ok == 0:
            st.error(
                "⚠️ **CBOE bloquea las peticiones desde este servidor.** "
                "Todos los contratos fallaron."
            )
        elif do_fut and n_failed > n_ok and n_ok > 0:
            st.warning(
                f"⚠️ Descarga parcial: {n_ok} OK, {n_failed} fallidos. "
                "Reintenta en unos minutos."
            )
        else:
            st.success(f"✅ Actualización completada en "
                       f"{result.duration_seconds:.1f}s — "
                       f"{n_ok} OK, {n_failed} fallidos.")
        with st.expander("📋 Ver detalle"):
            st.json(result.to_dict())

    st.markdown("<br>", unsafe_allow_html=True)
    st.markdown("### 📜 Log reciente")
    log_df = get_update_log(limit=15)
    if log_df.empty:
        st.caption("Sin entradas.")
    else:
        st.dataframe(log_df, use_container_width=True, hide_index=True)

    st.markdown("### 📦 Inventario de contratos")
    if info.empty:
        st.caption("Sin contratos registrados.")
    else:
        st.dataframe(info, use_container_width=True, hide_index=True)


# ═══════════════════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════════════════
def main():
    section = render_sidebar()
    if section.startswith("📊"):
        page_resumen()
    elif section.startswith("🎯"):
        page_vix_central()
    elif section.startswith("⚡"):
        page_spread_analyzer()
    elif section.startswith("🔄"):
        page_update()


if __name__ == "__main__":
    main()
