"""
VIX Term Structure Analytics — Streamlit app.

Punto de entrada. Sólo contiene UI y orquestación. No hace cálculos
(viven en analytics.py) ni I/O directo (vive en storage.py / updater.py).

Ejecutar:
    streamlit run app.py
"""
from __future__ import annotations

import datetime as dt
import logging

import pandas as pd
import streamlit as st

import analytics
import charts
import seasonality
from config import (
    APP_TITLE,
    PERCENTILE_CHEAP_THRESHOLD,
    PERCENTILE_EXPENSIVE_THRESHOLD,
    ROLLING_PERCENTILE_WINDOW_DAYS,
    ROLLING_ZSCORE_WINDOW_DAYS,
)
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
from utils import format_number, format_pct

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)

# -----------------------------------------------------------------------------
# CONFIG PÁGINA
# -----------------------------------------------------------------------------
st.set_page_config(
    page_title=APP_TITLE,
    page_icon="📈",
    layout="wide",
    initial_sidebar_state="expanded",
)

# Inicializar BD al arrancar
init_db()


# -----------------------------------------------------------------------------
# CACHE DE DATOS
# -----------------------------------------------------------------------------
# TTL de 1 hora: en modo dashboard es suficiente. Al ejecutar update manual
# invalidamos explícitamente.
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


# -----------------------------------------------------------------------------
# SIDEBAR
# -----------------------------------------------------------------------------
def render_sidebar() -> str:
    st.sidebar.title("📈 " + APP_TITLE)
    st.sidebar.markdown("---")

    section = st.sidebar.radio(
        "Navegación",
        [
            "Resumen",
            "Curva actual",
            "Curvas históricas",
            "Spreads",
            "Contango/Backwardation",
            "Valoración relativa",
            "Estacionalidad",
            "Actualización de datos",
        ],
    )

    st.sidebar.markdown("---")
    last = get_last_date_spot()
    info = load_contracts_info()
    n_active = int(info["is_active"].sum()) if not info.empty else 0
    n_total = len(info) if not info.empty else 0

    st.sidebar.metric("Última fecha spot",
                      last.isoformat() if last else "—")
    st.sidebar.metric("Contratos guardados", n_total)
    st.sidebar.metric("Contratos activos", n_active)

    last_update = get_last_update()
    if last_update:
        st.sidebar.caption(
            f"Última actualización exitosa: {last_update['run_at'][:19]}"
        )

    return section


# -----------------------------------------------------------------------------
# SECCIÓN: RESUMEN
# -----------------------------------------------------------------------------
def page_resumen():
    st.title("Resumen")

    df_spot = load_spot()
    df_cont = load_continuous()
    df_long = load_futures_long()

    if df_spot.empty and df_cont.empty:
        st.warning("No hay datos locales. Ve a **Actualización de datos** y "
                   "ejecuta una primera descarga.")
        return

    # KPIs principales
    col1, col2, col3, col4 = st.columns(4)

    if not df_spot.empty and "vix" in df_spot.columns:
        last_spot = df_spot["vix"].dropna().iloc[-1]
        prev_spot = df_spot["vix"].dropna().iloc[-2] if len(df_spot["vix"].dropna()) > 1 else last_spot
        col1.metric("VIX spot", f"{last_spot:.2f}",
                    delta=f"{(last_spot - prev_spot):+.2f}")

    if not df_cont.empty:
        last = df_cont.iloc[-1]
        if pd.notna(last.get("m1")):
            col2.metric("M1", f"{last['m1']:.2f}")
        if pd.notna(last.get("m2")):
            col3.metric("M2", f"{last['m2']:.2f}")
        if pd.notna(last.get("m1")) and pd.notna(last.get("m2")) and last["m1"] > 0:
            ratio = last["m2"] / last["m1"] - 1
            col4.metric("M2/M1 - 1", f"{ratio*100:+.2f}%")

    st.markdown("---")

    # Interpretación automática
    st.subheader("Interpretación cuantitativa")
    lines = analytics.auto_interpretation(df_spot, df_cont)
    for line in lines:
        st.markdown(f"- {line}")

    st.markdown("---")

    # Curva actual
    if not df_long.empty:
        st.subheader("Curva de futuros actual")
        curve = analytics.current_curve(df_long)
        spot_val = df_spot["vix"].dropna().iloc[-1] if not df_spot.empty else None
        fig = charts.plot_curve(curve, spot_value=spot_val)
        st.plotly_chart(fig, use_container_width=True)

    # VIX spot plot
    if not df_spot.empty:
        st.subheader("VIX spot histórico")
        fig = charts.plot_vix_spot(df_spot.tail(252 * 2))
        st.plotly_chart(fig, use_container_width=True)


# -----------------------------------------------------------------------------
# SECCIÓN: CURVA ACTUAL
# -----------------------------------------------------------------------------
def page_curva_actual():
    st.title("Curva actual")
    df_long = load_futures_long()
    df_spot = load_spot()

    if df_long.empty:
        st.warning("Sin datos de futuros. Ejecuta una actualización primero.")
        return

    available_dates = sorted(df_long["date"].dt.date.unique(), reverse=True)
    selected = st.selectbox("Fecha de referencia",
                            options=available_dates[:500],
                            index=0)
    selected_ts = pd.Timestamp(selected)

    curve = analytics.current_curve(df_long, as_of=selected_ts)
    if curve.empty:
        st.error("No hay datos en esa fecha.")
        return

    spot_val = None
    if not df_spot.empty and "vix" in df_spot.columns:
        spot_series = df_spot["vix"].dropna()
        # Spot más cercano anterior o igual
        spot_at = spot_series[spot_series.index <= selected_ts]
        if not spot_at.empty:
            spot_val = spot_at.iloc[-1]

    col1, col2 = st.columns([3, 1])
    with col1:
        fig = charts.plot_curve(curve, spot_value=spot_val,
                                 title=f"Curva VX al {selected}")
        st.plotly_chart(fig, use_container_width=True)

    with col2:
        st.metric("Slope (pts/día)", f"{analytics.curve_slope(curve):.4f}")
        if spot_val is not None:
            st.metric("Spot", f"{spot_val:.2f}")
        st.metric("Nº contratos", len(curve))
        if len(curve) >= 2:
            ratio = curve["settle"].iloc[1] / curve["settle"].iloc[0] - 1
            st.metric("M2/M1 ratio", f"{ratio*100:+.2f}%")

    st.markdown("### Detalle de la curva")
    st.dataframe(
        curve.assign(
            date=lambda d: d["date"].dt.date,
            expiry_date=lambda d: d["expiry_date"].dt.date,
        ),
        use_container_width=True,
        hide_index=True,
    )


# -----------------------------------------------------------------------------
# SECCIÓN: CURVAS HISTÓRICAS
# -----------------------------------------------------------------------------
def page_curvas_historicas():
    st.title("Curvas históricas")
    df_long = load_futures_long()

    if df_long.empty:
        st.warning("Sin datos.")
        return

    available = sorted(df_long["date"].dt.date.unique(), reverse=True)

    st.markdown("Selecciona hasta 6 fechas para comparar curvas:")
    default_picks = available[:3] if len(available) >= 3 else available
    selected_dates = st.multiselect(
        "Fechas", options=available[:1000],
        default=default_picks, max_selections=6,
    )

    if not selected_dates:
        st.info("Selecciona al menos una fecha.")
        return

    curves = {}
    for d in selected_dates:
        curve = analytics.curve_on_date(df_long, pd.Timestamp(d))
        if not curve.empty:
            curves[d.isoformat()] = curve

    fig = charts.plot_historical_curves(curves)
    st.plotly_chart(fig, use_container_width=True)

    st.markdown("### Slope por fecha seleccionada")
    slopes = {d: analytics.curve_slope(c) for d, c in curves.items()}
    st.dataframe(
        pd.DataFrame(
            {"Fecha": list(slopes.keys()),
             "Slope (pts/día)": [f"{v:.4f}" for v in slopes.values()]}
        ),
        use_container_width=True, hide_index=True,
    )


# -----------------------------------------------------------------------------
# SECCIÓN: SPREADS
# -----------------------------------------------------------------------------
def page_spreads():
    st.title("Spreads entre vencimientos continuos")
    df_cont = load_continuous()

    if df_cont.empty:
        st.warning("Sin series continuas. Ejecuta actualización.")
        return

    pairs = [("m1", "m2"), ("m2", "m3"), ("m3", "m4"),
             ("m4", "m5"), ("m5", "m6"), ("m6", "m7"),
             ("m1", "m4"), ("m4", "m7")]
    pair_labels = [f"{far.upper()} - {near.upper()}" for near, far in pairs]

    col1, col2 = st.columns([1, 3])
    with col1:
        idx = st.selectbox("Par", options=range(len(pairs)),
                           format_func=lambda i: pair_labels[i])
        near, far = pairs[idx]
        mode = st.radio("Modo", ["Absoluto", "Ratio"])

    if mode == "Absoluto":
        series = analytics.compute_spread(df_cont, near, far)
        label = f"{far.upper()} - {near.upper()}"
    else:
        series = analytics.spread_ratio(df_cont, near, far)
        label = f"{far.upper()}/{near.upper()} - 1"

    if series.empty:
        st.error("Sin datos para este par.")
        return

    stats = analytics.compute_stats_bundle(series)
    current = series.iloc[-1]

    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Actual", format_number(current, 3))
    if not stats.empty:
        col2.metric("Percentil rolling",
                    format_number(stats["pct_rolling"].iloc[-1], 1))
        col3.metric("Percentil full",
                    format_number(stats["pct_full"].iloc[-1], 1))
        col4.metric("Z-score",
                    format_number(stats["zscore"].iloc[-1], 2))

    # Clasificación
    st.markdown(
        f"**Clasificación por percentil rolling:** "
        f"{analytics.classify_by_percentile(stats['pct_rolling'].iloc[-1])}  \n"
        f"**Clasificación por z-score:** "
        f"{analytics.classify_by_zscore(stats['zscore'].iloc[-1])}"
    )

    # Histórico con bandas
    bands = {
        "P10": series.quantile(0.10),
        "P50": series.median(),
        "P90": series.quantile(0.90),
    }
    fig1 = charts.plot_spread_history(series, title=f"Histórico: {label}",
                                       percentile_bands=bands)
    st.plotly_chart(fig1, use_container_width=True)

    # Distribución
    fig2 = charts.plot_spread_distribution(
        series, current_value=current,
        title=f"Distribución: {label}",
    )
    st.plotly_chart(fig2, use_container_width=True)

    st.caption(
        f"Ventanas: percentil rolling={ROLLING_PERCENTILE_WINDOW_DAYS} días, "
        f"z-score={ROLLING_ZSCORE_WINDOW_DAYS} días."
    )


# -----------------------------------------------------------------------------
# SECCIÓN: CONTANGO
# -----------------------------------------------------------------------------
def page_contango():
    st.title("Contango / Backwardation")
    df_spot = load_spot()
    df_cont = load_continuous()

    if df_cont.empty:
        st.warning("Sin datos de series continuas.")
        return

    df_metrics = analytics.contango_metrics(df_spot, df_cont)
    if df_metrics.empty:
        st.error("No se pudo calcular métricas.")
        return

    last = df_metrics.iloc[-1]
    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Spot vs M1",
                format_pct(last.get("spot_vs_m1_ratio")) if pd.notna(last.get("spot_vs_m1_ratio")) else "N/A")
    col2.metric("M1 vs M2",
                format_pct(last.get("m1_vs_m2_ratio")))
    col3.metric("M2 vs M3",
                format_pct(last.get("m2_vs_m3_ratio")))
    if "m4_vs_m7_ratio" in df_metrics.columns:
        col4.metric("M4 vs M7",
                    format_pct(last.get("m4_vs_m7_ratio")))

    st.markdown(
        f"**Estado actual (M1→M2):** "
        f"{analytics.classify_contango(last.get('m1_vs_m2_ratio'))}"
    )

    # Serie temporal múltiple
    fig = charts.plot_contango_metrics(df_metrics)
    st.plotly_chart(fig, use_container_width=True)

    # Área coloreada M1-M2
    st.markdown("### Zona contango / backwardation (M2/M1)")
    fig2 = charts.plot_contango_colored_area(
        df_metrics["m1_vs_m2_ratio"],
        title="Ratio M2/M1 - 1 (verde=contango, rojo=backwardation)",
    )
    st.plotly_chart(fig2, use_container_width=True)

    # Percentil de la métrica M1/M2
    pct_series = analytics.rolling_percentile(df_metrics["m1_vs_m2_ratio"])
    if not pct_series.dropna().empty:
        st.metric("Percentil rolling M2/M1",
                  f"{pct_series.dropna().iloc[-1]:.1f}")


# -----------------------------------------------------------------------------
# SECCIÓN: VALORACIÓN RELATIVA
# -----------------------------------------------------------------------------
def page_valoracion():
    st.title("Valoración relativa futuro vs spot")
    df_spot = load_spot()
    df_cont = load_continuous()

    if df_spot.empty or df_cont.empty:
        st.warning("Faltan datos de spot o continuas.")
        return

    month = st.selectbox("Mes continuo", options=[1, 2, 3, 4, 5, 6, 7, 8],
                         index=0, format_func=lambda m: f"M{m}")
    df = analytics.premium_spot_vs_future(df_spot, df_cont, month=month)
    if df.empty:
        st.error("Sin datos suficientes.")
        return

    last = df.iloc[-1]
    col1, col2, col3 = st.columns(3)
    col1.metric("Spot", f"{last['spot']:.2f}")
    col2.metric(f"M{month}", f"{last['future']:.2f}")
    col3.metric(f"M{month} - Spot", f"{last['diff']:+.2f}")

    col4, col5, col6 = st.columns(3)
    col4.metric("Ratio M/Spot - 1", f"{last['ratio']*100:+.2f}%")
    col5.metric("Percentil rolling",
                format_number(last.get("diff_pct_rolling"), 1))
    col6.metric("Z-score", format_number(last.get("diff_zscore"), 2))

    st.markdown(
        f"**Clasificación (percentil rolling):** "
        f"{analytics.classify_by_percentile(last.get('diff_pct_rolling'))}  \n"
        f"**Clasificación (z-score):** "
        f"{analytics.classify_by_zscore(last.get('diff_zscore'))}"
    )

    st.caption(
        f"Un percentil > {PERCENTILE_EXPENSIVE_THRESHOLD} indica que la prima "
        f"está en la parte alta de su distribución histórica; < "
        f"{PERCENTILE_CHEAP_THRESHOLD}, en la parte baja."
    )

    fig = charts.plot_premium_over_time(
        df, title=f"Prima M{month} sobre spot — evolución"
    )
    st.plotly_chart(fig, use_container_width=True)


# -----------------------------------------------------------------------------
# SECCIÓN: ESTACIONALIDAD
# -----------------------------------------------------------------------------
def page_estacionalidad():
    st.title("Estacionalidad")
    df_spot = load_spot()
    df_cont = load_continuous()

    col1, col2, col3 = st.columns(3)
    with col1:
        source = st.selectbox("Serie", ["VIX spot", "Spread M1-M2"])
    with col2:
        granularity = st.selectbox(
            "Granularidad",
            ["Mes del año", "Semana del año", "Día del año"],
        )
    with col3:
        use_returns = st.checkbox("Usar returns (recomendado)", value=True)

    if source == "VIX spot":
        if df_spot.empty:
            st.warning("Sin spot.")
            return
        series = df_spot["vix"].dropna()
    else:
        if df_cont.empty:
            st.warning("Sin continuas.")
            return
        series = (df_cont["m2"] - df_cont["m1"]).dropna()

    if granularity == "Mes del año":
        agg = seasonality.seasonality_by_month(series, use_returns=use_returns)
        overlay = seasonality.current_year_overlay(
            series, "month", use_returns=use_returns)
        x_col, x_label = "month", "Mes"
    elif granularity == "Semana del año":
        agg = seasonality.seasonality_by_week(series, use_returns=use_returns)
        overlay = seasonality.current_year_overlay(
            series, "week", use_returns=use_returns)
        x_col, x_label = "week", "Semana ISO"
    else:
        agg = seasonality.seasonality_by_dayofyear(
            series, use_returns=use_returns)
        overlay = seasonality.current_year_overlay(
            series, "doy", use_returns=use_returns)
        x_col, x_label = "doy", "Día del año"

    if agg.empty:
        st.error("Sin datos suficientes.")
        return

    fig = charts.plot_seasonality(
        agg, overlay, x_col=x_col, x_label=x_label,
        title=f"Estacionalidad — {source} ({granularity.lower()})",
    )
    st.plotly_chart(fig, use_container_width=True)

    st.markdown("### Detalle agregado")
    display_df = agg.copy()
    for col in ["mean", "median", "p25", "p75"]:
        if col in display_df.columns:
            display_df[col] = (display_df[col] * 100).round(3)
    st.dataframe(display_df, use_container_width=True, hide_index=True)


# -----------------------------------------------------------------------------
# SECCIÓN: ACTUALIZACIÓN DE DATOS
# -----------------------------------------------------------------------------
def page_update():
    st.title("Actualización de datos")

    info = load_contracts_info()
    last_spot_date = get_last_date_spot()
    last_update = get_last_update()

    col1, col2, col3 = st.columns(3)
    col1.metric("Última fecha spot",
                last_spot_date.isoformat() if last_spot_date else "—")
    col2.metric("Contratos en BD", len(info) if not info.empty else 0)
    col3.metric("Última ejecución OK",
                last_update["run_at"][:19] if last_update else "—")

    st.markdown("---")
    st.subheader("Ejecutar actualización")

    col1, col2 = st.columns(2)
    with col1:
        full = st.checkbox("Histórico completo (lento, primera vez)",
                           value=(info.empty if info is not None else True))
    with col2:
        only = st.radio("Alcance",
                        ["Todo", "Sólo spot", "Sólo futuros"],
                        horizontal=True)

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

        # Diagnóstico inteligente del resultado
        n_failed = len(result.contracts_failed)
        n_ok = result.contracts_updated

        if do_fut and n_failed > 0 and n_ok == 0:
            st.error(
                "⚠️ **CBOE bloquea las peticiones desde este servidor.** "
                "Todos los contratos de futuros fallaron (probablemente "
                "con 403 Forbidden). Esto es típico cuando Streamlit Cloud "
                "corre en IPs de datacenter que el WAF de CBOE rechaza."
            )
            st.info(
                "**Qué puedes hacer ahora:**\n\n"
                "1. **El spot VIX sí se ha descargado** (Yahoo Finance "
                "no bloquea). Puedes usar la sección 'Resumen' y ver la "
                "serie spot.\n"
                "2. Para tener datos de futuros, usa **GitHub Codespaces** "
                "para ejecutar `python updater.py --full` y luego commit "
                "de los datos al repo (Estrategia B del README).\n"
                "3. Alternativamente, modifica `updater.py` para usar "
                "Stooq como fuente primaria (Estrategia C)."
            )
        elif do_fut and n_failed > n_ok and n_ok > 0:
            st.warning(
                f"⚠️ Descarga parcial: {n_ok} contratos OK, "
                f"{n_failed} fallidos. Puede ser rate-limit temporal. "
                "Vuelve a ejecutar en unos minutos para reintentar "
                "los que faltan."
            )
        else:
            st.success(f"Actualización completada en "
                       f"{result.duration_seconds:.1f}s — "
                       f"{n_ok} contratos OK, {n_failed} fallidos.")

        st.json(result.to_dict())

    st.markdown("---")
    st.subheader("Log reciente")
    log_df = get_update_log(limit=20)
    if log_df.empty:
        st.caption("Sin entradas en el log.")
    else:
        st.dataframe(log_df, use_container_width=True, hide_index=True)

    st.markdown("---")
    st.subheader("Inventario de contratos")
    if info.empty:
        st.caption("Sin contratos registrados.")
    else:
        st.dataframe(info, use_container_width=True, hide_index=True)


# -----------------------------------------------------------------------------
# MAIN
# -----------------------------------------------------------------------------
def main():
    section = render_sidebar()
    if section == "Resumen":
        page_resumen()
    elif section == "Curva actual":
        page_curva_actual()
    elif section == "Curvas históricas":
        page_curvas_historicas()
    elif section == "Spreads":
        page_spreads()
    elif section == "Contango/Backwardation":
        page_contango()
    elif section == "Valoración relativa":
        page_valoracion()
    elif section == "Estacionalidad":
        page_estacionalidad()
    elif section == "Actualización de datos":
        page_update()


if __name__ == "__main__":
    main()
