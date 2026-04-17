"""
Smoke test: verifica que todo el pipeline analítico funciona con datos
sintéticos realistas (sin tocar internet).
"""
import sys
import datetime as dt
import numpy as np
import pandas as pd

sys.path.insert(0, ".")

import analytics
import seasonality
import storage
import utils
import charts

# -----------------------------------------------------------------------------
# 1. Probar cálculo de expiraciones
# -----------------------------------------------------------------------------
print("1. Expiraciones VX...")
c = utils.make_contract(2025, 1)
assert c.code == "VX_F25", f"code={c.code}"
print(f"   VX F25 → expira {c.expiry}")
# Tercer viernes enero 2025 = 17 enero. 30 días antes = 18 dic 2024 (miércoles).
# Pero el mes SIGUIENTE al mes del contrato → enero → febrero
c_feb = utils.make_contract(2025, 2)
print(f"   VX G25 → expira {c_feb.expiry}")

# -----------------------------------------------------------------------------
# 2. Simular datos spot y continuos realistas
# -----------------------------------------------------------------------------
print("\n2. Generando datos sintéticos...")
np.random.seed(42)
dates = pd.bdate_range("2018-01-01", "2026-04-15")
n = len(dates)

# VIX spot realista: mean-reverting con shocks ocasionales
vix = np.zeros(n)
vix[0] = 15
for i in range(1, n):
    vix[i] = max(9, vix[i-1] + 0.1 * (16 - vix[i-1]) + np.random.randn() * 0.8)
# Añadir spike tipo COVID
spike_idx = len(dates) // 2
vix[spike_idx:spike_idx+20] += np.linspace(30, 0, 20)

df_spot = pd.DataFrame({
    "vix": vix,
    "vix9d": vix * (1 + np.random.randn(n) * 0.02),
    "vix3m": vix * (1 + 0.05 + np.random.randn(n) * 0.01),
    "vix6m": vix * (1 + 0.08 + np.random.randn(n) * 0.01),
}, index=dates)
df_spot.index.name = "date"

# Continuas M1..M8 (M_i = spot * (1 + 0.03*i + ruido))
df_cont = pd.DataFrame({
    f"m{i}": vix * (1 + 0.03 * i + np.random.randn(n) * 0.015)
    for i in range(1, 9)
}, index=dates)
df_cont.index.name = "date"

print(f"   df_spot: {df_spot.shape}, rango {df_spot.index.min().date()} → {df_spot.index.max().date()}")
print(f"   df_cont: {df_cont.shape}")

# -----------------------------------------------------------------------------
# 3. Probar cálculos analíticos
# -----------------------------------------------------------------------------
print("\n3. Probando analytics...")

# Spread
spread = analytics.compute_spread(df_cont, "m1", "m2")
print(f"   Spread M2-M1: len={len(spread)}, last={spread.iloc[-1]:.3f}")
assert not spread.empty

# Stats bundle
stats = analytics.compute_stats_bundle(spread)
print(f"   Stats: pct_rolling last={stats['pct_rolling'].iloc[-1]:.1f}, "
      f"zscore last={stats['zscore'].iloc[-1]:.2f}")
assert not stats.empty

# Contango metrics
metrics = analytics.contango_metrics(df_spot, df_cont)
print(f"   Contango metrics: shape={metrics.shape}")
print(f"   Last M1/M2 ratio: {metrics['m1_vs_m2_ratio'].iloc[-1]*100:+.2f}%")
assert "m1_vs_m2_ratio" in metrics.columns
assert "slope_m1_m4" in metrics.columns

# Prima
prem = analytics.premium_spot_vs_future(df_spot, df_cont, month=1)
print(f"   Premium M1-spot: last diff={prem['diff'].iloc[-1]:.2f}")
assert not prem.empty

# Clasificación
cls = analytics.classify_contango(metrics['m1_vs_m2_ratio'].iloc[-1])
print(f"   Clasificación: {cls}")

# Interpretación
lines = analytics.auto_interpretation(df_spot, df_cont)
print(f"   Interpretación: {len(lines)} líneas generadas")
for line in lines:
    print(f"      - {line}")

# -----------------------------------------------------------------------------
# 4. Probar curva con datos de futuros long
# -----------------------------------------------------------------------------
print("\n4. Construyendo df_futures_long sintético y curva...")
long_rows = []
as_of = dates[-1]
for i in range(1, 9):
    long_rows.append({
        "date": as_of,
        "contract_code": f"VX_M{i}",
        "expiry_date": as_of + pd.Timedelta(days=30 * i),
        "settle": df_cont.iloc[-1][f"m{i}"],
        "open": np.nan, "high": np.nan, "low": np.nan,
        "volume": 1000, "open_interest": 5000,
        "dte": 30 * i,
    })
df_long = pd.DataFrame(long_rows)
curve = analytics.current_curve(df_long)
print(f"   Curva: {len(curve)} puntos, slope={analytics.curve_slope(curve):.4f} pts/día")
assert len(curve) == 8

# -----------------------------------------------------------------------------
# 5. Probar estacionalidad
# -----------------------------------------------------------------------------
print("\n5. Probando seasonality...")
season = seasonality.seasonality_by_month(df_spot["vix"])
print(f"   Estacionalidad mensual: {len(season)} meses")
assert len(season) == 12

overlay = seasonality.current_year_overlay(df_spot["vix"], "month")
print(f"   Overlay año actual: {len(overlay)} filas")

# -----------------------------------------------------------------------------
# 6. Probar charts (que devuelvan Figure sin petar)
# -----------------------------------------------------------------------------
print("\n6. Probando charts...")
fig1 = charts.plot_curve(curve, spot_value=18.5)
fig2 = charts.plot_spread_history(spread, percentile_bands={"P10": 0.2, "P90": 2.5})
fig3 = charts.plot_spread_distribution(spread, current_value=spread.iloc[-1])
fig4 = charts.plot_contango_metrics(metrics)
fig5 = charts.plot_contango_colored_area(metrics["m1_vs_m2_ratio"])
fig6 = charts.plot_premium_over_time(prem)
fig7 = charts.plot_seasonality(season, overlay, "month", "Mes")
fig8 = charts.plot_vix_spot(df_spot)
print(f"   8 figuras Plotly generadas correctamente")

# -----------------------------------------------------------------------------
# 7. Probar storage (write/read round-trip)
# -----------------------------------------------------------------------------
print("\n7. Probando storage round-trip...")
storage.init_db()
# Recortar a algo pequeño para el test
small_spot = df_spot.tail(100)
storage.write_spot(small_spot)
read_back = storage.read_spot()
assert len(read_back) == len(small_spot)
print(f"   Spot write+read OK ({len(read_back)} filas)")

# Probar merge
new_data = df_spot.tail(5).copy()
new_data.iloc[-1] = new_data.iloc[-1] + 0.1  # modificar última
added = storage.merge_spot(new_data)
print(f"   Merge spot: filas netas añadidas = {added}")

# Log
storage.log_update("test", 100, "ok")
log = storage.get_update_log(5)
assert not log.empty
print(f"   Log update OK ({len(log)} entradas)")

print("\n✅ TODOS LOS TESTS PASADOS")
