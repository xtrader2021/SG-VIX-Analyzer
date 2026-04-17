"""
Script de diagnóstico: verifica que las fuentes de datos son accesibles
desde tu red ANTES de lanzar una carga completa.

Ejecuta:
    python diagnose.py

Prueba:
  1. Conexión básica a internet
  2. Yahoo Finance (spot VIX)
  3. CBOE (un contrato expirado reciente)
  4. Stooq (fallback)

Si CBOE devuelve 403 sistemáticamente desde tu entorno, el mensaje final
te orientará sobre qué hacer.
"""
from __future__ import annotations

import datetime as dt
import sys

import requests


def test(name: str, fn) -> bool:
    """Ejecuta un test, imprime OK/FAIL y devuelve bool."""
    print(f"  [{name}] ", end="", flush=True)
    try:
        result = fn()
        if result:
            print(f"✅ OK  {result}")
            return True
        else:
            print("❌ FAIL (sin datos)")
            return False
    except Exception as e:
        print(f"❌ FAIL: {e}")
        return False


def test_internet():
    r = requests.get("https://httpbin.org/get", timeout=10)
    return f"status={r.status_code}"


def test_yahoo():
    import yfinance as yf
    df = yf.download("^VIX", period="5d", progress=False, auto_adjust=False)
    return f"{len(df)} filas descargadas"


def test_cboe():
    from data_loader import CBOEFuturesSource
    from utils import make_contract
    # Contrato expirado de 2024 — datos estables
    c = make_contract(2024, 6)
    src = CBOEFuturesSource()
    df = src.fetch_contract(c)
    if df.empty:
        return None
    return f"{len(df)} filas de {c.code}"


def test_stooq():
    from data_loader import StooqContinuousSource
    src = StooqContinuousSource()
    df = src.fetch_continuous(month=1)
    if df.empty:
        return None
    return f"{len(df)} filas de VX1 continuo"


def main():
    print("=" * 60)
    print("  DIAGNÓSTICO DE CONECTIVIDAD — VIX Term Structure")
    print("=" * 60)
    print()

    results = {
        "Internet": test("Conexión internet", test_internet),
        "Yahoo": test("Yahoo Finance (VIX spot)", test_yahoo),
        "CBOE": test("CBOE (futuros VX)", test_cboe),
        "Stooq": test("Stooq (fallback continuas)", test_stooq),
    }

    print()
    print("=" * 60)
    print("  RESUMEN")
    print("=" * 60)

    if results["Internet"] and results["Yahoo"] and results["CBOE"]:
        print("✅ Todo OK. Puedes lanzar: python updater.py --full")
    elif results["Internet"] and results["Yahoo"] and not results["CBOE"]:
        print("⚠️  CBOE bloqueado (403) desde este entorno.")
        print()
        print("Posibles causas y soluciones:")
        print()
        print("1. Estás detrás de un proxy corporativo / VPN que bloquea CBOE.")
        print("   → Desactiva la VPN y vuelve a probar.")
        print()
        print("2. Tu IP ha sido rate-limited tras muchas peticiones seguidas.")
        print("   → Espera 15-30 min y reintenta. El updater ya tiene pausas.")
        print()
        print("3. Estás ejecutando en un servidor cloud con IP bloqueada por "
              "el WAF de CBOE (ocurre en Streamlit Cloud, algunas regiones "
              "de AWS/GCP).")
        print("   → Opción A: ejecuta el updater en local, sube el parquet/db")
        print("      al repo Git (o usa rsync/scp) y la app cloud los leerá.")
        print("   → Opción B: considera contratar Databento o usar Stooq para")
        print("      series continuas M1/M2 (menos granular pero accesible).")
        print()
        if results["Stooq"]:
            print("✅ Stooq SÍ funciona: puedes al menos tener M1/M2 continuos.")
    elif not results["Internet"]:
        print("❌ Sin acceso a internet. Revisa tu conexión.")
    else:
        print("⚠️  Fallos parciales. Revisa los mensajes de arriba.")


if __name__ == "__main__":
    sys.path.insert(0, ".")
    main()
