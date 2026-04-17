"""
Configuration centralizada para la app VIX Term Structure.

Todos los parámetros modificables sin tocar lógica viven aquí: rutas,
umbrales de clasificación, ventanas estadísticas, tickers, fechas de
arranque del histórico, etc.
"""
from pathlib import Path

# -----------------------------------------------------------------------------
# RUTAS
# -----------------------------------------------------------------------------
BASE_DIR = Path(__file__).parent
DATA_DIR = BASE_DIR / "data"
SPOT_DIR = DATA_DIR / "spot"
FUTURES_DIR = DATA_DIR / "futures"
CONTINUOUS_DIR = DATA_DIR / "continuous"
DB_PATH = BASE_DIR / "vix_data.db"

# Crear directorios si no existen (idempotente)
for _d in (DATA_DIR, SPOT_DIR, FUTURES_DIR, CONTINUOUS_DIR):
    _d.mkdir(parents=True, exist_ok=True)

# -----------------------------------------------------------------------------
# FECHAS
# -----------------------------------------------------------------------------
# Histórico desde 2013: estructura de contrato estable, datos limpios
HISTORY_START_DATE = "2013-01-01"

# Margen de días hacia atrás en cada update incremental para cubrir
# reprocesos/republicaciones de CBOE
UPDATE_LOOKBACK_DAYS = 5

# -----------------------------------------------------------------------------
# TICKERS YAHOO (spot e índices sintéticos)
# -----------------------------------------------------------------------------
YAHOO_TICKERS = {
    "vix": "^VIX",
    "vix9d": "^VIX9D",
    "vix3m": "^VIX3M",
    "vix6m": "^VIX6M",
}

# -----------------------------------------------------------------------------
# CBOE — códigos de mes de futuros
# -----------------------------------------------------------------------------
# Código de futuros (estándar CME/CBOE): letra → mes
FUTURES_MONTH_CODES = {
    1: "F", 2: "G", 3: "H", 4: "J", 5: "K", 6: "M",
    7: "N", 8: "Q", 9: "U", 10: "V", 11: "X", 12: "Z",
}
MONTH_CODE_TO_NUM = {v: k for k, v in FUTURES_MONTH_CODES.items()}

# Patrón de URL para CSV histórico de un contrato VX individual.
# {month_name} = nombre del mes en inglés, {year} = año completo (YYYY)
# Ej: https://cdn.cboe.com/resources/futures/archive/volume-and-price/CFE_F13_VX.csv
# pero el patrón moderno es por archive con month+year en sufijo corto:
# Ej: https://cdn.cboe.com/data/us/futures/market_statistics/historical_data/products/csv/VX/2024/CFE_F24_VX.csv
# CBOE cambia con frecuencia la ruta. Usamos una lista de patrones candidatos
# y el data_loader prueba cada uno hasta que uno funcione.
CBOE_URL_PATTERNS = [
    # Patrón moderno (2023+): fecha de expiración en YYYY-MM-DD
    "https://cdn.cboe.com/data/us/futures/market_statistics/historical_data/VX/VX_{expiry}.csv",
    # Patrón intermedio por año
    "https://cdn.cboe.com/data/us/futures/market_statistics/historical_data/products/csv/VX/{year}/CFE_{month_code}{yy}_VX.csv",
    # Patrón legacy (archivo)
    "https://cdn.cboe.com/resources/futures/archive/volume-and-price/CFE_{month_code}{yy}_VX.csv",
    # Dominio markets
    "https://markets.cboe.com/us/futures/market_statistics/historical_data/products/csv/VX/{year}/CFE_{month_code}{yy}_VX.csv",
]

# -----------------------------------------------------------------------------
# ANÁLISIS — ventanas y umbrales
# -----------------------------------------------------------------------------
# Ventana rolling para percentiles (default UI)
ROLLING_PERCENTILE_WINDOW_DAYS = 756  # ~3 años hábiles

# Ventana rolling para z-score
ROLLING_ZSCORE_WINDOW_DAYS = 252  # ~1 año hábil

# Umbrales de clasificación por percentil (0-100)
PERCENTILE_CHEAP_THRESHOLD = 20
PERCENTILE_EXPENSIVE_THRESHOLD = 80

# Umbrales de clasificación por z-score
ZSCORE_CHEAP_THRESHOLD = -1.5
ZSCORE_EXPENSIVE_THRESHOLD = 1.5

# Número máximo de meses continuos a reconstruir (M1..M8)
MAX_CONTINUOUS_MONTHS = 8

# -----------------------------------------------------------------------------
# UI
# -----------------------------------------------------------------------------
APP_TITLE = "VIX Term Structure Analytics"
DEFAULT_PLOT_HEIGHT = 500
