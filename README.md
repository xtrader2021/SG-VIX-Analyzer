# VIX Term Structure Analytics

Aplicación en Streamlit para análisis de la estructura temporal del VIX:
spot, futuros por vencimiento, series continuas M1–M8, spreads,
contango/backwardation, valoración relativa y estacionalidad.

---

## Estructura del proyecto

```
vix-term-structure/
├── app.py              ← UI Streamlit, navegación entre secciones
├── config.py           ← rutas, tickers, umbrales, ventanas
├── utils.py            ← calendario expiraciones, códigos de contrato
├── storage.py          ← I/O de parquet + SQLite
├── data_loader.py      ← descarga desde Yahoo (spot) + CBOE + Stooq (fallback)
├── updater.py          ← actualización incremental, orquestador y CLI
├── analytics.py        ← curva, spreads, contango, percentiles, z-score
├── seasonality.py      ← estacionalidad por mes/semana/día
├── charts.py           ← funciones Plotly
├── diagnose.py         ← test de conectividad con las fuentes
├── smoke_test.py       ← test del pipeline con datos sintéticos
├── requirements.txt
├── .streamlit/config.toml
└── data/               ← generado al ejecutar el primer update
    ├── spot/
    ├── futures/
    └── continuous/
```

---

## Instalación local (paso a paso)

### 1. Requisitos previos

Necesitas **Python 3.10 o superior**. Compruébalo:

```bash
python --version
```

Si no tienes Python instalado o es antiguo, descárgalo desde
[python.org/downloads](https://www.python.org/downloads/).

### 2. Descarga el proyecto

Coloca la carpeta `vix-term-structure/` donde prefieras, por ejemplo
`C:\Users\tu_usuario\proyectos\` (Windows) o `~/proyectos/` (Mac/Linux).

Abre una terminal y entra en la carpeta:

```bash
cd ruta/a/vix-term-structure
```

### 3. Crea un entorno virtual (muy recomendado)

Un "entorno virtual" es una carpeta aislada donde se instalan las
librerías del proyecto sin ensuciar tu Python global.

```bash
python -m venv venv
```

Actívalo:

- **Windows**: `venv\Scripts\activate`
- **Mac/Linux**: `source venv/bin/activate`

Sabrás que está activo porque tu terminal mostrará `(venv)` al inicio.

### 4. Instala las dependencias

```bash
pip install -r requirements.txt
```

Esto tardará 1–2 minutos la primera vez.

### 5. Carga inicial de datos

Antes de abrir la app, conviene llenar la base de datos con el histórico
desde 2013. Esto puede tardar **15–35 minutos** la primera vez porque
hay que descargar ~150 contratos individuales de CBOE con pausas de
cortesía entre peticiones.

**Primero, prueba la conectividad** (muy recomendado):

```bash
python diagnose.py
```

Te dirá si Yahoo, CBOE y Stooq son accesibles desde tu red. Si los tres
están OK, adelante. Si CBOE devuelve 403, lee la sección **"Problema:
CBOE devuelve 403"** más abajo antes de seguir.

Una vez verificada la conectividad:

```bash
python updater.py --full
```

Verás una barra de progreso. Si alguna descarga falla individualmente
no pasa nada: se listan al final en `contracts_failed` y se reintentan
en la siguiente ejecución.

### 6. Lanza la app

```bash
streamlit run app.py
```

Se abrirá en tu navegador en `http://localhost:8501`.

### 7. Actualizaciones diarias posteriores

Una vez al día, cuando quieras tener datos frescos:

- **Desde la app**: pestaña "Actualización de datos" → botón "Ejecutar".
- **Desde terminal**: `python updater.py` (sin `--full`). Tardará segundos.

---

## Despliegue en la web (Streamlit Community Cloud)

Como es tu primer despliegue, los pasos detallados.

> ⚠️ **Lee primero esta advertencia crítica**: CBOE bloquea con 403 las
> peticiones que vienen de algunos rangos de IP de datacenter, incluyendo
> posiblemente las de Streamlit Cloud. Esto significa que **el
> updater puede NO funcionar desde la app desplegada**, aunque sí
> funcione perfectamente en tu ordenador local.
>
> La estrategia recomendada, que es la que tiene más probabilidad de
> funcionar sin fricciones, es:
>
> 1. Ejecutar el updater **en local** (tu ordenador).
> 2. Subir los parquet y la SQLite al repositorio de Git.
> 3. La app desplegada lee esos ficheros; el botón de update en la UI
>    queda más bien como "refresco desde local" que como pipeline cloud.
>
> Más abajo, en la sección **"Estrategia de datos para despliegue"**,
> explico exactamente cómo hacerlo.

### Paso 1. Crea una cuenta de GitHub

Si no la tienes, entra en [github.com](https://github.com) y regístrate.
Es gratis.

### Paso 2. Crea un repositorio nuevo

Una vez dentro de GitHub, pulsa el botón **"+"** arriba a la derecha →
**"New repository"**.

- **Repository name**: `vix-term-structure` (o el nombre que prefieras)
- Marca **Public** (Streamlit Community Cloud gratuito lo requiere).
- **NO** marques "Add a README" (ya lo tenemos).
- Pulsa **"Create repository"**.

### Paso 3. Sube tu código al repositorio

Desde tu terminal, en la carpeta del proyecto:

```bash
git init
git add .
git commit -m "Initial commit"
git branch -M main
git remote add origin https://github.com/TU_USUARIO/vix-term-structure.git
git push -u origin main
```

Sustituye `TU_USUARIO` por tu nombre de usuario de GitHub. Te pedirá
credenciales — si es la primera vez, GitHub te pedirá crear un
**Personal Access Token** en Settings → Developer settings → Tokens.

### Paso 4. Despliega en Streamlit Cloud

1. Ve a [share.streamlit.io](https://share.streamlit.io) y entra con
   tu cuenta de GitHub.
2. Pulsa **"New app"**.
3. Selecciona tu repositorio `vix-term-structure`.
4. Branch: `main`. Main file path: `app.py`.
5. Pulsa **"Deploy"**.

Tardará 2–3 minutos en instalar dependencias y arrancar.

### Paso 5. Estrategia de datos para despliegue (IMPORTANTE)

Como `.gitignore` excluye la carpeta `data/` y `vix_data.db`, al arrancar
la app en la nube estará vacía. Tienes tres estrategias según lo que
funcione en tu caso:

**Estrategia A — Update desde la app cloud** *(la más cómoda si funciona)*

Entra a la app desplegada → "Actualización de datos" → marca "Histórico
completo" → "Ejecutar". Si CBOE responde desde el datacenter de Streamlit
Cloud, se cargarán los datos. Prueba primero con esta opción — si falla
con muchos `contracts_failed`, cambia a la Estrategia B.

**Estrategia B — Datos precargados en el repo** *(la más fiable)*

1. Ejecuta en local `python updater.py --full`.
2. Edita `.gitignore` y **quita** las líneas `data/`, `*.parquet`, `*.db`.
3. `git add data/ vix_data.db && git commit -m "Add initial data" && git push`
4. La app cloud ya tendrá los datos al próximo deploy.
5. Para updates posteriores: ejecuta en local `python updater.py`, haz
   commit y push. La app se redeploya automáticamente con los datos
   nuevos.

El único inconveniente de esta estrategia es que el repo crece (los
parquet de VX rondan 10–50 KB por contrato, ~3–5 MB total; la SQLite
es ligera). GitHub soporta bien repos hasta 1 GB.

**Estrategia C — Sólo series continuas desde Stooq** *(plan B minimalista)*

Si CBOE no es accesible ni en local ni en cloud, Stooq publica series
VX1 y VX2 continuas sin bloqueo. El código ya incluye `StooqContinuousSource`
en `data_loader.py`. Tendrías que adaptar `updater.py` para usarla como
fuente primaria (sólo tendrás M1 y M2, no la curva completa ni históricos
por contrato). Es un plan B, no lo recomendado.

### Actualizaciones posteriores del código

Cada vez que hagas cambios en el código local:

```bash
git add .
git commit -m "Descripción del cambio"
git push
```

Streamlit Cloud detectará el push y redeployará automáticamente en
2–3 minutos.

---

## Sistema de actualización incremental

### Qué hace `update_historical_data()`

1. **Spot (Yahoo Finance)**
   - Lee la última fecha disponible localmente.
   - Descarga desde `(última_fecha − 5 días)` hasta hoy (el margen de
     5 días cubre republicaciones de datos).
   - Deduplica por fecha, guarda.

2. **Futuros (CBOE)**
   - Distingue entre contratos **expirados** (descarga histórico
     completo una vez) y **activos** (re-descarga incremental).
   - Sólo los ~8 contratos activos se re-descargan en cada ejecución
     diaria. Los históricos se cachean para siempre.
   - Valida schema, deduplica por fecha, guarda un parquet por contrato.

3. **Series continuas M1–M8**
   - Se reconstruyen enteramente en cada update (es barato: <1s).
   - Para cada fecha, ordena los contratos vivos por DTE ascendente
     y asigna M1 = front-month, M2 = siguiente, etc.

4. **Log de ejecución**
   - Cada ejecución deja una fila en `update_log` con run_at,
     rows_added, status y error.

### Tres formas de ejecutarlo

- **Al iniciar la app**: no fuerza update, sólo muestra la fecha del
  último dato. Se actualiza a petición.
- **Manualmente en la UI**: pestaña "Actualización de datos".
- **Por cron / script externo** (para automatizar):

  ```bash
  # En tu terminal, para probar:
  python updater.py

  # Ejemplo de crontab en Linux/Mac (todos los días 22:30):
  30 22 * * 1-5 cd /ruta/a/vix-term-structure && /ruta/a/venv/bin/python updater.py >> update.log 2>&1

  # En Windows, usar el Programador de tareas con:
  # Programa: C:\ruta\a\venv\Scripts\python.exe
  # Argumentos: updater.py
  # Iniciar en: C:\ruta\a\vix-term-structure
  ```

### Flags disponibles

```bash
python updater.py --help
python updater.py --full           # redescarga todo el histórico
python updater.py --spot-only      # sólo spot
python updater.py --futures-only   # sólo futuros
python updater.py --no-continuous  # no reconstruye M1..M8
python updater.py -v               # verbose, útil para depurar
```

---

---

## Troubleshooting

### Problema: CBOE devuelve 403 (Forbidden)

CBOE protege sus CDNs con un WAF que bloquea algunas IPs. Orden de
actuaciones si te pasa:

1. **Ejecuta `python diagnose.py`** para confirmar si el 403 es sistemático.
2. **Desactiva VPN/proxy** si los usas y vuelve a probar.
3. **Espera 15–30 minutos** — si vienes de ejecutar muchas peticiones
   seguidas, puede ser rate-limit temporal. El updater ya pausa 0,3 s
   entre peticiones para ser respetuoso.
4. **Prueba desde otra red** (móvil como hotspot, por ejemplo) para
   descartar bloqueo por tu ISP.
5. **Desde Streamlit Cloud**: si los 403 son persistentes, sigue la
   **Estrategia B** (datos precargados en el repo) de la sección de
   despliegue.
6. **Última opción**: mira el archivo `config.py`, busca
   `CBOE_URL_PATTERNS` y añade patrones nuevos si CBOE los ha cambiado.
   Para descubrir el patrón actual: ve a
   [cboe.com/us/futures/market_statistics/historical_data](https://www.cboe.com/us/futures/market_statistics/historical_data/),
   abre herramientas de desarrollador → Red, pulsa descargar un contrato,
   y copia la URL exacta que hace la petición.

### Problema: Yahoo Finance devuelve NaN en VIX9D, VIX3M o VIX6M

Ocurre puntualmente. El sistema los deja como NaN y no rompe; al día
siguiente normalmente vuelven. Si es sistemático, revisa que `yfinance`
esté actualizado: `pip install --upgrade yfinance`.

### Problema: la app dice "Sin datos" en todas las secciones

Significa que los parquet/SQLite están vacíos. Ejecuta `python updater.py --full`
o, si ya lo has ejecutado, comprueba que los ficheros `data/spot/vix_spot.parquet`
y algún contrato en `data/futures/` existan y tengan tamaño > 0.

### Problema: el update se queda colgado

Si tras varios minutos sin progreso, corta con Ctrl+C y vuelve a lanzar.
El sistema es idempotente: no redescargará lo que ya está guardado.

---

## Limitaciones reales conocidas

1. **CBOE tiene WAF que bloquea 403 algunas IPs** (datacenter, regiones
   geográficas, rangos dinámicos). Tu red doméstica en España debería
   funcionar; Streamlit Cloud puede funcionar o no. Hay fallback a Stooq
   y estrategia de carga local + commit al repo.
2. **CBOE cambia URLs sin avisar**. El código prueba 4 patrones diferentes
   (incluyendo el patrón moderno con fecha de expiración `YYYY-MM-DD`).
   Si todos fallan, hay que añadir el patrón nuevo en `config.py`.
3. **No hay intradía gratis fiable** para futuros VX. Todo es EOD
   (settlement del cierre).
4. **Yahoo Finance puede dar días con NaN** esporádicos en `^VIX9D`,
   `^VIX3M`, `^VIX6M`. El sistema los marca como NaN, no rompe.
5. **Calendario de festivos**: usamos `USFederalHolidayCalendar` como
   proxy del calendario CFE. Coincide >99% de días; en casos excepcionales
   podría haber 1 día de desfase en la clasificación activo/expirado.
6. **Antes de 2013** la estructura de contrato cambió y los datos son
   más ruidosos; por eso el histórico arranca en 2013.
7. **Stooq fallback es limitado**: solo da M1/M2 continuos, no histórico
   por contrato individual. Útil como plan B pero no sustituye a CBOE.

---

## Metodología cuantitativa

- **Percentiles**: default = **rolling 3 años** (756 días hábiles)
  porque el régimen de volatilidad cambia y el full-sample sobrepondera
  2008 y 2020. Ambos se muestran en pantalla.
- **Z-score**: ventana rolling 252 días hábiles. Simple y estándar.
- **Clasificación caro/barato**: percentil < 20 = barato; > 80 = caro.
  Umbrales configurables en `config.py`.
- **Contango/backwardation**: cuatro definiciones simultáneas
  (spot-M1, M1-M2, M2-M3, M4-M7) porque ninguna es "la correcta" —
  cada una captura un tramo distinto de la curva.
- **Estacionalidad**: calculada sobre **returns** (pct_change), no
  niveles, porque el nivel del VIX no es estacionario.
- **Slope**: regresión lineal de settle sobre DTE usando todos los
  contratos vivos.

---

## Soporte / ampliaciones futuras

Ideas naturales de extensión cuando quieras:
- Añadir índices VVIX, SKEW desde el mismo pipeline.
- Migrar a DuckDB para consultas analíticas más potentes.
- Añadir fuente Databento para intradía.
- Señal operativa basada en contango + percentiles.
- Backtesting de estrategias de roll (e.g. VXX-VXZ).
