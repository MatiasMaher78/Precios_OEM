# Precios_OEM

Proyecto para extracción de precios SIN IVA desde Ecooparts.

## Requisitos
- Python 3.10+ (probado con entorno virtual `.venv`).
- Paquetes runtime: ver `requirements.txt`.
- Paquetes de desarrollo (opcional): ver `requirements-dev.txt`.
- Playwright: `pip install playwright` y `python -m playwright install chromium`.

## Instalación
```bash
pip install -r requirements.txt
# Desarrollo (lint, tests)
pip install -r requirements-dev.txt
python -m playwright install chromium
```

## Estructura de carpetas
- `Input/`: archivo Excel de entrada (debe existir un único `.xlsx`/`.xls`).
- `Output/`: resultados (`Output_YYYYMMDD.xlsx`) y `cache_oem.json`.
- `Done/`: archivo de entrada movido si se usa `--move-to-done`.

## Uso
```bash
python scrap.py \
  --scrap-folder "C:/Users/<usuario>/Desktop/Precios_OEM" \
  --batch 500 --start 0 --delay 0.2 \
  --timeout 10000 --max-pages 5 --per-page 30 \
  --proxy <opcional> --verbose --headful --move-to-done --no-blocking \
  --detail-delay 0.25 --detail-retries 2 --max-details-per-query 0
```
Notas:
- El flujo principal usa el listado paginado, con `early exit` (≥50 precios) y `scroll_rounds`.
- Fallback: si no hay resultados, se intenta el token alfanumérico más largo (código OEM).
- `--capture-xhr`: presente por compatibilidad; hoy solo muestra un aviso y termina.

## Tests y Lint
```bash
# Tests
python -m pytest -q
# Formateo
python -m black .
# Lint (config en pyproject.toml)
python -m flake8 .
```

## Limpieza rápida
```bash
# Ver qué se eliminaría sin borrar
python tools/clean.py --dry-run

# Eliminar caches y HTMLs de debug
python tools/clean.py
```

## Errores comunes
- Falta archivo en `Input/`: el programa informa y termina.
- Error al guardar Excel: se genera un alternativo con timestamp.

## Rendimiento
- `max_pages=5`, `scroll_rounds=3`, `timeout_ms=10000`, `scroll_wait_ms=500`.
- Bloqueo de recursos (imágenes, fuentes, etc.) para acelerar carga del listado.
