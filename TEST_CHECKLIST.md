# Checklist de Pruebas (Manual)

Este checklist cubre los flujos principales del proyecto sin depender de servicios externos en CI.

- Extracción en listado:
  - Buscar una pieza conocida; verificar que `Units` coincide con la cantidad de enlaces recolectados del listado.
  - Confirmar `Min/Max Price` a partir de los precios del listado (sin IVA cuando esté presente).

- Fallback de código OEM:
  - Con una query como "CAJA MARIPOSA AIRE 9640795280" que inicialmente no arroja resultados, asegurar que se intenta únicamente el token alfanumérico más largo (`9640795280`) y que produce resultados.
  - Verificar que NO se prueban palabras sueltas ("caja", "mariposa", "aire").

- Comportamiento de caché:
  - Ejecutar dos veces con el mismo input y verificar que se crea `Output/cache_oem.json` y que la segunda corrida reutiliza caché cuando hay resultados.

- Manejo de errores:
  - Falta de archivo en `Input/`: el programa reporta un error claro y termina.
  - Error de permisos al guardar Excel: se crea un archivo alternativo con sufijo de timestamp.

- Optimizaciones de rendimiento:
  - `max_pages=5`, `scroll_rounds=3`, early exit al tener suficientes precios (≥50) o cuando la página trae menos que `per_page`.
  - `timeout_ms=10000` y menor `scroll_wait_ms` mantienen la extracción estable.

- Deprecaciones:
  - Las rutas de detalle (`_fetch_details_async` y `_extract_siniva_from_detail`) muestran advertencias si se llegan a invocar; la extracción principal usa el listado.

Nota: Flags mencionados anteriormente como `--units-from-links` y `--no-price-dedupe` ya no existen en la versión actual; el conteo de unidades prioriza enlaces recolectados y la deduplicación de precios se aplica automáticamente para min/max cuando hay más precios que enlaces.
