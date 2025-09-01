# Proyecto TID Precision dispositos GNSS

## 0. Advertencias de Uso

Este proyecto se basa en una toma de dato en 2 instancias, una para los protocolos 1 y 2 y otra para el protocolo 3. Ten en cuenta esto a la hopra de limpiar los datos, sin tu hiciste el registro en una sola instancia, entonces quizas tengas que ca,mbiar algunas referencias para que extraiga datos del mismo archvio. Para esto revisa main.py

Antes de ejecutar los scripts asegúrate de colocar los datos en las ubicaciones esperadas y declarar sus nombres en `config/params.json`:

- Archivos GPX: por defecto deben ir en la carpeta `GPX/` en la raíz del repositorio (puedes cambiarlo vía `"gpx_folder"` en `config/params.json`). Ejemplo:

  - `"gpx_folder": "GPX"`
  - Si pones una ruta relativa se resolverá respecto a la raíz del proyecto; puedes usar una ruta absoluta si lo prefieres.

- Archivo KML de referencias: por defecto el orquestador busca un `.kml` en la raíz; o especifica su nombre/ruta en `config/params.json` usando `"kml_file"` o `"kml_path"`. Ejemplo:

  - `"kml_file": "Planificacion TID GPS.kml"`
  - Si la ruta es relativa se resolverá respecto a la raíz del proyecto; también aceptará rutas absolutas.

- Nombres de dispositivos: los GPX se asocian a nombres canónicos según el nombre del archivo GPX. Los dispositivos usados en este proyecto (y que el parser reconoce por defecto) son:

  - `Garmin_Fenix_5x`
  - `Garmin_Fenix_3`
  - `Huawei_GT5`
  - `Iphone_12`
    Si tus archivos GPX usan otros nombres, añade/renombra los archivos o actualiza la función `parse_device_name()` en `src/Cleaning_Formatter/main.py`.

- Nota: si los archivos ya están versionados en el repo, añadir sus patrones a `.gitignore` no los eliminará del índice; usa `git rm --cached` para dejarlos de rastrear sin borrarlos del disco (ver sección de troubleshooting).

## 1. Contexto y objetivos (resumen)

Proyecto para evaluar precisión de dispositivos GNSS: leer protocolos (KML) y trayectorias medidas (GPX), limpiar y proyectar a UTM, calcular métricas y generar reportes. Objetivos principales:

- Ingestar KML/GPX y preparar referencias geoespaciales.
- Limpiar/normalizar streams por parámetros experimentales.
- Proyectar a CRS métrico (UTM) para cálculos de distancia.
- Calcular métricas por protocolo/lap y exportar resultados.

## 2. Índice (tabla de contenidos)

Accede rápidamente a las secciones principales del README:

- 1. Contexto y objetivos
- 2. Índice (esta sección)
- 3. Folders (documentación por componente)
  - 3.1 Cleaning_Formatter (`src/Cleaning_Formatter`)
    - `GPX_cleaner_formatter.py` (limpieza y export)
    - `KML_protocol_analyzer.py` (análisis de KML por protocolo)
    - `main.py` (orquestador)
  - 3.2 Plot result (`src/Plot_results`)
    - `plot_protocol.py` (mapas y resúmenes)
    - `compute_p2_stats.py` (debug P2 stats)
- 4. Salidas producidas
  - `Clean_Files/protocolo1|2|3/` (puntos y kml_refs)
  - `Plot_results/protocolo1|2|3/` (mapas HTML, geojson, csv)
- 5. Configuración y parámetros
  - `config/params.json` (start/end, to_utm, min_points, tz, kml_path)
- 6. Cómo ejecutar
  - Limpiar: `python src/Cleaning_Formatter/main.py`
  - Generar plots: `python src/Plot_results/plot_protocol.py`
- 7. Troubleshooting / debugging
  - Ver `src/Plot_results/compute_p2_stats.py` si falta la sección P2 en el HTML
- 8. Dependencias
  - geopandas, pandas, shapely, fiona, folium, branca, gpxpy

Usa estos enlaces como guía para navegar el README y añadir documentación por archivo siguiendo la plantilla en la sección correspondiente.

## 3. Folders (documentación por archivo)

### 3.1 Cleaning_Formatter (carpeta `src/Cleaning_Formatter`)

Esta sección documenta las utilidades actuales para ingestión y limpieza de GPX, así como el análisis de KML de referencias por protocolo.

Ficheros relevantes:

- `src/Cleaning_Formatter/GPX_cleaner_formatter.py`

  - Exporta la clase `GPXCleanerFormatter`.
  - Funciones/clases principales: `load()`, `filter_time_range()`, `to_utm()`, `process_files()`, `save_gpkg()`/`save_geojson()`.
  - Contrato resumido:
    - Inputs: rutas a archivos GPX y parámetros por archivo (por ejemplo `start`, `end`, `min_points`, `drop_duplicates`, `to_utm`, `epsg`).
    - Outputs: GeoDataFrames concatenados por protocolo en EPSG:4326 y opcionalmente proyecciones métricas (UTM) disponibles en `gdf_utm`.
    - Nota: el pipeline preserva lecturas duplicadas por diseño; la eliminación de duplicados es opt-in.

- `src/Cleaning_Formatter/KML_protocol_analyzer.py`

  - Detecta y extrae las geometrías del archivo KML de protocolos.
  - Produce una estructura `protocol_map` con GeoDataFrames por protocolo (`p1`,`p2`,`p3`).
  - Cada GeoDataFrame de referencias incluye metadatos mínimos: `name` y `kind` (por ejemplo `point`, `outer`, `inner`, `start_line`, `trail`, `crossing`).
  - También incluye utilitarios para proyectar referencias a UTM cuando se necesitan mediciones métricas.

- `src/Cleaning_Formatter/main.py`
  - Orquesta el proceso completo (lee `config/params.json`, procesa GPX, extrae refs KML y escribe salidas en `Clean_Files/`).
  - Exporta los archivos limpios por protocolo en `Clean_Files/protocolo1..3/`.
  - Novedades importantes:
    - Los archivos de salida de puntos se nombran por dispositivo (p.ej. `Garmin_Fenix_5x_points.gpkg`), gracias a `parse_device_name()`.
    - Los KML refs se exportan por `kind` en capas separadas dentro de los GeoPackage (cuando `kind` está presente) y también como GeoJSON combinados.

Requerimientos y dependencias (resumen)

- Python 3.9+ y las bibliotecas: geopandas, pandas, shapely, fiona, folium, branca, gpxpy.
- En Windows se recomienda instalar `geopandas`/`fiona`/`gdal` vía `conda -c conda-forge` para evitar problemas de compilación.

Ejemplo de uso del orquestador (desde la raíz del repo):

```powershell
python src/Cleaning_Formatter/main.py
```

Esto detectará archivos GPX en `GPX/`, leerá `config/params.json` si existe y generará las salidas organizadas en `Clean_Files/protocolo1/`, `Clean_Files/protocolo2/`, `Clean_Files/protocolo3/`.

#### `config/params.json` (campos relevantes)

El fichero de parámetros centraliza opciones por protocolo. Campos usados por los scripts actuales:

- `start` / `end`: ISO datetime strings (o null) para filtrar puntos por tiempo.
- `to_utm`: booleano — si True solicita proyección métrica (UTM) durante el procesamiento.
- `min_points`: entero — mínimo de puntos requerido para considerar válida la salida por archivo.
- `tz`: string opcional con la zona horaria esperada (por ejemplo `UTC` o `America/Santiago`).
- `kml_path`: ruta opcional al archivo KML con referencias (si no se proporciona, `main.py` intentará localizarlo automáticamente).

Si algún campo falta, el orquestador usa valores por defecto razonables.

### 3.2 Plot result (carpeta `src/Plot_results`)

Esta carpeta contiene utilidades para generar mapas interactivos y resúmenes a partir de las salidas en `Clean_Files/`.

Ficheros relevantes:

- `src/Plot_results/plot_protocol.py`

  - Script principal para generar mapas por protocolo.
  - Comportamiento:
    - Lee `Clean_Files/protocoloX/*_points.*` (GeoPackage, GeoJSON o Parquet) y carga todos los puntos en un `GeoDataFrame`.
    - Lee `Clean_Files/protocoloX/kml_refs_pX.*` si existe y carga las referencias KML.
    - Normaliza CRS a EPSG:4326 para el render en `folium` (necesario para que las circunferencias en metros sean correctas).
    - Dibuja:
      - Puntos por dispositivo en capas separadas (color por dispositivo).
      - Referencias KML: puntos como markers, LineString/MultiLineString como `PolyLine`, Polygon/MultiPolygon como contornos (exterior rings).
      - Para `p1`: alrededor del punto de referencia dibuja anillos concéntricos en metros (0.1, 0.5, 1, 3, 5 m).
    - Genera salidas en `Plot_results/protocoloX/`:
      - `map_pX.html` — mapa interactivo con capas y leyenda.
      - `points_pX.geojson` — copia de `points_all` en EPSG:4326 usada para plotting.
      - `summary_pX.csv` — tabla plana con atributos de los puntos (sin geometría) si es posible.
  - Uso:
    ```powershell
    python src/Plot_results/plot_protocol.py
    ```

- `src/Plot_results/compute_p2_stats.py`
  - Script auxiliar que reproduce en terminal el cálculo de estadísticas para protocolo 2 (porcentaje de puntos dentro del anillo entre `outer` e `inner`).
  - Útil para debug cuando la sección de estadísticas no aparece en el HTML.

Salidas esperadas (ejemplo para protocolo 2):

- `Plot_results/protocolo2/map_p2.html` — contiene la leyenda flotante con:
  - Colores por dispositivo y tabla con referencias KML (nombre, lat, lon).
  - Sección fija "P2 stats (points inside outer-inner)" que muestra por dispositivo `inside/total` y `%` o `No data available` si no hay resultados.

Por qué puede no aparecer la tabla de P2 en la vista:

1. Las geometrías `outer`/`inner` no fueron encontradas en `kml_refs_p2.*` (nombres o `kind` distintos). El script intenta emparejar por `kind` o por que la `name` contenga las palabras `outer`/`inner`.
2. `ring_area` quedó vacío tras la diferencia geométrica (por ejemplo `inner` no está contenido en `outer`) — entonces no hay área válida para contar puntos.
3. No hay puntos cargados en `Clean_Files/protocolo2/*_points.*`.
4. CRS desincronizado (el código fuerza EPSG:4326, pero si alguna geometría es inválida la operación geométrica puede fallar y se silencia para no romper el HTML).

Pasos rápidos de verificación (si no aparece la tabla en el HTML):

1. Ejecutar el script auxiliar y revisar la salida:

```powershell
python src/Plot_results/compute_p2_stats.py
```

2. Verificar contenidos:

- `Clean_Files/protocolo2/kml_refs_p2.geojson` o `.gpkg` — asegúrate de que contiene features con `name`/`kind` que correspondan a `outer` e `inner`.
- `Clean_Files/protocolo2/*_points.geojson` — asegúrate de que existan y contengan puntos.

3. Regenerar el mapa:

```powershell
python src/Plot_results/plot_protocol.py
start Plot_results/protocolo2/map_p2.html
```

Si quieres, puedo añadir una prueba unitaria mínima que cargue `kml_refs_p2.geojson` y `*_points.geojson` de ejemplo y valide que el bloque `P2 stats` se genera con al menos una fila esperada.

---

Fin de la documentación de `src/Plot_results`.
