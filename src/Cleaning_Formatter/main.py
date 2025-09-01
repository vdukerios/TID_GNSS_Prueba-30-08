from pathlib import Path
import sys
from typing import Dict, List, Optional
from GPX_cleaner_formatter import GPXCleanerFormatter
from KML_protocol_analyzer import KMLProtocolAnalyzer
import re
REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))
from src.config import load_params
import geopandas as gpd
import pandas as pd

cfg = load_params()           # devuelve dict o lanza/retorna {}
gpx_folder_name = (cfg or {}).get("gpx_folder", "GPX")

BASE = Path(__file__).resolve().parents[2]
GPX_DIR = BASE / gpx_folder_name
OUT_DIR = BASE / "Clean_Files"


def discover_gpx(root: Path) -> List[Path]:
    return sorted(p for p in root.rglob("*.gpx"))


def parse_device_name(fp: Path) -> str:
    """Return a normalized device name parsed from the GPX filename.

    Examples produced: 'Garmin_Fenix_5x', 'Garmin_Fenix_3', 'Huawei_GT5', 'Iphone_12'.
    The function attempts to match common device substrings; if none match it falls
    back to a sanitized stem.
    """
    stem = fp.stem.lower()
    # Lógica de detección de dispositivo:
    # 1) Se intenta leer la lista `device_patterns` desde `config/params.json` (clave JSON: una lista
    #    de objetos {"pattern": "<regex>", "name": "<canonical_name>"}).
    #    - Cada `pattern` es una expresión regular que se prueba contra el stem del archivo GPX.
    #    - Si coincide, se devuelve el `name` canónico (por ejemplo 'Garmin_Fenix_5x').
    # 2) Si `device_patterns` no existe o falla la carga, se usa una lista por defecto embebida
    #    en este módulo (fallback).
    # Cómo cambiar/añadir patrones para nuevos dispositivos:
    # - Preferible: editar `config/params.json` y añadir una entrada en "device_patterns", p.ej:
    #   "device_patterns": [{"pattern": "mi_dispositivo\\s*v1", "name": "MiDevice_v1"}, ...]
    # - Alternativa: modificar la lista por defecto en este archivo (no recomendado si se usa
    #   la configuración centralizada).
    # Notas:
    # - Las expresiones regulares deben escribirse como en Python; se prueban con re.IGNORECASE.
    # - El primer patrón que coincida wins; ordena patrones más específicos antes de los genéricos.
    # - Si ninguno coincide, el nombre devuelto es el stem sanitizado del fichero.
    # load patterns from config if available, otherwise fallback to hardcoded
    patterns = []
    try:
        cfg_local = load_params() or {}
        dev_patterns = cfg_local.get("device_patterns")
        if isinstance(dev_patterns, list):
            for entry in dev_patterns:
                pat = entry.get("pattern")
                name = entry.get("name")
                if pat and name:
                    patterns.append((pat, name))
    except Exception:
        patterns = []

    if not patterns:
        patterns = [
            (r'fenix\s*5\+?', 'Garmin_Fenix_5x'),
            (r'fenix\s*3', 'Garmin_Fenix_3'),
            (r'huawei\s*gt\s*5', 'Huawei_GT5'),
            (r'gt\s*5', 'Huawei_GT5'),
            (r'iphone\s*12', 'Iphone_12'),
        ]

    for pat, name in patterns:
        if re.search(pat, stem, flags=re.IGNORECASE):
            return name
    # fallback: sanitize stem into a safe filename
    safe = re.sub(r"[^0-9A-Za-z]+", "_", fp.stem).strip("_")
    return safe


def GPX_process_per_file(fp: Path, params: dict) -> GPXCleanerFormatter:
    """Procesa un solo archivo y retorna el objeto cleaner con resultados en memoria."""
    cleaner = GPXCleanerFormatter()
    cleaner.load([str(fp)])
    start, end = params.get("start"), params.get("end")
    if start or end:
        cleaner.filter_time_range(start or "1970-01-01", end or "2100-01-01", inplace=True)
    # note: deduplication intentionally disabled here; preserve raw device readings
    if params.get("to_utm"):
        cleaner.to_utm(inplace=True)
    return cleaner

def run_GPX_pipeline(gpx_root: Optional[Path] = None, out_dir: Path = OUT_DIR, params_by_protocol: dict = None, *, save: bool = True) -> dict:
    """Run the cleaning pipeline and return cleaned objects.

    Returns a dict with keys: 'protocol1', 'protocol2', 'protocol3' each mapping to
    a dict {str(path): GPXCleanerFormatter}.
    """
    out_dir.mkdir(parents=True, exist_ok=True)
    # default gpx_root to standard GPX folder if not provided
    if gpx_root is None:
        gpx_root = GPX_DIR
    files = discover_gpx(gpx_root)

    protocolos_12 = [p for p in files if "Protocolos 1 y 2" in str(p)]
    protocolos_3 = [p for p in files if "Protocolo 3" in str(p)]

    cleaned_protocol1: Dict[str, GPXCleanerFormatter] = {}
    cleaned_protocol2: Dict[str, GPXCleanerFormatter] = {}
    cleaned_protocol3: Dict[str, GPXCleanerFormatter] = {}

    # defaults
    if params_by_protocol is None:
        params_by_protocol = {}
    p1 = params_by_protocol.get("p1", {"start": None, "end": None, "to_utm": True})
    p2 = params_by_protocol.get("p2", {"start": None, "end": None, "to_utm": True})
    p3 = params_by_protocol.get("p3", {"start": None, "end": None, "to_utm": True})

    # Note: routes (LineString) generation removed - we store only points per GPX

    for fp in protocolos_12:
        # protocolo 1 (primer pase)
        cleaner1 = GPX_process_per_file(fp, p1)
        cleaned_protocol1[str(fp)] = cleaner1
        if save:
            device_name = parse_device_name(fp)
            pdir = out_dir / "protocolo1"
            pdir.mkdir(parents=True, exist_ok=True)
            # save points (GeoPackage + GeoJSON) using device-based name
            cleaner1.save_gpkg(str(pdir / f"{device_name}_points"), use_utm=True)
            # routes generation removed by user request; only points are saved

        # protocolo 2 (segundo pase)
        cleaner2 = GPX_process_per_file(fp, p2)
        cleaned_protocol2[str(fp)] = cleaner2
        if save:
            device_name = parse_device_name(fp)
            pdir = out_dir / "protocolo2"
            pdir.mkdir(parents=True, exist_ok=True)
            cleaner2.save_gpkg(str(pdir / f"{device_name}_points"), use_utm=True)
            # routes generation removed by user request; only points are saved

    for fp in protocolos_3:
        cleaner3 = GPX_process_per_file(fp, p3)
        cleaned_protocol3[str(fp)] = cleaner3
        if save:
            device_name = parse_device_name(fp)
            pdir = out_dir / "protocolo3"
            pdir.mkdir(parents=True, exist_ok=True)
            cleaner3.save_gpkg(str(pdir / f"{device_name}_points"), use_utm=True)
            # routes generation removed by user request; only points are saved

    return {"protocol1": cleaned_protocol1, "protocol2": cleaned_protocol2, "protocol3": cleaned_protocol3}

def KML_process_per_file(fp: Path, params: dict) -> KMLProtocolAnalyzer:
    """Procesa un archivo KML (o localiza uno) y retorna un KMLProtocolAnalyzer configurado.

    Comportamiento:
    - si `fp` apunta a un archivo .kml se usa directamente; si no existe, se busca el primer .kml
      en el proyecto.
    - carga el KML, rellena `protocol_map` con `split_by_protocol` (usando `params.get('protocols')` si
      está presente).
    - si `params` contiene la clave 'gdf_points' con un GeoDataFrame de puntos, proyecta las
      referencias al EPSG calculado desde dichos puntos (llama `project_refs_to_utm_for_points`).

    Retorna el `KMLProtocolAnalyzer` listo para consultas o exportación.
    """
    # localizar archivo kml: si fp es kml válido lo usamos, si no intentamos encontrar uno
    kml_path = Path(fp) if fp is not None else None
    if kml_path is None or not kml_path.exists() or kml_path.suffix.lower() != ".kml":
        # buscar primero en params (accept both 'kml_path' and 'kml_file')
        alt = None
        if isinstance(params, dict):
            alt = params.get("kml_path") or params.get("kml_file")
        if alt:
            altp = Path(alt)
            if altp.exists():
                kml_path = altp
        # fallback: buscar primer kml en project root
    if kml_path is None or not kml_path.exists():
        candidates = list(BASE.glob("*.kml"))
        if candidates:
            kml_path = candidates[0]

    if kml_path is None or not kml_path.exists():
        raise FileNotFoundError("KML file not found (provide kml_path in params or place a .kml in project root)")

    analyzer = KMLProtocolAnalyzer()
    analyzer.load(kml_path)
    proto_map = params.get("protocols") if isinstance(params, dict) else None
    analyzer.split_by_protocol(proto_map)

    # optional: if caller provides gdf_points, project the refs to that UTM
    gdf_points = params.get("gdf_points") if isinstance(params, dict) else None
    if gdf_points is not None:
        try:
            analyzer.project_refs_to_utm_for_points(gdf_points)
        except Exception:
            # don't fail hard here; leave refs in original CRS
            pass

    return analyzer


def collect_points_for_protocol(cleaned_dict: Dict[str, GPXCleanerFormatter]) -> Optional[gpd.GeoDataFrame]:
    """Concatena los GeoDataFrames (preferentemente UTM) de un dict de cleaners.

    Retorna None si no hay puntos.
    """
    parts = []
    for cleaner in cleaned_dict.values():
        if not isinstance(cleaner, GPXCleanerFormatter):
            continue
        g_utm = getattr(cleaner, "gdf_utm", None)
        g_latlon = getattr(cleaner, "gdf", None)
        g = None
        if g_utm is not None and not getattr(g_utm, "empty", True):
            g = g_utm
        elif g_latlon is not None and not getattr(g_latlon, "empty", True):
            g = g_latlon
        if g is not None:
            parts.append(g.copy())
    if not parts:
        return None
    # ensure consistent CRS
    base_crs = parts[0].crs
    normalized = []
    for g in parts:
        try:
            if g.crs != base_crs:
                g = g.to_crs(base_crs)
        except Exception:
            pass
        normalized.append(g)
    combined = gpd.GeoDataFrame(pd.concat(normalized, ignore_index=True), crs=base_crs)
    return combined


def main():
    # load params from config (if present) and pass protocol params to pipeline
    cfg = {}
    try:
        cfg = load_params()
        print(f"Loaded params config version={cfg.get('version')}")
    except Exception:
        print("No params config found or failed to load; using defaults")

    protocols = cfg.get("protocols") if isinstance(cfg, dict) else None
    # determine GPX folder from config (gpx_folder) - accept relative names
    gpx_folder_name = None
    if isinstance(cfg, dict):
        gpx_folder_name = cfg.get("gpx_folder")
    if not gpx_folder_name:
        gpx_root = GPX_DIR
    else:
        p = Path(gpx_folder_name)
        gpx_root = p if p.is_absolute() else BASE / p

    results = run_GPX_pipeline(gpx_root=gpx_root, params_by_protocol=protocols)
    total = sum(len(v) for v in results.values())
    print(f"GPX cleaning complete. {total} cleaned items (protocol1={len(results['protocol1'])}, protocol2={len(results['protocol2'])}, protocol3={len(results['protocol3'])}).")

    # --- KML processing per protocol ---
    # localizar KML: preferir cfg['kml_file'] o cfg['kml_path'] si existen
    kml_path = None
    try:
        if isinstance(cfg, dict):
            kp = cfg.get("kml_file") or cfg.get("kml_path")
            if kp:
                p = Path(kp)
                # if relative path, resolve against BASE
                if not p.is_absolute():
                    p = BASE / p
                if p.exists():
                    kml_path = p
    except Exception:
        kml_path = None

    if kml_path is None:
        candidates = list(BASE.glob("*.kml"))
        if candidates:
            kml_path = candidates[0]

    if kml_path is None:
        print("No KML found; skipping KML export.")
        return

    # for each protocol, collect points and call analyzer
    proto_map = {"p1": results["protocol1"], "p2": results["protocol2"], "p3": results["protocol3"]}
    for key, cleaned_dict in proto_map.items():
        gdf_points = collect_points_for_protocol(cleaned_dict)
        params = {}
        if gdf_points is not None:
            params["gdf_points"] = gdf_points

        try:
            analyzer = KML_process_per_file(kml_path, params)
        except Exception as e:
            print(f"KML processing failed for {key}: {e}")
            continue

        out_dir_proto = OUT_DIR / f"protocolo{key[-1]}"
        out_dir_proto.mkdir(parents=True, exist_ok=True)
        # write KML refs as GeoPackage + GeoJSON
        base_out = out_dir_proto / f"kml_refs_{key}"
        out_gpkg = base_out.with_suffix('.gpkg')
        out_geojson = base_out.with_suffix('.geojson')
        gdf_refs = analyzer.protocol_map.get(key)
        try:
            if gdf_refs is None or gdf_refs.empty:
                # create an empty GeoDataFrame with a name column
                empty = gpd.GeoDataFrame(pd.DataFrame(columns=["name"]), geometry=[], crs='EPSG:4326')
                empty.to_file(str(out_gpkg), driver='GPKG', layer=f'kml_refs_{key}')
                empty.to_file(str(out_geojson), driver='GeoJSON')
            else:
                # normalize CRS to 4326 for writing
                try:
                    gdf_refs_w = gdf_refs.to_crs('EPSG:4326') if getattr(gdf_refs, 'crs', None) is not None else gdf_refs.set_crs('EPSG:4326')
                except Exception:
                    try:
                        gdf_refs_w = gdf_refs.copy()
                        gdf_refs_w.set_crs('EPSG:4326', inplace=True)
                    except Exception:
                        gdf_refs_w = gdf_refs.copy()

                # if gdf has a 'kind' column, write separate layers per kind inside the gpkg
                if 'kind' in gdf_refs_w.columns:
                    # write one layer per kind for easier consumption
                    for kind_val, sub in gdf_refs_w.groupby('kind'):
                        layer_name = f'kml_refs_{key}_{kind_val}'
                        try:
                            sub.to_file(str(out_gpkg), driver='GPKG', layer=layer_name)
                        except Exception:
                            # fallback: try resetting crs on subset
                            try:
                                sub.to_crs('EPSG:4326').to_file(str(out_gpkg), driver='GPKG', layer=layer_name)
                            except Exception:
                                pass
                    # also write a combined geojson for web use
                    try:
                        gdf_refs_w.to_file(str(out_geojson), driver='GeoJSON')
                    except Exception:
                        pass
                else:
                    # no kinds: write single layer
                    try:
                        gdf_refs_w.to_file(str(out_gpkg), driver='GPKG', layer=f'kml_refs_{key}')
                    except Exception:
                        try:
                            gdf_refs_w.copy().to_crs('EPSG:4326').to_file(str(out_gpkg), driver='GPKG', layer=f'kml_refs_{key}')
                        except Exception:
                            pass
                    try:
                        gdf_refs_w.to_file(str(out_geojson), driver='GeoJSON')
                    except Exception:
                        pass
            print(f"Wrote KML refs for {key} -> {out_gpkg} and {out_geojson}")
        except Exception as e:
            print(f"Failed to write KML refs for {key}: {e}")


if __name__ == "__main__":
    main()
