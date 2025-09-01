from pathlib import Path
from typing import Optional
import geopandas as gpd
import pandas as pd
import folium
from shapely import wkt
from shapely.geometry import Point
from shapely.ops import unary_union as _shapely_unary_union
import json
from branca.element import Template, MacroElement, Element


BASE = Path(__file__).resolve().parents[2]
CLEAN_DIR = BASE / "Clean_Files"
OUT_ROOT = BASE / "Plot_results"


def _load_parquet_to_gdf(p: Path, prefer_utm: bool = False) -> Optional[gpd.GeoDataFrame]:
    """Load a GIS file (GeoPackage/GeoJSON/Parquet) and return a GeoDataFrame in EPSG:4326.

    Accepts:
    - .gpkg, .geojson: read with geopandas.read_file
    - .parquet: load with pandas then convert (legacy fallback)

    Returns None if file missing or empty.
    """
    if not p.exists():
        return None

    suffix = p.suffix.lower()
    gdf = None
    # GeoPackage or GeoJSON: prefer geopandas reader
    if suffix in (".gpkg", ".geojson", ".json"):
        try:
            # geopandas will pick the first layer for gpkg; that's fine for our files
            gdf = gpd.read_file(str(p))
        except Exception:
            return None
        if gdf is None or len(gdf) == 0:
            return None
    elif suffix == ".parquet":
        try:
            df = pd.read_parquet(p)
        except Exception:
            return None
        if df.shape[0] == 0:
            return None

        # If already a geodataframe saved, try reading with geopandas conversion
        if "geometry" in df.columns:
            try:
                gdf = gpd.GeoDataFrame(df, geometry="geometry")
            except Exception:
                if "wkt" in df.columns:
                    try:
                        df["geometry"] = df["wkt"].apply(lambda x: wkt.loads(x) if pd.notna(x) else None)
                        gdf = gpd.GeoDataFrame(df, geometry="geometry")
                    except Exception:
                        return None
                else:
                    return None
        elif "wkt" in df.columns:
            df = df.copy()
            df["geometry"] = df["wkt"].apply(lambda x: wkt.loads(x) if pd.notna(x) else None)
            gdf = gpd.GeoDataFrame(df, geometry="geometry")
        else:
            lon_candidates = [c for c in df.columns if c.lower() in ("lon", "longitude", "lng")]
            lat_candidates = [c for c in df.columns if c.lower() in ("lat", "latitude")]
            if lon_candidates and lat_candidates:
                lonc, latc = lon_candidates[0], lat_candidates[0]
                df = df.copy()
                df["geometry"] = df.apply(lambda r: Point(float(r[lonc]), float(r[latc])), axis=1)
                gdf = gpd.GeoDataFrame(df, geometry="geometry")
            else:
                return None

    else:
        # unknown extension
        return None

    # ensure CRS -> 4326 for plotting
    try:
        if gdf.crs is None:
            gdf.set_crs(epsg=4326, inplace=True)
        else:
            gdf = gdf.to_crs(epsg=4326)
    except Exception:
        try:
            gdf.set_crs(epsg=4326, inplace=True)
        except Exception:
            pass

    return gdf


def plot_protocol(protocol_key: str, clean_dir: Path, out_dir: Path):
    """Create outputs for a single protocol.

    Expects parquet files in `clean_dir`:
      - *_points.parquet (one or several)
      - kml_refs_pX.parquet (optional)
    Produces in `out_dir`:
      - points_protocoloX.geojson
      - kml_refs_pX.geojson
      - summary_protocoloX.csv
      - map_protocoloX.html
    """
    out_dir.mkdir(parents=True, exist_ok=True)

    # collect points files (prefer GeoPackage, then GeoJSON, then parquet)
    points_files = []
    points_files.extend(sorted(clean_dir.glob("*_points.gpkg")))
    points_files.extend(sorted(clean_dir.glob("*_points.geojson")))
    points_files.extend(sorted(clean_dir.glob("*_points.parquet")))
    points_gdfs = []
    for p in points_files:
        g = _load_parquet_to_gdf(p)
        if g is None:
            pass
        else:
            # tag with source
            g = g.copy()
            g["_source_file"] = p.stem
            points_gdfs.append(g)

    if points_gdfs:
        points_all = gpd.GeoDataFrame(pd.concat(points_gdfs, ignore_index=True), crs=points_gdfs[0].crs)
    else:
        points_all = None

    # Ensure points_all is in EPSG:4326 for folium (meters in Circle are interpreted as meters on Earth)
    if points_all is not None and not points_all.empty:
        try:
            if points_all.crs is None:
                points_all.set_crs(epsg=4326, inplace=True)
            else:
                points_all = points_all.to_crs(epsg=4326)
        except Exception:
            try:
                points_all.set_crs(epsg=4326, inplace=True)
            except Exception:
                pass

    # defensive: ensure both points and refs are in EPSG:4326 (lat/lon) so folium's
    # radius (meters) and placement are correct even if source files were UTM.
    if points_all is not None:
        try:
            points_all = points_all.to_crs(epsg=4326)
        except Exception:
            try:
                points_all.set_crs(epsg=4326, inplace=True)
            except Exception:
                pass

    # load kml refs (try gpkg, geojson, parquet)
    kml_file = None
    for ext in (".gpkg", ".geojson", ".parquet"):
        cand = clean_dir / f"kml_refs_{protocol_key}{ext}"
        if cand.exists():
            kml_file = cand
            break
    refs = _load_parquet_to_gdf(kml_file) if kml_file is not None else None
    if kml_file is not None:
        if refs is None:
            print(f"[DEBUG] kml refs file exists but failed to load or empty: {kml_file}")
        else:
                try:
                    _ = len(refs)
                except Exception:
                    pass

    # ensure refs are in EPSG:4326 so folium.Circle radius (meters) maps correctly
    if refs is not None and not refs.empty:
        try:
            if refs.crs is None:
                refs.set_crs(epsg=4326, inplace=True)
            else:
                refs = refs.to_crs(epsg=4326)
        except Exception:
            try:
                refs.set_crs(epsg=4326, inplace=True)
            except Exception:
                pass

    # defensive: ensure refs are lat/lon for plotting (folium expects lat/lon coordinates)
    if refs is not None:
        try:
            refs = refs.to_crs(epsg=4326)
        except Exception:
            try:
                refs.set_crs(epsg=4326, inplace=True)
            except Exception:
                pass

    # write geojson + csv summary
    if points_all is not None and not points_all.empty:
        pts_out = out_dir / f"points_{protocol_key}.geojson"
        try:
            points_all.to_file(pts_out, driver="GeoJSON")
        except Exception:
            pass
        # summary csv
        try:
            summary = points_all.drop(columns=[c for c in points_all.columns if c == 'geometry'])
            summary_csv = out_dir / f"summary_{protocol_key}.csv"
            summary.to_csv(summary_csv, index=False)
        except Exception:
            pass
    else:
        pts_out = None

    if refs is not None and not refs.empty:
        refs_out = out_dir / f"kml_refs_{protocol_key}.geojson"
        try:
            refs.to_file(refs_out, driver="GeoJSON")
        except Exception:
            pass
    else:
        refs_out = None

    # create folium map
    center = None
    if points_all is not None and not points_all.empty:
        center = [points_all.geometry.y.mean(), points_all.geometry.x.mean()]
    elif refs is not None and not refs.empty:
        center = [refs.geometry.y.mean(), refs.geometry.x.mean()]
    else:
        center = [0, 0]

    # Create a blank (no basemap) Folium canvas so only points are shown.
    # This allows a clean, scale-focused view where points define the extent.
    m = folium.Map(location=center, zoom_start=16, tiles=None, control_scale=True)

    # KML refs will be shown as a separate layer of markers
    if refs is not None and not refs.empty:
        fg_refs = folium.FeatureGroup(name='KML refs', show=True)
        # color mapping for kinds
        kind_colors = {
            'outer': 'blue',
            'inner': 'green',
            'start_line': 'red',
            'trail': 'purple',
            'crossing': 'darkred',
            'point': 'red'
        }
        for _, r in refs.iterrows():
            geom = r.geometry
            if geom is None:
                continue
            kind = (r.get('kind') or '').lower() if 'kind' in r.index else ''
            name = r.get('name') or r.get('Name') or ''
            popup = f"{name} ({kind})" if name else f"KML ref ({kind})"

            # Points (including crossing)
            if geom.geom_type == 'Point' or kind in ('point', 'crossing'):
                lat, lon = geom.y, geom.x
                folium.CircleMarker(location=[lat, lon], radius=6, color=kind_colors.get(kind, 'red'), fill=True, popup=popup).add_to(fg_refs)
                # p1: draw concentric real-meter rings around the reference point
                if protocol_key == 'p1' and (kind == 'point' or 'p1' in popup.lower()):
                    radii_m = [0.1, 0.5, 1, 3, 5]
                    for rr in radii_m:
                        tooltip = f"{rr} m"
                        folium.Circle(location=[lat, lon], radius=rr, color='#333333', weight=1, fill=False, opacity=0.6, tooltip=tooltip).add_to(fg_refs)

            # Line or polygon geometries: LineString/MultiLineString or Polygon/MultiPolygon (plot exterior rings)
            elif geom.geom_type in ('LineString', 'MultiLineString', 'Polygon', 'MultiPolygon') or kind in ('outer', 'inner', 'start_line', 'trail'):
                color = kind_colors.get(kind, 'black')
                # handle MultiLineString by plotting each component
                def plot_coords_from_sequence(seq):
                    coords = []
                    for c in seq:
                        try:
                            x = c[0]; y = c[1]
                        except Exception:
                            continue
                        coords.append((y, x))
                    if coords:
                        folium.PolyLine(locations=coords, color=color, weight=3, popup=popup).add_to(fg_refs)

                if geom.geom_type == 'LineString':
                    plot_coords_from_sequence(geom.coords)
                elif geom.geom_type == 'MultiLineString':
                    for part in geom.geoms:
                        try:
                            plot_coords_from_sequence(part.coords)
                        except Exception:
                            continue
                elif geom.geom_type == 'Polygon':
                    try:
                        plot_coords_from_sequence(geom.exterior.coords)
                    except Exception:
                        # fallback to centroid marker
                        c = geom.centroid
                        folium.CircleMarker(location=[c.y, c.x], radius=4, color=color, fill=False, popup=popup).add_to(fg_refs)
                elif geom.geom_type == 'MultiPolygon':
                    for poly in geom.geoms:
                        try:
                            plot_coords_from_sequence(poly.exterior.coords)
                        except Exception:
                            continue
            else:
                # fallback: place a centroid marker
                try:
                    c = geom.centroid
                    folium.CircleMarker(location=[c.y, c.x], radius=5, color='gray', fill=True, popup=popup).add_to(fg_refs)
                except Exception:
                    continue

        fg_refs.add_to(m)

    # add points layer colored by source file
    legend_rows = []
    device_layers = []
    if points_all is not None and not points_all.empty:
        groups = list(points_all.groupby('_source_file')) if '_source_file' in points_all.columns else [('points', points_all)]
        palette = ['blue', 'green', 'orange', 'purple', 'darkred', 'cadetblue', 'darkgreen', 'black']
        for i, (src, grp) in enumerate(groups):
            color = palette[i % len(palette)]
            # layer name sanitized for display
            layer_name = str(src)
            legend_rows.append((layer_name, color))
            fg = folium.FeatureGroup(name=layer_name, show=True)
            for _, r in grp.iterrows():
                geom = r.geometry
                lat, lon = geom.y, geom.x
                popup = ''
                if 'time' in r.index:
                    popup = str(r['time'])
                folium.CircleMarker(location=[lat, lon], radius=3, color=color, fill=True, popup=popup).add_to(fg)
            fg.add_to(m)
            device_layers.append(layer_name)

    # fit map to points/ref bounds if available
    try:
        if points_all is not None and not points_all.empty:
            minx, miny, maxx, maxy = points_all.total_bounds
            m.fit_bounds([[miny, minx], [maxy, maxx]])
        elif refs is not None and not refs.empty:
            minx, miny, maxx, maxy = refs.total_bounds
            m.fit_bounds([[miny, minx], [maxy, maxx]])
    except Exception:
        pass

    # build floating HTML legend + KML refs table
    legend_html_rows = ''
    for src, color in legend_rows:
        legend_html_rows += f"<tr><td style='padding:4px'><div style='width:18px;height:12px;background:{color};'></div></td><td style='padding:4px'>{src}</td></tr>"

    refs_table_rows = ''
    if refs is not None and not refs.empty:
        for _, r in refs.iterrows():
            name = r.get('name') or r.get('Name') or ''
            geom = r.geometry
            if geom is None:
                continue
            if geom.geom_type == 'Point':
                lon, lat = geom.x, geom.y
            else:
                c = geom.centroid
                lon, lat = c.x, c.y
            try:
                refs_table_rows += f"<tr><td style='padding:4px'>{name}</td><td style='padding:4px'>{lat:.6f}</td><td style='padding:4px'>{lon:.6f}</td></tr>"
            except Exception:
                refs_table_rows += f"<tr><td style='padding:4px'>{name}</td><td style='padding:4px'>{lat}</td><td style='padding:4px'>{lon}</td></tr>"

    # For protocolo p2: compute the area between outer and inner and percentage of points per device inside it
    p2_stats_rows = ''
    try:
        if protocol_key == 'p2' and refs is not None and not refs.empty and points_all is not None and not points_all.empty:
            # helper to find geometries by kind (or name fallback)
            def _pick_by_kind(k):
                if 'kind' in refs.columns:
                    sel = refs[refs['kind'].str.lower() == k]
                else:
                    sel = refs[refs['name'].str.lower().str.contains(k, na=False)] if 'name' in refs.columns else refs[[]]
                return [g for g in sel.geometry if g is not None]

            outer_geoms = _pick_by_kind('outer')
            inner_geoms = _pick_by_kind('inner')

            def _union_geoms(geoms):
                if not geoms:
                    return None
                try:
                    gs = gpd.GeoSeries(geoms)
                    # geopandas newer versions provide union_all()
                    if hasattr(gs, 'union_all'):
                        return gs.union_all()
                    # fallback to unary_union attribute if present
                    if hasattr(gs, 'unary_union'):
                        return gs.unary_union
                except Exception:
                    pass
                # final fallback to shapely.ops.unary_union
                try:
                    return _shapely_unary_union(list(geoms))
                except Exception:
                    return None

            outer_union = _union_geoms(outer_geoms)
            inner_union = _union_geoms(inner_geoms)

            ring_area = None
            if outer_union is not None:
                if inner_union is not None:
                    try:
                        ring_area = outer_union.difference(inner_union)
                    except Exception:
                        ring_area = outer_union
                else:
                    ring_area = outer_union

            if ring_area is not None and not ring_area.is_empty:
                # draw ring outline on the map (stroke only)
                try:
                    folium.GeoJson(data=gpd.GeoSeries([ring_area]).to_json(), name='p2_ring', style_function=lambda f: {'color': 'blue', 'weight': 2, 'fill': False, 'opacity': 0.4}).add_to(m)
                except Exception:
                    pass

                # compute per-device percentages
                if '_source_file' in points_all.columns:
                    groups = list(points_all.groupby('_source_file'))
                else:
                    groups = [('points', points_all)]
                for src, grp in groups:
                    try:
                        total = len(grp)
                        inside = int(grp.geometry.within(ring_area).sum()) if total > 0 else 0
                        pct = 100.0 * inside / total if total > 0 else 0.0
                        p2_stats_rows += f"<tr><td style='padding:4px'>{src}</td><td style='padding:4px'>{inside}/{total}</td><td style='padding:4px'>{pct:.1f}%</td></tr>"
                    except Exception:
                        continue
    except Exception:
        # don't fail plotting for any unexpected geometry ops
        p2_stats_rows = ''

    # prepare P2 stats HTML block (always include section; show 'No data' when empty)
    if p2_stats_rows:
        p2_stats_section = f"<hr/><b>P2 stats (points inside outer-inner)</b><table style='border-collapse:collapse'><tr><th>Device</th><th>Inside/Total</th><th>Percent</th></tr>{p2_stats_rows}</table>"
    else:
        p2_stats_section = "<hr/><b>P2 stats (points inside outer-inner)</b><div style='padding:4px;color:#666;'>No data available</div>"

    template = f"""
    <div style="position: fixed; bottom: 10px; left: 10px; z-index:1000; background: white; padding: 10px; border:1px solid #999; max-height: 40vh; overflow:auto; font-size:12px;">
      <b>Legend (device -> color)</b>
      <table style='border-collapse:collapse'>{legend_html_rows}</table>
      <hr/>
      <b>KML references</b>
      <table style='border-collapse:collapse'><tr><th>Name</th><th>Lat</th><th>Lon</th></tr>{refs_table_rows}</table>
      {p2_stats_section}
    </div>
    """
    # ensure the HTML gets injected into the saved page
    try:
        m.get_root().html.add_child(Element(template))
    except Exception:
        try:
            macro = MacroElement()
            macro._template = Template(template)
            m.get_root().add_child(macro)
        except Exception:
            # last fallback: attach to map as a DivIcon marker at center
            try:
                folium.map.Marker(location=center, icon=folium.DivIcon(html=template)).add_to(m)
            except Exception:
                pass

    folium.LayerControl().add_to(m)
    map_out = out_dir / f"map_{protocol_key}.html"
    try:
        m.save(str(map_out))
    except Exception:
        pass

    print(f"Protocol {protocol_key}: map={map_out}, points={pts_out}, refs={refs_out}")


def main():
    # iterate protocolo1, protocolo2, protocolo3 directories inside Clean_Files
    for i in (1, 2, 3):
        pdir = CLEAN_DIR / f"protocolo{i}"
        if not pdir.exists():
            print(f"Skipping protocolo{i}: no directory")
            continue
        out = OUT_ROOT / f"protocolo{i}"
        plot_protocol(f"p{i}", pdir, out)


if __name__ == '__main__':
    main()
