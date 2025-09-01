from pathlib import Path
from typing import Dict, Optional

import geopandas as gpd
from shapely.geometry import Point


class KMLProtocolAnalyzer:
    """Clase orientada a objetos para cargar y analizar un KML de referencias por protocolo.

    Uso típico:
        analyzer = KMLProtocolAnalyzer()
        analyzer.load('Planificacion TID GPS.kml')
        analyzer.split_by_protocol()
        refs_p1 = analyzer.get_protocol_refs('p1')
    """

    def __init__(self):
        self.kml_path: Optional[Path] = None
        self.gdf: Optional[gpd.GeoDataFrame] = None
        self.protocol_map: Dict[str, gpd.GeoDataFrame] = {}

    def load(self, kml_path: Path) -> gpd.GeoDataFrame:
        """Carga el KML y guarda un GeoDataFrame EPSG:4326 en `self.gdf`.

        Lanza la excepción si geopandas/fiona no pueden leer el archivo.
        """
        kml_path = Path(kml_path)
        self.kml_path = kml_path
        gdf = gpd.read_file(str(kml_path), driver="KML")
        if "Name" in gdf.columns and "name" not in gdf.columns:
            gdf = gdf.rename(columns={"Name": "name"})
        # asegurar columna name
        if "name" not in gdf.columns:
            gdf["name"] = None
        self.gdf = gdf
        return gdf

    def split_by_protocol(self, protocols: Optional[Dict[str, str]] = None) -> Dict[str, gpd.GeoDataFrame]:
        """Rellena `self.protocol_map` con sub-GeoDataFrames por protocolo.

        protocols: mapping opcional {'p1': 'Protocolo 1', ...}. Si no se pasa, se usan
        'Protocolo 1', 'Protocolo 2', 'Protocolo 3' como búsqueda en `name`.
        """
        if self.gdf is None:
            raise RuntimeError("No KML loaded. Call load() first.")

        proto_keys = list(protocols.keys()) if protocols else ["p1", "p2", "p3"]
        self.protocol_map = {}

        # helper to test multiple candidate patterns and fields
        def _match_patterns(df, patterns):
            if df is None or df.empty:
                return pd.Series([False] * 0)
            masks = []
            for pat in patterns:
                try:
                    masks.append(df["name"].astype(str).str.contains(pat, case=False, na=False))
                except Exception:
                    masks.append(pd.Series([False] * len(df)))
                # also try Description column if present
                if "Description" in df.columns:
                    try:
                        masks.append(df["Description"].astype(str).str.contains(pat, case=False, na=False))
                    except Exception:
                        masks.append(pd.Series([False] * len(df)))
            if not masks:
                return pd.Series([False] * len(df))
            # combine masks OR-wise
            combined = masks[0]
            for m in masks[1:]:
                combined = combined | m
            return combined

        import pandas as pd

        for k in proto_keys:
            if protocols and k in protocols:
                pats = [protocols.get(k)]
            else:
                # generate multiple candidate patterns to increase match chance
                n = k[-1]
                pats = [f"Protocolo {n}", f"Protocolo_{n}", f"P{n}", f"Protocol {n}", f"p{n}"]

            mask = _match_patterns(self.gdf, pats)
            subset = self.gdf.loc[mask].copy()

            # if no matches found, try looser match: look for the protocol number anywhere
            if subset.empty:
                try:
                    mask2 = self.gdf["name"].astype(str).str.contains(n, na=False) | (self.gdf.get("Description", pd.Series([""] * len(self.gdf))).astype(str).str.contains(n, na=False))
                    subset = self.gdf.loc[mask2].copy()
                except Exception:
                    subset = self.gdf.loc[[]].copy()

            self.protocol_map[k] = subset

        # build simplified artifacts per protocol (point/lines) for easier export
        self._build_protocol_artifacts()
        return self.protocol_map

    def _get_first_named_geom(self, name_substr: str) -> Optional[gpd.GeoSeries]:
        """Return the first geometry whose name contains name_substr (case-insensitive)."""
        if self.gdf is None:
            return None
        mask = self.gdf["name"].astype(str).str.contains(name_substr, case=False, na=False)
        if mask.any():
            row = self.gdf.loc[mask].iloc[0]
            return row.geometry
        return None

    def _build_protocol_artifacts(self):
        """Construct simple GeoDataFrames for each protocol:

        - p1: single point (reference)
        - p2: three LineString features with kinds: outer, inner, start_line
        - p3: trail LineString and crossing point with p2 start_line
        """
        # p1: prefer named 'P1' or 'P1 Point'
        try:
            # build p1
            geom_p1 = self._get_first_named_geom("P1 Point") or self._get_first_named_geom("P1")
            if geom_p1 is not None:
                g_p1 = gpd.GeoDataFrame([{"name": "P1 Point", "kind": "point", "geometry": geom_p1}], crs=self.gdf.crs)
            else:
                g_p1 = gpd.GeoDataFrame(columns=["name", "kind", "geometry"], geometry="geometry", crs=self.gdf.crs)
            self.protocol_map.setdefault("p1", g_p1)
        except Exception:
            self.protocol_map.setdefault("p1", gpd.GeoDataFrame(columns=["name", "kind", "geometry"], geometry="geometry", crs=self.gdf.crs if self.gdf is not None else None))

        # p2: outer, inner, start_line
        try:
            outer = self._get_first_named_geom("P2 Outer") or self._get_first_named_geom("OuterLine")
            inner = self._get_first_named_geom("P2 Inner") or self._get_first_named_geom("Inner Line")
            start = self._get_first_named_geom("P2 Start") or self._get_first_named_geom("Start Line")
            rows = []
            if outer is not None:
                rows.append({"name": "P2 OuterLine", "kind": "outer", "geometry": outer})
            if inner is not None:
                rows.append({"name": "P2 InnerLine", "kind": "inner", "geometry": inner})
            if start is not None:
                rows.append({"name": "P2 Start Line", "kind": "start_line", "geometry": start})
            # fallback: if none found, try to pull LineStrings from protocol subset
            if not rows and "p2" in self.protocol_map and self.protocol_map["p2"] is not None:
                candidates = self.protocol_map["p2"]
                lines = candidates[candidates.geometry.type.isin(["LineString", "MultiLineString"])].copy()
                for i, (_, r) in enumerate(lines.iterrows()):
                    kind = ["outer", "inner", "start_line"][i] if i < 3 else f"line_{i}"
                    rows.append({"name": r.get("name", f"p2_line_{i}"), "kind": kind, "geometry": r.geometry})
            g_p2 = gpd.GeoDataFrame(rows, crs=self.gdf.crs) if rows else gpd.GeoDataFrame(columns=["name", "kind", "geometry"], geometry="geometry", crs=self.gdf.crs)
            self.protocol_map.setdefault("p2", g_p2)
        except Exception:
            self.protocol_map.setdefault("p2", gpd.GeoDataFrame(columns=["name", "kind", "geometry"], geometry="geometry", crs=self.gdf.crs if self.gdf is not None else None))

        # p3: trail and crossing with p2 start_line
        try:
            trail = self._get_first_named_geom("P3 Trail") or self._get_first_named_geom("Trail")
            # try to get start_line from p2 artifacts
            start_line = None
            if "p2" in self.protocol_map and not self.protocol_map["p2"].empty:
                df2 = self.protocol_map["p2"]
                matches = df2[df2["kind"] == "start_line"]
                if not matches.empty:
                    start_line = matches.iloc[0].geometry
            if start_line is None:
                start_line = self._get_first_named_geom("P2 Start") or self._get_first_named_geom("Start Line")

            rows = []
            if trail is not None:
                rows.append({"name": "P3 Trail", "kind": "trail", "geometry": trail})
            # compute crossing point if possible
            crossing = None
            if trail is not None and start_line is not None:
                try:
                    cross_geom = trail.intersection(start_line)
                    # handle multi/complex results
                    if cross_geom.is_empty:
                        crossing = None
                    else:
                        if cross_geom.geom_type == "Point":
                            crossing = cross_geom
                        else:
                            # take first point or centroid
                            try:
                                crossing = list(cross_geom.geoms)[0]
                            except Exception:
                                crossing = cross_geom.centroid
                except Exception:
                    crossing = None
            if crossing is not None:
                rows.append({"name": "P3 Crossing", "kind": "crossing", "geometry": crossing})

            g_p3 = gpd.GeoDataFrame(rows, crs=self.gdf.crs) if rows else gpd.GeoDataFrame(columns=["name", "kind", "geometry"], geometry="geometry", crs=self.gdf.crs)
            self.protocol_map.setdefault("p3", g_p3)
        except Exception:
            self.protocol_map.setdefault("p3", gpd.GeoDataFrame(columns=["name", "kind", "geometry"], geometry="geometry", crs=self.gdf.crs if self.gdf is not None else None))

    def get_protocol_refs(self, key: str) -> gpd.GeoDataFrame:
        """Retorna el GeoDataFrame de referencias para el protocolo `key`."""
        if key not in self.protocol_map:
            raise KeyError(f"Protocol key not found: {key}")
        return self.protocol_map[key].copy()

    def get_named_ref(self, name: str) -> gpd.GeoDataFrame:
        """Retorna las features del KML cuyo nombre contenga `name` (case-insensitive)."""
        if self.gdf is None:
            raise RuntimeError("No KML loaded. Call load() first.")
        mask = self.gdf["name"].astype(str).str.contains(name, case=False, na=False)
        return self.gdf.loc[mask].copy()

    @staticmethod
    def lonlat_to_utm_epsg(lon: float, lat: float) -> int:
        """Calcular EPSG UTM (WGS84) para lon/lat."""
        zone = int(( (lon + 180) // 6 ) % 60) + 1
        if lat >= 0:
            return 32600 + zone
        return 32700 + zone

    def project_refs_to_utm_for_points(self, gdf_points: gpd.GeoDataFrame, protocol_key: Optional[str] = None) -> int:
        """Calcula EPSG UTM desde el centroid de `gdf_points` y proyecta las referencias a ese EPSG.

        Si `protocol_key` se proporciona, proyecta solo ese subset en `self.protocol_map`.
        Retorna el EPSG calculado.
        """
        if gdf_points is None or gdf_points.empty:
            raise ValueError("gdf_points is empty or None")
        # asegurarnos que gdf_points esté en EPSG:4326 antes de calcular lon/lat
        pts_geo = gdf_points
        if pts_geo.crs is not None and pts_geo.crs.to_string() != "EPSG:4326":
            try:
                pts_geo = pts_geo.to_crs("EPSG:4326")
            except Exception:
                # si no es posible reproyectar, asumimos ya está en lon/lat
                pts_geo = gdf_points

        centroid = pts_geo.unary_union.centroid
        lon, lat = float(centroid.x), float(centroid.y)
        epsg = self.lonlat_to_utm_epsg(lon, lat)
        # proyectar
        if protocol_key is None:
            if self.gdf is not None:
                self.gdf = self.gdf.to_crs(f"EPSG:{epsg}")
            for k, df in list(self.protocol_map.items()):
                self.protocol_map[k] = df.to_crs(f"EPSG:{epsg}")
            return epsg
        if protocol_key not in self.protocol_map:
            raise KeyError(protocol_key)
        self.protocol_map[protocol_key] = self.protocol_map[protocol_key].to_crs(f"EPSG:{epsg}")
        return epsg

    def get_p1_point(self) -> gpd.GeoDataFrame:
        """Retorna el feature llamado 'P1 Point' si existe."""
        return self.get_named_ref("P1 Point")

    def get_p2_components(self) -> Dict[str, gpd.GeoDataFrame]:
        """Retorna dict con keys: inner, outer, start_line para P2 si existen."""
        return {
            "inner": self.get_named_ref("P2 Inner Line"),
            "outer": self.get_named_ref("P2 OuterLine"),
            "start_line": self.get_named_ref("P2 Start Line"),
        }

    def get_p3_components(self) -> Dict[str, gpd.GeoDataFrame]:
        """Retorna dict con keys: trail, start_line para P3 si existen."""
        return {
            "trail": self.get_named_ref("P3 Trail"),
            "start_line": self.get_named_ref("P3 Start Line"),
        }

    def project_refs_to_epsg(self, epsg: int, protocol_key: Optional[str] = None) -> None:
        """Proyecta las referencias (todas o una) al EPSG indicado y actualiza los objetos guardados."""
        if self.gdf is None:
            raise RuntimeError("No KML loaded. Call load() first.")
        if protocol_key is None:
            # proyectar la capa completa y también cada subset si existe
            self.gdf = self.gdf.to_crs(f"EPSG:{epsg}")
            for k, df in list(self.protocol_map.items()):
                self.protocol_map[k] = df.to_crs(f"EPSG:{epsg}")
            return
        if protocol_key not in self.protocol_map:
            raise KeyError(protocol_key)
        self.protocol_map[protocol_key] = self.protocol_map[protocol_key].to_crs(f"EPSG:{epsg}")

    def attach_refs_to_gdf(self, gdf_points: gpd.GeoDataFrame, protocol_key: str = "p1", how: str = "nearest", distance_col: str = "dist_m") -> gpd.GeoDataFrame:
        """Adjunta referencias al GeoDataFrame de puntos.

        Requiere que `gdf_points` y las referencias estén en el mismo CRS métrico (UTM) cuando se use 'nearest'.
        """
        if protocol_key not in self.protocol_map:
            raise KeyError(protocol_key)
        gdf_refs = self.protocol_map[protocol_key]
        if gdf_points.crs != gdf_refs.crs:
            raise ValueError("CRS mismatch: project both GeoDataFrames to the same CRS (metric) before joining")
        if how == "nearest":
            joined = gpd.sjoin_nearest(gdf_points, gdf_refs, how="left", distance_col=distance_col)
            return joined
        elif how == "within":
            joined = gpd.sjoin(gdf_points, gdf_refs, how="left", predicate="within")
            return joined
        else:
            raise ValueError("Unknown how: expected 'nearest' or 'within'")


if __name__ == "__main__":
    import sys

    if len(sys.argv) < 2:
        print("Usage: python -m src.KML_protocol_analyzer path/to/Planificacion.kml")
        raise SystemExit(1)
    path = Path(sys.argv[1])
    a = KMLProtocolAnalyzer()
    gdf = a.load(path)
    print("Loaded features:", len(gdf))
    a.split_by_protocol()
    for k in a.protocol_map:
        print(k, len(a.protocol_map[k]))
