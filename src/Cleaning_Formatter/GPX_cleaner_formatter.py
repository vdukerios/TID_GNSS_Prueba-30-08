"""
GPX_cleaner_formatter.py

Clase orientada a objetos para limpiar (filtrar por tiempo) y formatear tracks GPX a UTM.
"""

from __future__ import annotations
from typing import List, Optional, Union, Tuple
import os
from pathlib import Path
import math
import warnings
from datetime import datetime
import pandas as pd
import numpy as np
import gpxpy
import geopandas as gpd
from shapely.geometry import Point


class GPXCleanerFormatter:
	"""
	Clase para cargar múltiples GPX, filtrar por rango horario y proyectar a UTM.

	Uso resumido:
		cleaner = GPXCleanerFormatter()
		cleaner.load(["a.gpx", "b.gpx"])
		cleaner.filter_time_range("2025-08-01T10:00:00", "2025-08-01T10:05:00")
		cleaner.to_utm()
		cleaner.save_gpkg("outputs/protocols_clean.gpkg")
	"""

	def __init__(self):
		self.file_paths: List[str] = []
		self.df: Optional[pd.DataFrame] = None
		self.gdf: Optional[gpd.GeoDataFrame] = None  # EPSG:4326
		self.gdf_utm: Optional[gpd.GeoDataFrame] = None

	def _normalize_source_files(self, source_files: Optional[Union[str, List[str]]]) -> Optional[List[str]]:
		"""Internal: normaliza un source_files a lista o None."""
		if source_files is None:
			return None
		if isinstance(source_files, str):
			return [source_files]
		return list(source_files)

	def _mask_for_sources(self, source_files: Optional[Union[str, List[str]]]) -> Optional[pd.Series]:
		"""Devuelve una máscara booleana sobre self.gdf para los source_files dados, o None si no aplica."""
		sources = self._normalize_source_files(source_files)
		if sources is None:
			return None
		if self.gdf is None:
			raise RuntimeError("No data loaded. Run load() first.")
		return self.gdf["source_file"].isin(sources)

	def load(self, file_paths: List[str], progress: bool = False) -> gpd.GeoDataFrame:
		"""
		Lee y concatena múltiples archivos GPX en un GeoDataFrame en EPSG:4326.

		- file_paths: lista de rutas a archivos GPX
		- progress: imprime progreso mínimo

		Retorna la GeoDataFrame creada y la almacena en self.gdf.
		"""
		self.file_paths = list(file_paths)
		rows = []
		for i, fp in enumerate(self.file_paths):
			start_len = len(rows)
			if not os.path.isfile(fp):
				warnings.warn(f"GPX file not found: {fp}")
				continue
			if progress:
				print(f"Reading GPX {i+1}/{len(self.file_paths)}: {fp}")
			with open(fp, "r", encoding="utf-8") as fh:
				gpx = gpxpy.parse(fh)

				# tracks -> segments -> points
			for t_idx, track in enumerate(gpx.tracks):
				for s_idx, segment in enumerate(track.segments):
					for p_idx, pt in enumerate(segment.points):
						rows.append(
							{
								"latitude": pt.latitude,
								"longitude": pt.longitude,
								"elevation": pt.elevation,
								"time": pt.time,
								"source_file": fp,
								"track_id": t_idx,
								"segment_id": s_idx,
								"pt_index": p_idx,
							}
						)

			# waypoints
			for w in getattr(gpx, "waypoints", []):
				rows.append(
					{
						"latitude": w.latitude,
						"longitude": w.longitude,
						"elevation": w.elevation,
						"time": getattr(w, "time", None),
						"source_file": fp,
						"track_id": None,
						"segment_id": None,
						"pt_index": None,
					}
				)

			# routes (flatten)
			for r_idx, route in enumerate(getattr(gpx, "routes", [])):
				for p_idx, pt in enumerate(route.points):
					rows.append(
						{
							"latitude": pt.latitude,
							"longitude": pt.longitude,
							"elevation": pt.elevation,
							"time": getattr(pt, "time", None),
							"source_file": fp,
							"track_id": f"route_{r_idx}",
							"segment_id": None,
							"pt_index": p_idx,
						}
					)
			# report per-file count
			end_len = len(rows)
			new_pts = end_len - start_len
			print(f"DBG -> loaded {new_pts} points from {fp}")
		
		if not rows:
			self.df = pd.DataFrame(columns=["latitude", "longitude", "elevation", "time", "source_file", "track_id", "segment_id", "pt_index"])
			self.gdf = gpd.GeoDataFrame(self.df.copy(), geometry=[], crs="EPSG:4326")
			return self.gdf

		self.df = pd.DataFrame(rows)
		print(f"DBG -> total rows after loading all files: {len(self.df)}")
		# normalize time
		if "time" in self.df.columns:
			self.df["time"] = pd.to_datetime(self.df["time"])

		# use geopandas helper to create geometry Series (safer and preserves length)
		try:
			geometry = gpd.points_from_xy(self.df.longitude, self.df.latitude)
		except Exception:
			# fallback to manual creation
			geometry = [Point(xy) for xy in zip(self.df.longitude, self.df.latitude)]
		print(f"DBG -> geometry count: {len(geometry)}")
		self.gdf = gpd.GeoDataFrame(self.df.copy(), geometry=geometry, crs="EPSG:4326")
		return self.gdf

	def filter_time_range(self, start: Union[str, datetime], end: Union[str, datetime], inplace: bool = True, source_files: Optional[Union[str, List[str]]] = None) -> gpd.GeoDataFrame:
		"""
		Filtra puntos por rango de tiempo inclusivo. start/end aceptan strings parseables por pandas o datetimes.

		Si `source_files` es None, el filtro se aplica sobre todo el GeoDataFrame cargado.
		Si `source_files` es una ruta o lista de rutas, el filtro se aplicará solo a esos archivos y
		- si inplace=True reemplazará las filas correspondientes dentro de `self.gdf`.

		- inplace: si True actualiza self.gdf/self.df (comportamiento descrito arriba)
		"""
		if self.gdf is None:
			raise RuntimeError("No data loaded. Run load() first.")

		start_ts = pd.to_datetime(start)
		end_ts = pd.to_datetime(end)

		# Normalize timezone awareness: make start/end compatible with self.gdf['time']
		series_tz = getattr(self.gdf["time"].dt, "tz", None)
		def _normalize_ts(ts, target_tz):
			# ensure pandas Timestamp
			ts = pd.to_datetime(ts)
			# pandas Timestamp stores tz in .tz
			try:
				ts_tz = ts.tz
			except Exception:
				ts_tz = None
			# target_tz not None => make ts tz-aware in that tz
			if target_tz is not None:
				if ts_tz is None:
					return ts.tz_localize(target_tz)
				else:
					return ts.tz_convert(target_tz)
			# target_tz is None => make ts tz-naive
			if ts_tz is not None:
				# fallback: convert to python datetime and drop tzinfo
				py = ts.to_pydatetime().replace(tzinfo=None)
				return pd.Timestamp(py)
			return ts

		start_ts = _normalize_ts(start_ts, series_tz)
		end_ts = _normalize_ts(end_ts, series_tz)
		source_mask = self._mask_for_sources(source_files)
		if source_mask is None:
			mask = (self.gdf["time"] >= start_ts) & (self.gdf["time"] <= end_ts)
			filtered = self.gdf.loc[mask].copy()
			if inplace:
				self.gdf = filtered
				self.df = pd.DataFrame(filtered.drop(columns="geometry"))
			return filtered
		# si source_mask no es None, aplicamos solo a esa porción
		subset = self.gdf.loc[source_mask].copy()
		mask = (subset["time"] >= start_ts) & (subset["time"] <= end_ts)
		filtered_subset = subset.loc[mask].copy()
		if inplace:
			# reemplazar filas en self.gdf solamente para esos source_files
			other = self.gdf.loc[~source_mask].copy()
			self.gdf = pd.concat([other, filtered_subset], ignore_index=True)
			# reconstruir df
			self.df = pd.DataFrame(self.gdf.drop(columns="geometry"))
		return filtered_subset

	@staticmethod
	def lonlat_to_utm_epsg(lon: float, lat: float) -> int:
		"""Devuelve EPSG UTM (WGS84) desde lon/lat."""
		zone = int((math.floor((lon + 180) / 6) % 60) + 1)
		if lat >= 0:
			return 32600 + zone
		return 32700 + zone

	def to_utm(self, epsg: Optional[int] = None, inplace: bool = True, source_files: Optional[Union[str, List[str]]] = None) -> gpd.GeoDataFrame:
		"""
		Proyecta self.gdf (EPSG:4326) a UTM. Si epsg es None se calcula por el centroid de la capa o del subset
		cuando `source_files` está presente.
		"""
		if self.gdf is None:
			raise RuntimeError("No data loaded. Run load() first.")
		source_mask = self._mask_for_sources(source_files)
		if source_mask is None:
			# comportamiento existente: proyectar toda la capa
			if epsg is None:
				# centroid puede fallar si la geometría está vacía o es inválida; usar fallback
				try:
					centroid = self.gdf.unary_union.centroid
					lon, lat = centroid.x, centroid.y
					# centroid puede no tener coordenadas útiles
					if lon is None or lat is None:
						raise ValueError("empty centroid")
				except Exception:
					# fallback: usar media aritmética de las coordenadas de los puntos
					xs = self.gdf.geometry.x
					ys = self.gdf.geometry.y
					lon = float(xs.mean()) if not xs.empty else 0.0
					lat = float(ys.mean()) if not ys.empty else 0.0
				epsg = self.lonlat_to_utm_epsg(lon, lat)
			target = f"EPSG:{epsg}"
			proj = self.gdf.to_crs(target)
			if inplace:
				self.gdf_utm = proj
				return self.gdf_utm
			return proj.copy()
		# si se pide por source_files, proyectar solo esa porción y retornarla
		subset = self.gdf.loc[source_mask].copy()
		if subset.empty:
			return gpd.GeoDataFrame(columns=list(self.gdf.columns), crs=self.gdf.crs)
		if epsg is None:
			# igual que arriba: intentar centroid y caer a media de puntos si falla
			try:
				centroid = subset.unary_union.centroid
				lon, lat = centroid.x, centroid.y
				if lon is None or lat is None:
					raise ValueError("empty centroid")
			except Exception:
				xs = subset.geometry.x
				ys = subset.geometry.y
				lon = float(xs.mean()) if not xs.empty else 0.0
				lat = float(ys.mean()) if not ys.empty else 0.0
			epsg = self.lonlat_to_utm_epsg(lon, lat)
		target = f"EPSG:{epsg}"
		proj_subset = subset.to_crs(target)
		if inplace:
			# merge en self.gdf_utm (crear si no existe)
			if self.gdf_utm is None:
				# proyectar todo primero para mantener consistencia de columnas
				self.gdf_utm = self.gdf.to_crs(target)
			else:
				# reemplazar filas que correspondan a source_files
				mask = self.gdf_utm["source_file"].isin(self._normalize_source_files(source_files))
				other = self.gdf_utm.loc[~mask].copy()
				# append proyectado subset
				self.gdf_utm = pd.concat([other, proj_subset], ignore_index=True)
			return proj_subset
		return proj_subset.copy()

	def save_gpkg(self, path: str, use_utm: bool = True, source_files: Optional[Union[str, List[str]]] = None) -> None:
		"""Guarda el GeoDataFrame a GeoPackage. Si `source_files` dado guarda solo ese subset."""
		# choose the best available GeoDataFrame (prefer non-empty UTM when requested)
		gdf = None
		if source_files is not None:
			src_mask = self._mask_for_sources(source_files)
			if src_mask is None:
				raise RuntimeError("source_files provided but no data loaded")
			# prefer UTM subset if available and non-empty
			if use_utm and self.gdf_utm is not None and not getattr(self.gdf_utm, "empty", True):
				gdf = self.gdf_utm.loc[src_mask].copy()
			elif self.gdf is not None and not getattr(self.gdf, "empty", True):
				gdf = self.gdf.loc[src_mask].copy()
		else:
			if use_utm and self.gdf_utm is not None and not getattr(self.gdf_utm, "empty", True):
				gdf = self.gdf_utm
			elif self.gdf is not None and not getattr(self.gdf, "empty", True):
				gdf = self.gdf

		# if nothing selected but we have file_paths, attempt to reload (recover from prior mutation)
		if (gdf is None or getattr(gdf, "empty", True)) and (self.df is None or getattr(self.df, "empty", True)) and getattr(self, "file_paths", None):
			print("WARN -> data appears empty at save time; attempting to reload source files from self.file_paths")
			try:
				self.load(self.file_paths)
			except Exception as e:
				print(f"WARN -> reload failed: {e}")
			# retry selection after reload
			if use_utm and self.gdf_utm is not None and not getattr(self.gdf_utm, "empty", True):
				gdf = self.gdf_utm
			elif self.gdf is not None and not getattr(self.gdf, "empty", True):
				gdf = self.gdf

		if gdf is None:
			raise RuntimeError("No GeoDataFrame to save. Load data first.")

		# Diagnostics and normalization before writing
		out_path = Path(path)
		out_dir = out_path.parent
		os.makedirs(str(out_dir), exist_ok=True)
		# we'll write GeoPackage (and GeoJSON) regardless of requested extension
		out_gpkg = out_path.with_suffix('.gpkg')
		out_geojson = out_path.with_suffix('.geojson')
		layer_name = out_path.stem
		print(f"DBG -> preparing outputs: gpkg={out_gpkg}, geojson={out_geojson}")
		print(f"DBG -> candidate gdf rows={len(gdf)}, columns={list(gdf.columns)}")
		try:
			print(gdf.dtypes)
		except Exception:
			pass
		# geometry checks
		try:
			geom_valid = int(gdf.geometry.is_valid.sum()) if "geometry" in gdf.columns else 0
			print(f"DBG -> geometry valid: {geom_valid}/{len(gdf)}")
			print(f"DBG -> bounds: {gdf.total_bounds}")
		except Exception:
			pass

		# Normalize time columns to avoid timezone serialization issues in pyarrow/pyarrow-pandas
		if "time" in gdf.columns:
			try:
				# create an ISO string column as a portable fallback
				gdf["time_iso"] = gdf["time"].apply(lambda x: x.isoformat() if pd.notnull(x) else None)
			except Exception:
				# last-resort: stringify
				gdf["time_iso"] = gdf["time"].astype(str)
			# also try to produce a tz-naive UTC timestamp column for consumers that expect datetimes
			try:
				times = pd.to_datetime(gdf["time"], utc=True)
				# convert to numpy datetime64[ns] (UTC) then back to pandas Timestamp (tz-naive)
				gdf["time_utc"] = pd.to_datetime(times.values.astype("datetime64[ns]"))
			except Exception:
				# ignore if conversion fails; consumers can use time_iso
				pass

		# If gdf is empty, try fallback to self.df to preserve rows
		if getattr(gdf, "empty", True):
			print(f"WARN -> selected GeoDataFrame is empty; attempting fallback to original attributes")
			if self.df is not None and not self.df.empty:
				df_attr = self.df.copy()
				# try to reconstruct geometry from lat/lon
				try:
					geom = gpd.points_from_xy(df_attr.longitude, df_attr.latitude)
				except Exception:
					geom = [Point(xy) for xy in zip(df_attr.longitude, df_attr.latitude)]
				gdf_write = gpd.GeoDataFrame(df_attr.drop(columns=[c for c in df_attr.columns if c == 'geometry' ], errors='ignore'), geometry=geom, crs='EPSG:4326')
			else:
				# nothing to write
				print("ERROR -> no data available to write (both gdf and self.df empty)")
				return
		else:
			gdf_write = gdf.copy()

		# Ensure we write GeoPackage in EPSG:4326 (common portable CRS) and GeoJSON in 4326
		try:
			# write gpkg with given layer name
			gdf_write.to_file(str(out_gpkg), driver="GPKG", layer=layer_name)
			print(f"WROTE -> {out_gpkg} layer={layer_name} rows={len(gdf_write)}")
		except Exception as e:
			print(f"ERROR -> failed writing GeoPackage: {e}")

		try:
			gdf_write.to_crs("EPSG:4326").to_file(str(out_geojson), driver="GeoJSON")
			print(f"WROTE -> {out_geojson} rows={len(gdf_write)}")
		except Exception as e:
			print(f"ERROR -> failed writing GeoJSON: {e}")

		"""Retorna DataFrame (atributos) completo o subset indicado por `source_files`."""
		if self.df is None:
			raise RuntimeError("df not available. Run load() first.")
		if source_files is None:
			return self.df
		srcs = self._normalize_source_files(source_files)
		return self.df[self.df["source_file"].isin(srcs)].copy()


__all__ = ["GPXCleanerFormatter"]

