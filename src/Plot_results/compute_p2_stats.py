from pathlib import Path
import geopandas as gpd
import pandas as pd
from shapely.ops import unary_union

BASE = Path(__file__).resolve().parents[2]
CLEAN_DIR = BASE / "Clean_Files"
P2_DIR = CLEAN_DIR / "protocolo2"

print('Loading refs...')
refs_p = P2_DIR / 'kml_refs_p2.geojson'
refs = gpd.read_file(refs_p)
print('Refs rows:', len(refs))
print(refs[['name']].to_string(index=False))

# load points
pts = []
for p in sorted(P2_DIR.glob('*_points.geojson')):
    g = gpd.read_file(p)
    if g is None or g.empty:
        continue
    g['_source_file'] = p.stem
    pts.append(g)

if not pts:
    print('No points found')
    raise SystemExit(0)

points_all = gpd.GeoDataFrame(pd.concat(pts, ignore_index=True), crs=pts[0].crs)
print('Total points:', len(points_all))

# ensure 4326
points_all = points_all.to_crs(epsg=4326)
refs = refs.to_crs(epsg=4326)

# find outer and inner
print('\nSearching for outer/inner by kind or name substring')
if 'kind' in refs.columns:
    print('Refs has kind column')

inner = refs[refs['name'].str.lower().str.contains('inner', na=False)]
outer = refs[refs['name'].str.lower().str.contains('outer', na=False)]
print('Found inner:', len(inner))
print('Found outer:', len(outer))

if len(outer)==0:
    print('No outer geometry found - abort')
    raise SystemExit(0)

# union
try:
    outer_union = outer.geometry.unary_union if hasattr(outer.geometry, 'unary_union') else unary_union(list(outer.geometry))
except Exception as e:
    print('outer union failed:', e)
    outer_union = unary_union(list(outer.geometry))

if len(inner)>0:
    try:
        inner_union = inner.geometry.unary_union if hasattr(inner.geometry, 'unary_union') else unary_union(list(inner.geometry))
    except Exception as e:
        print('inner union failed:', e)
        inner_union = unary_union(list(inner.geometry))
else:
    inner_union = None

# ring
if inner_union is not None:
    ring = outer_union.difference(inner_union)
else:
    ring = outer_union

print('Ring type:', type(ring), 'empty?', ring.is_empty)

# compute per-device inside counts
if '_source_file' in points_all.columns:
    groups = list(points_all.groupby('_source_file'))
else:
    groups = [('points', points_all)]

rows = []
for src, grp in groups:
    total = len(grp)
    try:
        inside = int(grp.geometry.within(ring).sum())
    except Exception as e:
        print('within failed for', src, 'error:', e)
        inside = 0
    pct = 100.0 * inside / total if total>0 else 0.0
    rows.append((src, inside, total, pct))

print('\nPer-device results:')
for r in rows:
    print(f"{r[0]}: {r[1]}/{r[2]} -> {r[3]:.1f}%")

# Also print a small HTML-like snippet similar to what the map would include
html_rows = ''.join([f"<tr><td>{r[0]}</td><td>{r[1]}/{r[2]}</td><td>{r[3]:.1f}%</td></tr>" for r in rows])
if html_rows:
    print('\nHTML table rows:\n')
    print(html_rows)
else:
    print('\nNo p2 stats calculated')
