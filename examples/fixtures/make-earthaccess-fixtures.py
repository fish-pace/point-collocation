# This shows how the examples of earthaccess search results and PACE L2 and L3 netcdfs look like

# data point
time = "2025-04-09"
lat = 30.0
lon = -89.0
import earthaccess
import xarray as xr

# Login
auth = earthaccess.login()
results = earthaccess.search_data(
    short_name="PACE_OCI_L3M_RRS",
    temporal=(time, time),
    granule_name="*.DAY.*.4km.*"
)
import json
from pathlib import Path

out = Path("/home/jovyan/earthaccess_matchupexamples/fixtures/earthaccess_results_sample_l3.json")

samples = []
for r in results[:5]:
    # pick the best extraction method that works in your env
    d = getattr(r, "data", None)
    if d is None:
        d = r.__dict__
    samples.append(d)

out.write_text(json.dumps(samples, indent=2, default=str))
print("wrote:", out.resolve())

import earthaccess
import xarray as xr

# Login
auth = earthaccess.login()
results = earthaccess.search_data(
    short_name = 'PACE_OCI_L2_AOP',
    temporal = ("2025-03-05", "2025-03-05"),
    bounding_box = (-90.0, 40.0, -75.0, 47.0)
)
import json
from pathlib import Path

out = Path("/home/jovyan/earthaccess_matchup/examples/fixtures/earthaccess_results_sample_l2.json")

samples = []
for r in results:
    d = getattr(r, "data", None)
    if d is None:
        d = r.__dict__
    samples.append(d)

out.write_text(json.dumps(samples, indent=2, default=str))
print("wrote:", out.resolve())

# Each NetCDF fixture is <1 MB and intended only for structural inspection and examples.
import earthaccess
import xarray as xr
auth = earthaccess.login()
results = earthaccess.search_data(
    short_name = 'PACE_OCI_L3M_RRS',
    temporal = ("2025-03-05", "2025-03-05"),
)
f = earthaccess.open(results)
ds = xr.open_dataset(f[0])
ds = ds.sel(
    lon=slice(-162, -160),
    lat=slice(2, 0)
)
ds.to_netcdf("/home/jovyan/earthaccess_matchup/examples/fixtures/pace_l3_sample.nc")

import earthaccess
import xarray as xr
auth = earthaccess.login()
results = earthaccess.search_data(
    short_name = 'PACE_OCI_L2_AOP',
    temporal = ("2025-03-05", "2025-03-05"),
    bounding_box = (-90.0, 40.0, -75.0, 47.0)
)
f = earthaccess.open(results)
dt = xr.open_datatree(f[0])
dt_small = dt.isel(number_of_bands=slice(0,10), 
        number_of_reflective_bands=slice(0,10),
        number_of_lines=slice(0,10),
        pixels_per_line=slice(0,10))
dt_small.to_netcdf("/home/jovyan/earthaccess_matchup/examples/fixtures/pace_l2_sample.nc")
