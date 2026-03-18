# Level 2 matchups with PACE data

* Create a plan for files to use `pc.plan()`
* Print the plan to check it `print(plan.summary())`
* Do the plan and get matchups `pc.matchup(plan, open_method="datatree-merge", spatial_method="xoak-kdtree")`

## Prerequisite -- Login to EarthData

The examples here use NASA EarthData and you need to have an account with EarthData. Make sure you can login.


```python
import earthaccess
import xoak
earthaccess.login()
```




    <earthaccess.auth.Auth at 0x7fc30a694d70>



## Here are the level 2 datasets


```python
import earthaccess
results = earthaccess.search_datasets(instrument="oci")

short_names = [
    item.summary()["short-name"]
    for item in results
    if "L2" in item.summary()["short-name"]
]

print(short_names)
```

    ['PACE_OCI_L2_UVAI_UAA_NRT', 'PACE_OCI_L2_UVAI_UAA', 'PACE_OCI_L2_AER_UAA_NRT', 'PACE_OCI_L2_AER_UAA', 'PACE_OCI_L2_AOP_NRT', 'PACE_OCI_L2_AOP', 'PACE_OCI_L2_CLOUD_MASK_NRT', 'PACE_OCI_L2_CLOUD_MASK', 'PACE_OCI_L2_CLOUD_NRT', 'PACE_OCI_L2_CLOUD', 'PACE_OCI_L2_LANDVI_NRT', 'PACE_OCI_L2_LANDVI', 'PACE_OCI_L2_BGC_NRT', 'PACE_OCI_L2_BGC', 'PACE_OCI_L2_IOP_NRT', 'PACE_OCI_L2_IOP', 'PACE_OCI_L2_PAR_NRT', 'PACE_OCI_L2_PAR', 'PACE_OCI_L2_SFREFL_NRT', 'PACE_OCI_L2_SFREFL', 'PACE_OCI_L2_TRGAS_NRT', 'PACE_OCI_L2_TRGAS']


## Load some points


```python
import pandas as pd
url = (
    "https://raw.githubusercontent.com/"
    "fish-pace/point-collocation/main/"
    "examples/fixtures/points.csv"
)
df_points = pd.read_csv(url)
print(len(df_points))
df_points.head()
```

    595





<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>lat</th>
      <th>lon</th>
      <th>date</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>27.3835</td>
      <td>-82.7375</td>
      <td>2024-06-13</td>
    </tr>
    <tr>
      <th>1</th>
      <td>27.1190</td>
      <td>-82.7125</td>
      <td>2024-06-14</td>
    </tr>
    <tr>
      <th>2</th>
      <td>26.9435</td>
      <td>-82.8170</td>
      <td>2024-06-14</td>
    </tr>
    <tr>
      <th>3</th>
      <td>26.6875</td>
      <td>-82.8065</td>
      <td>2024-06-14</td>
    </tr>
    <tr>
      <th>4</th>
      <td>26.6675</td>
      <td>-82.6455</td>
      <td>2024-06-14</td>
    </tr>
  </tbody>
</table>
</div>



## Get a plan for matchups for 1st 50 points from PACE data


```python
%%time
# time 11 s / 5 s
import point_collocation as pc
plan = pc.plan(
    df_points[0:50], 	
    data_source="earthaccess",
    source_kwargs={
        "short_name": "PACE_OCI_L2_AOP",
    },
    time_buffer="12h"
)
```

    CPU times: user 640 ms, sys: 61 ms, total: 701 ms
    Wall time: 9.02 s



```python
plan.summary()
```

    Plan: 50 points → 13 unique granule(s)
      Points with 0 matches : 0
      Points with >1 matches: 10
      Time buffer: 0 days 12:00:00
    
    First 5 point(s):
      [0] lat=27.3835, lon=-82.7375, time=2024-06-13 12:00:00: 2 match(es)
        → https://obdaac-tea.earthdatacloud.nasa.gov/ob-cumulus-prod-public/PACE_OCI.20240613T171620.L2.OC_AOP.V3_1.nc
        → https://obdaac-tea.earthdatacloud.nasa.gov/ob-cumulus-prod-public/PACE_OCI.20240613T184939.L2.OC_AOP.V3_1.nc
      [1] lat=27.1190, lon=-82.7125, time=2024-06-14 12:00:00: 1 match(es)
        → https://obdaac-tea.earthdatacloud.nasa.gov/ob-cumulus-prod-public/PACE_OCI.20240614T175104.L2.OC_AOP.V3_1.nc
      [2] lat=26.9435, lon=-82.8170, time=2024-06-14 12:00:00: 1 match(es)
        → https://obdaac-tea.earthdatacloud.nasa.gov/ob-cumulus-prod-public/PACE_OCI.20240614T175104.L2.OC_AOP.V3_1.nc
      [3] lat=26.6875, lon=-82.8065, time=2024-06-14 12:00:00: 1 match(es)
        → https://obdaac-tea.earthdatacloud.nasa.gov/ob-cumulus-prod-public/PACE_OCI.20240614T175104.L2.OC_AOP.V3_1.nc
      [4] lat=26.6675, lon=-82.6455, time=2024-06-14 12:00:00: 1 match(es)
        → https://obdaac-tea.earthdatacloud.nasa.gov/ob-cumulus-prod-public/PACE_OCI.20240614T175104.L2.OC_AOP.V3_1.nc



```python
%%time
# This uses open_method="auto". It will try xr.open_dataset
# discover no lat/lon and then try xr.open_datatree + merge. 
# If you know, the netcdfs are grouped, you can pass in
# open_method="datatree-merge" yourself
plan.open_dataset(0)
```

    open_method: {'xarray_open': 'datatree', 'open_kwargs': {'chunks': {}, 'engine': 'h5netcdf', 'decode_timedelta': False}, 'coords': 'auto', 'set_coords': True, 'dim_renames': None, 'auto_align_phony_dims': None, 'merge': 'all', 'merge_kwargs': {}}
    
    Dimensions: {'number_of_bands': 286, 'number_of_reflective_bands': 286, 'wavelength_3d': 172, 'number_of_lines': 1710, 'pixels_per_line': 1272}
    
    Variables: ['wavelength', 'vcal_gain', 'vcal_offset', 'F0', 'aw', 'bbw', 'k_oz', 'k_no2', 'Tau_r', 'year', 'day', 'msec', 'time', 'detnum', 'mside', 'slon', 'clon', 'elon', 'slat', 'clat', 'elat', 'csol_z', 'Rrs', 'Rrs_unc', 'aot_865', 'angstrom', 'avw', 'nflh', 'l2_flags', 'longitude', 'latitude', 'tilt']
    
    Geolocation: ('longitude', 'latitude') — lon dims=('number_of_lines', 'pixels_per_line'), lat dims=('number_of_lines', 'pixels_per_line')
    
    DataTree groups (detail):
      /
        Dimensions: {}
        Variables: []
      /sensor_band_parameters
        Dimensions: {'number_of_bands': 286, 'number_of_reflective_bands': 286, 'wavelength_3d': 172}
        Variables: ['wavelength', 'vcal_gain', 'vcal_offset', 'F0', 'aw', 'bbw', 'k_oz', 'k_no2', 'Tau_r']
      /scan_line_attributes
        Dimensions: {'number_of_lines': 1710}
        Variables: ['year', 'day', 'msec', 'time', 'detnum', 'mside', 'slon', 'clon', 'elon', 'slat', 'clat', 'elat', 'csol_z']
      /geophysical_data
        Dimensions: {'number_of_lines': 1710, 'pixels_per_line': 1272, 'wavelength_3d': 172}
        Variables: ['Rrs', 'Rrs_unc', 'aot_865', 'angstrom', 'avw', 'nflh', 'l2_flags']
      /navigation_data
        Dimensions: {'number_of_lines': 1710, 'pixels_per_line': 1272}
        Variables: ['longitude', 'latitude', 'tilt']
      /processing_control
        Dimensions: {}
        Variables: []
      /processing_control/input_parameters
        Dimensions: {}
        Variables: []
      /processing_control/flag_percentages
        Dimensions: {}
        Variables: []
    CPU times: user 513 ms, sys: 40.7 ms, total: 554 ms
    Wall time: 1.25 s


## Get the matchups using that plan

`pc.matchup()` with `open_method="datatree-merge"` opens each L2 granule as a DataTree and merges all groups into a flat dataset. Use `spatial_method="xoak-kdtree"` for 2-D swath geolocation. I turn on `batch_size=5` and `silent=False` to watch the progress.

Notice, that point 0 is matched to 2 granules and so has 2 rows with the same `pc_id`.


```python
%%time
# 1 min /
res = pc.matchup(plan, spatial_method="xoak-kdtree", variables=["Rrs"])
```

    CPU times: user 32.3 s, sys: 2.28 s, total: 34.6 s
    Wall time: 58.9 s



```python
res.head()
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>lat</th>
      <th>lon</th>
      <th>time</th>
      <th>pc_id</th>
      <th>granule_id</th>
      <th>granule_time</th>
      <th>granule_lat</th>
      <th>granule_lon</th>
      <th>Rrs_346</th>
      <th>Rrs_348</th>
      <th>...</th>
      <th>Rrs_706</th>
      <th>Rrs_707</th>
      <th>Rrs_708</th>
      <th>Rrs_709</th>
      <th>Rrs_711</th>
      <th>Rrs_712</th>
      <th>Rrs_713</th>
      <th>Rrs_714</th>
      <th>Rrs_717</th>
      <th>Rrs_719</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>27.3835</td>
      <td>-82.7375</td>
      <td>2024-06-13 12:00:00</td>
      <td>0</td>
      <td>https://obdaac-tea.earthdatacloud.nasa.gov/ob-...</td>
      <td>2024-06-13 17:18:49+00:00</td>
      <td>27.443144</td>
      <td>-82.612923</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>...</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
    </tr>
    <tr>
      <th>1</th>
      <td>27.3835</td>
      <td>-82.7375</td>
      <td>2024-06-13 12:00:00</td>
      <td>0</td>
      <td>https://obdaac-tea.earthdatacloud.nasa.gov/ob-...</td>
      <td>2024-06-13 18:52:08+00:00</td>
      <td>27.383293</td>
      <td>-82.721527</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>...</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
    </tr>
    <tr>
      <th>2</th>
      <td>27.1190</td>
      <td>-82.7125</td>
      <td>2024-06-14 12:00:00</td>
      <td>1</td>
      <td>https://obdaac-tea.earthdatacloud.nasa.gov/ob-...</td>
      <td>2024-06-14 17:53:34+00:00</td>
      <td>27.101389</td>
      <td>-82.717186</td>
      <td>0.01299</td>
      <td>0.012946</td>
      <td>...</td>
      <td>0.000238</td>
      <td>0.000228</td>
      <td>0.000198</td>
      <td>0.000194</td>
      <td>0.000186</td>
      <td>0.000172</td>
      <td>0.000152</td>
      <td>0.000122</td>
      <td>0.000108</td>
      <td>0.000094</td>
    </tr>
    <tr>
      <th>3</th>
      <td>26.9435</td>
      <td>-82.8170</td>
      <td>2024-06-14 12:00:00</td>
      <td>2</td>
      <td>https://obdaac-tea.earthdatacloud.nasa.gov/ob-...</td>
      <td>2024-06-14 17:53:34+00:00</td>
      <td>26.954554</td>
      <td>-82.810219</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>...</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
    </tr>
    <tr>
      <th>4</th>
      <td>26.6875</td>
      <td>-82.8065</td>
      <td>2024-06-14 12:00:00</td>
      <td>3</td>
      <td>https://obdaac-tea.earthdatacloud.nasa.gov/ob-...</td>
      <td>2024-06-14 17:53:34+00:00</td>
      <td>26.703817</td>
      <td>-82.817726</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>...</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
    </tr>
  </tbody>
</table>
<p>5 rows × 180 columns</p>
</div>



## Predefined profiles for opening granules

Granules that have groups can be opened with `xr.open_datatree()` but the user will need to specify how the groups are merged so that the lat, lon and variables can be found.  `point-collocation` has predefined profiles that you can use or modify.


```python
import point_collocation.profiles as pf
pf.pace_l2
```




    {'xarray_open': 'datatree', 'merge': 'all'}



You could modify this for PACE level 2 netcdfs by telling it to only merge the relevant groups. This doesn't actually affect speed or performance in this case.


```python
test = pf.pace_l2
test['merge'] = ['/geophysical_data', '/navigation_data']
```

Pass to `open_method`:


```python
%%time
out = pc.matchup(plan, open_method=test, variables=["Rrs"],
                     spatial_method="xoak-kdtree")
```

    CPU times: user 28 s, sys: 1.41 s, total: 29.4 s
    Wall time: 52 s



```python
plan.open_dataset(0, open_method=test)
```

    open_method: {'xarray_open': 'datatree', 'merge': ['/geophysical_data', '/navigation_data'], 'open_kwargs': {'chunks': {}, 'engine': 'h5netcdf', 'decode_timedelta': False}, 'coords': 'auto', 'set_coords': True, 'dim_renames': None, 'auto_align_phony_dims': None, 'merge_kwargs': {}}
    
    Dimensions: {'number_of_lines': 1710, 'pixels_per_line': 1272, 'wavelength_3d': 172}
    
    Variables: ['Rrs', 'Rrs_unc', 'aot_865', 'angstrom', 'avw', 'nflh', 'l2_flags', 'longitude', 'latitude', 'tilt']
    
    Geolocation: ('longitude', 'latitude') — lon dims=('number_of_lines', 'pixels_per_line'), lat dims=('number_of_lines', 'pixels_per_line')
    
    DataTree groups (detail):
      /
        Dimensions: {}
        Variables: []
      /sensor_band_parameters
        Dimensions: {'number_of_bands': 286, 'number_of_reflective_bands': 286, 'wavelength_3d': 172}
        Variables: ['wavelength', 'vcal_gain', 'vcal_offset', 'F0', 'aw', 'bbw', 'k_oz', 'k_no2', 'Tau_r']
      /scan_line_attributes
        Dimensions: {'number_of_lines': 1710}
        Variables: ['year', 'day', 'msec', 'time', 'detnum', 'mside', 'slon', 'clon', 'elon', 'slat', 'clat', 'elat', 'csol_z']
      /geophysical_data
        Dimensions: {'number_of_lines': 1710, 'pixels_per_line': 1272, 'wavelength_3d': 172}
        Variables: ['Rrs', 'Rrs_unc', 'aot_865', 'angstrom', 'avw', 'nflh', 'l2_flags']
      /navigation_data
        Dimensions: {'number_of_lines': 1710, 'pixels_per_line': 1272}
        Variables: ['longitude', 'latitude', 'tilt']
      /processing_control
        Dimensions: {}
        Variables: []
      /processing_control/input_parameters
        Dimensions: {}
        Variables: []
      /processing_control/flag_percentages
        Dimensions: {}
        Variables: []



```python

```
