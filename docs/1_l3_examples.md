# Level 3 matchups with PACE data

```
import point_collocation as pc
```

* Create a plan for files to use `pc.plan()`
* Print the plan to check it `print(plan.summary())`
* Do the plan and get matchups `pc.matchup(plan)`

## Read in some points


```python
import pandas as pd
time = "2025-04-09"
lat = 30.0
lon = -89.0

df = pd.DataFrame(
    {
        "lat": [lat],
        "lon": [lon],
        "time": [time],
    }
)
df["time"] = pd.to_datetime(df["time"])
```

## Create a plan


```python
%%time
# 485 ms / 7 s
import point_collocation as pc
plan = pc.plan(
    df,
    data_source="earthaccess",
    source_kwargs={
        "short_name": "PACE_OCI_L3M_Rrs",
        "granule_name": "*.8D.*.4km.*",
    }
)
```

    CPU times: user 26.6 ms, sys: 0 ns, total: 26.6 ms
    Wall time: 7.67 s


### Look at variables in that dataset


```python
%%time
# 359 ms
plan.show_variables()
```

    open_method  : 'auto'
    Dimensions : {'lat': 4320, 'lon': 8640, 'wavelength': 172, 'rgb': 3, 'eightbitcolor': 256}
    Variables  : ['Rrs', 'palette']
    
    Geolocation: ('lon', 'lat') — lon dims=('lon',), lat dims=('lat',)
    CPU times: user 97.5 ms, sys: 40.5 ms, total: 138 ms
    Wall time: 359 ms



```python
plan.summary()
```

    Plan: 1 points → 1 unique granule(s)
      Points with 0 matches : 0
      Points with >1 matches: 0
      Time buffer: 0 days 00:00:00
    
    First 1 point(s):
      [0] lat=30.0000, lon=-89.0000, time=2025-04-09 00:00:00: 1 match(es)
        → https://obdaac-tea.earthdatacloud.nasa.gov/ob-cumulus-prod-public/PACE_OCI.20250407_20250414.L3m.8D.RRS.V3_1.Rrs.4km.nc


## Get the matchups

For variables with a 3rd dimension, like wavelength, all variables will be shown with `_3rd dim value`.  The lat, lon, and time for the matching granules is added as a column. `pc_id` is the point id/row from the data you are matching. This is added in case there are multiple granules (files) per data point.|


```python
%%time
# orig / new 2 second
res = pc.matchup(plan, variables=["Rrs"])
res
```

    CPU times: user 767 ms, sys: 122 ms, total: 889 ms
    Wall time: 1.53 s





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
      <td>30.0</td>
      <td>-89.0</td>
      <td>2025-04-09</td>
      <td>0</td>
      <td>https://obdaac-tea.earthdatacloud.nasa.gov/ob-...</td>
      <td>2025-04-10 23:59:59+00:00</td>
      <td>30.020832</td>
      <td>-89.020828</td>
      <td>0.000306</td>
      <td>0.000488</td>
      <td>...</td>
      <td>0.003598</td>
      <td>0.003496</td>
      <td>0.003386</td>
      <td>0.003268</td>
      <td>0.003138</td>
      <td>0.003004</td>
      <td>0.00286</td>
      <td>0.002662</td>
      <td>0.002098</td>
      <td>0.001644</td>
    </tr>
  </tbody>
</table>
<p>1 rows × 180 columns</p>
</div>



### Datasets with only lat, lon

In this case, just the variable appears.


```python
%%time
# old 1.5 s / new 1.2 sseems to vary
import point_collocation as pc
plan = pc.plan(
    df,
    data_source="earthaccess",
    source_kwargs={
        "short_name": "PACE_OCI_L3M_AVW",
        "granule_name": "*.DAY.*.4km.*",
    }
)
res = pc.matchup(plan, variables=["avw"])
res
```

    CPU times: user 464 ms, sys: 21 ms, total: 485 ms
    Wall time: 1.2 s





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
      <th>avw</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>30.0</td>
      <td>-89.0</td>
      <td>2025-04-09</td>
      <td>0</td>
      <td>https://obdaac-tea.earthdatacloud.nasa.gov/ob-...</td>
      <td>2025-04-09 11:59:59+00:00</td>
      <td>30.020832</td>
      <td>-89.020828</td>
      <td>547.153259</td>
    </tr>
  </tbody>
</table>
</div>



## Plan with many files

If you are not sure what files to use, you can use a short name without `granule_name`. Then look at the plan summary to see the file names. You just need to look at one file (`n=1`). In this example, there are 16 files that match. 2 resolutions (4km and 0.1 deg) and 8 temporal resolutions:

* `R32`: rolling 32 days starting every 7 days, 4 dates
* `SNSP`: seasonal/quarterly
* `8D`: 8 day
* `DAY`: daily
* `MO`: monthly starting 1st day of each month to last


```python
%%time
import point_collocation as pc
plan = pc.plan(
    df,
    data_source="earthaccess",
    source_kwargs={
        "short_name": "PACE_OCI_L3M_AVW",
    }
)
```

    CPU times: user 33.9 ms, sys: 0 ns, total: 33.9 ms
    Wall time: 512 ms



```python
plan.summary(n=1)
```

    Plan: 1 points → 16 unique granule(s)
      Points with 0 matches : 0
      Points with >1 matches: 1
      Variables  : []
      Time buffer: 0 days 00:00:00
    
    First 1 point(s):
      [0] lat=30.0000, lon=-89.0000, time=2025-04-09 00:00:00: 16 match(es)
        → https://obdaac-tea.earthdatacloud.nasa.gov/ob-cumulus-prod-public/PACE_OCI.20250314_20250414.L3m.R32.AVW.V3_1.avw.0p1deg.nc
        → https://obdaac-tea.earthdatacloud.nasa.gov/ob-cumulus-prod-public/PACE_OCI.20250314_20250414.L3m.R32.AVW.V3_1.avw.4km.nc
        → https://obdaac-tea.earthdatacloud.nasa.gov/ob-cumulus-prod-public/PACE_OCI.20250321_20250620.L3m.SNSP.AVW.V3_1.avw.0p1deg.nc
        → https://obdaac-tea.earthdatacloud.nasa.gov/ob-cumulus-prod-public/PACE_OCI.20250321_20250620.L3m.SNSP.AVW.V3_1.avw.4km.nc
        → https://obdaac-tea.earthdatacloud.nasa.gov/ob-cumulus-prod-public/PACE_OCI.20250322_20250422.L3m.R32.AVW.V3_1.avw.0p1deg.nc
        → https://obdaac-tea.earthdatacloud.nasa.gov/ob-cumulus-prod-public/PACE_OCI.20250322_20250422.L3m.R32.AVW.V3_1.avw.4km.nc
        → https://obdaac-tea.earthdatacloud.nasa.gov/ob-cumulus-prod-public/PACE_OCI.20250330_20250430.L3m.R32.AVW.V3_1.avw.0p1deg.nc
        → https://obdaac-tea.earthdatacloud.nasa.gov/ob-cumulus-prod-public/PACE_OCI.20250330_20250430.L3m.R32.AVW.V3_1.avw.4km.nc
        → https://obdaac-tea.earthdatacloud.nasa.gov/ob-cumulus-prod-public/PACE_OCI.20250401_20250430.L3m.MO.AVW.V3_1.avw.0p1deg.nc
        → https://obdaac-tea.earthdatacloud.nasa.gov/ob-cumulus-prod-public/PACE_OCI.20250401_20250430.L3m.MO.AVW.V3_1.avw.4km.nc
        → https://obdaac-tea.earthdatacloud.nasa.gov/ob-cumulus-prod-public/PACE_OCI.20250407_20250414.L3m.8D.AVW.V3_1.avw.4km.nc
        → https://obdaac-tea.earthdatacloud.nasa.gov/ob-cumulus-prod-public/PACE_OCI.20250407_20250414.L3m.8D.AVW.V3_1.avw.0p1deg.nc
        → https://obdaac-tea.earthdatacloud.nasa.gov/ob-cumulus-prod-public/PACE_OCI.20250407_20250508.L3m.R32.AVW.V3_1.avw.4km.nc
        → https://obdaac-tea.earthdatacloud.nasa.gov/ob-cumulus-prod-public/PACE_OCI.20250407_20250508.L3m.R32.AVW.V3_1.avw.0p1deg.nc
        → https://obdaac-tea.earthdatacloud.nasa.gov/ob-cumulus-prod-public/PACE_OCI.20250409.L3m.DAY.AVW.V3_1.avw.0p1deg.nc
        → https://obdaac-tea.earthdatacloud.nasa.gov/ob-cumulus-prod-public/PACE_OCI.20250409.L3m.DAY.AVW.V3_1.avw.4km.nc


### Filter to the files you want

Once you see the files names, you can filter to the ones you want. using `granule_name`. For example `*.SNSP.*.4km.*` to get the seasonal (quarterly) values. `*` are wildcard values.


```python
%%time
import point_collocation as pc
plan = pc.plan(
    df,
    data_source="earthaccess",
    source_kwargs={
        "short_name": "PACE_OCI_L3M_AVW",
        "granule_name": "*.SNSP.*.4km.*"
    }
)
```

    CPU times: user 19.2 ms, sys: 0 ns, total: 19.2 ms
    Wall time: 2.6 s



```python
plan.summary()
```

    Plan: 1 points → 1 unique granule(s)
      Points with 0 matches : 0
      Points with >1 matches: 0
      Variables  : []
      Time buffer: 0 days 00:00:00
    
    First 1 point(s):
      [0] lat=30.0000, lon=-89.0000, time=2025-04-09 00:00:00: 1 match(es)
        → https://obdaac-tea.earthdatacloud.nasa.gov/ob-cumulus-prod-public/PACE_OCI.20250321_20250620.L3m.SNSP.AVW.V3_1.avw.4km.nc


## Try many points


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



## Get a plan for matchups from PACE data

For this example, we will just get a plan for the first 100 points so that it runs quickly.


```python
%%time
# 4-5 s
import earthaccess
import point_collocation as pc

earthaccess.login()

plan = pc.plan(
    df_points[0:100],
    data_source="earthaccess",
    source_kwargs={
        "short_name": "PACE_OCI_L3M_AVW",
        "granule_name": "*.DAY.*.4km.*",
    }
)
```

    CPU times: user 55 ms, sys: 0 ns, total: 55 ms
    Wall time: 5.89 s



```python
plan.summary(n=0)
```

    Plan: 100 points → 18 unique granule(s)
      Points with 0 matches : 0
      Points with >1 matches: 0
      Time buffer: 0 days 00:00:00


## Get 100 matchups using that plan


```python
%%time
# 8.3 s / 20.6s, later 8.6, 14, 11, 10 !!
res = pc.matchup(plan, variables = ["avw"])
```

    CPU times: user 7.02 s, sys: 298 ms, total: 7.32 s
    Wall time: 10.3 s



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
      <th>avw</th>
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
      <td>2024-06-13 11:59:59+00:00</td>
      <td>27.395832</td>
      <td>-82.729164</td>
      <td>NaN</td>
    </tr>
    <tr>
      <th>1</th>
      <td>27.1190</td>
      <td>-82.7125</td>
      <td>2024-06-14 12:00:00</td>
      <td>1</td>
      <td>https://obdaac-tea.earthdatacloud.nasa.gov/ob-...</td>
      <td>2024-06-14 11:59:59+00:00</td>
      <td>27.104164</td>
      <td>-82.729164</td>
      <td>NaN</td>
    </tr>
    <tr>
      <th>2</th>
      <td>26.9435</td>
      <td>-82.8170</td>
      <td>2024-06-14 12:00:00</td>
      <td>2</td>
      <td>https://obdaac-tea.earthdatacloud.nasa.gov/ob-...</td>
      <td>2024-06-14 11:59:59+00:00</td>
      <td>26.937498</td>
      <td>-82.812500</td>
      <td>NaN</td>
    </tr>
    <tr>
      <th>3</th>
      <td>26.6875</td>
      <td>-82.8065</td>
      <td>2024-06-14 12:00:00</td>
      <td>3</td>
      <td>https://obdaac-tea.earthdatacloud.nasa.gov/ob-...</td>
      <td>2024-06-14 11:59:59+00:00</td>
      <td>26.687498</td>
      <td>-82.812500</td>
      <td>NaN</td>
    </tr>
    <tr>
      <th>4</th>
      <td>26.6675</td>
      <td>-82.6455</td>
      <td>2024-06-14 12:00:00</td>
      <td>4</td>
      <td>https://obdaac-tea.earthdatacloud.nasa.gov/ob-...</td>
      <td>2024-06-14 11:59:59+00:00</td>
      <td>26.687498</td>
      <td>-82.645828</td>
      <td>NaN</td>
    </tr>
  </tbody>
</table>
</div>



## Try lots of products

Pick a recent data point so NRT works. Not all products have files.


```python
import pandas as pd
time = "2026-01-09"
lat = 30.0
lon = -89.0

df = pd.DataFrame(
    {
        "lat": [lat],
        "lon": [lon],
        "time": [time],
    }
)
df["time"] = pd.to_datetime(df["time"])
```


```python
import earthaccess
results = earthaccess.search_datasets(instrument="oci")

short_names = [
    item.summary()["short-name"]
    for item in results
    if "L3M" in item.summary()["short-name"]
]

print(short_names)
```

    ['PACE_OCI_L3M_UVAI_UAA_NRT', 'PACE_OCI_L3M_UVAI_UAA', 'PACE_OCI_L3M_AER_UAA_NRT', 'PACE_OCI_L3M_AER_UAA', 'PACE_OCI_L3M_AOT_NRT', 'PACE_OCI_L3M_AOT', 'PACE_OCI_L3M_AVW_NRT', 'PACE_OCI_L3M_AVW', 'PACE_OCI_L3M_CHL_NRT', 'PACE_OCI_L3M_CHL', 'PACE_OCI_L3M_CLOUD_MASK_NRT', 'PACE_OCI_L3M_CLOUD_MASK', 'PACE_OCI_L3M_CLOUD_NRT', 'PACE_OCI_L3M_CLOUD', 'PACE_OCI_L3M_KD_NRT', 'PACE_OCI_L3M_KD', 'PACE_OCI_L3M_FLH_NRT', 'PACE_OCI_L3M_FLH', 'PACE_OCI_L3M_LANDVI_NRT', 'PACE_OCI_L3M_LANDVI', 'PACE_OCI_L3M_IOP_NRT', 'PACE_OCI_L3M_IOP', 'PACE_OCI_L3M_PIC_NRT', 'PACE_OCI_L3M_PIC', 'PACE_OCI_L3M_POC_NRT', 'PACE_OCI_L3M_POC', 'PACE_OCI_L3M_PAR_NRT', 'PACE_OCI_L3M_PAR', 'PACE_OCI_L3M_CARBON', 'PACE_OCI_L3M_CARBON_NRT', 'PACE_OCI_L3M_RRS_NRT', 'PACE_OCI_L3M_RRS', 'PACE_OCI_L3M_SFREFL_NRT', 'PACE_OCI_L3M_SFREFL', 'PACE_OCI_L3M_TRGAS_NRT', 'PACE_OCI_L3M_TRGAS']



```python
%%time
import point_collocation as pc
for short_name in short_names:
    print(f"\n===== {short_name} =====")
    
    try:
        plan = pc.plan(
            df,
            data_source="earthaccess",
            source_kwargs={
                "short_name": short_name,
                "granule_name":"*.DAY.*",
             }
        )
        plan.show_variables()
    except Exception as e:
        print("Failed:", e)
```

    
    ===== PACE_OCI_L3M_UVAI_UAA_NRT =====
    Failed: No granules in plan — cannot show variables.
    
    ===== PACE_OCI_L3M_UVAI_UAA =====
    Failed: No granules in plan — cannot show variables.
    
    ===== PACE_OCI_L3M_AER_UAA_NRT =====
    open_method  : 'auto'
    Dimensions : {'lat': 180, 'lon': 360}
    Variables  : ['Aerosol_Optical_Depth_354', 'Aerosol_Optical_Depth_388', 'Aerosol_Optical_Depth_480', 'Aerosol_Optical_Depth_550', 'Aerosol_Optical_Depth_670', 'Aerosol_Optical_Depth_870', 'Aerosol_Optical_Depth_1240', 'Aerosol_Optical_Depth_2200', 'Optical_Depth_Ratio_Small_Ocean_used', 'NUV_AerosolCorrCloudOpticalDepth', 'NUV_AerosolOpticalDepthOverCloud_354', 'NUV_AerosolOpticalDepthOverCloud_388', 'NUV_AerosolOpticalDepthOverCloud_550', 'NUV_AerosolIndex', 'NUV_CloudOpticalDepth', 'AAOD_354', 'AAOD_388', 'AAOD_550']
    
    Geolocation: ('lon', 'lat') — lon dims=('lon',), lat dims=('lat',)
    
    ===== PACE_OCI_L3M_AER_UAA =====
    open_method  : 'auto'
    Dimensions : {'lat': 180, 'lon': 360}
    Variables  : ['Aerosol_Optical_Depth_354', 'Aerosol_Optical_Depth_388', 'Aerosol_Optical_Depth_480', 'Aerosol_Optical_Depth_550', 'Aerosol_Optical_Depth_670', 'Aerosol_Optical_Depth_870', 'Aerosol_Optical_Depth_1240', 'Aerosol_Optical_Depth_2200', 'Optical_Depth_Ratio_Small_Ocean_used', 'NUV_AerosolCorrCloudOpticalDepth', 'NUV_AerosolOpticalDepthOverCloud_354', 'NUV_AerosolOpticalDepthOverCloud_388', 'NUV_AerosolOpticalDepthOverCloud_550', 'NUV_AerosolIndex', 'NUV_CloudOpticalDepth', 'AAOD_354', 'AAOD_388', 'AAOD_550']
    
    Geolocation: ('lon', 'lat') — lon dims=('lon',), lat dims=('lat',)
    
    ===== PACE_OCI_L3M_AOT_NRT =====
    Failed: No granules in plan — cannot show variables.
    
    ===== PACE_OCI_L3M_AOT =====
    Failed: No granules in plan — cannot show variables.
    
    ===== PACE_OCI_L3M_AVW_NRT =====
    open_method  : 'auto'
    Dimensions : {'lat': 1800, 'lon': 3600, 'rgb': 3, 'eightbitcolor': 256}
    Variables  : ['avw', 'palette']
    
    Geolocation: ('lon', 'lat') — lon dims=('lon',), lat dims=('lat',)
    
    ===== PACE_OCI_L3M_AVW =====
    open_method  : 'auto'
    Dimensions : {'lat': 1800, 'lon': 3600, 'rgb': 3, 'eightbitcolor': 256}
    Variables  : ['avw', 'palette']
    
    Geolocation: ('lon', 'lat') — lon dims=('lon',), lat dims=('lat',)
    
    ===== PACE_OCI_L3M_CHL_NRT =====
    open_method  : 'auto'
    Dimensions : {'lat': 1800, 'lon': 3600, 'rgb': 3, 'eightbitcolor': 256}
    Variables  : ['chlor_a', 'palette']
    
    Geolocation: ('lon', 'lat') — lon dims=('lon',), lat dims=('lat',)
    
    ===== PACE_OCI_L3M_CHL =====
    open_method  : 'auto'
    Dimensions : {'lat': 1800, 'lon': 3600, 'rgb': 3, 'eightbitcolor': 256}
    Variables  : ['chlor_a', 'palette']
    
    Geolocation: ('lon', 'lat') — lon dims=('lon',), lat dims=('lat',)
    
    ===== PACE_OCI_L3M_CLOUD_MASK_NRT =====
    Failed: No granules in plan — cannot show variables.
    
    ===== PACE_OCI_L3M_CLOUD_MASK =====
    Failed: No granules in plan — cannot show variables.
    
    ===== PACE_OCI_L3M_CLOUD_NRT =====
    open_method  : 'auto'
    Dimensions : {'lat': 180, 'lon': 360}
    Variables  : ['cloud_fraction', 'ice_cloud_fraction', 'water_cloud_fraction', 'ctt', 'ctp', 'cth', 'cth_cot', 'cth_alb', 'ctt_water', 'ctp_water', 'cth_water', 'cth_cot_water', 'cth_alb_water', 'ctt_ice', 'ctp_ice', 'cth_ice', 'cth_cot_ice', 'cth_alb_ice', 'cer_16', 'cot_16', 'cwp_16', 'cer_16_water', 'cot_16_water', 'cwp_16_water', 'cer_16_ice', 'cot_16_ice', 'cwp_16_ice', 'cer_21', 'cot_21', 'cwp_21', 'cer_21_water', 'cot_21_water', 'cwp_21_water', 'cer_21_ice', 'cot_21_ice', 'cwp_21_ice', 'cer_22', 'cot_22', 'cwp_22', 'cer_22_water', 'cot_22_water', 'cwp_22_water', 'cer_22_ice', 'cot_22_ice', 'cwp_22_ice']
    
    Geolocation: ('lon', 'lat') — lon dims=('lon',), lat dims=('lat',)
    
    ===== PACE_OCI_L3M_CLOUD =====
    open_method  : 'auto'
    Dimensions : {'lat': 180, 'lon': 360}
    Variables  : ['cloud_fraction', 'ice_cloud_fraction', 'water_cloud_fraction', 'ctt', 'ctp', 'cth', 'cth_cot', 'cth_alb', 'ctt_water', 'ctp_water', 'cth_water', 'cth_cot_water', 'cth_alb_water', 'ctt_ice', 'ctp_ice', 'cth_ice', 'cth_cot_ice', 'cth_alb_ice', 'cer_16', 'cot_16', 'cwp_16', 'cer_16_water', 'cot_16_water', 'cwp_16_water', 'cer_16_ice', 'cot_16_ice', 'cwp_16_ice', 'cer_21', 'cot_21', 'cwp_21', 'cer_21_water', 'cot_21_water', 'cwp_21_water', 'cer_21_ice', 'cot_21_ice', 'cwp_21_ice', 'cer_22', 'cot_22', 'cwp_22', 'cer_22_water', 'cot_22_water', 'cwp_22_water', 'cer_22_ice', 'cot_22_ice', 'cwp_22_ice']
    
    Geolocation: ('lon', 'lat') — lon dims=('lon',), lat dims=('lat',)
    
    ===== PACE_OCI_L3M_KD_NRT =====
    open_method  : 'auto'
    Dimensions : {'lat': 1800, 'lon': 3600, 'wavelength': 17, 'rgb': 3, 'eightbitcolor': 256}
    Variables  : ['Kd', 'palette']
    
    Geolocation: ('lon', 'lat') — lon dims=('lon',), lat dims=('lat',)
    
    ===== PACE_OCI_L3M_KD =====
    open_method  : 'auto'
    Dimensions : {'lat': 1800, 'lon': 3600, 'wavelength': 17, 'rgb': 3, 'eightbitcolor': 256}
    Variables  : ['Kd', 'palette']
    
    Geolocation: ('lon', 'lat') — lon dims=('lon',), lat dims=('lat',)
    
    ===== PACE_OCI_L3M_FLH_NRT =====
    open_method  : 'auto'
    Dimensions : {'lat': 1800, 'lon': 3600, 'rgb': 3, 'eightbitcolor': 256}
    Variables  : ['nflh', 'palette']
    
    Geolocation: ('lon', 'lat') — lon dims=('lon',), lat dims=('lat',)
    
    ===== PACE_OCI_L3M_FLH =====
    open_method  : 'auto'
    Dimensions : {'lat': 1800, 'lon': 3600, 'rgb': 3, 'eightbitcolor': 256}
    Variables  : ['nflh', 'palette']
    
    Geolocation: ('lon', 'lat') — lon dims=('lon',), lat dims=('lat',)
    
    ===== PACE_OCI_L3M_LANDVI_NRT =====
    open_method  : 'auto'
    Dimensions : {'lat': 1800, 'lon': 3600, 'rgb': 3, 'eightbitcolor': 256}
    Variables  : ['ndvi', 'evi', 'ndwi', 'ndii', 'cci', 'ndsi', 'pri', 'cire', 'car', 'mari', 'palette']
    
    Geolocation: ('lon', 'lat') — lon dims=('lon',), lat dims=('lat',)
    
    ===== PACE_OCI_L3M_LANDVI =====
    open_method  : 'auto'
    Dimensions : {'lat': 1800, 'lon': 3600, 'rgb': 3, 'eightbitcolor': 256}
    Variables  : ['ndvi', 'evi', 'ndwi', 'ndii', 'cci', 'ndsi', 'pri', 'cire', 'car', 'mari', 'palette']
    
    Geolocation: ('lon', 'lat') — lon dims=('lon',), lat dims=('lat',)
    
    ===== PACE_OCI_L3M_IOP_NRT =====
    open_method  : 'auto'
    Dimensions : {'lat': 1800, 'lon': 3600, 'rgb': 3, 'eightbitcolor': 256}
    Variables  : ['adg_s', 'palette']
    
    Geolocation: ('lon', 'lat') — lon dims=('lon',), lat dims=('lat',)
    
    ===== PACE_OCI_L3M_IOP =====
    open_method  : 'auto'
    Dimensions : {'lat': 1800, 'lon': 3600, 'rgb': 3, 'eightbitcolor': 256}
    Variables  : ['adg_442', 'palette']
    
    Geolocation: ('lon', 'lat') — lon dims=('lon',), lat dims=('lat',)
    
    ===== PACE_OCI_L3M_PIC_NRT =====
    Failed: No granules in plan — cannot show variables.
    
    ===== PACE_OCI_L3M_PIC =====
    Failed: No granules in plan — cannot show variables.
    
    ===== PACE_OCI_L3M_POC_NRT =====
    open_method  : 'auto'
    Dimensions : {'lat': 1800, 'lon': 3600, 'rgb': 3, 'eightbitcolor': 256}
    Variables  : ['poc', 'palette']
    
    Geolocation: ('lon', 'lat') — lon dims=('lon',), lat dims=('lat',)
    
    ===== PACE_OCI_L3M_POC =====
    open_method  : 'auto'
    Dimensions : {'lat': 1800, 'lon': 3600, 'rgb': 3, 'eightbitcolor': 256}
    Variables  : ['poc', 'palette']
    
    Geolocation: ('lon', 'lat') — lon dims=('lon',), lat dims=('lat',)
    
    ===== PACE_OCI_L3M_PAR_NRT =====
    open_method  : 'auto'
    Dimensions : {'lat': 1800, 'lon': 3600, 'rgb': 3, 'eightbitcolor': 256}
    Variables  : ['par_day_scalar_below', 'par_day_planar_above', 'par_day_planar_below', 'ipar_planar_above', 'ipar_planar_below', 'ipar_scalar_below', 'palette']
    
    Geolocation: ('lon', 'lat') — lon dims=('lon',), lat dims=('lat',)
    
    ===== PACE_OCI_L3M_PAR =====
    open_method  : 'auto'
    Dimensions : {'lat': 1800, 'lon': 3600, 'rgb': 3, 'eightbitcolor': 256}
    Variables  : ['par_day_scalar_below', 'par_day_planar_above', 'par_day_planar_below', 'ipar_planar_above', 'ipar_planar_below', 'ipar_scalar_below', 'palette']
    
    Geolocation: ('lon', 'lat') — lon dims=('lon',), lat dims=('lat',)
    
    ===== PACE_OCI_L3M_CARBON =====
    open_method  : 'auto'
    Dimensions : {'lat': 1800, 'lon': 3600, 'rgb': 3, 'eightbitcolor': 256}
    Variables  : ['carbon_phyto', 'palette']
    
    Geolocation: ('lon', 'lat') — lon dims=('lon',), lat dims=('lat',)
    
    ===== PACE_OCI_L3M_CARBON_NRT =====
    open_method  : 'auto'
    Dimensions : {'lat': 1800, 'lon': 3600, 'rgb': 3, 'eightbitcolor': 256}
    Variables  : ['carbon_phyto', 'palette']
    
    Geolocation: ('lon', 'lat') — lon dims=('lon',), lat dims=('lat',)
    
    ===== PACE_OCI_L3M_RRS_NRT =====
    open_method  : 'auto'
    Dimensions : {'lat': 1800, 'lon': 3600, 'wavelength': 172, 'rgb': 3, 'eightbitcolor': 256}
    Variables  : ['Rrs', 'palette']
    
    Geolocation: ('lon', 'lat') — lon dims=('lon',), lat dims=('lat',)
    
    ===== PACE_OCI_L3M_RRS =====
    open_method  : 'auto'
    Dimensions : {'lat': 1800, 'lon': 3600, 'wavelength': 172, 'rgb': 3, 'eightbitcolor': 256}
    Variables  : ['Rrs', 'palette']
    
    Geolocation: ('lon', 'lat') — lon dims=('lon',), lat dims=('lat',)
    
    ===== PACE_OCI_L3M_SFREFL_NRT =====
    open_method  : 'auto'
    Dimensions : {'lat': 4320, 'lon': 8640, 'wavelength': 122, 'rgb': 3, 'eightbitcolor': 256}
    Variables  : ['rhos', 'palette']
    
    Geolocation: ('lon', 'lat') — lon dims=('lon',), lat dims=('lat',)
    
    ===== PACE_OCI_L3M_SFREFL =====
    open_method  : 'auto'
    Dimensions : {'lat': 1800, 'lon': 3600, 'wavelength': 122, 'rgb': 3, 'eightbitcolor': 256}
    Variables  : ['rhos', 'palette']
    
    Geolocation: ('lon', 'lat') — lon dims=('lon',), lat dims=('lat',)
    
    ===== PACE_OCI_L3M_TRGAS_NRT =====
    open_method  : 'auto'
    Dimensions : {'lat': 1800, 'lon': 3600}
    Variables  : ['total_column_o3', 'total_column_no2']
    
    Geolocation: ('lon', 'lat') — lon dims=('lon',), lat dims=('lat',)
    
    ===== PACE_OCI_L3M_TRGAS =====
    open_method  : 'auto'
    Dimensions : {'lat': 1800, 'lon': 3600}
    Variables  : ['total_column_o3', 'total_column_no2']
    
    Geolocation: ('lon', 'lat') — lon dims=('lon',), lat dims=('lat',)
    CPU times: user 3.07 s, sys: 246 ms, total: 3.31 s
    Wall time: 27.8 s



```python

```
