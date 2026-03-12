# Quickstart

`point-collocation` gets matchups to lat/lon using the pixel center that is closest to the lat/lon point (equivalent to method="nearest"). For time, you can select a buffer of 0, which means the time of the point must be within the time range of the file or a buffer like buffer="1D" to find files within 1 day of the point. Using a buffer can help for L2 files with short windows (minutes) or collections with infrequent files.

* Create a plan for files to use `pc.plan()`
* Print the plan to check it `plan.summary()`
* Do the plan and get matchups for variables `pc.matchup(plan, variables=['var'])`

## Prerequisite -- Login to EarthData

The examples here use NASA EarthData and you need to have an account with EarthData. Make sure you can login.


```python
import earthaccess
earthaccess.login()
```




    <earthaccess.auth.Auth at 0x7f6b2c60c920>



## Get some points to matchup


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



## Start plan -- Take a look at the files in a collection

Now we use the point_collocation package. First we will look at the files available and figure out which ones we want.


```python
%%time
import point_collocation as pc
plan = pc.plan(
    df_points,
    data_source="earthaccess",
    source_kwargs={
        "short_name": "PACE_OCI_L3M_RRS",
    }
)
```

    CPU times: user 217 ms, sys: 20.5 ms, total: 237 ms
    Wall time: 10.9 s



```python
plan.summary(n=1)
```

    Plan: 595 points → 210 unique granule(s)
      Points with 0 matches : 0
      Points with >1 matches: 595
      Time buffer: 0 days 00:00:00
    
    First 1 point(s):
      [0] lat=27.3835, lon=-82.7375, time=2024-06-13 12:00:00: 16 match(es)
        → https://obdaac-tea.earthdatacloud.nasa.gov/ob-cumulus-prod-public/PACE_OCI.20240321_20240620.L3m.SNSP.RRS.V3_1.Rrs.0p1deg.nc
        → https://obdaac-tea.earthdatacloud.nasa.gov/ob-cumulus-prod-public/PACE_OCI.20240321_20240620.L3m.SNSP.RRS.V3_1.Rrs.4km.nc
        → https://obdaac-tea.earthdatacloud.nasa.gov/ob-cumulus-prod-public/PACE_OCI.20240516_20240616.L3m.R32.RRS.V3_1.Rrs.0p1deg.nc
        → https://obdaac-tea.earthdatacloud.nasa.gov/ob-cumulus-prod-public/PACE_OCI.20240516_20240616.L3m.R32.RRS.V3_1.Rrs.4km.nc
        → https://obdaac-tea.earthdatacloud.nasa.gov/ob-cumulus-prod-public/PACE_OCI.20240524_20240624.L3m.R32.RRS.V3_1.Rrs.0p1deg.nc
        → https://obdaac-tea.earthdatacloud.nasa.gov/ob-cumulus-prod-public/PACE_OCI.20240524_20240624.L3m.R32.RRS.V3_1.Rrs.4km.nc
        → https://obdaac-tea.earthdatacloud.nasa.gov/ob-cumulus-prod-public/PACE_OCI.20240601_20240630.L3m.MO.RRS.V3_1.Rrs.0p1deg.nc
        → https://obdaac-tea.earthdatacloud.nasa.gov/ob-cumulus-prod-public/PACE_OCI.20240601_20240630.L3m.MO.RRS.V3_1.Rrs.4km.nc
        → https://obdaac-tea.earthdatacloud.nasa.gov/ob-cumulus-prod-public/PACE_OCI.20240601_20240702.L3m.R32.RRS.V3_1.Rrs.4km.nc
        → https://obdaac-tea.earthdatacloud.nasa.gov/ob-cumulus-prod-public/PACE_OCI.20240601_20240702.L3m.R32.RRS.V3_1.Rrs.0p1deg.nc
        → https://obdaac-tea.earthdatacloud.nasa.gov/ob-cumulus-prod-public/PACE_OCI.20240609_20240616.L3m.8D.RRS.V3_1.Rrs.0p1deg.nc
        → https://obdaac-tea.earthdatacloud.nasa.gov/ob-cumulus-prod-public/PACE_OCI.20240609_20240616.L3m.8D.RRS.V3_1.Rrs.4km.nc
        → https://obdaac-tea.earthdatacloud.nasa.gov/ob-cumulus-prod-public/PACE_OCI.20240609_20240710.L3m.R32.RRS.V3_1.Rrs.0p1deg.nc
        → https://obdaac-tea.earthdatacloud.nasa.gov/ob-cumulus-prod-public/PACE_OCI.20240609_20240710.L3m.R32.RRS.V3_1.Rrs.4km.nc
        → https://obdaac-tea.earthdatacloud.nasa.gov/ob-cumulus-prod-public/PACE_OCI.20240613.L3m.DAY.RRS.V3_1.Rrs.0p1deg.nc
        → https://obdaac-tea.earthdatacloud.nasa.gov/ob-cumulus-prod-public/PACE_OCI.20240613.L3m.DAY.RRS.V3_1.Rrs.4km.nc


## Create new plan with filter on file names

We will use the monthly 4km files.


```python
%%time
import point_collocation as pc
plan = pc.plan(
    df_points,
    data_source="earthaccess",
    source_kwargs={
        "short_name": "PACE_OCI_L3M_RRS",
        "granule_name": "*.MO.*.4km.*",
    }
)
```

    CPU times: user 53.5 ms, sys: 344 μs, total: 53.8 ms
    Wall time: 3.45 s



```python
# check the plan and see how many files per point
# we want 1 file per point in this case
# Looks like 6 monthly files
plan.summary()
```

    Plan: 595 points → 4 unique granule(s)
      Points with 0 matches : 0
      Points with >1 matches: 0
      Time buffer: 0 days 00:00:00
    
    First 5 point(s):
      [0] lat=27.3835, lon=-82.7375, time=2024-06-13 12:00:00: 1 match(es)
        → https://obdaac-tea.earthdatacloud.nasa.gov/ob-cumulus-prod-public/PACE_OCI.20240601_20240630.L3m.MO.RRS.V3_1.Rrs.4km.nc
      [1] lat=27.1190, lon=-82.7125, time=2024-06-14 12:00:00: 1 match(es)
        → https://obdaac-tea.earthdatacloud.nasa.gov/ob-cumulus-prod-public/PACE_OCI.20240601_20240630.L3m.MO.RRS.V3_1.Rrs.4km.nc
      [2] lat=26.9435, lon=-82.8170, time=2024-06-14 12:00:00: 1 match(es)
        → https://obdaac-tea.earthdatacloud.nasa.gov/ob-cumulus-prod-public/PACE_OCI.20240601_20240630.L3m.MO.RRS.V3_1.Rrs.4km.nc
      [3] lat=26.6875, lon=-82.8065, time=2024-06-14 12:00:00: 1 match(es)
        → https://obdaac-tea.earthdatacloud.nasa.gov/ob-cumulus-prod-public/PACE_OCI.20240601_20240630.L3m.MO.RRS.V3_1.Rrs.4km.nc
      [4] lat=26.6675, lon=-82.6455, time=2024-06-14 12:00:00: 1 match(es)
        → https://obdaac-tea.earthdatacloud.nasa.gov/ob-cumulus-prod-public/PACE_OCI.20240601_20240630.L3m.MO.RRS.V3_1.Rrs.4km.nc


## Check the variables in the files

This will open one file and show us the variables. We want 'Rrs' in this case.


```python
plan.show_variables()
```

    geometry     : 'grid'
    open_method  : 'dataset'
    Dimensions : {'lat': 4320, 'lon': 8640, 'wavelength': 172, 'rgb': 3, 'eightbitcolor': 256}
    Variables  : ['Rrs', 'palette']
    
    Geolocation: ('lon', 'lat') — lon dims=('lon',), lat dims=('lat',)


## Get the matchups using our plan

Let's start with 100 points since 595 might take awhile. The lat, lon, and time for the matching granules is added as a column. `pc_id` is the point id/row from the data you are matching. This is added in case there are multiple granules (files) per data point.


```python
%%time
res = pc.matchup(plan[0:100], variables=["Rrs"])
```

    CPU times: user 12.3 s, sys: 992 ms, total: 13.3 s
    Wall time: 18.8 s



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
      <td>2024-06-15 23:59:59+00:00</td>
      <td>27.395832</td>
      <td>-82.729164</td>
      <td>0.004034</td>
      <td>0.004070</td>
      <td>...</td>
      <td>0.000224</td>
      <td>0.000202</td>
      <td>0.000190</td>
      <td>0.000176</td>
      <td>0.000168</td>
      <td>0.000156</td>
      <td>0.000144</td>
      <td>0.000134</td>
      <td>0.000158</td>
      <td>0.000202</td>
    </tr>
    <tr>
      <th>1</th>
      <td>27.1190</td>
      <td>-82.7125</td>
      <td>2024-06-14 12:00:00</td>
      <td>1</td>
      <td>https://obdaac-tea.earthdatacloud.nasa.gov/ob-...</td>
      <td>2024-06-15 23:59:59+00:00</td>
      <td>27.104164</td>
      <td>-82.729164</td>
      <td>0.004562</td>
      <td>0.004616</td>
      <td>...</td>
      <td>0.000108</td>
      <td>0.000094</td>
      <td>0.000084</td>
      <td>0.000078</td>
      <td>0.000072</td>
      <td>0.000066</td>
      <td>0.000060</td>
      <td>0.000048</td>
      <td>0.000062</td>
      <td>0.000098</td>
    </tr>
    <tr>
      <th>2</th>
      <td>26.9435</td>
      <td>-82.8170</td>
      <td>2024-06-14 12:00:00</td>
      <td>2</td>
      <td>https://obdaac-tea.earthdatacloud.nasa.gov/ob-...</td>
      <td>2024-06-15 23:59:59+00:00</td>
      <td>26.937498</td>
      <td>-82.812500</td>
      <td>0.005112</td>
      <td>0.005282</td>
      <td>...</td>
      <td>0.000118</td>
      <td>0.000108</td>
      <td>0.000102</td>
      <td>0.000098</td>
      <td>0.000098</td>
      <td>0.000092</td>
      <td>0.000086</td>
      <td>0.000068</td>
      <td>0.000052</td>
      <td>0.000066</td>
    </tr>
    <tr>
      <th>3</th>
      <td>26.6875</td>
      <td>-82.8065</td>
      <td>2024-06-14 12:00:00</td>
      <td>3</td>
      <td>https://obdaac-tea.earthdatacloud.nasa.gov/ob-...</td>
      <td>2024-06-15 23:59:59+00:00</td>
      <td>26.687498</td>
      <td>-82.812500</td>
      <td>0.004648</td>
      <td>0.004904</td>
      <td>...</td>
      <td>0.000178</td>
      <td>0.000158</td>
      <td>0.000148</td>
      <td>0.000138</td>
      <td>0.000130</td>
      <td>0.000126</td>
      <td>0.000126</td>
      <td>0.000120</td>
      <td>0.000158</td>
      <td>0.000230</td>
    </tr>
    <tr>
      <th>4</th>
      <td>26.6675</td>
      <td>-82.6455</td>
      <td>2024-06-14 12:00:00</td>
      <td>4</td>
      <td>https://obdaac-tea.earthdatacloud.nasa.gov/ob-...</td>
      <td>2024-06-15 23:59:59+00:00</td>
      <td>26.687498</td>
      <td>-82.645828</td>
      <td>0.004944</td>
      <td>0.005064</td>
      <td>...</td>
      <td>0.000094</td>
      <td>0.000078</td>
      <td>0.000068</td>
      <td>0.000062</td>
      <td>0.000058</td>
      <td>0.000054</td>
      <td>0.000052</td>
      <td>0.000050</td>
      <td>0.000106</td>
      <td>0.000166</td>
    </tr>
  </tbody>
</table>
<p>5 rows × 180 columns</p>
</div>



## Open files in plan

Sometimes it is helpful to look at the granules. There are helper functions for that. You need to specify the format of the data, "grid" for level 3 gridded or "swath" for level 2 swath data.


```python
ds = plan.open_dataset(plan[0])
ds
```




<div><svg style="position: absolute; width: 0; height: 0; overflow: hidden">
<defs>
<symbol id="icon-database" viewBox="0 0 32 32">
<path d="M16 0c-8.837 0-16 2.239-16 5v4c0 2.761 7.163 5 16 5s16-2.239 16-5v-4c0-2.761-7.163-5-16-5z"></path>
<path d="M16 17c-8.837 0-16-2.239-16-5v6c0 2.761 7.163 5 16 5s16-2.239 16-5v-6c0 2.761-7.163 5-16 5z"></path>
<path d="M16 26c-8.837 0-16-2.239-16-5v6c0 2.761 7.163 5 16 5s16-2.239 16-5v-6c0 2.761-7.163 5-16 5z"></path>
</symbol>
<symbol id="icon-file-text2" viewBox="0 0 32 32">
<path d="M28.681 7.159c-0.694-0.947-1.662-2.053-2.724-3.116s-2.169-2.030-3.116-2.724c-1.612-1.182-2.393-1.319-2.841-1.319h-15.5c-1.378 0-2.5 1.121-2.5 2.5v27c0 1.378 1.122 2.5 2.5 2.5h23c1.378 0 2.5-1.122 2.5-2.5v-19.5c0-0.448-0.137-1.23-1.319-2.841zM24.543 5.457c0.959 0.959 1.712 1.825 2.268 2.543h-4.811v-4.811c0.718 0.556 1.584 1.309 2.543 2.268zM28 29.5c0 0.271-0.229 0.5-0.5 0.5h-23c-0.271 0-0.5-0.229-0.5-0.5v-27c0-0.271 0.229-0.5 0.5-0.5 0 0 15.499-0 15.5 0v7c0 0.552 0.448 1 1 1h7v19.5z"></path>
<path d="M23 26h-14c-0.552 0-1-0.448-1-1s0.448-1 1-1h14c0.552 0 1 0.448 1 1s-0.448 1-1 1z"></path>
<path d="M23 22h-14c-0.552 0-1-0.448-1-1s0.448-1 1-1h14c0.552 0 1 0.448 1 1s-0.448 1-1 1z"></path>
<path d="M23 18h-14c-0.552 0-1-0.448-1-1s0.448-1 1-1h14c0.552 0 1 0.448 1 1s-0.448 1-1 1z"></path>
</symbol>
</defs>
</svg>
<style>/* CSS stylesheet for displaying xarray objects in notebooks */

:root {
  --xr-font-color0: var(
    --jp-content-font-color0,
    var(--pst-color-text-base rgba(0, 0, 0, 1))
  );
  --xr-font-color2: var(
    --jp-content-font-color2,
    var(--pst-color-text-base, rgba(0, 0, 0, 0.54))
  );
  --xr-font-color3: var(
    --jp-content-font-color3,
    var(--pst-color-text-base, rgba(0, 0, 0, 0.38))
  );
  --xr-border-color: var(
    --jp-border-color2,
    hsl(from var(--pst-color-on-background, white) h s calc(l - 10))
  );
  --xr-disabled-color: var(
    --jp-layout-color3,
    hsl(from var(--pst-color-on-background, white) h s calc(l - 40))
  );
  --xr-background-color: var(
    --jp-layout-color0,
    var(--pst-color-on-background, white)
  );
  --xr-background-color-row-even: var(
    --jp-layout-color1,
    hsl(from var(--pst-color-on-background, white) h s calc(l - 5))
  );
  --xr-background-color-row-odd: var(
    --jp-layout-color2,
    hsl(from var(--pst-color-on-background, white) h s calc(l - 15))
  );
}

html[theme="dark"],
html[data-theme="dark"],
body[data-theme="dark"],
body.vscode-dark {
  --xr-font-color0: var(
    --jp-content-font-color0,
    var(--pst-color-text-base, rgba(255, 255, 255, 1))
  );
  --xr-font-color2: var(
    --jp-content-font-color2,
    var(--pst-color-text-base, rgba(255, 255, 255, 0.54))
  );
  --xr-font-color3: var(
    --jp-content-font-color3,
    var(--pst-color-text-base, rgba(255, 255, 255, 0.38))
  );
  --xr-border-color: var(
    --jp-border-color2,
    hsl(from var(--pst-color-on-background, #111111) h s calc(l + 10))
  );
  --xr-disabled-color: var(
    --jp-layout-color3,
    hsl(from var(--pst-color-on-background, #111111) h s calc(l + 40))
  );
  --xr-background-color: var(
    --jp-layout-color0,
    var(--pst-color-on-background, #111111)
  );
  --xr-background-color-row-even: var(
    --jp-layout-color1,
    hsl(from var(--pst-color-on-background, #111111) h s calc(l + 5))
  );
  --xr-background-color-row-odd: var(
    --jp-layout-color2,
    hsl(from var(--pst-color-on-background, #111111) h s calc(l + 15))
  );
}

.xr-wrap {
  display: block !important;
  min-width: 300px;
  max-width: 700px;
  line-height: 1.6;
  padding-bottom: 4px;
}

.xr-text-repr-fallback {
  /* fallback to plain text repr when CSS is not injected (untrusted notebook) */
  display: none;
}

.xr-header {
  padding-top: 6px;
  padding-bottom: 6px;
}

.xr-header {
  border-bottom: solid 1px var(--xr-border-color);
  margin-bottom: 4px;
}

.xr-header > div,
.xr-header > ul {
  display: inline;
  margin-top: 0;
  margin-bottom: 0;
}

.xr-obj-type,
.xr-obj-name {
  margin-left: 2px;
  margin-right: 10px;
}

.xr-obj-type,
.xr-group-box-contents > label {
  color: var(--xr-font-color2);
  display: block;
}

.xr-sections {
  padding-left: 0 !important;
  display: grid;
  grid-template-columns: 150px auto auto 1fr 0 20px 0 20px;
  margin-block-start: 0;
  margin-block-end: 0;
}

.xr-section-item {
  display: contents;
}

.xr-section-item > input,
.xr-group-box-contents > input,
.xr-array-wrap > input {
  display: block;
  opacity: 0;
  height: 0;
  margin: 0;
}

.xr-section-item > input + label,
.xr-var-item > input + label {
  color: var(--xr-disabled-color);
}

.xr-section-item > input:enabled + label,
.xr-var-item > input:enabled + label,
.xr-array-wrap > input:enabled + label,
.xr-group-box-contents > input:enabled + label {
  cursor: pointer;
  color: var(--xr-font-color2);
}

.xr-section-item > input:focus-visible + label,
.xr-var-item > input:focus-visible + label,
.xr-array-wrap > input:focus-visible + label,
.xr-group-box-contents > input:focus-visible + label {
  outline: auto;
}

.xr-section-item > input:enabled + label:hover,
.xr-var-item > input:enabled + label:hover,
.xr-array-wrap > input:enabled + label:hover,
.xr-group-box-contents > input:enabled + label:hover {
  color: var(--xr-font-color0);
}

.xr-section-summary {
  grid-column: 1;
  color: var(--xr-font-color2);
  font-weight: 500;
  white-space: nowrap;
}

.xr-section-summary > em {
  font-weight: normal;
}

.xr-span-grid {
  grid-column-end: -1;
}

.xr-section-summary > span {
  display: inline-block;
  padding-left: 0.3em;
}

.xr-group-box-contents > input:checked + label > span {
  display: inline-block;
  padding-left: 0.6em;
}

.xr-section-summary-in:disabled + label {
  color: var(--xr-font-color2);
}

.xr-section-summary-in + label:before {
  display: inline-block;
  content: "►";
  font-size: 11px;
  width: 15px;
  text-align: center;
}

.xr-section-summary-in:disabled + label:before {
  color: var(--xr-disabled-color);
}

.xr-section-summary-in:checked + label:before {
  content: "▼";
}

.xr-section-summary-in:checked + label > span {
  display: none;
}

.xr-section-summary,
.xr-section-inline-details,
.xr-group-box-contents > label {
  padding-top: 4px;
}

.xr-section-inline-details {
  grid-column: 2 / -1;
}

.xr-section-details {
  grid-column: 1 / -1;
  margin-top: 4px;
  margin-bottom: 5px;
}

.xr-section-summary-in ~ .xr-section-details {
  display: none;
}

.xr-section-summary-in:checked ~ .xr-section-details {
  display: contents;
}

.xr-children {
  display: inline-grid;
  grid-template-columns: 100%;
  grid-column: 1 / -1;
  padding-top: 4px;
}

.xr-group-box {
  display: inline-grid;
  grid-template-columns: 0px 30px auto;
}

.xr-group-box-vline {
  grid-column-start: 1;
  border-right: 0.2em solid;
  border-color: var(--xr-border-color);
  width: 0px;
}

.xr-group-box-hline {
  grid-column-start: 2;
  grid-row-start: 1;
  height: 1em;
  width: 26px;
  border-bottom: 0.2em solid;
  border-color: var(--xr-border-color);
}

.xr-group-box-contents {
  grid-column-start: 3;
  padding-bottom: 4px;
}

.xr-group-box-contents > label::before {
  content: "📂";
  padding-right: 0.3em;
}

.xr-group-box-contents > input:checked + label::before {
  content: "📁";
}

.xr-group-box-contents > input:checked + label {
  padding-bottom: 0px;
}

.xr-group-box-contents > input:checked ~ .xr-sections {
  display: none;
}

.xr-group-box-contents > input + label > span {
  display: none;
}

.xr-group-box-ellipsis {
  font-size: 1.4em;
  font-weight: 900;
  color: var(--xr-font-color2);
  letter-spacing: 0.15em;
  cursor: default;
}

.xr-array-wrap {
  grid-column: 1 / -1;
  display: grid;
  grid-template-columns: 20px auto;
}

.xr-array-wrap > label {
  grid-column: 1;
  vertical-align: top;
}

.xr-preview {
  color: var(--xr-font-color3);
}

.xr-array-preview,
.xr-array-data {
  padding: 0 5px !important;
  grid-column: 2;
}

.xr-array-data,
.xr-array-in:checked ~ .xr-array-preview {
  display: none;
}

.xr-array-in:checked ~ .xr-array-data,
.xr-array-preview {
  display: inline-block;
}

.xr-dim-list {
  display: inline-block !important;
  list-style: none;
  padding: 0 !important;
  margin: 0;
}

.xr-dim-list li {
  display: inline-block;
  padding: 0;
  margin: 0;
}

.xr-dim-list:before {
  content: "(";
}

.xr-dim-list:after {
  content: ")";
}

.xr-dim-list li:not(:last-child):after {
  content: ",";
  padding-right: 5px;
}

.xr-has-index {
  font-weight: bold;
}

.xr-var-list,
.xr-var-item {
  display: contents;
}

.xr-var-item > div,
.xr-var-item label,
.xr-var-item > .xr-var-name span {
  background-color: var(--xr-background-color-row-even);
  border-color: var(--xr-background-color-row-odd);
  margin-bottom: 0;
  padding-top: 2px;
}

.xr-var-item > .xr-var-name:hover span {
  padding-right: 5px;
}

.xr-var-list > li:nth-child(odd) > div,
.xr-var-list > li:nth-child(odd) > label,
.xr-var-list > li:nth-child(odd) > .xr-var-name span {
  background-color: var(--xr-background-color-row-odd);
  border-color: var(--xr-background-color-row-even);
}

.xr-var-name {
  grid-column: 1;
}

.xr-var-dims {
  grid-column: 2;
}

.xr-var-dtype {
  grid-column: 3;
  text-align: right;
  color: var(--xr-font-color2);
}

.xr-var-preview {
  grid-column: 4;
}

.xr-index-preview {
  grid-column: 2 / 5;
  color: var(--xr-font-color2);
}

.xr-var-name,
.xr-var-dims,
.xr-var-dtype,
.xr-preview,
.xr-attrs dt {
  white-space: nowrap;
  overflow: hidden;
  text-overflow: ellipsis;
  padding-right: 10px;
}

.xr-var-name:hover,
.xr-var-dims:hover,
.xr-var-dtype:hover,
.xr-attrs dt:hover {
  overflow: visible;
  width: auto;
  z-index: 1;
}

.xr-var-attrs,
.xr-var-data,
.xr-index-data {
  display: none;
  border-top: 2px dotted var(--xr-background-color);
  padding-bottom: 20px !important;
  padding-top: 10px !important;
}

.xr-var-attrs-in + label,
.xr-var-data-in + label,
.xr-index-data-in + label {
  padding: 0 1px;
}

.xr-var-attrs-in:checked ~ .xr-var-attrs,
.xr-var-data-in:checked ~ .xr-var-data,
.xr-index-data-in:checked ~ .xr-index-data {
  display: block;
}

.xr-var-data > table {
  float: right;
}

.xr-var-data > pre,
.xr-index-data > pre,
.xr-var-data > table > tbody > tr {
  background-color: transparent !important;
}

.xr-var-name span,
.xr-var-data,
.xr-index-name div,
.xr-index-data,
.xr-attrs {
  padding-left: 25px !important;
}

.xr-attrs,
.xr-var-attrs,
.xr-var-data,
.xr-index-data {
  grid-column: 1 / -1;
}

dl.xr-attrs {
  padding: 0;
  margin: 0;
  display: grid;
  grid-template-columns: 125px auto;
}

.xr-attrs dt,
.xr-attrs dd {
  padding: 0;
  margin: 0;
  float: left;
  padding-right: 10px;
  width: auto;
}

.xr-attrs dt {
  font-weight: normal;
  grid-column: 1;
}

.xr-attrs dt:hover span {
  display: inline-block;
  background: var(--xr-background-color);
  padding-right: 10px;
}

.xr-attrs dd {
  grid-column: 2;
  white-space: pre-wrap;
  word-break: break-all;
}

.xr-icon-database,
.xr-icon-file-text2,
.xr-no-icon {
  display: inline-block;
  vertical-align: middle;
  width: 1em;
  height: 1.5em !important;
  stroke-width: 0;
  stroke: currentColor;
  fill: currentColor;
}

.xr-var-attrs-in:checked + label > .xr-icon-file-text2,
.xr-var-data-in:checked + label > .xr-icon-database,
.xr-index-data-in:checked + label > .xr-icon-database {
  color: var(--xr-font-color0);
  filter: drop-shadow(1px 1px 5px var(--xr-font-color2));
  stroke-width: 0.8px;
}
</style><pre class='xr-text-repr-fallback'>&lt;xarray.Dataset&gt; Size: 26GB
Dimensions:     (lat: 4320, lon: 8640, wavelength: 172, rgb: 3,
                 eightbitcolor: 256)
Coordinates:
  * lat         (lat) float32 17kB 89.98 89.94 89.9 ... -89.9 -89.94 -89.98
  * lon         (lon) float32 35kB -180.0 -179.9 -179.9 ... 179.9 179.9 180.0
  * wavelength  (wavelength) float64 1kB 346.0 348.0 351.0 ... 714.0 717.0 719.0
Dimensions without coordinates: rgb, eightbitcolor
Data variables:
    Rrs         (lat, lon, wavelength) float32 26GB dask.array&lt;chunksize=(16, 1024, 8), meta=np.ndarray&gt;
    palette     (rgb, eightbitcolor) uint8 768B dask.array&lt;chunksize=(3, 256), meta=np.ndarray&gt;
Attributes: (12/64)
    product_name:                      PACE_OCI.20240601_20240630.L3m.MO.RRS....
    instrument:                        OCI
    title:                             OCI Level-3 Standard Mapped Image
    project:                           Ocean Biology Processing Group (NASA/G...
    platform:                          PACE
    source:                            satellite observations from OCI-PACE
    ...                                ...
    identifier_product_doi:            10.5067/PACE/OCI/L3M/RRS/3.1
    keywords:                          Earth Science &gt; Oceans &gt; Ocean Optics ...
    keywords_vocabulary:               NASA Global Change Master Directory (G...
    data_bins:                         16464585
    data_minimum:                      -0.009998
    data_maximum:                      0.09856601</pre><div class='xr-wrap' style='display:none'><div class='xr-header'><div class='xr-obj-type'>xarray.Dataset</div></div><ul class='xr-sections'><li class='xr-section-item'><input id='section-2be13f72-98fd-48ba-a275-02cd9478c34a' class='xr-section-summary-in' type='checkbox' disabled /><label for='section-2be13f72-98fd-48ba-a275-02cd9478c34a' class='xr-section-summary'>Dimensions:</label><div class='xr-section-inline-details'><ul class='xr-dim-list'><li><span class='xr-has-index'>lat</span>: 4320</li><li><span class='xr-has-index'>lon</span>: 8640</li><li><span class='xr-has-index'>wavelength</span>: 172</li><li><span>rgb</span>: 3</li><li><span>eightbitcolor</span>: 256</li></ul></div></li><li class='xr-section-item'><input id='section-9376b360-4ab9-48d6-848d-8951e89973af' class='xr-section-summary-in' type='checkbox' checked /><label for='section-9376b360-4ab9-48d6-848d-8951e89973af' class='xr-section-summary' title='Expand/collapse section'>Coordinates: <span>(3)</span></label><div class='xr-section-inline-details'></div><div class='xr-section-details'><ul class='xr-var-list'><li class='xr-var-item'><div class='xr-var-name'><span class='xr-has-index'>lat</span></div><div class='xr-var-dims'>(lat)</div><div class='xr-var-dtype'>float32</div><div class='xr-var-preview xr-preview'>89.98 89.94 89.9 ... -89.94 -89.98</div><input id='attrs-604068a2-60ab-4ed7-acc6-858a0b4de1ff' class='xr-var-attrs-in' type='checkbox' ><label for='attrs-604068a2-60ab-4ed7-acc6-858a0b4de1ff' title='Show/Hide attributes'><svg class='icon xr-icon-file-text2'><use xlink:href='#icon-file-text2'></use></svg></label><input id='data-c6d83428-9d4f-4819-919a-4300bd2c6840' class='xr-var-data-in' type='checkbox'><label for='data-c6d83428-9d4f-4819-919a-4300bd2c6840' title='Show/Hide data repr'><svg class='icon xr-icon-database'><use xlink:href='#icon-database'></use></svg></label><div class='xr-var-attrs'><dl class='xr-attrs'><dt><span>long_name :</span></dt><dd>Latitude</dd><dt><span>units :</span></dt><dd>degrees_north</dd><dt><span>standard_name :</span></dt><dd>latitude</dd><dt><span>valid_min :</span></dt><dd>-90.0</dd><dt><span>valid_max :</span></dt><dd>90.0</dd></dl></div><div class='xr-var-data'><pre>array([ 89.979164,  89.9375  ,  89.895836, ..., -89.895836, -89.93751 ,
       -89.97917 ], shape=(4320,), dtype=float32)</pre></div></li><li class='xr-var-item'><div class='xr-var-name'><span class='xr-has-index'>lon</span></div><div class='xr-var-dims'>(lon)</div><div class='xr-var-dtype'>float32</div><div class='xr-var-preview xr-preview'>-180.0 -179.9 ... 179.9 180.0</div><input id='attrs-1511f165-8600-456a-ab31-870301af2c22' class='xr-var-attrs-in' type='checkbox' ><label for='attrs-1511f165-8600-456a-ab31-870301af2c22' title='Show/Hide attributes'><svg class='icon xr-icon-file-text2'><use xlink:href='#icon-file-text2'></use></svg></label><input id='data-c90b9f93-9096-45f0-a746-5f52a6f8f1a7' class='xr-var-data-in' type='checkbox'><label for='data-c90b9f93-9096-45f0-a746-5f52a6f8f1a7' title='Show/Hide data repr'><svg class='icon xr-icon-database'><use xlink:href='#icon-database'></use></svg></label><div class='xr-var-attrs'><dl class='xr-attrs'><dt><span>long_name :</span></dt><dd>Longitude</dd><dt><span>units :</span></dt><dd>degrees_east</dd><dt><span>standard_name :</span></dt><dd>longitude</dd><dt><span>valid_min :</span></dt><dd>-180.0</dd><dt><span>valid_max :</span></dt><dd>180.0</dd></dl></div><div class='xr-var-data'><pre>array([-179.97917, -179.9375 , -179.89583, ...,  179.89584,  179.93752,
        179.97917], shape=(8640,), dtype=float32)</pre></div></li><li class='xr-var-item'><div class='xr-var-name'><span class='xr-has-index'>wavelength</span></div><div class='xr-var-dims'>(wavelength)</div><div class='xr-var-dtype'>float64</div><div class='xr-var-preview xr-preview'>346.0 348.0 351.0 ... 717.0 719.0</div><input id='attrs-5b858776-ba21-4839-b283-9bf288be764a' class='xr-var-attrs-in' type='checkbox' ><label for='attrs-5b858776-ba21-4839-b283-9bf288be764a' title='Show/Hide attributes'><svg class='icon xr-icon-file-text2'><use xlink:href='#icon-file-text2'></use></svg></label><input id='data-56b8cca0-432f-4f31-add8-837e1cad6157' class='xr-var-data-in' type='checkbox'><label for='data-56b8cca0-432f-4f31-add8-837e1cad6157' title='Show/Hide data repr'><svg class='icon xr-icon-database'><use xlink:href='#icon-database'></use></svg></label><div class='xr-var-attrs'><dl class='xr-attrs'><dt><span>long_name :</span></dt><dd>wavelengths</dd><dt><span>units :</span></dt><dd>nm</dd><dt><span>valid_min :</span></dt><dd>0</dd><dt><span>valid_max :</span></dt><dd>20000</dd></dl></div><div class='xr-var-data'><pre>array([346., 348., 351., 353., 356., 358., 361., 363., 366., 368., 371., 373.,
       375., 378., 380., 383., 385., 388., 390., 393., 395., 398., 400., 403.,
       405., 408., 410., 413., 415., 418., 420., 422., 425., 427., 430., 432.,
       435., 437., 440., 442., 445., 447., 450., 452., 455., 457., 460., 462.,
       465., 467., 470., 472., 475., 477., 480., 482., 485., 487., 490., 492.,
       495., 497., 500., 502., 505., 507., 510., 512., 515., 517., 520., 522.,
       525., 527., 530., 532., 535., 537., 540., 542., 545., 547., 550., 553.,
       555., 558., 560., 563., 565., 568., 570., 573., 575., 578., 580., 583.,
       586., 588., 613., 615., 618., 620., 623., 625., 627., 630., 632., 635.,
       637., 640., 641., 642., 643., 645., 646., 647., 648., 650., 651., 652.,
       653., 655., 656., 657., 658., 660., 661., 662., 663., 665., 666., 667.,
       668., 670., 671., 672., 673., 675., 676., 677., 678., 679., 681., 682.,
       683., 684., 686., 687., 688., 689., 691., 692., 693., 694., 696., 697.,
       698., 699., 701., 702., 703., 704., 706., 707., 708., 709., 711., 712.,
       713., 714., 717., 719.])</pre></div></li></ul></div></li><li class='xr-section-item'><input id='section-17b92fdd-d565-465f-be1f-f73e682f7c28' class='xr-section-summary-in' type='checkbox' checked /><label for='section-17b92fdd-d565-465f-be1f-f73e682f7c28' class='xr-section-summary' title='Expand/collapse section'>Data variables: <span>(2)</span></label><div class='xr-section-inline-details'></div><div class='xr-section-details'><ul class='xr-var-list'><li class='xr-var-item'><div class='xr-var-name'><span>Rrs</span></div><div class='xr-var-dims'>(lat, lon, wavelength)</div><div class='xr-var-dtype'>float32</div><div class='xr-var-preview xr-preview'>dask.array&lt;chunksize=(16, 1024, 8), meta=np.ndarray&gt;</div><input id='attrs-d04ec4a2-9002-4f48-a16c-50d43e3cf3ac' class='xr-var-attrs-in' type='checkbox' ><label for='attrs-d04ec4a2-9002-4f48-a16c-50d43e3cf3ac' title='Show/Hide attributes'><svg class='icon xr-icon-file-text2'><use xlink:href='#icon-file-text2'></use></svg></label><input id='data-a781e500-564b-4f56-8227-c8f04deed97f' class='xr-var-data-in' type='checkbox'><label for='data-a781e500-564b-4f56-8227-c8f04deed97f' title='Show/Hide data repr'><svg class='icon xr-icon-database'><use xlink:href='#icon-database'></use></svg></label><div class='xr-var-attrs'><dl class='xr-attrs'><dt><span>long_name :</span></dt><dd>Remote sensing reflectance</dd><dt><span>units :</span></dt><dd>sr^-1</dd><dt><span>standard_name :</span></dt><dd>surface_ratio_of_upwelling_radiance_emerging_from_sea_water_to_downwelling_radiative_flux_in_air</dd><dt><span>valid_min :</span></dt><dd>-30000</dd><dt><span>valid_max :</span></dt><dd>25000</dd><dt><span>display_scale :</span></dt><dd>linear</dd><dt><span>display_min :</span></dt><dd>0.0</dd><dt><span>display_max :</span></dt><dd>0.025</dd></dl></div><div class='xr-var-data'><table>
    <tr>
        <td>
            <table style="border-collapse: collapse;">
                <thead>
                    <tr>
                        <td> </td>
                        <th> Array </th>
                        <th> Chunk </th>
                    </tr>
                </thead>
                <tbody>

                    <tr>
                        <th> Bytes </th>
                        <td> 23.92 GiB </td>
                        <td> 512.00 kiB </td>
                    </tr>

                    <tr>
                        <th> Shape </th>
                        <td> (4320, 8640, 172) </td>
                        <td> (16, 1024, 8) </td>
                    </tr>
                    <tr>
                        <th> Dask graph </th>
                        <td colspan="2"> 53460 chunks in 2 graph layers </td>
                    </tr>
                    <tr>
                        <th> Data type </th>
                        <td colspan="2"> float32 numpy.ndarray </td>
                    </tr>
                </tbody>
            </table>
        </td>
        <td>
        <svg width="124" height="205" style="stroke:rgb(0,0,0);stroke-width:1" >

  <!-- Horizontal lines -->
  <line x1="10" y1="0" x2="45" y2="35" style="stroke-width:2" />
  <line x1="10" y1="14" x2="45" y2="49" />
  <line x1="10" y1="28" x2="45" y2="63" />
  <line x1="10" y1="42" x2="45" y2="77" />
  <line x1="10" y1="56" x2="45" y2="92" />
  <line x1="10" y1="71" x2="45" y2="106" />
  <line x1="10" y1="85" x2="45" y2="120" />
  <line x1="10" y1="99" x2="45" y2="134" />
  <line x1="10" y1="113" x2="45" y2="149" />
  <line x1="10" y1="120" x2="45" y2="155" style="stroke-width:2" />

  <!-- Vertical lines -->
  <line x1="10" y1="0" x2="10" y2="120" style="stroke-width:2" />
  <line x1="11" y1="1" x2="11" y2="121" />
  <line x1="13" y1="3" x2="13" y2="123" />
  <line x1="15" y1="5" x2="15" y2="125" />
  <line x1="17" y1="7" x2="17" y2="127" />
  <line x1="19" y1="9" x2="19" y2="129" />
  <line x1="21" y1="11" x2="21" y2="131" />
  <line x1="22" y1="12" x2="22" y2="132" />
  <line x1="24" y1="14" x2="24" y2="134" />
  <line x1="26" y1="16" x2="26" y2="136" />
  <line x1="28" y1="18" x2="28" y2="138" />
  <line x1="30" y1="20" x2="30" y2="140" />
  <line x1="32" y1="22" x2="32" y2="142" />
  <line x1="34" y1="24" x2="34" y2="144" />
  <line x1="35" y1="25" x2="35" y2="145" />
  <line x1="37" y1="27" x2="37" y2="147" />
  <line x1="39" y1="29" x2="39" y2="149" />
  <line x1="41" y1="31" x2="41" y2="151" />
  <line x1="43" y1="33" x2="43" y2="153" />
  <line x1="45" y1="35" x2="45" y2="155" style="stroke-width:2" />

  <!-- Colored Rectangle -->
  <polygon points="10.0,0.0 45.294117647058826,35.294117647058826 45.294117647058826,155.29411764705884 10.0,120.0" style="fill:#8B4903A0;stroke-width:0"/>

  <!-- Horizontal lines -->
  <line x1="10" y1="0" x2="39" y2="0" style="stroke-width:2" />
  <line x1="11" y1="1" x2="40" y2="1" />
  <line x1="13" y1="3" x2="42" y2="3" />
  <line x1="15" y1="5" x2="44" y2="5" />
  <line x1="17" y1="7" x2="46" y2="7" />
  <line x1="19" y1="9" x2="48" y2="9" />
  <line x1="21" y1="11" x2="50" y2="11" />
  <line x1="22" y1="12" x2="51" y2="12" />
  <line x1="24" y1="14" x2="53" y2="14" />
  <line x1="26" y1="16" x2="55" y2="16" />
  <line x1="28" y1="18" x2="57" y2="18" />
  <line x1="30" y1="20" x2="59" y2="20" />
  <line x1="32" y1="22" x2="61" y2="22" />
  <line x1="34" y1="24" x2="63" y2="24" />
  <line x1="35" y1="25" x2="64" y2="25" />
  <line x1="37" y1="27" x2="66" y2="27" />
  <line x1="39" y1="29" x2="68" y2="29" />
  <line x1="41" y1="31" x2="70" y2="31" />
  <line x1="43" y1="33" x2="72" y2="33" />
  <line x1="45" y1="35" x2="74" y2="35" style="stroke-width:2" />

  <!-- Vertical lines -->
  <line x1="10" y1="0" x2="45" y2="35" style="stroke-width:2" />
  <line x1="11" y1="0" x2="46" y2="35" />
  <line x1="12" y1="0" x2="47" y2="35" />
  <line x1="14" y1="0" x2="49" y2="35" />
  <line x1="15" y1="0" x2="50" y2="35" />
  <line x1="16" y1="0" x2="52" y2="35" />
  <line x1="18" y1="0" x2="53" y2="35" />
  <line x1="20" y1="0" x2="56" y2="35" />
  <line x1="22" y1="0" x2="57" y2="35" />
  <line x1="23" y1="0" x2="58" y2="35" />
  <line x1="24" y1="0" x2="60" y2="35" />
  <line x1="26" y1="0" x2="61" y2="35" />
  <line x1="27" y1="0" x2="62" y2="35" />
  <line x1="30" y1="0" x2="65" y2="35" />
  <line x1="31" y1="0" x2="66" y2="35" />
  <line x1="32" y1="0" x2="68" y2="35" />
  <line x1="34" y1="0" x2="69" y2="35" />
  <line x1="35" y1="0" x2="70" y2="35" />
  <line x1="36" y1="0" x2="72" y2="35" />
  <line x1="39" y1="0" x2="74" y2="35" style="stroke-width:2" />

  <!-- Colored Rectangle -->
  <polygon points="10.0,0.0 39.00452664260737,0.0 74.29864428966619,35.294117647058826 45.294117647058826,35.294117647058826" style="fill:#8B4903A0;stroke-width:0"/>

  <!-- Horizontal lines -->
  <line x1="45" y1="35" x2="74" y2="35" style="stroke-width:2" />
  <line x1="45" y1="49" x2="74" y2="49" />
  <line x1="45" y1="63" x2="74" y2="63" />
  <line x1="45" y1="77" x2="74" y2="77" />
  <line x1="45" y1="92" x2="74" y2="92" />
  <line x1="45" y1="106" x2="74" y2="106" />
  <line x1="45" y1="120" x2="74" y2="120" />
  <line x1="45" y1="134" x2="74" y2="134" />
  <line x1="45" y1="149" x2="74" y2="149" />
  <line x1="45" y1="155" x2="74" y2="155" style="stroke-width:2" />

  <!-- Vertical lines -->
  <line x1="45" y1="35" x2="45" y2="155" style="stroke-width:2" />
  <line x1="46" y1="35" x2="46" y2="155" />
  <line x1="47" y1="35" x2="47" y2="155" />
  <line x1="49" y1="35" x2="49" y2="155" />
  <line x1="50" y1="35" x2="50" y2="155" />
  <line x1="52" y1="35" x2="52" y2="155" />
  <line x1="53" y1="35" x2="53" y2="155" />
  <line x1="56" y1="35" x2="56" y2="155" />
  <line x1="57" y1="35" x2="57" y2="155" />
  <line x1="58" y1="35" x2="58" y2="155" />
  <line x1="60" y1="35" x2="60" y2="155" />
  <line x1="61" y1="35" x2="61" y2="155" />
  <line x1="62" y1="35" x2="62" y2="155" />
  <line x1="65" y1="35" x2="65" y2="155" />
  <line x1="66" y1="35" x2="66" y2="155" />
  <line x1="68" y1="35" x2="68" y2="155" />
  <line x1="69" y1="35" x2="69" y2="155" />
  <line x1="70" y1="35" x2="70" y2="155" />
  <line x1="72" y1="35" x2="72" y2="155" />
  <line x1="74" y1="35" x2="74" y2="155" style="stroke-width:2" />

  <!-- Colored Rectangle -->
  <polygon points="45.294117647058826,35.294117647058826 74.29864428966619,35.294117647058826 74.29864428966619,155.29411764705884 45.294117647058826,155.29411764705884" style="fill:#8B4903A0;stroke-width:0"/>

  <!-- Text -->
  <text x="59.796380968362506" y="175.29411764705884" font-size="1.0rem" font-weight="100" text-anchor="middle" >172</text>
  <text x="94.29864428966619" y="95.29411764705884" font-size="1.0rem" font-weight="100" text-anchor="middle" transform="rotate(-90,94.29864428966619,95.29411764705884)">8640</text>
  <text x="17.647058823529413" y="157.64705882352942" font-size="1.0rem" font-weight="100" text-anchor="middle" transform="rotate(45,17.647058823529413,157.64705882352942)">4320</text>
</svg>
        </td>
    </tr>
</table></div></li><li class='xr-var-item'><div class='xr-var-name'><span>palette</span></div><div class='xr-var-dims'>(rgb, eightbitcolor)</div><div class='xr-var-dtype'>uint8</div><div class='xr-var-preview xr-preview'>dask.array&lt;chunksize=(3, 256), meta=np.ndarray&gt;</div><input id='attrs-41441138-e59b-4687-a837-2b02090cf364' class='xr-var-attrs-in' type='checkbox' disabled><label for='attrs-41441138-e59b-4687-a837-2b02090cf364' title='Show/Hide attributes'><svg class='icon xr-icon-file-text2'><use xlink:href='#icon-file-text2'></use></svg></label><input id='data-ff34f285-a8ab-4eb3-993a-6a4f52050ac1' class='xr-var-data-in' type='checkbox'><label for='data-ff34f285-a8ab-4eb3-993a-6a4f52050ac1' title='Show/Hide data repr'><svg class='icon xr-icon-database'><use xlink:href='#icon-database'></use></svg></label><div class='xr-var-attrs'><dl class='xr-attrs'></dl></div><div class='xr-var-data'><table>
    <tr>
        <td>
            <table style="border-collapse: collapse;">
                <thead>
                    <tr>
                        <td> </td>
                        <th> Array </th>
                        <th> Chunk </th>
                    </tr>
                </thead>
                <tbody>

                    <tr>
                        <th> Bytes </th>
                        <td> 768 B </td>
                        <td> 768 B </td>
                    </tr>

                    <tr>
                        <th> Shape </th>
                        <td> (3, 256) </td>
                        <td> (3, 256) </td>
                    </tr>
                    <tr>
                        <th> Dask graph </th>
                        <td colspan="2"> 1 chunks in 2 graph layers </td>
                    </tr>
                    <tr>
                        <th> Data type </th>
                        <td colspan="2"> uint8 numpy.ndarray </td>
                    </tr>
                </tbody>
            </table>
        </td>
        <td>
        <svg width="170" height="76" style="stroke:rgb(0,0,0);stroke-width:1" >

  <!-- Horizontal lines -->
  <line x1="0" y1="0" x2="120" y2="0" style="stroke-width:2" />
  <line x1="0" y1="26" x2="120" y2="26" style="stroke-width:2" />

  <!-- Vertical lines -->
  <line x1="0" y1="0" x2="0" y2="26" style="stroke-width:2" />
  <line x1="120" y1="0" x2="120" y2="26" style="stroke-width:2" />

  <!-- Colored Rectangle -->
  <polygon points="0.0,0.0 120.0,0.0 120.0,26.188049901537102 0.0,26.188049901537102" style="fill:#ECB172A0;stroke-width:0"/>

  <!-- Text -->
  <text x="60.0" y="46.1880499015371" font-size="1.0rem" font-weight="100" text-anchor="middle" >256</text>
  <text x="140.0" y="13.094024950768551" font-size="1.0rem" font-weight="100" text-anchor="middle" transform="rotate(0,140.0,13.094024950768551)">3</text>
</svg>
        </td>
    </tr>
</table></div></li></ul></div></li><li class='xr-section-item'><input id='section-2911b7d8-f122-43c8-b85a-f23fbba0c67f' class='xr-section-summary-in' type='checkbox' /><label for='section-2911b7d8-f122-43c8-b85a-f23fbba0c67f' class='xr-section-summary' title='Expand/collapse section'>Attributes: <span>(64)</span></label><div class='xr-section-inline-details'></div><div class='xr-section-details'><dl class='xr-attrs'><dt><span>product_name :</span></dt><dd>PACE_OCI.20240601_20240630.L3m.MO.RRS.V3_1.Rrs.4km.nc</dd><dt><span>instrument :</span></dt><dd>OCI</dd><dt><span>title :</span></dt><dd>OCI Level-3 Standard Mapped Image</dd><dt><span>project :</span></dt><dd>Ocean Biology Processing Group (NASA/GSFC/OBPG)</dd><dt><span>platform :</span></dt><dd>PACE</dd><dt><span>source :</span></dt><dd>satellite observations from OCI-PACE</dd><dt><span>temporal_range :</span></dt><dd>month</dd><dt><span>processing_version :</span></dt><dd>3.1</dd><dt><span>date_created :</span></dt><dd>2025-09-21T23:56:08.000Z</dd><dt><span>history :</span></dt><dd>l3mapgen par=PACE_OCI.20240601_20240630.L3m.MO.RRS.V3_1.Rrs.4km.nc.param </dd><dt><span>l2_flag_names :</span></dt><dd>ATMFAIL,LAND,HILT,HISATZEN,STRAYLIGHT,CLDICE,COCCOLITH,LOWLW,CHLWARN,CHLFAIL,NAVWARN,MAXAERITER,HISOLZEN,NAVFAIL,FILTER,HIGLINT</dd><dt><span>time_coverage_start :</span></dt><dd>2024-06-01T00:24:11.162Z</dd><dt><span>time_coverage_end :</span></dt><dd>2024-07-01T01:28:03.894Z</dd><dt><span>start_orbit_number :</span></dt><dd>0</dd><dt><span>end_orbit_number :</span></dt><dd>0</dd><dt><span>map_projection :</span></dt><dd>Equidistant Cylindrical</dd><dt><span>latitude_units :</span></dt><dd>degrees_north</dd><dt><span>longitude_units :</span></dt><dd>degrees_east</dd><dt><span>northernmost_latitude :</span></dt><dd>90.0</dd><dt><span>southernmost_latitude :</span></dt><dd>-90.0</dd><dt><span>westernmost_longitude :</span></dt><dd>-180.0</dd><dt><span>easternmost_longitude :</span></dt><dd>180.0</dd><dt><span>geospatial_lat_max :</span></dt><dd>90.0</dd><dt><span>geospatial_lat_min :</span></dt><dd>-90.0</dd><dt><span>geospatial_lon_max :</span></dt><dd>180.0</dd><dt><span>geospatial_lon_min :</span></dt><dd>-180.0</dd><dt><span>latitude_step :</span></dt><dd>0.041666668</dd><dt><span>longitude_step :</span></dt><dd>0.041666668</dd><dt><span>sw_point_latitude :</span></dt><dd>-89.979164</dd><dt><span>sw_point_longitude :</span></dt><dd>-179.97917</dd><dt><span>spatialResolution :</span></dt><dd>4.638312 km</dd><dt><span>geospatial_lon_resolution :</span></dt><dd>4.638312 km</dd><dt><span>geospatial_lat_resolution :</span></dt><dd>4.638312 km</dd><dt><span>geospatial_lat_units :</span></dt><dd>degrees_north</dd><dt><span>geospatial_lon_units :</span></dt><dd>degrees_east</dd><dt><span>number_of_lines :</span></dt><dd>4320</dd><dt><span>number_of_columns :</span></dt><dd>8640</dd><dt><span>measure :</span></dt><dd>Mean</dd><dt><span>suggested_image_scaling_minimum :</span></dt><dd>0.0</dd><dt><span>suggested_image_scaling_maximum :</span></dt><dd>0.025</dd><dt><span>suggested_image_scaling_type :</span></dt><dd>LINEAR</dd><dt><span>suggested_image_scaling_applied :</span></dt><dd>No</dd><dt><span>_lastModified :</span></dt><dd>2025-09-21T23:56:08.000Z</dd><dt><span>Conventions :</span></dt><dd>CF-1.6 ACDD-1.3</dd><dt><span>institution :</span></dt><dd>NASA Goddard Space Flight Center, Ocean Ecology Laboratory, Ocean Biology Processing Group</dd><dt><span>standard_name_vocabulary :</span></dt><dd>CF Standard Name Table v36</dd><dt><span>naming_authority :</span></dt><dd>gov.nasa.gsfc.sci.oceandata</dd><dt><span>id :</span></dt><dd>3.1/L3/PACE_OCI.20240601_20240630.L3b.MO.RRS.V3_1.nc</dd><dt><span>license :</span></dt><dd>https://science.nasa.gov/earth-science/earth-science-data/data-information-policy/</dd><dt><span>creator_name :</span></dt><dd>NASA/GSFC/OBPG</dd><dt><span>publisher_name :</span></dt><dd>NASA/GSFC/OBPG</dd><dt><span>creator_email :</span></dt><dd>data@oceancolor.gsfc.nasa.gov</dd><dt><span>publisher_email :</span></dt><dd>data@oceancolor.gsfc.nasa.gov</dd><dt><span>creator_url :</span></dt><dd>https://oceandata.sci.gsfc.nasa.gov</dd><dt><span>publisher_url :</span></dt><dd>https://oceandata.sci.gsfc.nasa.gov</dd><dt><span>processing_level :</span></dt><dd>L3 Mapped</dd><dt><span>cdm_data_type :</span></dt><dd>grid</dd><dt><span>identifier_product_doi_authority :</span></dt><dd>http://dx.doi.org</dd><dt><span>identifier_product_doi :</span></dt><dd>10.5067/PACE/OCI/L3M/RRS/3.1</dd><dt><span>keywords :</span></dt><dd>Earth Science &gt; Oceans &gt; Ocean Optics &gt; Reflectance</dd><dt><span>keywords_vocabulary :</span></dt><dd>NASA Global Change Master Directory (GCMD) Science Keywords</dd><dt><span>data_bins :</span></dt><dd>16464585</dd><dt><span>data_minimum :</span></dt><dd>-0.009998</dd><dt><span>data_maximum :</span></dt><dd>0.09856601</dd></dl></div></li></ul></div></div>




```python

```
