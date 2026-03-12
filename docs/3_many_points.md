# Large number of point matchups

When we have 10s of thousands of points for matchups, we need to be careful with memory. `point-collocation` will try to minimize memory accummulation, but you may still need to use a machine with more RAM to handle the task. 5Gb RAM is needed for the example with 15k+ points here. The amount of memory needed depends a bit on how your points are distributed across the grid and how they match up with the underlying netcdf chunking. The user doesn't have to worry abou that however. The package is designed to take care of using memory efficient approaches.

Also keeping a very large dataframe in memory will consume RAM. So if RAM is limited, or you are concerned about your machine crashing and losing work, you can save your matchups as you go along.

## Load points

The examples shows how to process 15k+ matchups (from BGC-Argo buoys) and save to intermediate parquet files. We do not have to save to intermediate files, but when doing a long processing job, this is wise so that you don't lose work in case your machine crashes or the job stopped for some reason. `point-collocation` can start from specific granules so that you can start from where your saved work stopped.


```python
import pandas as pd
df = pd.read_parquet("https://raw.githubusercontent.com/fish-pace/fish-pace-datasets/main/datasets/chla_z/data/CHLA_argo_profiles.parquet")
df_points = df[["TIME", "LATITUDE", "LONGITUDE"]].rename(
    columns={
        "TIME": "time",
        "LATITUDE": "lat",
        "LONGITUDE": "lon"
    }
)
print(len(df_points))
df_points.head()
```

    15833





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
      <th>time</th>
      <th>lat</th>
      <th>lon</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>2024-03-01 21:23:16.002000128</td>
      <td>54.6582</td>
      <td>-19.2434</td>
    </tr>
    <tr>
      <th>1</th>
      <td>2024-03-11 20:45:53.002000128</td>
      <td>54.9187</td>
      <td>-18.9609</td>
    </tr>
    <tr>
      <th>2</th>
      <td>2024-03-21 21:21:39.002000128</td>
      <td>55.2967</td>
      <td>-18.8331</td>
    </tr>
    <tr>
      <th>3</th>
      <td>2024-03-31 21:31:53.002000128</td>
      <td>55.7268</td>
      <td>-18.8653</td>
    </tr>
    <tr>
      <th>4</th>
      <td>2024-03-07 18:01:17.002000128</td>
      <td>17.6665</td>
      <td>-46.0155</td>
    </tr>
  </tbody>
</table>
</div>



## We create a plan for these points

15k points takes about 15 seconds to search EarthData catalog and develop a plan. Note some of these data point are covered in the near real time product (NRT), so show up as having no granules.


```python
%%time
import point_collocation as pc
plan = pc.plan(
    df_points,
    data_source="earthaccess",
    source_kwargs={
        "short_name": "PACE_OCI_L3M_RRS",
        "granule_name": "*.DAY.*.4km.*",
    }
)
```

    CPU times: user 933 ms, sys: 110 ms, total: 1.04 s
    Wall time: 3.06 s



```python
plan.summary(n=0)
```

    Plan: 15833 points → 620 unique granule(s)
      Points with 0 matches : 416
      Points with >1 matches: 0
      Time buffer: 0 days 00:00:00


## Now matchup

We use `save_dir` to specify that the batches (of 10) should be saved to `_temp_dir`. They will be saved as `plan_A_B.parquet` showing what granules/files were processed. If you need to rerun some granules or pick up where you left off, you can pass in `granule_range=[x,y]`.

This takes about 2 hrs.


```python
%%time
res = pc.matchup(plan, variables=["Rrs"], 
                 save_dir="_temp_data",
                batch_size=10)
```

## Rerun some granules

If you machine crashes and you need to rerun some granules:


```python
# resume matchups from granule 200
res = pc.matchup(plan, 
                 variables=["Rrs"], 
                 save_dir="_temp_data",
                 batch_size=10,
                 granule_range=[200:])
```

## Merging the saved parquet

The intermediary saved matched are in batches. You can load and merge as:


```python
df_full = pd.concat([pd.read_parquet(f) for f in sorted(Path("_temp_data").glob("*.parquet"))])
```
