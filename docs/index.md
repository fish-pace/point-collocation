# point-collocation

Point-based lat/lon/time matchups against cloud-hosted NASA EarthData granules. A [NASA EarthData account](https://urs.earthdata.nasa.gov/) (free) is required for accessing the data.

## Key Features

- **Search** — find granules that cover your points using NASA Earthdata (`earthaccess`)
- **Plan** — preview point-to-granule routing before committing to a full extraction
- **Matchup** — extract variables at each (lat, lon, time) location in one call
- **Grid & Swath** — works with L3/gridded (1-D lat/lon) and L2/swath (2-D lat/lon) data

## Quick Install

```bash
pip install point-collocation
```

Available on [PyPI](https://pypi.org/project/point-collocation/).

## Requirement for the data frame passes to `pc.plan()`

Your data should be a pandas dataframe with one row per point. Each needs `lat` (not `latitude`), `lon` (not `longitude`) and `time` (or `date`). The time should resolve to a date. If time is not present, the time is assumed to be noon UTC on the date for matching to granules. Time matching uses the granule start/end metadata from the CMR metadata returned by `earthaccess.search_data()` not by opening and inspecting the actual granule. Determination of whether the point is matched to a granule spatially also uses the CMR metadata returned by `earthaccess.search_data()`.

Optional: `pc_id` column. This will be used as the points identifier for cases where a point matches multiple granules. In that case, the returned data frame will have multiple rows with that `pc_id`, where each row corresponds to a matched granule.

*Additional columns*: Any additional columns are ignored but will be returned as part of the dataframe with original points with matchup data added.

## Minimal Example

See the detailed examples in left nav bar.

```python
# make sure you can authenticate and are authenticated
import earthaccess
earthaccess.login()

import point_collocation as pc
import pandas as pd

df = pd.DataFrame({
    "lat":  [34.5, 35.1],
    "lon":  [-120.3, -119.8],
    "time": pd.to_datetime(["2023-06-01", "2023-06-02"]),
})

p = pc.plan(
    df,
    data_source="earthaccess",
    source_kwargs={
        "short_name": "PACE_OCI_L3M_RRS", 
        "granule_name": "*.DAY.*.4km.*"},
)

# out contains the matchuped Rrs data
out = pc.matchup(p, variables=["Rrs"])
```

## Navigation

- [Installation](installation.md) — full install options
- [Quickstart](quickstart.md) — end-to-end example for gridded (L3) data
- [Examples](1_l3_examples.md) — L3, L2 swath, many points, ICESat-2, and more
- [API Reference](api.md) — auto-generated from source docstrings
- [Contributing](contributing.md) — dev setup, tests, local docs preview
