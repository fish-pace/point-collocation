# point-collocation

Point-based lat/lon/time matchups against cloud-hosted NASA EarthData granules.

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

## Minimal Example

```python
import earthaccess
import point_collocation as pc
import pandas as pd

earthaccess.login()

df = pd.DataFrame({
    "lat":  [34.5, 35.1],
    "lon":  [-120.3, -119.8],
    "time": pd.to_datetime(["2023-06-01", "2023-06-02"]),
})

p = pc.plan(
    df,
    data_source="earthaccess",
    source_kwargs={"short_name": "PACE_OCI_L3M_RRS", "granule_name": "*.DAY.*.4km.*"},
)
p.summary()

out = pc.matchup(p, variables=["Rrs"])
print(out)
```

## Navigation

- [Installation](installation.md) — full install options
- [Quickstart](quickstart.md) — end-to-end example for gridded (L3) data
- [Examples](1_l3_examples.md) — L3, L2 swath, many points, ICESat-2, and more
- [API Reference](api.md) — auto-generated from source docstrings
- [Contributing](contributing.md) — dev setup, tests, local docs preview
