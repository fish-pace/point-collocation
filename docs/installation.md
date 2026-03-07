# Installation

## Requirements

- Python ≥ 3.10
- A free [NASA Earthdata account](https://urs.earthdata.nasa.gov/) (required to search and download data)

## Standard Install

```bash
pip install point-collocation[earthaccess]
```

This installs the core package plus [`earthaccess`](https://github.com/nsidc/earthaccess) for NASA Earthdata integration. The package is available on [PyPI](https://pypi.org/project/point-collocation/).

## L2 / Swath Data

To work with L2 swath data (2-D lat/lon arrays) you also need [`xoak`](https://xoak.readthedocs.io/) and `scikit-learn`:

```bash
pip install point-collocation[earthaccess,swath]
```

## Development Install

Clone the repository and install in editable mode with all dev dependencies:

```bash
git clone https://github.com/fish-pace/earthaccess_matchup.git
cd earthaccess_matchup
pip install -e ".[earthaccess,swath,dev]"
```

## Verify

```python
import point_collocation as pc
print(pc.__version__)  # should print 0.1.0 (or newer)
```
