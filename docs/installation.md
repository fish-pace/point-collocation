# Installation

## Requirements

- Python ≥ 3.10
- A free [NASA Earthdata account](https://urs.earthdata.nasa.gov/) (required to search and download data)

## Standard Install

```bash
pip install point-collocation
```

This installs the core package with all core functionality: NASA Earthdata integration (`earthaccess`), CF-convention support (`cf-xarray`), and spatial interpolation (`scipy`). The package is available on [PyPI](https://pypi.org/project/point-collocation/).

## L2 / Swath Data with xoak

To use advanced xoak-based spatial lookup algorithms (2-D lat/lon swath data via `xoak` and `scikit-learn`):

```bash
pip install point-collocation[xoak-extra]
```

## All Optional Features

```bash
pip install point-collocation[all]
```

## Development Install

Clone the repository and install in editable mode with all dev dependencies:

```bash
git clone https://github.com/fish-pace/point-collocation.git
cd point-collocation
pip install -e ".[xoak-extra,dev]"
```

## Verify

```python
import point_collocation as pc
print(pc.__version__)  # should print 0.5.0 (or newer)
```
