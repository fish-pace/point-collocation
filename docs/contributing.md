# Contributing

Thank you for your interest in improving **point-collocation**!

## Set Up a Dev Environment

```bash
git clone https://github.com/fish-pace/point-collocation.git
cd point-collocation
pip install -e ".[xoak-extra,dev]"
```

Install additional test dependencies required by some test modules (e.g. `test_adapters.py` needs `netcdf4`; `h5netcdf` and `dask` are used by integration tests):

```bash
pip install netcdf4 h5netcdf dask
```

## Run Tests

```bash
python -m pytest tests/ --ignore=tests/test_adapters.py
```

To include the adapter tests (requires a running netCDF4 installation):

```bash
python -m pytest tests/
```

## Code Style

The project uses [Ruff](https://docs.astral.sh/ruff/) for linting and formatting:

```bash
ruff check src/
ruff format src/
```

Type checking with mypy:

```bash
mypy src/
```

## Run Documentation Locally

Install docs dependencies:

```bash
pip install -e ".[docs]"
```

Serve locally with live reload:

```bash
mkdocs serve
```

Then open <http://127.0.0.1:8000> in your browser.

## Build the Docs

```bash
mkdocs build
```

Output is written to the `site/` directory (excluded from version control).

## Submitting a PR

1. Fork the repository and create a feature branch.
2. Make your changes with tests where appropriate.
3. Run `ruff check`, `mypy`, and `pytest` before opening a PR.
4. Open a pull request against `main`.
