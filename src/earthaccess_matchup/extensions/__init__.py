"""Optional extensions that layer on top of the core engine.

Extensions
----------
spatial   : spatial averaging / neighbourhood extraction
qa        : quality-flag filtering before extraction
accessor  : optional ``xarray.Dataset.eam`` accessor

Each extension is imported lazily so the optional dependencies they
need do not have to be installed unless the extension is actually used.
"""
