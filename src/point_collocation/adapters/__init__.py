"""Source adapters that normalise heterogeneous inputs into the SourceProtocol.

Built-in adapters
-----------------
earthaccess  : wraps file-like objects returned by ``earthaccess.open()``

Future adapters (not yet implemented)
--------------------------------------
stac         : STAC item assets
url          : plain HTTPS URLs
local        : local file paths
"""

from point_collocation.adapters.base import SourceAdapter

__all__ = ["SourceAdapter"]
