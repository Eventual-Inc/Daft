from __future__ import annotations

import sys
from warnings import warn

import tqdm

IS_INTERACTIVE = False
try:
    get_ipython = sys.modules["IPython"].get_ipython
    if "IPKernelApp" not in get_ipython().config:  # pragma: no cover
        raise ImportError("console")
    from tqdm.notebook import WARN_NOIPYW, IProgress

    if IProgress is None:
        from tqdm.std import TqdmWarning

        warn(WARN_NOIPYW, TqdmWarning, stacklevel=2)
        raise ImportError("ipywidgets")
except Exception:
    from tqdm.std import tqdm, trange
else:  # pragma: no cover

    from tqdm.notebook import tqdm, trange

    IS_INTERACTIVE = True
__all__ = ["tqdm", "trange"]
