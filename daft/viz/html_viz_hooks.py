from __future__ import annotations

import base64
import io
from typing import TYPE_CHECKING, Callable, TypeVar

if TYPE_CHECKING:
    import numpy as np
    import PIL.Image


HookClass = TypeVar("HookClass")

_VIZ_HOOKS_REGISTRY = {}


def register_viz_hook(klass: type[HookClass], hook: Callable[[object], str]):
    """Registers a visualization hook that returns the appropriate HTML for
    visualizing a specific class in HTML"""
    _VIZ_HOOKS_REGISTRY[klass] = hook


def get_viz_hook(val: object) -> Callable[[object], str] | None:
    for klass in _VIZ_HOOKS_REGISTRY:
        if isinstance(val, klass):
            return _VIZ_HOOKS_REGISTRY[klass]
    return None


###
# Default hooks, registered at import-time
###

HAS_PILLOW = True
try:
    import PIL.Image
except ImportError:
    HAS_PILLOW = False

HAS_NUMPY = True
try:
    import numpy as np
except ImportError:
    HAS_NUMPY = False

if HAS_PILLOW:

    def _viz_pil_image(val: PIL.Image.Image) -> str:
        img = val.copy()
        img.thumbnail((128, 128))
        bio = io.BytesIO()
        img.save(bio, "JPEG")
        base64_img = base64.b64encode(bio.getvalue())
        return f'<img style="max-height:128px;width:auto" src="data:image/png;base64, {base64_img.decode("utf-8")}" alt="{str(val)}" />'

    register_viz_hook(PIL.Image.Image, _viz_pil_image)

if HAS_NUMPY:

    def _viz_numpy(val: np.ndarray) -> str:
        return f"&ltnp.ndarray<br>shape={val.shape}<br>dtype={val.dtype}&gt"

    register_viz_hook(np.ndarray, _viz_numpy)
