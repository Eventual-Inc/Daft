from __future__ import annotations

import base64
import io
from typing import Callable, TypeVar

from daft.dependencies import np, pil_image

HookClass = TypeVar("HookClass")

_VIZ_HOOKS_REGISTRY = {}
_NUMPY_REGISTERED = False
_PILLOW_REGISTERED = False


def register_viz_hook(klass: type[HookClass], hook: Callable[[object], str]):
    """Registers a visualization hook that returns the appropriate HTML for visualizing a specific class in HTML."""
    _VIZ_HOOKS_REGISTRY[klass] = hook


def get_viz_hook(val: object) -> Callable[[object], str] | None:
    global _NUMPY_REGISTERED
    global _PILLOW_REGISTERED
    if np.module_available() and not _NUMPY_REGISTERED:

        def _viz_numpy(val: np.ndarray) -> str:
            return f"&ltnp.ndarray<br>shape={val.shape}<br>dtype={val.dtype}&gt"

        register_viz_hook(np.ndarray, _viz_numpy)
        _NUMPY_REGISTERED = True

    if pil_image.module_available() and not _PILLOW_REGISTERED:

        def _viz_pil_image(val: pil_image.Image) -> str:
            img = val.copy()
            img.thumbnail((128, 128))
            bio = io.BytesIO()
            img.save(bio, "JPEG")
            base64_img = base64.b64encode(bio.getvalue())
            return f'<img style="max-height:128px;width:auto" src="data:image/png;base64, {base64_img.decode("utf-8")}" alt="{val!s}" />'

        register_viz_hook(pil_image.Image, _viz_pil_image)
        _PILLOW_REGISTERED = True

    for klass in _VIZ_HOOKS_REGISTRY:
        if isinstance(val, klass):
            return _VIZ_HOOKS_REGISTRY[klass]
    return None
