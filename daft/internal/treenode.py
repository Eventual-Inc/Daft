from __future__ import annotations

from typing import Generic, List, TypeVar

T = TypeVar("T", bound="TreeNode")


class TreeNode(Generic[T]):
    def __init__(self) -> None:
        self._registered_children: List[T] = []

    def _children(self) -> List[T]:
        return self._registered_children

    def _register_child(self, child: T) -> T:
        self._registered_children.append(child)
        return child
