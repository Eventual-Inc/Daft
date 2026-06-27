from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from typing import TYPE_CHECKING, Any, overload

from daft.datatype import MediaType
from daft.dependencies import h5py, np
from daft.file.file import File
from daft.file.typing import Hdf5ObjectMetadata

if TYPE_CHECKING:
    from collections.abc import Callable

    from daft.daft import PyDaftFile, PyFileReference
    from daft.io import IOConfig

_HDF5_EXTENSIONS = (".h5", ".hdf5")
HDF5_SCAN_BUFFER_SIZE = 1024
HDF5_DEFAULT_BUFFER_SIZE = 64 * 1024


def _join_h5path(group: str, name: str) -> str:
    group = group.strip("/")
    if not group:
        return name
    return f"{group}/{name}"


class Hdf5File(File):
    """Represents an HDF5 file backed by Daft file IO.

    This class keeps ``File.open()`` as the inherited raw byte-stream API and
    provides HDF5-specific helpers that mirror common h5py ``File`` and
    ``Group`` operations. HDF5 access uses a smaller default file buffer than
    the generic ``File`` type because h5py performs frequent small reads after
    seeks while traversing metadata and chunk indexes.
    """

    @staticmethod
    def _from_file_reference(reference: PyFileReference) -> Hdf5File:
        instance = Hdf5File.__new__(Hdf5File)
        instance._inner = reference
        return instance

    def __init__(self, url: str, io_config: IOConfig | None = None) -> None:
        if not h5py.module_available():  # ty:ignore[unresolved-attribute]
            raise ImportError(
                "The 'daft[hdf5]' extra is required to read HDF5 files. "
                "Please install it with: pip install 'daft[hdf5]'"
            )
        if not np.module_available():  # type: ignore[attr-defined]  # ty:ignore[unresolved-attribute]
            raise ImportError(
                "The 'numpy' module is required to read HDF5 files and is included in the 'daft[hdf5]' extra. "
                "Please install it with: pip install 'daft[hdf5]'"
            )
        super().__init__(url, io_config, MediaType.hdf5())

        if not self.is_hdf5():
            raise ValueError(f"File {self} is not an HDF5 file")

    def open(self, buffer_size: int | None = HDF5_DEFAULT_BUFFER_SIZE) -> PyDaftFile:
        return super().open(buffer_size=buffer_size)

    @contextmanager
    def _open_h5py(self, buffer_size: int | None = HDF5_DEFAULT_BUFFER_SIZE) -> Iterator[h5py.File]:
        with self.open(buffer_size=buffer_size) as file, h5py.File(file, "r") as h5:
            yield h5

    def metadata(self, group: str = "/") -> list[Hdf5ObjectMetadata]:
        """Collect object metadata below an HDF5 group.

        This is a Daft convenience around the same recursive traversal used by
        ``h5py.File.visititems()``. It visits groups and datasets under
        ``group`` and returns DataFrame-friendly dictionaries with stable keys.

        Args:
            group: Group path to traverse. Defaults to the root group ``/``.

        Returns:
            A list of metadata dictionaries containing ``h5path``, ``kind``,
            ``shape``, ``dtype``, ``chunks``, and ``compression``.

        Raises:
            TypeError: If ``group`` resolves to a dataset instead of a group.
        """
        with self._open_h5py(HDF5_SCAN_BUFFER_SIZE) as h5:
            node = h5[group]
            if not hasattr(node, "visititems"):
                raise TypeError(f"{group} is not an HDF5 group")

            objects: list[Hdf5ObjectMetadata] = []

            def collect(name: str, obj: Any) -> None:
                if hasattr(obj, "shape") and hasattr(obj, "dtype"):
                    objects.append(
                        Hdf5ObjectMetadata(
                            h5path=_join_h5path(group, name),
                            kind="dataset",
                            shape=list(obj.shape),
                            dtype=str(obj.dtype),
                            chunks=list(obj.chunks) if obj.chunks is not None else [],
                            compression=obj.compression or "",
                        )
                    )
                else:
                    objects.append(
                        Hdf5ObjectMetadata(
                            h5path=_join_h5path(group, name),
                            kind="group",
                            shape=[],
                            dtype="",
                            chunks=[],
                            compression="",
                        )
                    )

            node.visititems(collect)
            return objects

    def keys(self, group: str = "/") -> list[str]:
        """Return member names directly under an HDF5 group.

        Mirrors h5py ``Group.keys()``, but returns a concrete ``list[str]``
        instead of a view object.

        Args:
            group: Group path whose immediate members should be listed.
                Defaults to the root group ``/``.

        Returns:
            Names of child groups and datasets directly under ``group``.
        """
        with self._open_h5py(HDF5_SCAN_BUFFER_SIZE) as h5:
            node = h5[group]
            return list(node.keys())

    def attrs(self, h5path: str = "/") -> dict[str, Any]:
        """Return attributes attached to an HDF5 object.

        Mirrors h5py's ``<object>.attrs`` dictionary-style interface and
        materializes the attributes as a plain Python dictionary.

        Args:
            h5path: Group or dataset path. Defaults to the root group ``/``.

        Returns:
            A dictionary of attribute names to values. Values follow h5py's
            normal conversion rules, such as NumPy scalars or arrays.
        """
        with self._open_h5py(HDF5_SCAN_BUFFER_SIZE) as h5:
            return dict(h5[h5path].attrs)

    @overload
    def visit(self, *, group: str = "/") -> list[str]: ...

    @overload
    def visit(self, func: Callable[[str], Any], *, group: str = "/") -> Any: ...

    def visit(self, func: Callable[[str], Any] | None = None, *, group: str = "/") -> Any:
        """Recursively visit object names below an HDF5 group.

        Thin wrapper around h5py ``Group.visit``. When ``func`` is provided, it
        is called once per visited object name. Returning ``None`` from
        ``func`` continues traversal; returning any other value stops traversal
        and returns that value.

        If ``func`` is omitted, this method collects and returns all visited
        names as a list. This matches the common h5py pattern of passing
        ``names.append`` as the visitor.

        Args:
            func: Optional visitor callable with signature ``func(name)``.
            group: Group path where traversal should start. Defaults to ``/``.

        Returns:
            The visitor's first non-``None`` return value, or ``None`` if the
            visitor completed without one. If ``func`` is omitted, returns
            ``list[str]``.
        """
        with self._open_h5py(HDF5_SCAN_BUFFER_SIZE) as h5:
            node = h5[group]
            if func is None:
                names: list[str] = []
                node.visit(names.append)
                return names
            return node.visit(func)

    @staticmethod
    def _read_dataset(h5: Any, dataset: str) -> np.ndarray[Any, Any]:
        data = h5[dataset]
        if not hasattr(data, "shape"):
            raise TypeError(f"{dataset} is not an HDF5 dataset")
        return data[()]

    @overload
    def read(self, dataset: str) -> np.ndarray[Any, Any]: ...

    @overload
    def read(self, dataset: list[str]) -> dict[str, np.ndarray[Any, Any]]: ...

    def read(
        self,
        dataset: str | list[str],
    ) -> np.ndarray[Any, Any] | dict[str, np.ndarray[Any, Any]]:
        """Read one or more HDF5 datasets into NumPy arrays.

        For a single dataset path, this is equivalent to opening the file with
        h5py and evaluating ``h5[dataset][()]``. Passing a sequence reads
        multiple datasets with one file open.

        Args:
            dataset: A dataset path or sequence of dataset paths.

        Returns:
            A NumPy array for one dataset. For multiple datasets, a dictionary
            keyed by dataset path.

        Raises:
            TypeError: If any requested path resolves to a group instead of a
                dataset.
        """
        with self._open_h5py() as h5:
            if isinstance(dataset, str):
                return self._read_dataset(h5, dataset)
            return {h5path: self._read_dataset(h5, h5path) for h5path in dataset}
