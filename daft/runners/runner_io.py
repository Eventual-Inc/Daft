from __future__ import annotations

from abc import abstractmethod
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from daft.daft import FileFormatConfig, FileInfos, IOConfig


class RunnerIO:
    """Reading and writing data from the Runner.

    This is an abstract class and each runner must write their own implementation.
    """

    @abstractmethod
    def glob_paths_details(
        self,
        source_paths: list[str],
        file_format_config: FileFormatConfig | None = None,
        io_config: IOConfig | None = None,
    ) -> FileInfos:
        """Globs the specified filepath to construct a FileInfos object containing file and dir metadata.

        Args:
            source_paths (list[str]): path to glob

        Raises:
            FileNotFoundError: If none of files found with the glob source paths.

        Returns:
            FileInfo: The file infos for the globed paths.
        """
        raise NotImplementedError()
