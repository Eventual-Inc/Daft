""" Contexts for *with* statement providing temporary directories
"""
from __future__ import annotations

import os
from tempfile import TemporaryDirectory
from types import TracebackType


class InTemporaryDirectory:
    """Create, return, and change directory to a temporary directory

    Examples
    --------
    >>> import os
    >>> my_cwd = os.getcwd()
    >>> with InTemporaryDirectory() as tmpdir:
    ...     _ = open('test.txt', 'wt').write('some text')
    ...     assert os.path.isfile('test.txt')
    ...     assert os.path.isfile(os.path.join(tmpdir, 'test.txt'))
    >>> os.path.exists(tmpdir)
    False
    >>> os.getcwd() == my_cwd
    True
    """

    def __init__(self) -> None:
        self._tmpdir = TemporaryDirectory()

    @property
    def name(self) -> str:
        return self._tmpdir.name

    def __enter__(self) -> str:
        self._pwd = os.getcwd()
        os.chdir(self._tmpdir.name)
        return self._tmpdir.__enter__()

    def __exit__(
        self,
        exc: type[BaseException] | None,
        value: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        os.chdir(self._pwd)
        return self._tmpdir.__exit__(exc, value, tb)


class InGivenDirectory:
    """Change directory to given directory for duration of ``with`` block

    Useful when you want to use `InTemporaryDirectory` for the final test, but
    you are still debugging.  For example, you may want to do this in the end:

    >>> with InTemporaryDirectory() as tmpdir:
    ...     # do something complicated which might break
    ...     pass

    But indeed the complicated thing does break, and meanwhile the
    ``InTemporaryDirectory`` context manager wiped out the directory with the
    temporary files that you wanted for debugging.  So, while debugging, you
    replace with something like:

    >>> with InGivenDirectory() as tmpdir: # Use working directory by default
    ...     # do something complicated which might break
    ...     pass

    You can then look at the temporary file outputs to debug what is happening,
    fix, and finally replace ``InGivenDirectory`` with ``InTemporaryDirectory``
    again.
    """

    def __init__(self, path: str | None = None) -> None:
        """Initialize directory context manager

        Parameters
        ----------
        path : None or str, optional
            path to change directory to, for duration of ``with`` block.
            Defaults to ``os.getcwd()`` if None
        """
        if path is None:
            path = os.getcwd()
        self.name = os.path.abspath(path)

    def __enter__(self) -> str:
        self._pwd = os.path.abspath(os.getcwd())
        if not os.path.isdir(self.name):
            os.mkdir(self.name)
        os.chdir(self.name)
        return self.name

    def __exit__(
        self,
        exc: type[BaseException] | None,
        value: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        os.chdir(self._pwd)
