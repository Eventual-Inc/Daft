from __future__ import annotations

import contextlib
import fcntl
import os
import signal
from typing import Any, Iterator


@contextlib.contextmanager
def _timeout(seconds: int, lockfile: str) -> Iterator[None]:
    """Context manager for triggering a timeout via SIGALRM if a blocking call
    does not return in `seconds` number of seconds.
    Usage:
    >>> with _timeout(5, "somefile"):
    >>>     some_long_running_function()
    In the above example, if some_long_running_function takes more than 5 seconds, a
    TimeoutError will be raised from the `with _timeout(5, "somefile")` context.
    """
    if seconds == -1.0:
        # No timeout specified, we execute without registering a SIGALRM
        yield
    else:

        def timeout_handler(signum: Any, frame: Any) -> None:
            """Handles receiving a SIGALRM by throwing a TimeoutError with a useful
            error message
            """
            raise TimeoutError(f"{lockfile} could not be acquired after {seconds}s")

        # Register `timeout_handler` as the handler for any SIGALRMs that are raised
        # We store the original SIGALRM handler if available to restore it afterwards
        original_handler = signal.signal(signal.SIGALRM, timeout_handler)

        try:
            # Raise a SIGALRM in `seconds` number of seconds, and then yield to code
            # in the context manager's code-block.
            signal.alarm(seconds)
            yield
        # Make sure to always restore the original SIGALRM handler
        finally:
            signal.alarm(0)
            signal.signal(signal.SIGALRM, original_handler)


class SimpleUnixFileLock:
    """Simple blocking lock class that uses files to achieve system-wide locking
    1. If timeout is not specified, waits forever by default
    2. Does not support recursive locking and WILL break if called recursively
    """

    def __init__(self, lock_file: str, timeout_seconds: int = -1):
        # The path to the lock file.
        self._lock_file = lock_file

        # The file descriptor for the *_lock_file* as it is returned by the
        # os.open() function.
        # This file lock is only NOT None, if the object currently holds the
        # lock.
        self._lock_file_fd: int | None = None

        # The default timeout value.
        self.timeout_seconds = timeout_seconds

        return None

    def __enter__(self) -> SimpleUnixFileLock:
        open_mode = os.O_RDWR | os.O_CREAT | os.O_TRUNC
        fd = os.open(self._lock_file, open_mode)
        try:
            with _timeout(self.timeout_seconds, self._lock_file):
                fcntl.lockf(fd, fcntl.LOCK_EX)
        except (OSError, TimeoutError):
            os.close(fd)
            raise
        else:
            self._lock_file_fd = fd
        return self

    def __exit__(self, exc_type: Any, exc_value: Any, traceback: Any) -> None:
        assert self._lock_file_fd is not None, "self._lock_file_fd not set by __enter__"
        fd = self._lock_file_fd
        self._lock_file_fd = None
        fcntl.flock(fd, fcntl.LOCK_UN)
        os.close(fd)
        return None
