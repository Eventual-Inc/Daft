# Do not modify or delete these exceptions before checking where they are used in rust
# src/common/error/src/python.rs


class DaftCoreException(ValueError):
    """DaftCore Base Exception"""

    pass


class DaftTypeError(DaftCoreException):
    """Type Error that occurred in Daft Core"""

    pass


class DaftTransientError(DaftCoreException):
    """Daft Transient Error
    This is typically raised when there is a network issue such as timeout or throttling. This can usually be retried.
    """

    pass


class ConnectTimeoutError(DaftTransientError):
    """Daft Connection Timeout Error
    Daft client was not able to make a connection to the server in the connect timeout time.
    """

    pass


class ReadTimeoutError(DaftTransientError):
    """Daft Read Timeout Error
    Daft client was not able to read bytes from server under the read timeout time.
    """

    pass


class ByteStreamError(DaftTransientError):
    """Daft Byte Stream Error
    Daft client had an error while reading bytes in a stream from the server.
    """

    pass


class SocketError(DaftTransientError):
    """Daft Socket Error
    Daft client had a socket error while reading bytes in a stream from the server.
    """

    pass
