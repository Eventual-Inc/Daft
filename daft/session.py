from __future__ import annotations

from daft.daft import PySession


class Session:
    """Session holds a connection's state and orchestrates execution of DataFrame and SQL queries against catalogs."""

    _session: PySession

    def __init__(self):
        raise NotImplementedError("We do not support creating a Session via __init__ ")

    ###
    # factory methods
    ###

    @staticmethod
    def empty() -> Session:
        """Creates an empty session."""
        s = Session.__new__(Session)
        s._session = PySession.empty()
        return s

    @staticmethod
    def _from_pysession(session: PySession) -> Session:
        """Creates a session from a rust session wrapper."""
        s = Session.__new__(Session)
        s._session = session
        return s

    @staticmethod
    def _from_env() -> Session:
        """Creates a session from the environment's configuration."""
        # todo session builders, raise if DAFT_SESSION=0
        return Session.empty()


###
# global active session
###

_SESSION: Session | None = None


def _session() -> Session:
    # Consider registering into the global context
    # ```
    # ctx = get_context()
    # if not ctx._session
    #     set_session(Session.from_env())
    # return ctx._session
    # ```
    global _SESSION
    if not _SESSION:
        _SESSION = Session._from_env()
    return _SESSION


###
# session state
###


def current_session() -> Session:
    """Returns the global context's current session."""
    return _session()


###
# set_.* (session management)
###


def set_session(session: Session):
    """Sets the global context's current session."""
    # Consider registering into the global context.
    # ```
    # ctx = get_context()
    # with ctx._lock:
    #     ctx._session = session
    # ```
    global _SESSION
    _SESSION = session
