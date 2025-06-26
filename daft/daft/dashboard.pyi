DAFT_DASHBOARD_ENV_ENABLED: str
DAFT_DASHBOARD_ENV_NAME: str
DAFT_DASHBOARD_URL: str
DAFT_DASHBOARD_QUERIES_URL: str

class ConnectionHandle:
    def shutdown(self, noop_if_shutdown: bool) -> None: ...

def launch(detach: bool = False, noop_if_initialized: bool = False) -> ConnectionHandle: ...
