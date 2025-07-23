from typing import Tuple
from daft.daft import PyRecordBatch
from daft.recordbatch.recordbatch import RecordBatch


DAFT_DASHBOARD_ENV_ENABLED: str
DAFT_DASHBOARD_ENV_NAME: str
DAFT_DASHBOARD_URL: str
DAFT_DASHBOARD_QUERIES_URL: str

class ConnectionHandle:
    def shutdown(self, noop_if_shutdown: bool) -> None: ...

def launch(detach: bool = False, noop_if_initialized: bool = False) -> ConnectionHandle: ...
def register_dataframe_for_display(record_batch: PyRecordBatch) -> Tuple[str, str, int]: ...
def generate_interactive_html(record_batch: RecordBatch, df_id: str) -> str: ...