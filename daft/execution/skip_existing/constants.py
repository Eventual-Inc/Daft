from __future__ import annotations

import os

# For skip_existing in ray mode, timeout for the placement group to be ready
PLACEMENT_GROUP_READY_TIMEOUT_SECONDS = int(
    os.environ.get("DAFT_SKIP_EXISTING_PLACEMENT_GROUP_READY_TIMEOUT_SECONDS", "50")
)
# For skip_existing in ray mode, timeout for the async await to complete
ASYNC_AWAIT_TIMEOUT_SECONDS = int(os.environ.get("DAFT_SKIP_EXISTING_ASYNC_AWAIT_TIMEOUT_SECONDS", "1000"))
