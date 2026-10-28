from __future__ import annotations

import threading

# Serializes HuggingFace model loading to avoid meta tensor races.
model_loading_lock = threading.Lock()
