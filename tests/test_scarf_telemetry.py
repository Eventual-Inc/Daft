import os
from daft.scarf_telemetry import scarf_analytics

def test_telemetry_respects_env_vars():
    # Test opt-out works
    os.environ["SCARF_NO_ANALYTICS"] = "true"
    scarf_analytics()  # Should not make request

    os.environ["SCARF_NO_ANALYTICS"] = "false"
    os.environ["DO_NOT_TRACK"] = "true"
    scarf_analytics()  # Should not make request