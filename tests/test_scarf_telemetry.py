import os
import unittest
from unittest.mock import patch

from daft.scarf_telemetry import scarf_analytics


class TestTelemetry(unittest.TestCase):
    @patch("requests.get")  # Mock requests.get
    def test_scarf_analytics_no_request_when_disabled(self, mock_get):
        # Test when SCARF_NO_ANALYTICS is set to "true"
        os.environ["SCARF_NO_ANALYTICS"] = "true"
        scarf_analytics()
        mock_get.assert_not_called()  # Verify no request was made

        # Test when DO_NOT_TRACK is set to "true"
        os.environ["SCARF_NO_ANALYTICS"] = "false"  # Reset to false
        os.environ["DO_NOT_TRACK"] = "true"
        scarf_analytics()
        mock_get.assert_not_called()  # Verify no request was made

    @patch("requests.get")  # Mock requests.get
    def test_scarf_analytics_makes_request(self, mock_get):
        # Test when both environment variables are not set to "true"
        os.environ["SCARF_NO_ANALYTICS"] = "false"
        os.environ["DO_NOT_TRACK"] = "false"

        scarf_analytics()  # Should make a request
        mock_get.assert_called_once()  # Verify that a request was made
