from __future__ import annotations

import os
import socket
import urllib
from unittest.mock import MagicMock, patch

from daft.scarf_telemetry import scarf_analytics

PUBLISHER_THREAD_SLEEP_INTERVAL_SECONDS = 0.1


@patch("urllib.request.urlopen")
def test_scarf_analytics_no_request_when_disabled(mock_urlopen: MagicMock):
    # Test when SCARF_NO_ANALYTICS is set to "true"
    os.environ["SCARF_NO_ANALYTICS"] = "true"
    scarf_analytics()
    mock_urlopen.assert_not_called()  # Verify no request was made

    # Test when DO_NOT_TRACK is set to "true"
    os.environ["SCARF_NO_ANALYTICS"] = "false"  # Reset to false
    os.environ["DO_NOT_TRACK"] = "true"
    scarf_analytics()
    mock_urlopen.assert_not_called()  # Verify no request was made


@patch("urllib.request.urlopen")
def test_scarf_analytics_makes_request(mock_urlopen: MagicMock):
    # Test when both environment variables are not set to "true"
    os.environ["SCARF_NO_ANALYTICS"] = "false"
    os.environ["DO_NOT_TRACK"] = "false"

    scarf_analytics()  # Should make a request
    mock_urlopen.assert_called_once()  # Verify that a request was made


@patch("urllib.request.urlopen")
def test_scarf_analytics_timeout(mock_urlopen: MagicMock):
    mock_urlopen.side_effect = urllib.error.URLError(socket.timeout("Timeout"))
    os.environ["SCARF_NO_ANALYTICS"] = "false"
    os.environ["DO_NOT_TRACK"] = "false"

    # Call `scarf_analytics` and ensure that it handles the timeout
    scarf_analytics()
    mock_urlopen.assert_called_once()  # Verify that a request was made


@patch("urllib.request.urlopen")
def test_scarf_analytics_disabled(mock_urlopen: MagicMock):
    os.environ["SCARF_NO_ANALYTICS"] = "false"
    os.environ["DO_NOT_TRACK"] = "false"

    # Disable analytics
    os.environ["SCARF_NO_ANALYTICS"] = "true"
    scarf_analytics()
    mock_urlopen.assert_not_called()  # Verify no request was made


# class TestTelemetry(unittest.TestCase):
#     @patch("urllib.request.urlopen")  # Mock requests.get
#     def test_scarf_analytics_no_request_when_disabled(self, mock_get):
#         # Test when SCARF_NO_ANALYTICS is set to "true"
#         os.environ["SCARF_NO_ANALYTICS"] = "true"
#         scarf_analytics()
#         mock_get.assert_not_called()  # Verify no request was made

#         # Test when DO_NOT_TRACK is set to "true"
#         os.environ["SCARF_NO_ANALYTICS"] = "false"  # Reset to false
#         os.environ["DO_NOT_TRACK"] = "true"
#         scarf_analytics()
#         mock_get.assert_not_called()  # Verify no request was made

#     @patch("urllib.request.urlopen")  # Mock requests.get
#     def test_scarf_analytics_makes_request(self, mock_get):
#         # Test when both environment variables are not set to "true"
#         os.environ["SCARF_NO_ANALYTICS"] = "false"
#         os.environ["DO_NOT_TRACK"] = "false"

#         scarf_analytics()  # Should make a request
#         mock_get.assert_called_once()  # Verify that a request was made
