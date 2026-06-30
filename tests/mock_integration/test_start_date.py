import unittest
from unittest.mock import patch

from tap_intacct.sync import sync_stream

from .base import IntacctBaseTest


class StartDateMockedIntegrationTest(IntacctBaseTest, unittest.TestCase):
    @patch("tap_intacct.sync.s3.get_input_files_for_table")
    @patch("tap_intacct.sync.utils.strptime_with_tz")
    @patch("tap_intacct.sync.singer.get_bookmark")
    def test_start_date_used_when_bookmark_missing(
        self,
        mock_get_bookmark,
        mock_strptime,
        mock_get_input_files,
    ):
        stream = self.make_stream("CUSTOMERS")
        parsed_date = object()

        mock_get_bookmark.return_value = None
        mock_strptime.return_value = parsed_date
        mock_get_input_files.return_value = []

        sync_stream(self.config, self.state, stream)

        mock_strptime.assert_called_once_with(self.config["start_date"])
        mock_get_input_files.assert_called_once_with(self.config, "CUSTOMERS", parsed_date)

    @patch("tap_intacct.sync.s3.get_input_files_for_table")
    @patch("tap_intacct.sync.utils.strptime_with_tz")
    @patch("tap_intacct.sync.singer.get_bookmark")
    def test_bookmark_overrides_start_date(
        self,
        mock_get_bookmark,
        mock_strptime,
        mock_get_input_files,
    ):
        stream = self.make_stream("CUSTOMERS")
        bookmark_value = "2024-05-01T00:00:00+00:00"
        parsed_date = object()

        mock_get_bookmark.return_value = bookmark_value
        mock_strptime.return_value = parsed_date
        mock_get_input_files.return_value = []

        sync_stream(self.config, self.state, stream)

        mock_strptime.assert_called_once_with(bookmark_value)
        mock_get_input_files.assert_called_once_with(self.config, "CUSTOMERS", parsed_date)
