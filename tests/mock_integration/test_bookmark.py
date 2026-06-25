import unittest
from datetime import datetime, timezone
from unittest.mock import patch

from tap_intacct.sync import sync_stream

from .base import IntacctBaseTest


class BookmarkMockedIntegrationTest(IntacctBaseTest, unittest.TestCase):
    @patch("tap_intacct.sync.singer.write_state")
    @patch("tap_intacct.sync.singer.write_bookmark")
    @patch("tap_intacct.sync.sync_table_file")
    @patch("tap_intacct.sync.s3.get_input_files_for_table")
    def test_bookmark_written_after_each_processed_file(
        self,
        mock_get_input_files,
        mock_sync_table_file,
        mock_write_bookmark,
        _mock_write_state,
    ):
        stream = self.make_stream("CUSTOMERS")
        modified_1 = datetime(2024, 1, 2, tzinfo=timezone.utc)
        modified_2 = datetime(2024, 1, 3, tzinfo=timezone.utc)
        mock_get_input_files.return_value = [
            {"key": "company/CUSTOMERS.001.csv", "last_modified": modified_1},
            {"key": "company/CUSTOMERS.002.csv", "last_modified": modified_2},
        ]
        mock_sync_table_file.side_effect = [2, 3]

        def _write_bookmark_side_effect(state, stream_name, key, value):
            state.setdefault("bookmarks", {}).setdefault(stream_name, {})[key] = value
            return state

        mock_write_bookmark.side_effect = _write_bookmark_side_effect

        counter = sync_stream(self.config, self.state, stream)

        self.assertEqual(counter, 5)
        self.assertIn("bookmarks", self.state)
        self.assertEqual(
            self.state["bookmarks"]["CUSTOMERS"]["modified_since"],
            modified_2.isoformat(),
        )
