import unittest
from unittest.mock import MagicMock, patch

from tap_intacct.sync import sync_table_file

from .base import IntacctBaseTest


class AllFieldsMockedIntegrationTest(IntacctBaseTest, unittest.TestCase):
    @patch("tap_intacct.sync.singer.write_record")
    @patch("tap_intacct.sync.csv.get_row_iterator")
    @patch("tap_intacct.sync.s3.get_file_handle")
    def test_all_fields_from_row_and_sdc_columns_are_written(
        self,
        mock_get_file_handle,
        mock_get_row_iterator,
        mock_write_record,
    ):
        stream = self.make_stream("CUSTOMERS")

        mock_get_file_handle.return_value = MagicMock(_raw_stream=object())
        mock_get_row_iterator.return_value = iter(
            [
                {"RECORDNO": "1", "NAME": "Alice", "UPDATED_AT": "2024-01-01T00:00:00Z"},
            ]
        )

        record_count = sync_table_file(self.config, "mock-company/CUSTOMERS.csv", stream)

        self.assertEqual(record_count, 1)
        self.assertEqual(mock_write_record.call_count, 1)

        args, _kwargs = mock_write_record.call_args
        self.assertEqual(args[0], "CUSTOMERS")
        written_record = args[1]

        self.assertEqual(written_record["RECORDNO"], "1")
        self.assertEqual(written_record["NAME"], "Alice")
        self.assertEqual(written_record["UPDATED_AT"], "2024-01-01T00:00:00Z")
        self.assertEqual(written_record["_sdc_source_bucket"], self.config["bucket"])
        self.assertEqual(written_record["_sdc_source_file"], "mock-company/CUSTOMERS.csv")
        self.assertEqual(written_record["_sdc_source_lineno"], 2)
