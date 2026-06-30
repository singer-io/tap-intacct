import unittest
from unittest.mock import patch

from singer import metadata

from tap_intacct.discover import discover_streams

from .base import IntacctBaseTest


class DiscoveryMockedIntegrationTest(IntacctBaseTest, unittest.TestCase):
    @patch("tap_intacct.discover.s3.get_sampled_schema_for_table")
    @patch("tap_intacct.discover.s3.get_exported_tables")
    def test_discovery_returns_streams_with_expected_metadata(self, mock_get_tables, mock_get_schema):
        mock_get_tables.return_value = {"CUSTOMERS", "INVOICES"}
        mock_get_schema.return_value = self.sample_schema()

        streams = discover_streams(self.config)

        self.assertEqual({stream["tap_stream_id"] for stream in streams}, {"CUSTOMERS", "INVOICES"})

        for stream in streams:
            with self.subTest(stream=stream["tap_stream_id"]):
                mdata = metadata.to_map(stream["metadata"])
                root = mdata[()]
                self.assertEqual(root.get("forced-replication-method"), "INCREMENTAL")
                self.assertIn("RECORDNO", root.get("table-key-properties", []))
