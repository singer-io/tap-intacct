import unittest
from unittest.mock import patch

from singer import metadata

from tap_intacct.discover import discover_streams

from .base import IntacctBaseTest


class AutomaticFieldsMockedIntegrationTest(IntacctBaseTest, unittest.TestCase):
    @patch("tap_intacct.discover.s3.get_sampled_schema_for_table")
    @patch("tap_intacct.discover.s3.get_exported_tables")
    def test_all_discovered_fields_are_automatic(
        self,
        mock_get_tables,
        mock_get_schema,
    ):
        schema = self.sample_schema()
        mock_get_tables.return_value = {"CUSTOMERS"}
        mock_get_schema.return_value = schema

        streams = discover_streams(self.config)
        self.assertEqual(len(streams), 1)

        mdata = metadata.to_map(streams[0]["metadata"])
        for field_name in schema["properties"].keys():
            with self.subTest(field=field_name):
                self.assertEqual(
                    metadata.get(mdata, ("properties", field_name), "inclusion"),
                    "automatic",
                )

    @patch("tap_intacct.discover.s3.get_sampled_schema_for_table")
    @patch("tap_intacct.discover.s3.get_exported_tables")
    def test_recordno_is_marked_as_table_key(self, mock_get_tables, mock_get_schema):
        mock_get_tables.return_value = {"CUSTOMERS"}
        mock_get_schema.return_value = self.sample_schema()

        streams = discover_streams(self.config)
        mdata = metadata.to_map(streams[0]["metadata"])

        self.assertIn("RECORDNO", mdata[()].get("table-key-properties", []))
