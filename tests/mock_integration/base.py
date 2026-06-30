"""Base helpers for tap-intacct mocked integration tests."""

from singer import metadata


class IntacctBaseTest:
    default_start_date = "2020-01-01T00:00:00Z"

    @staticmethod
    def get_mock_config():
        return {
            "start_date": "2020-01-01T00:00:00Z",
            "bucket": "mock-bucket",
            "company_id": "mock-company",
        }

    def setUp(self):
        self.config = self.get_mock_config()
        self.state = {}

    @staticmethod
    def sample_schema():
        return {
            "type": "object",
            "properties": {
                "RECORDNO": {"type": ["null", "string"]},
                "NAME": {"type": ["null", "string"]},
                "UPDATED_AT": {"type": ["null", "string"]},
                "_sdc_source_bucket": {"type": ["null", "string"]},
                "_sdc_source_file": {"type": ["null", "string"]},
                "_sdc_source_lineno": {"type": ["null", "integer"]},
            },
        }

    @staticmethod
    def make_metadata(schema):
        mdata = metadata.new()
        for field_name in schema.get("properties", {}).keys():
            mdata = metadata.write(mdata, ("properties", field_name), "inclusion", "automatic")
        mdata = metadata.write(mdata, (), "table-key-properties", ["RECORDNO"])
        mdata = metadata.write(mdata, (), "forced-replication-method", "INCREMENTAL")
        return metadata.to_list(mdata)

    def make_stream(self, stream_name="CUSTOMERS", selected=True, schema=None):
        schema = schema or self.sample_schema()
        stream = {
            "stream": stream_name,
            "tap_stream_id": stream_name,
            "schema": schema,
            "metadata": self.make_metadata(schema),
        }
        mdata = metadata.to_map(stream["metadata"])
        mdata[()] = dict(mdata.get((), {}))
        mdata[()]["selected"] = selected
        stream["metadata"] = metadata.to_list(mdata)
        return stream

    def make_catalog(self, stream_names):
        return {"streams": [self.make_stream(name, selected=True) for name in stream_names]}
