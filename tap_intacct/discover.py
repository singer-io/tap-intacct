from singer import metadata
from tap_intacct import s3

def discover_streams(config):
    streams = []

    exported_tables = s3.get_exported_tables(config['bucket'], config['company_id'], path=config.get('path'))

    for exported_table in exported_tables:
        schema = s3.get_sampled_schema_for_table(config, exported_table)
        streams.append({'stream': exported_table, 'tap_stream_id': exported_table, 'schema': schema, 'metadata': load_metadata(schema)})
    return streams


def load_metadata(schema):
    mdata = metadata.new()

    for field_name in schema.get('properties', {}).keys():
        mdata = metadata.write(mdata, ('properties', field_name), 'inclusion', 'automatic')
        if field_name == "RECORDNO":
            mdata = metadata.write(mdata, (), 'table-key-properties', "RECORDNO")

    # Since it reads CSV files from S3 individually hence FULL_TABLE replication
    mdata = metadata.write(mdata, (), 'forced-replication-method', 'FULL_TABLE')

    return metadata.to_list(mdata)
