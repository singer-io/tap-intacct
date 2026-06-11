import singer
from botocore.exceptions import ClientError
from singer import metadata

from tap_intacct import s3

LOGGER = singer.get_logger()


def discover_streams(config: dict) -> list:
    """
    Discover available tables from the configured S3 bucket, verify read access
    for each table, and return a catalog stream list. Tables the credentials
    cannot access (HTTP 403 / AccessDenied / 401 / Forbidden) are silently excluded from the
    returned catalog.
    """
    streams = []

    exported_tables = s3.get_exported_tables(config['bucket'], config['company_id'], path=config.get('path'))
    accessible_tables = _apply_access_checks(config, exported_tables)

    for exported_table in accessible_tables:
        schema = s3.get_sampled_schema_for_table(config, exported_table)
        streams.append({'stream': exported_table, 'tap_stream_id': exported_table, 'schema': schema, 'metadata': load_metadata(schema)})
    return streams


def load_metadata(schema):
    mdata = metadata.new()

    for field_name in schema.get('properties', {}).keys():
        mdata = metadata.write(mdata, ('properties', field_name), 'inclusion', 'automatic')
        if field_name == "RECORDNO":
            mdata = metadata.write(mdata, (), 'table-key-properties', "RECORDNO")

    mdata = metadata.write(mdata, (), 'forced-replication-method', 'INCREMENTAL')

    return metadata.to_list(mdata)


def _is_access_denied(client_error: ClientError) -> bool:
    """Return True if the ClientError represents an HTTP 403 / AccessDenied response."""
    error_code = client_error.response['Error']['Code']
    return error_code in ('403', 'AccessDenied', 'Forbidden', '401')


def _check_table_access(config: dict, table_name: str) -> bool:
    """
    Verify that the AWS credentials have read access to the given table's S3 files.
    Returns True if accessible, False if a 403 / AccessDenied error is raised.
    """
    try:
        s3.get_input_files_for_table(config, table_name)
        return True
    except ClientError as e:
        if _is_access_denied(e):
            LOGGER.warning(
                "Table '%s' does not have read permission, excluding from catalog.",
                table_name,
            )
            return False
        raise


def _apply_access_checks(config: dict, exported_tables) -> list:
    """
    Probe each exported table for S3 read access and filter out inaccessible tables.
    Raises an exception if no tables are accessible due to permission errors.
    Returns the list of accessible table names.
    """
    inaccessible_tables = [
        table_name
        for table_name in exported_tables
        if not _check_table_access(config, table_name)
    ]
    accessible_tables = [t for t in exported_tables if t not in inaccessible_tables]

    if inaccessible_tables:
        if not accessible_tables:
            raise Exception(
                "HTTP-error-code: 403, Error: The account credentials doesn't have access to any streams. Please re check the configuration"
            )
        LOGGER.warning(
            "The account credentials supplied do not have 'read' access to the following table(s): %s. "
            "These tables have been excluded from the catalog.",
            ", ".join(inaccessible_tables),
        )

    return accessible_tables
