import unittest
from unittest.mock import patch, MagicMock
import sys
import os

# Add the parent directory to the path so we can import tap_intacct
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

# Mock external dependencies before importing
sys.modules['boto3'] = MagicMock()
sys.modules['singer_encodings'] = MagicMock()
sys.modules['singer_encodings.csv'] = MagicMock()
sys.modules['backoff'] = MagicMock()
sys.modules['botocore'] = MagicMock()
sys.modules['botocore.credentials'] = MagicMock()
sys.modules['botocore.session'] = MagicMock()

# Provide a real exception class for ClientError so that except-clauses work.
class MockClientError(Exception):
    """Minimal stand-in for botocore.exceptions.ClientError."""
    def __init__(self, error_response, operation_name='GetObject'):
        self.response = error_response
        super().__init__(str(error_response))

mock_botocore_exceptions = MagicMock()
mock_botocore_exceptions.ClientError = MockClientError
sys.modules['botocore.exceptions'] = mock_botocore_exceptions

# Create a proper mock for singer.metadata
class MockMetadataStore:
    def __init__(self):
        self._metadata_store = []

mock_singer = MagicMock()
mock_metadata = MagicMock()

# Mock the metadata functions to work like the real ones
def mock_new():
    return MockMetadataStore()

def mock_write(mdata, breadcrumb, key, value):
    # Store metadata in a way that can be retrieved
    mdata._metadata_store.append({
        'breadcrumb': list(breadcrumb),
        'metadata': {key: value}
    })
    return mdata

def mock_to_list(mdata):
    if not hasattr(mdata, '_metadata_store'):
        return []
    
    # Group metadata by breadcrumb
    grouped = {}
    for item in mdata._metadata_store:
        breadcrumb_key = tuple(item['breadcrumb'])
        if breadcrumb_key not in grouped:
            grouped[breadcrumb_key] = {'breadcrumb': item['breadcrumb'], 'metadata': {}}
        grouped[breadcrumb_key]['metadata'].update(item['metadata'])
    
    return list(grouped.values())

mock_metadata.new = mock_new
mock_metadata.write = mock_write
mock_metadata.to_list = mock_to_list
mock_singer.metadata = mock_metadata
sys.modules['singer'] = mock_singer

from tap_intacct.discover import (
    load_metadata,
    discover_streams,
    _is_access_denied,
    _check_table_access,
    _apply_access_checks,
)


class TestDiscoverMetadata(unittest.TestCase):
    """Test cases for metadata generation in discovery."""

    def test_load_metadata_includes_forced_replication_method(self):
        """Test that forced-replication-method is included in metadata."""
        schema = {
            'properties': {
                'id': {'type': 'string'},
                'name': {'type': 'string'}
            }
        }
        
        metadata_list = load_metadata(schema)
        
        # Convert list to dict for easier testing
        metadata_dict = {}
        for item in metadata_list:
            breadcrumb = tuple(item['breadcrumb'])
            metadata_dict[breadcrumb] = item['metadata']
        
        # Check that forced-replication-method is set at table level
        table_metadata = metadata_dict.get((), {})
        self.assertEqual(table_metadata.get('forced-replication-method'), 'INCREMENTAL')

    def test_load_metadata_sets_field_inclusion_automatic(self):
        """Test that all fields have inclusion set to automatic."""
        schema = {
            'properties': {
                'id': {'type': 'string'},
                'name': {'type': 'string'},
                'email': {'type': 'string'}
            }
        }
        
        metadata_list = load_metadata(schema)
        
        # Convert list to dict for easier testing
        metadata_dict = {}
        for item in metadata_list:
            breadcrumb = tuple(item['breadcrumb'])
            metadata_dict[breadcrumb] = item['metadata']
        
        # Check that all fields have automatic inclusion
        for field_name in schema['properties'].keys():
            field_metadata = metadata_dict.get(('properties', field_name), {})
            self.assertEqual(field_metadata.get('inclusion'), 'automatic')

    @patch('tap_intacct.discover._apply_access_checks')
    @patch('tap_intacct.s3.get_exported_tables')
    @patch('tap_intacct.s3.get_sampled_schema_for_table')
    def test_discover_streams_includes_forced_replication_method(self, mock_get_schema, mock_get_tables, mock_access_checks):
        """Test that discover_streams includes forced-replication-method in all streams."""
        # Mock the S3 functions
        mock_get_tables.return_value = ['table1', 'table2']
        # Access checks are a pass-through: all tables are accessible
        mock_access_checks.side_effect = lambda config, tables: list(tables)
        mock_get_schema.return_value = {
            'properties': {
                'RECORDNO': {'type': 'integer'},
                'name': {'type': 'string'}
            }
        }

        config = {
            'bucket': 'test-bucket',
            'company_id': 'test-company'
        }

        streams = discover_streams(config)

        # Verify we got the expected number of streams
        self.assertEqual(len(streams), 2)

        # Check each stream has the forced-replication-method in metadata
        for stream in streams:
            metadata_list = stream['metadata']

            # Find table-level metadata
            table_metadata = None
            for item in metadata_list:
                if item['breadcrumb'] == []:
                    table_metadata = item['metadata']
                    break

            self.assertIsNotNone(table_metadata)
            self.assertEqual(table_metadata.get('forced-replication-method'), 'INCREMENTAL')


class TestAccessChecks(unittest.TestCase):
    """Tests for the S3 access-check helpers added to the discovery flow."""

    def setUp(self):
        self.config = {
            'bucket': 'test-bucket',
            'company_id': 'test-company',
            'start_date': '2021-01-01T00:00:00Z',
        }

    # ------------------------------------------------------------------
    # _is_access_denied
    # ------------------------------------------------------------------

    def test_is_access_denied_returns_true_for_403(self):
        err = MockClientError({'Error': {'Code': '403', 'Message': 'Forbidden'}})
        self.assertTrue(_is_access_denied(err))

    def test_is_access_denied_returns_true_for_access_denied(self):
        err = MockClientError({'Error': {'Code': 'AccessDenied', 'Message': ''}})
        self.assertTrue(_is_access_denied(err))

    def test_is_access_denied_returns_true_for_forbidden(self):
        err = MockClientError({'Error': {'Code': 'Forbidden', 'Message': ''}})
        self.assertTrue(_is_access_denied(err))

    def test_is_access_denied_returns_false_for_other_codes(self):
        err = MockClientError({'Error': {'Code': '500', 'Message': 'InternalError'}})
        self.assertFalse(_is_access_denied(err))

    def test_is_access_denied_returns_false_for_no_such_key(self):
        err = MockClientError({'Error': {'Code': 'NoSuchKey', 'Message': ''}})
        self.assertFalse(_is_access_denied(err))

    def test_is_access_denied_returns_true_for_401(self):
        """'401' is included in the implementation alongside '403'."""
        err = MockClientError(
            {'Error': {'Code': '401', 'Message': 'Unauthorized'}}
        )
        self.assertTrue(_is_access_denied(err))

    # ------------------------------------------------------------------
    # _check_table_access
    # ------------------------------------------------------------------

    @patch('tap_intacct.discover.s3.get_input_files_for_table')
    def test_check_table_access_returns_true_when_accessible(self, mock_get_files):
        mock_get_files.return_value = [{'key': 'test.csv', 'last_modified': MagicMock()}]
        self.assertTrue(_check_table_access(self.config, 'invoices'))

    @patch('tap_intacct.discover.s3.get_input_files_for_table')
    def test_check_table_access_returns_false_on_403(self, mock_get_files):
        mock_get_files.side_effect = MockClientError(
            {'Error': {'Code': '403', 'Message': 'Forbidden'}}
        )
        self.assertFalse(_check_table_access(self.config, 'invoices'))

    @patch('tap_intacct.discover.s3.get_input_files_for_table')
    def test_check_table_access_returns_false_on_access_denied(self, mock_get_files):
        mock_get_files.side_effect = MockClientError(
            {'Error': {'Code': 'AccessDenied', 'Message': ''}}
        )
        self.assertFalse(_check_table_access(self.config, 'invoices'))

    @patch('tap_intacct.discover.s3.get_input_files_for_table')
    def test_check_table_access_reraises_non_access_errors(self, mock_get_files):
        mock_get_files.side_effect = MockClientError(
            {'Error': {'Code': '500', 'Message': 'InternalServerError'}}
        )
        with self.assertRaises(MockClientError):
            _check_table_access(self.config, 'invoices')

    # ------------------------------------------------------------------
    # _apply_access_checks
    # ------------------------------------------------------------------

    @patch('tap_intacct.discover._check_table_access')
    def test_apply_access_checks_returns_all_when_all_accessible(self, mock_check):
        mock_check.return_value = True
        tables = ['invoices', 'bills', 'journals']
        result = _apply_access_checks(self.config, tables)
        self.assertEqual(result, tables)

    @patch('tap_intacct.discover._check_table_access')
    def test_apply_access_checks_excludes_inaccessible_tables(self, mock_check):
        # 'bills' is inaccessible
        mock_check.side_effect = lambda cfg, table: table != 'bills'
        tables = ['invoices', 'bills', 'journals']
        result = _apply_access_checks(self.config, tables)
        self.assertEqual(result, ['invoices', 'journals'])

    @patch('tap_intacct.discover._check_table_access')
    def test_apply_access_checks_raises_when_no_tables_accessible(self, mock_check):
        mock_check.return_value = False
        tables = ['invoices', 'bills']
        with self.assertRaises(Exception) as ctx:
            _apply_access_checks(self.config, tables)
        self.assertIn('403', str(ctx.exception))

    @patch('tap_intacct.discover._check_table_access')
    def test_apply_access_checks_empty_tables_returns_empty(self, mock_check):
        result = _apply_access_checks(self.config, [])
        self.assertEqual(result, [])
        mock_check.assert_not_called()

    # ------------------------------------------------------------------
    # discover_streams – access-check integration
    # ------------------------------------------------------------------

    @patch('tap_intacct.discover._apply_access_checks')
    @patch('tap_intacct.s3.get_sampled_schema_for_table')
    @patch('tap_intacct.s3.get_exported_tables')
    def test_discover_streams_only_builds_accessible_tables(
        self, mock_get_tables, mock_get_schema, mock_access_checks
    ):
        """discover_streams must only build stream dicts for the tables returned
        by _apply_access_checks (i.e. the inaccessible table is excluded)."""
        mock_get_tables.return_value = ['invoices', 'bills', 'journals']
        mock_access_checks.return_value = ['invoices', 'journals']
        mock_get_schema.return_value = {
            'properties': {'RECORDNO': {'type': 'integer'}}
        }

        streams = discover_streams(self.config)

        stream_ids = [s['tap_stream_id'] for s in streams]
        self.assertIn('invoices', stream_ids)
        self.assertIn('journals', stream_ids)
        self.assertNotIn('bills', stream_ids)
        self.assertEqual(len(streams), 2)

    @patch('tap_intacct.discover._apply_access_checks')
    @patch('tap_intacct.s3.get_sampled_schema_for_table')
    @patch('tap_intacct.s3.get_exported_tables')
    def test_discover_streams_propagates_no_access_exception(
        self, mock_get_tables, mock_get_schema, mock_access_checks
    ):
        """discover_streams must propagate the exception raised by
        _apply_access_checks when no tables are accessible."""
        mock_get_tables.return_value = ['invoices']
        mock_access_checks.side_effect = Exception(
            "HTTP-error-code: 403, Error: The account credentials supplied do not have 'read' access to any "
            "of the tables in the configured bucket. Data collection cannot be initiated due to lack of permissions."
        )

        with self.assertRaises(Exception) as ctx:
            discover_streams(self.config)
        self.assertIn('403', str(ctx.exception))


class TestCheckTableAccessAllCodes(unittest.TestCase):
    """
    Verify _check_table_access returns False for every error code that
    _is_access_denied recognises ('403', 'AccessDenied', 'Forbidden', '401').
    The AccessDenied and 403 variants are covered in TestAccessChecks; here
    we add the remaining codes introduced by the new implementation.
    """

    def setUp(self):
        self.config = {'bucket': 'b', 'company_id': 'c'}

    @patch('tap_intacct.discover.s3.get_input_files_for_table')
    def test_check_table_access_returns_false_on_forbidden(self, mock_get_files):
        mock_get_files.side_effect = MockClientError(
            {'Error': {'Code': 'Forbidden', 'Message': ''}}
        )
        self.assertFalse(_check_table_access(self.config, 'table'))

    @patch('tap_intacct.discover.s3.get_input_files_for_table')
    def test_check_table_access_returns_false_on_401(self, mock_get_files):
        mock_get_files.side_effect = MockClientError(
            {'Error': {'Code': '401', 'Message': 'Unauthorized'}}
        )
        self.assertFalse(_check_table_access(self.config, 'table'))


class TestApplyAccessChecksDetails(unittest.TestCase):
    """Fine-grained tests for _apply_access_checks behaviour."""

    def setUp(self):
        self.config = {'bucket': 'b', 'company_id': 'c'}

    @patch('tap_intacct.discover._check_table_access')
    def test_exception_message_mentions_credentials_and_streams(
        self, mock_check
    ):
        """The exception raised when nothing is accessible must carry the
        expected 403 error text so downstream tooling can identify it."""
        mock_check.return_value = False
        with self.assertRaises(Exception) as ctx:
            _apply_access_checks(self.config, ['invoices'])
        msg = str(ctx.exception)
        self.assertIn('403', msg)
        self.assertIn('credentials', msg.lower())

    @patch('tap_intacct.discover._check_table_access')
    def test_check_called_once_per_table(self, mock_check):
        """_check_table_access must be probed for every exported table."""
        mock_check.return_value = True
        tables = ['a', 'b', 'c']
        _apply_access_checks(self.config, tables)
        self.assertEqual(mock_check.call_count, 3)
        called_tables = [call.args[1] for call in mock_check.call_args_list]
        self.assertEqual(called_tables, tables)

    @patch('tap_intacct.discover._check_table_access')
    def test_accessible_tables_preserve_input_order(self, mock_check):
        """The returned list must preserve the original ordering."""
        mock_check.side_effect = lambda cfg, t: t != 'bills'
        tables = ['journals', 'invoices', 'bills', 'vendors']
        result = _apply_access_checks(self.config, tables)
        self.assertEqual(result, ['journals', 'invoices', 'vendors'])

    @patch('tap_intacct.discover._check_table_access')
    def test_single_accessible_table_does_not_raise(self, mock_check):
        """When exactly one table is accessible the function must return it,
        not raise an exception."""
        mock_check.side_effect = lambda cfg, t: t == 'invoices'
        tables = ['invoices', 'bills', 'journals']
        result = _apply_access_checks(self.config, tables)
        self.assertEqual(result, ['invoices'])

    @patch('tap_intacct.discover.LOGGER')
    @patch('tap_intacct.discover._check_table_access')
    def test_warning_logged_with_inaccessible_table_names(
        self, mock_check, mock_logger
    ):
        """When some tables are excluded a warning must name each of them."""
        mock_check.side_effect = lambda cfg, t: t == 'invoices'
        _apply_access_checks(self.config, ['invoices', 'bills', 'vendors'])
        mock_logger.warning.assert_called_once()
        warning_msg = mock_logger.warning.call_args[0]
        # The second positional arg is the comma-joined list of excluded tables
        self.assertIn('bills', warning_msg[1])
        self.assertIn('vendors', warning_msg[1])


class TestDiscoverStreamsIntegration(unittest.TestCase):
    """Verify discover_streams integration with the new access-check flow."""

    def setUp(self):
        self.config = {
            'bucket': 'test-bucket',
            'company_id': 'test-company',
        }

    @patch('tap_intacct.discover._apply_access_checks')
    @patch('tap_intacct.s3.get_sampled_schema_for_table')
    @patch('tap_intacct.s3.get_exported_tables')
    def test_access_checks_called_with_config_and_exported_tables(
        self, mock_get_tables, mock_get_schema, mock_access_checks
    ):
        """_apply_access_checks must receive the original config dict and the
        full list of exported tables returned by get_exported_tables."""
        mock_get_tables.return_value = ['invoices', 'bills']
        mock_access_checks.return_value = ['invoices', 'bills']
        mock_get_schema.return_value = {'properties': {}}

        discover_streams(self.config)

        mock_access_checks.assert_called_once_with(
            self.config, ['invoices', 'bills']
        )

    @patch('tap_intacct.discover._apply_access_checks')
    @patch('tap_intacct.s3.get_sampled_schema_for_table')
    @patch('tap_intacct.s3.get_exported_tables')
    def test_stream_dict_has_required_keys(
        self, mock_get_tables, mock_get_schema, mock_access_checks
    ):
        """Each stream dict must contain stream, tap_stream_id, schema and
        metadata, and both name fields must equal the table name."""
        mock_get_tables.return_value = ['invoices']
        mock_access_checks.return_value = ['invoices']
        mock_get_schema.return_value = {
            'properties': {'RECORDNO': {'type': 'integer'}}
        }

        streams = discover_streams(self.config)

        self.assertEqual(len(streams), 1)
        s = streams[0]
        self.assertIn('stream', s)
        self.assertIn('tap_stream_id', s)
        self.assertIn('schema', s)
        self.assertIn('metadata', s)
        self.assertEqual(s['stream'], 'invoices')
        self.assertEqual(s['tap_stream_id'], 'invoices')

