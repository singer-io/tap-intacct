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
sys.modules['botocore.exceptions'] = MagicMock()
sys.modules['botocore.session'] = MagicMock()

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

from tap_intacct.discover import load_metadata, discover_streams


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
        self.assertEqual(table_metadata.get('forced-replication-method'), 'FULL_TABLE')

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

    @patch('tap_intacct.s3.get_exported_tables')
    @patch('tap_intacct.s3.get_sampled_schema_for_table')
    def test_discover_streams_includes_forced_replication_method(self, mock_get_schema, mock_get_tables):
        """Test that discover_streams includes forced-replication-method in all streams."""
        # Mock the S3 functions
        mock_get_tables.return_value = ['table1', 'table2']
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
            self.assertEqual(table_metadata.get('forced-replication-method'), 'FULL_TABLE')
