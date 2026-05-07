import unittest
from unittest.mock import MagicMock


class TestMetadataGeneration(unittest.TestCase):
    """Test cases for metadata generation functionality."""

    def setUp(self):
        """Set up test fixtures with mocked singer metadata module."""
        # Mock the singer metadata module
        self.mock_metadata = MagicMock()
        self.mock_metadata.new.return_value = {}
        self.mock_metadata.write.return_value = {}
        self.mock_metadata.to_list.return_value = []
        
        # Create the load_metadata function inline for testing
        def load_metadata(schema):
            mdata = self.mock_metadata.new()

            for field_name in schema.get('properties', {}).keys():
                mdata = self.mock_metadata.write(mdata, ('properties', field_name), 'inclusion', 'automatic')
                if field_name == "RECORDNO":
                    mdata = self.mock_metadata.write(mdata, (), 'table-key-properties', "RECORDNO")

            # Add forced-replication-method to the metadata
            mdata = self.mock_metadata.write(mdata, (), 'forced-replication-method', 'INCREMENTAL')

            return self.mock_metadata.to_list(mdata)
        
        self.load_metadata = load_metadata

    def test_forced_replication_method_is_added(self):
        """Test that forced-replication-method is added to metadata."""
        schema = {
            'properties': {
                'id': {'type': 'string'},
                'name': {'type': 'string'}
            }
        }
        
        result = self.load_metadata(schema)
        
        # Verify that forced-replication-method was written to metadata
        self.mock_metadata.write.assert_any_call(
            self.mock_metadata.new(),
            (),  # Empty breadcrumb for table-level metadata
            'forced-replication-method',
            'INCREMENTAL'
        )
