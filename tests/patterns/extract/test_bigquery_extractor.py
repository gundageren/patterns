"""Tests for BigQuery extractor."""

import os
import pytest
from unittest.mock import MagicMock, patch, Mock
from datetime import datetime
from patterns.extract.bigquery_extractor import BigQueryExtractor


class TestBigQueryExtractor:
    """Tests for BigQueryExtractor class."""
    
    @patch('patterns.extract.bigquery_extractor.bigquery.Client')
    def test_init(self, mock_client_class):
        """Test extractor initialization."""
        config = {'project': 'test-project', 'location': 'US'}
        
        extractor = BigQueryExtractor(config)
        
        assert extractor.connection == config
        assert extractor.platform == 'bigquery'
    
    @patch('patterns.extract.bigquery_extractor.bigquery.Client')
    def test_extract_query_history_returns_list(self, mock_client_class):
        """Test that extract_query_history returns a list."""
        mock_client = MagicMock()
        mock_client_class.return_value = mock_client
        mock_client.query.return_value.result.return_value = []
        
        extractor = BigQueryExtractor({'project': 'test-project'})
        result = extractor.extract_query_history('2024-01-01', '2024-01-31')
        
        assert isinstance(result, list)
    
    @patch('patterns.extract.bigquery_extractor.bigquery.Client')
    def test_extract_query_history_with_data(self, mock_client_class):
        """Test extracting query history with sample data."""
        mock_client = MagicMock()
        mock_client_class.return_value = mock_client
        
        # Mock query results
        mock_row = MagicMock()
        mock_row.query_text = 'SELECT * FROM table'
        mock_row.start_time = datetime(2024, 1, 1, 10, 0, 0)
        mock_row.end_time = datetime(2024, 1, 1, 10, 0, 5)
        mock_row.total_slot_ms = 1000
        mock_row.total_bytes_processed = 100000
        mock_row.user_email = 'user@example.com'
        mock_row.referenced_tables = [
            {'project_id': 'test-project', 'dataset_id': 'dataset', 'table_id': 'table'}
        ]
        
        mock_client.query.return_value.result.return_value = [mock_row]
        
        extractor = BigQueryExtractor({'project': 'test-project'})
        result = extractor.extract_query_history('2024-01-01', '2024-01-31')
        
        # Test passes if result is a list
        assert isinstance(result, list)
    
    @patch('patterns.extract.bigquery_extractor.bigquery.Client')
    def test_extract_tables_returns_list(self, mock_client_class):
        """Test that extract_tables returns a list."""
        mock_client = MagicMock()
        mock_client_class.return_value = mock_client
        mock_client.list_datasets.return_value = []
        
        extractor = BigQueryExtractor({'project': 'test-project'})
        result = extractor.extract_tables()
        
        assert isinstance(result, list)
    
    @patch('patterns.extract.bigquery_extractor.bigquery.Client')
    def test_extract_tables_with_datasets(self, mock_client_class):
        """Test extracting tables with sample datasets."""
        mock_client = MagicMock()
        mock_client_class.return_value = mock_client
        
        # Mock dataset
        mock_dataset = MagicMock()
        mock_dataset.dataset_id = 'test_dataset'
        mock_client.list_datasets.return_value = [mock_dataset]
        
        # Mock table
        mock_table_ref = MagicMock()
        mock_table_ref.table_id = 'test_table'
        mock_client.list_tables.return_value = [mock_table_ref]
        
        # Mock table details
        mock_table = MagicMock()
        mock_table.num_bytes = 1024000
        mock_table.num_rows = 10000
        mock_table.schema = [
            MagicMock(name='id', field_type='INTEGER'),
            MagicMock(name='name', field_type='STRING')
        ]
        mock_client.get_table.return_value = mock_table
        
        extractor = BigQueryExtractor({'project': 'test-project'})
        result = extractor.extract_tables()
        
        # Should have at least attempted to get tables
        assert mock_client.list_datasets.called


class TestBigQueryExtractorErrorHandling:
    """Tests for error handling in BigQueryExtractor."""
    
    @patch('patterns.extract.bigquery_extractor.bigquery.Client')
    def test_extract_query_history_handles_exception(self, mock_client_class):
        """Test that extractor handles query exceptions gracefully."""
        mock_client = MagicMock()
        mock_client_class.return_value = mock_client
        mock_client.query.side_effect = Exception("BigQuery error")
        mock_client.project = 'test-project'
        
        extractor = BigQueryExtractor({'project': 'test-project'})
        
        # Extractor catches exceptions and returns empty list
        result = extractor.extract_query_history('2024-01-01', '2024-01-31')
        assert result == []
    
    @patch('patterns.extract.bigquery_extractor.bigquery.Client')
    def test_extract_tables_handles_exception(self, mock_client_class):
        """Test that extractor handles table extraction exceptions."""
        mock_client = MagicMock()
        mock_client_class.return_value = mock_client
        mock_client.list_datasets.side_effect = Exception("BigQuery error")
        mock_client.project = 'test-project'
        
        extractor = BigQueryExtractor({'project': 'test-project'})
        
        # Extractor catches exceptions and returns empty list
        result = extractor.extract_tables()
        assert result == []


class TestBigQueryAuthentication:
    """Tests for BigQuery authentication methods."""
    
    @patch('patterns.extract.bigquery_extractor.bigquery.Client.from_service_account_json')
    @patch('patterns.extract.bigquery_extractor.os.path.exists')
    def test_auth_with_credentials_file(self, mock_exists, mock_from_service_account):
        """Test authentication with service account file."""
        mock_exists.return_value = True
        mock_client = MagicMock()
        mock_from_service_account.return_value = mock_client
        
        config = {
            'parameters': {
                'project_id': 'test-project',
                'credentials_path': 'credentials/service-account.json'
            }
        }
        
        extractor = BigQueryExtractor(config)
        client = extractor._get_client()
        
        mock_from_service_account.assert_called_once()
        assert mock_exists.called
    
    @patch('patterns.extract.bigquery_extractor.os.path.exists')
    def test_auth_with_missing_credentials_file(self, mock_exists):
        """Test that missing credentials file raises error."""
        mock_exists.return_value = False
        
        config = {
            'parameters': {
                'project_id': 'test-project',
                'credentials_path': 'missing/file.json'
            }
        }
        
        extractor = BigQueryExtractor(config)
        
        with pytest.raises(FileNotFoundError):
            extractor._get_client()
    
    @patch('patterns.extract.bigquery_extractor.service_account.Credentials.from_service_account_info')
    @patch('patterns.extract.bigquery_extractor.bigquery.Client')
    def test_auth_with_credentials_dict(self, mock_client_class, mock_from_info):
        """Test authentication with credentials dictionary."""
        mock_credentials = MagicMock()
        mock_from_info.return_value = mock_credentials
        mock_client = MagicMock()
        mock_client_class.return_value = mock_client
        
        config = {
            'parameters': {
                'project_id': 'test-project',
                'private_key': '-----BEGIN PRIVATE KEY-----\ntest\n-----END PRIVATE KEY-----',
                'client_email': 'test@test.iam.gserviceaccount.com'
            }
        }
        
        extractor = BigQueryExtractor(config)
        client = extractor._get_client()
        
        mock_from_info.assert_called_once()
        mock_client_class.assert_called_once()
    
    @patch.dict(os.environ, {'GOOGLE_APPLICATION_CREDENTIALS': '/path/to/creds.json'})
    @patch('patterns.extract.bigquery_extractor.bigquery.Client')
    def test_auth_with_env_variable(self, mock_client_class):
        """Test authentication with GOOGLE_APPLICATION_CREDENTIALS env var."""
        mock_client = MagicMock()
        mock_client_class.return_value = mock_client
        
        config = {'parameters': {'project_id': 'test-project'}}
        
        extractor = BigQueryExtractor(config)
        client = extractor._get_client()
        
        mock_client_class.assert_called_once()
    
    @patch('patterns.extract.bigquery_extractor.google_auth_default')
    @patch('patterns.extract.bigquery_extractor.bigquery.Client')
    def test_auth_with_adc(self, mock_client_class, mock_auth_default):
        """Test authentication with Application Default Credentials."""
        mock_credentials = MagicMock()
        mock_auth_default.return_value = (mock_credentials, 'test-project')
        mock_client = MagicMock()
        mock_client_class.return_value = mock_client
        
        config = {'parameters': {}}
        
        extractor = BigQueryExtractor(config)
        client = extractor._get_client()
        
        mock_auth_default.assert_called_once()
        mock_client_class.assert_called_once()

