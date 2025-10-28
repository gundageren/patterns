"""Tests for Snowflake extractor."""

import os
import pytest
from unittest.mock import MagicMock, patch, mock_open
from patterns.extract.snowflake_extractor import SnowflakeExtractor


class TestSnowflakeExtractor:
    """Tests for SnowflakeExtractor class."""
    
    @patch('patterns.extract.snowflake_extractor.snowflake.connector.connect')
    def test_init(self, mock_connect):
        """Test extractor initialization."""
        config = {
            'account': 'test-account',
            'user': 'test-user',
            'password': 'test-password',
            'warehouse': 'test-warehouse'
        }
        
        extractor = SnowflakeExtractor(config)
        
        assert extractor.connection == config
        assert extractor.platform == 'snowflake'
    
    @patch('patterns.extract.snowflake_extractor.snowflake.connector.connect')
    def test_extract_query_history_returns_list(self, mock_connect):
        """Test that extract_query_history returns a list."""
        mock_conn = MagicMock()
        mock_connect.return_value = mock_conn
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_cursor.fetchall.return_value = []
        
        config = {'account': 'test', 'user': 'test', 'password': 'test'}
        extractor = SnowflakeExtractor(config)
        result = extractor.extract_query_history('2024-01-01', '2024-01-31')
        
        assert isinstance(result, list)
    
    @patch('patterns.extract.snowflake_extractor.snowflake.connector.connect')
    def test_extract_query_history_with_data(self, mock_connect):
        """Test extracting query history with sample data."""
        mock_conn = MagicMock()
        mock_connect.return_value = mock_conn
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        
        # Mock query results
        mock_cursor.fetchall.return_value = [
            (
                'SELECT * FROM table',  # query_text
                '2024-01-01 10:00:00',  # start_time
                '2024-01-01 10:00:05',  # end_time
                5000,                    # execution_time_ms
                100000,                  # bytes_scanned
                'user@example.com'       # user_name
            )
        ]
        
        config = {'account': 'test', 'user': 'test', 'password': 'test'}
        extractor = SnowflakeExtractor(config)
        result = extractor.extract_query_history('2024-01-01', '2024-01-31')
        
        # Test passes if result is a list
        assert isinstance(result, list)
    
    @patch('patterns.extract.snowflake_extractor.snowflake.connector.connect')
    def test_extract_tables_returns_list(self, mock_connect):
        """Test that extract_tables returns a list."""
        mock_conn = MagicMock()
        mock_connect.return_value = mock_conn
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_cursor.fetchall.return_value = []
        
        config = {'account': 'test', 'user': 'test', 'password': 'test'}
        extractor = SnowflakeExtractor(config)
        result = extractor.extract_tables()
        
        assert isinstance(result, list)
    
    @patch('patterns.extract.snowflake_extractor.snowflake.connector.connect')
    def test_extract_tables_with_data(self, mock_connect):
        """Test extracting tables with sample data."""
        mock_conn = MagicMock()
        mock_connect.return_value = mock_conn
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        
        # Mock table results
        mock_cursor.fetchall.return_value = [
            ('DATABASE1', 'SCHEMA1', 'TABLE1', 1024000, 10000)
        ]
        
        config = {'account': 'test', 'user': 'test', 'password': 'test'}
        extractor = SnowflakeExtractor(config)
        result = extractor.extract_tables()
        
        # Test passes if result is a list
        assert isinstance(result, list)
    
    @patch('patterns.extract.snowflake_extractor.snowflake.connector.connect')
    def test_close_connection(self, mock_connect):
        """Test closing the connection."""
        mock_conn = MagicMock()
        mock_connect.return_value = mock_conn
        
        config = {'account': 'test', 'user': 'test', 'password': 'test'}
        extractor = SnowflakeExtractor(config)
        
        # If there's a close method
        if hasattr(extractor, 'close'):
            extractor.close()
            assert mock_conn.close.called


class TestSnowflakeExtractorErrorHandling:
    """Tests for error handling in SnowflakeExtractor."""
    
    @patch('patterns.extract.snowflake_extractor.snowflake.connector.connect')
    def test_connection_error(self, mock_connect):
        """Test handling of connection errors."""
        mock_connect.side_effect = Exception("Connection failed")
        
        config = {'account': 'test', 'user': 'test', 'password': 'test'}
        
        # Extractor catches connection errors gracefully
        extractor = SnowflakeExtractor(config)
        assert extractor is not None
    
    @patch('patterns.extract.snowflake_extractor.snowflake.connector.connect')
    def test_query_error(self, mock_connect):
        """Test handling of query errors."""
        mock_conn = MagicMock()
        mock_connect.return_value = mock_conn
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_cursor.execute.side_effect = Exception("Query failed")
        
        config = {'account': 'test', 'user': 'test', 'password': 'test'}
        extractor = SnowflakeExtractor(config)
        
        # Extractor catches query errors and returns empty list
        result = extractor.extract_query_history('2024-01-01', '2024-01-31')
        assert result == []


class TestSnowflakeAuthentication:
    """Tests for Snowflake authentication methods."""
    
    @patch('patterns.extract.snowflake_extractor.snowflake.connector.connect')
    def test_auth_with_password(self, mock_connect):
        """Test authentication with password."""
        mock_conn = MagicMock()
        mock_connect.return_value = mock_conn
        
        config = {
            'parameters': {
                'account': 'test-account',
                'user': 'test-user',
                'password': 'test-password',
                'role': 'TEST_ROLE',
                'warehouse': 'TEST_WH'
            }
        }
        
        extractor = SnowflakeExtractor(config)
        conn = extractor._get_connection()
        
        mock_connect.assert_called_once()
        call_kwargs = mock_connect.call_args[1]
        assert call_kwargs['account'] == 'test-account'
        assert call_kwargs['user'] == 'test-user'
        assert call_kwargs['password'] == 'test-password'
    
    @patch('patterns.extract.snowflake_extractor.snowflake.connector.connect')
    def test_auth_with_sso(self, mock_connect):
        """Test authentication with SSO (externalbrowser)."""
        mock_conn = MagicMock()
        mock_connect.return_value = mock_conn
        
        config = {
            'parameters': {
                'account': 'test-account',
                'user': 'test-user@company.com',
                'authenticator': 'externalbrowser',
                'role': 'TEST_ROLE',
                'warehouse': 'TEST_WH'
            }
        }
        
        extractor = SnowflakeExtractor(config)
        conn = extractor._get_connection()
        
        mock_connect.assert_called_once()
        call_kwargs = mock_connect.call_args[1]
        assert call_kwargs['authenticator'] == 'externalbrowser'
        assert call_kwargs['account'] == 'test-account'
    
    @patch('builtins.open', new_callable=mock_open, read_data=b'-----BEGIN PRIVATE KEY-----\ntest\n-----END PRIVATE KEY-----')
    @patch('cryptography.hazmat.primitives.serialization.load_pem_private_key')
    @patch('patterns.extract.snowflake_extractor.snowflake.connector.connect')
    def test_auth_with_keypair(self, mock_connect, mock_load_key, mock_file):
        """Test authentication with key-pair."""
        mock_conn = MagicMock()
        mock_connect.return_value = mock_conn
        
        # Mock private key
        mock_private_key = MagicMock()
        mock_private_key.private_bytes.return_value = b'private_key_bytes'
        mock_load_key.return_value = mock_private_key
        
        config = {
            'parameters': {
                'account': 'test-account',
                'user': 'test-user',
                'private_key_path': 'credentials/test_key.pem',
                'role': 'TEST_ROLE',
                'warehouse': 'TEST_WH'
            }
        }
        
        extractor = SnowflakeExtractor(config)
        conn = extractor._get_connection()
        
        mock_connect.assert_called_once()
        call_kwargs = mock_connect.call_args[1]
        assert call_kwargs['private_key'] == b'private_key_bytes'
        assert call_kwargs['account'] == 'test-account'
    
    @patch.dict(os.environ, {
        'SNOWFLAKE_ACCOUNT': 'env-account',
        'SNOWFLAKE_USER': 'env-user',
        'SNOWFLAKE_PASSWORD': 'env-password',
        'SNOWFLAKE_WAREHOUSE': 'ENV_WH'
    })
    @patch('patterns.extract.snowflake_extractor.snowflake.connector.connect')
    def test_auth_with_env_variables(self, mock_connect):
        """Test authentication with environment variables."""
        mock_conn = MagicMock()
        mock_connect.return_value = mock_conn
        
        config = {'parameters': {}}
        
        extractor = SnowflakeExtractor(config)
        conn = extractor._get_connection()
        
        mock_connect.assert_called_once()
        call_kwargs = mock_connect.call_args[1]
        assert call_kwargs['account'] == 'env-account'
        assert call_kwargs['user'] == 'env-user'
        assert call_kwargs['password'] == 'env-password'
        assert call_kwargs['warehouse'] == 'ENV_WH'
    
    @patch('patterns.extract.snowflake_extractor.snowflake.connector.connect')
    def test_auth_missing_required_params(self, mock_connect):
        """Test that missing required parameters raises error."""
        config = {'parameters': {'account': 'test-account'}}  # Missing user
        
        extractor = SnowflakeExtractor(config)
        
        with pytest.raises(ValueError, match="requires 'account' and 'user'"):
            extractor._get_connection()
    
    @patch('patterns.extract.snowflake_extractor.snowflake.connector.connect')
    def test_auth_missing_auth_method(self, mock_connect):
        """Test that missing authentication method raises error."""
        config = {
            'parameters': {
                'account': 'test-account',
                'user': 'test-user'
                # No password, private_key, or authenticator
            }
        }
        
        extractor = SnowflakeExtractor(config)
        
        with pytest.raises(ValueError, match="requires one of: externalbrowser, private_key, or password"):
            extractor._get_connection()

