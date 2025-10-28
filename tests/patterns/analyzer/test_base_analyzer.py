"""Tests for base analyzer functionality."""

import pytest
from datetime import datetime
from patterns.analyzer.bigquery_analyzer import BigQueryAnalyzer


class TestBaseAnalyzer:
    """Tests for base analyzer functionality through BigQuery analyzer."""
    
    def test_init(self, mock_storage):
        """Test analyzer initialization."""
        analyzer = BigQueryAnalyzer('bigquery', mock_storage)
        
        assert analyzer.storage == mock_storage
        assert analyzer.platform == 'bigquery'
    
    def test_find_read_table_queries(self, mock_storage, sample_query_data):
        """Test finding read table queries."""
        # Mock the load_queries to return sample data
        mock_storage.load_queries.return_value = [
            {
                'query_text': 'SELECT * FROM dataset.table',
                'start_time': datetime.now().isoformat(),
                'source_platform': 'bigquery',
                'source_project': 'test-project'
            }
        ]
        
        analyzer = BigQueryAnalyzer('bigquery', mock_storage)
        result = analyzer.find_read_table_queries(
            'bigquery', 'test-project', '2024-01-01', '2024-01-31'
        )
        
        assert isinstance(result, list)
    
    def test_find_star_queries(self, mock_storage):
        """Test finding SELECT * queries."""
        mock_storage.load_queries.return_value = [
            {
                'query_text': 'SELECT * FROM table',
                'start_time': datetime.now().isoformat(),
                'source_platform': 'bigquery',
                'source_project': 'test-project'
            },
            {
                'query_text': 'SELECT id, name FROM table',
                'start_time': datetime.now().isoformat(),
                'source_platform': 'bigquery',
                'source_project': 'test-project'
            }
        ]
        
        analyzer = BigQueryAnalyzer('bigquery', mock_storage)
        result = analyzer.find_star_queries(
            'bigquery', 'test-project', '2024-01-01', '2024-01-31'
        )
        
        assert isinstance(result, list)
    
    def test_extract_partition_cluster_candidates(self, mock_storage):
        """Test extracting partition/cluster candidates."""
        mock_storage.load_queries.return_value = [
            {
                'query_text': 'SELECT * FROM table WHERE created_at >= "2024-01-01"',
                'start_time': datetime.now().isoformat(),
                'source_platform': 'bigquery',
                'source_project': 'test-project'
            }
        ]
        
        analyzer = BigQueryAnalyzer('bigquery', mock_storage)
        result = analyzer.extract_partition_cluster_candidates(
            'bigquery', 'test-project', '2024-01-01', '2024-01-31'
        )
        
        assert isinstance(result, list)

