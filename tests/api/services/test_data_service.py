"""Tests for data service."""

import pytest
from datetime import datetime, timedelta
from api.services import data_service


class TestFilterTablesByCriteria:
    """Tests for filter_tables_by_criteria function."""
    
    def test_no_filters(self):
        """Test that all tables are returned when no filters applied."""
        tables = [
            {'source_platform': 'bigquery', 'database': 'db1', 'schema': 'schema1', 'table': 'table1'},
            {'source_platform': 'snowflake', 'database': 'db2', 'schema': 'schema2', 'table': 'table2'}
        ]
        
        result = data_service.filter_tables_by_criteria(tables)
        
        assert len(result) == 2
    
    def test_filter_by_platform(self):
        """Test filtering by source platform."""
        tables = [
            {'source_platform': 'bigquery', 'database': 'db1'},
            {'source_platform': 'snowflake', 'database': 'db2'},
            {'source_platform': 'bigquery', 'database': 'db3'}
        ]
        
        result = data_service.filter_tables_by_criteria(tables, source_platform='bigquery')
        
        assert len(result) == 2
        assert all(t['source_platform'] == 'bigquery' for t in result)
    
    def test_filter_case_insensitive(self):
        """Test that filtering is case-insensitive."""
        tables = [
            {'source_platform': 'BigQuery', 'database': 'MyDatabase'},
            {'source_platform': 'bigquery', 'database': 'mydatabase'}
        ]
        
        result = data_service.filter_tables_by_criteria(
            tables, source_platform='BIGQUERY', database='MYDATABASE'
        )
        
        assert len(result) == 2
    
    def test_filter_multiple_criteria(self):
        """Test filtering with multiple criteria."""
        tables = [
            {'source_platform': 'bigquery', 'database': 'db1', 'schema': 'schema1'},
            {'source_platform': 'bigquery', 'database': 'db1', 'schema': 'schema2'},
            {'source_platform': 'bigquery', 'database': 'db2', 'schema': 'schema1'}
        ]
        
        result = data_service.filter_tables_by_criteria(
            tables, source_platform='bigquery', database='db1', schema='schema1'
        )
        
        assert len(result) == 1
        assert result[0]['schema'] == 'schema1'


class TestLoadAndFilterQueryData:
    """Tests for load_and_filter_query_data function."""
    
    def test_load_and_filter(self, mock_storage):
        """Test loading and filtering query data."""
        mock_storage.load_read_table_queries.return_value = [
            {'table': 'users', 'count': 10},
            {'table': 'orders', 'count': 20}
        ]
        mock_storage.load_select_star_queries.return_value = [
            {'table': 'users', 'count': 5}
        ]
        mock_storage.load_partition_candidates.return_value = [
            {'table': 'users', 'column': 'created_at'}
        ]
        
        result = data_service.load_and_filter_query_data(
            mock_storage, 'bigquery', 'project', 'users', '2024-01-01', '2024-01-31'
        )
        
        assert len(result['read_queries']) == 1
        assert len(result['star_queries']) == 1
        assert len(result['partition_candidates']) == 1
        assert result['read_queries'][0]['table'] == 'users'
    
    def test_case_insensitive_filtering(self, mock_storage):
        """Test that table filtering is case-insensitive."""
        mock_storage.load_read_table_queries.return_value = [
            {'table': 'Users', 'count': 10},
            {'table': 'USERS', 'count': 20},
            {'table': 'users', 'count': 30}
        ]
        mock_storage.load_select_star_queries.return_value = []
        mock_storage.load_partition_candidates.return_value = []
        
        result = data_service.load_and_filter_query_data(
            mock_storage, 'bigquery', 'project', 'users', '2024-01-01', '2024-01-31'
        )
        
        assert len(result['read_queries']) == 3


class TestCalculateWeeklyStats:
    """Tests for calculate_weekly_stats function."""
    
    def test_empty_data(self):
        """Test with no query data."""
        result = data_service.calculate_weekly_stats([], [])
        
        assert len(result) == 0
    
    def test_single_week(self):
        """Test with queries from a single week."""
        read_queries = [
            {'start_time': '2024-01-01T10:00:00', 'count': 10},
            {'start_time': '2024-01-02T10:00:00', 'count': 15}
        ]
        star_queries = [
            {'start_time': '2024-01-01T10:00:00', 'count': 5}
        ]
        
        result = data_service.calculate_weekly_stats(read_queries, star_queries)
        
        assert len(result) == 1
        assert result[0]['total_queries'] == 25
        assert result[0]['star_queries'] == 5
    
    def test_multiple_weeks(self):
        """Test with queries spanning multiple weeks."""
        read_queries = [
            {'start_time': '2024-01-01T10:00:00', 'count': 10},
            {'start_time': '2024-01-08T10:00:00', 'count': 20}
        ]
        star_queries = [
            {'start_time': '2024-01-01T10:00:00', 'count': 5},
            {'start_time': '2024-01-08T10:00:00', 'count': 10}
        ]
        
        result = data_service.calculate_weekly_stats(read_queries, star_queries)
        
        assert len(result) >= 1  # At least one week
        assert all('week_start' in stat for stat in result)
        assert all('total_queries' in stat for stat in result)
        assert all('star_queries' in stat for stat in result)


class TestCalculateMonthlyStats:
    """Tests for calculate_monthly_stats function."""
    
    def test_single_month(self):
        """Test with queries from a single month."""
        read_queries = [
            {'start_time': '2024-01-01T10:00:00', 'count': 10},
            {'start_time': '2024-01-15T10:00:00', 'count': 20}
        ]
        star_queries = [
            {'start_time': '2024-01-05T10:00:00', 'count': 5}
        ]
        
        result = data_service.calculate_monthly_stats(read_queries, star_queries)
        
        assert len(result) == 1
        assert result[0]['month_start'] == '2024-01'
        assert result[0]['total_queries'] == 30
        assert result[0]['star_queries'] == 5
    
    def test_multiple_months(self):
        """Test with queries spanning multiple months."""
        read_queries = [
            {'start_time': '2024-01-01T10:00:00', 'count': 10},
            {'start_time': '2024-02-01T10:00:00', 'count': 20},
            {'start_time': '2024-03-01T10:00:00', 'count': 30}
        ]
        star_queries = []
        
        result = data_service.calculate_monthly_stats(read_queries, star_queries)
        
        assert len(result) == 3
        assert result[0]['month_start'] == '2024-01'
        assert result[1]['month_start'] == '2024-02'
        assert result[2]['month_start'] == '2024-03'


class TestFindTableMetadata:
    """Tests for find_table_metadata function."""
    
    def test_find_existing_table(self, mock_storage, sample_table_data):
        """Test finding metadata for an existing table."""
        mock_storage.load_tables.return_value = [sample_table_data]
        
        result = data_service.find_table_metadata(
            mock_storage,
            'bigquery',
            'test-project',
            'test_db',
            'test_schema',
            'test_table'
        )
        
        assert result is not None
        assert result['size_bytes'] == sample_table_data['size_bytes']
        assert result['row_count'] == sample_table_data['row_count']
    
    def test_find_nonexistent_table(self, mock_storage):
        """Test finding metadata for non-existent table returns None."""
        mock_storage.load_tables.return_value = []
        
        result = data_service.find_table_metadata(
            mock_storage, 'bigquery', 'project', 'db', 'schema', 'nonexistent'
        )
        
        assert result is None
    
    def test_case_insensitive_search(self, mock_storage, sample_table_data):
        """Test that table search is case-insensitive."""
        mock_storage.load_tables.return_value = [sample_table_data]
        
        result = data_service.find_table_metadata(
            mock_storage,
            'BIGQUERY',
            'TEST-PROJECT',
            'TEST_DB',
            'TEST_SCHEMA',
            'TEST_TABLE'
        )
        
        assert result is not None

