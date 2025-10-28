"""Tests for API response builders."""

import pytest
from api.utils.response_builders import (
    build_table_info_response,
    build_analysis_data,
    build_refresh_response
)


class TestBuildTableInfoResponse:
    """Tests for build_table_info_response function."""
    
    def test_basic_table_info(self):
        """Test building basic table info without optional fields."""
        result = build_table_info_response(
            'bigquery', 'my-project', 'database', 'schema', 'table'
        )
        
        assert result['source_platform'] == 'bigquery'
        assert result['source_project'] == 'my-project'
        assert result['database'] == 'database'
        assert result['schema'] == 'schema'
        assert result['table'] == 'table'
        assert 'target_warehouse' not in result
        assert 'size_bytes' not in result
    
    def test_with_target_warehouse(self):
        """Test building table info with target warehouse."""
        result = build_table_info_response(
            'bigquery', 'my-project', 'database', 'schema', 'table',
            target_warehouse='Snowflake'
        )
        
        assert result['target_warehouse'] == 'Snowflake'
    
    def test_with_table_metadata(self):
        """Test building table info with metadata."""
        metadata = {
            'size_bytes': 1024000,
            'row_count': 10000,
            'columns': [
                {'name': 'id', 'type': 'INTEGER'},
                {'name': 'name', 'type': 'STRING'}
            ]
        }
        
        result = build_table_info_response(
            'bigquery', 'my-project', 'database', 'schema', 'table',
            table_metadata=metadata
        )
        
        assert result['size_bytes'] == 1024000
        assert result['row_count'] == 10000
        assert result['column_count'] == 2
        assert result['columns'] == metadata['columns']
    
    def test_with_all_fields(self):
        """Test building table info with all optional fields."""
        metadata = {
            'size_bytes': 2048000,
            'row_count': 50000,
            'columns': [{'name': 'id', 'type': 'INTEGER'}]
        }
        
        result = build_table_info_response(
            'snowflake', 'project-2', 'db2', 'schema2', 'table2',
            target_warehouse='BigQuery',
            table_metadata=metadata
        )
        
        assert result['source_platform'] == 'snowflake'
        assert result['target_warehouse'] == 'BigQuery'
        assert result['size_bytes'] == 2048000
        assert result['column_count'] == 1


class TestBuildAnalysisData:
    """Tests for build_analysis_data function."""
    
    def test_analysis_completed(self):
        """Test building analysis data when completed."""
        results = {'queries': 100, 'tables': 20}
        
        result = build_analysis_data(
            analysis_completed=True,
            source_project='my-project',
            analysis_results=results
        )
        
        assert result['analysis_completed'] is True
        assert result['source_project'] == 'my-project'
        assert result['analysis_results'] == results
    
    def test_analysis_not_completed(self):
        """Test building analysis data when not completed."""
        result = build_analysis_data(
            analysis_completed=False,
            reason='No queries found'
        )
        
        assert result['analysis_completed'] is False
        assert result['reason'] == 'No queries found'
        assert 'source_project' not in result
        assert 'analysis_results' not in result
    
    def test_analysis_not_completed_default_reason(self):
        """Test default reason when analysis not completed."""
        result = build_analysis_data(analysis_completed=False)
        
        assert result['analysis_completed'] is False
        assert result['reason'] == 'Analysis not requested'


class TestBuildRefreshResponse:
    """Tests for build_refresh_response function."""
    
    def test_basic_refresh_response(self):
        """Test building basic refresh response."""
        result = build_refresh_response('2024-01-01', '2024-01-31')
        
        assert result['start_date'] == '2024-01-01'
        assert result['end_date'] == '2024-01-31'
        assert 'queries_extracted' not in result
        assert 'tables_extracted' not in result
    
    def test_with_queries_count(self):
        """Test building refresh response with queries count."""
        result = build_refresh_response(
            '2024-01-01', '2024-01-31', queries_count=150
        )
        
        assert result['queries_extracted'] == 150
    
    def test_with_tables_count(self):
        """Test building refresh response with tables count."""
        result = build_refresh_response(
            '2024-01-01', '2024-01-31', tables_count=25
        )
        
        assert result['tables_extracted'] == 25
    
    def test_with_analysis_data(self):
        """Test building refresh response with analysis data."""
        analysis_data = {
            'analysis_completed': True,
            'source_project': 'my-project',
            'analysis_results': {'queries': 100}
        }
        
        result = build_refresh_response(
            '2024-01-01', '2024-01-31',
            queries_count=100,
            tables_count=20,
            analysis_data=analysis_data
        )
        
        assert result['analysis'] == analysis_data
        assert result['queries_extracted'] == 100
        assert result['tables_extracted'] == 20
    
    def test_with_zero_counts(self):
        """Test that zero counts are included."""
        result = build_refresh_response(
            '2024-01-01', '2024-01-31',
            queries_count=0,
            tables_count=0
        )
        
        assert result['queries_extracted'] == 0
        assert result['tables_extracted'] == 0

