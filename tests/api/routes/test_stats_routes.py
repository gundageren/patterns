"""Tests for stats routes."""

import pytest
from unittest.mock import patch, MagicMock


class TestStatsRoutes:
    """Tests for statistics endpoints."""
    
    def test_table_weekly_stats_missing_params(self, flask_client):
        """Test table-weekly-stats with missing parameters."""
        response = flask_client.get('/table-weekly-stats')
        
        assert response.status_code == 400
        data = response.get_json()
        assert data['success'] is False
        assert 'error' in data
    
    def test_table_weekly_stats_success(self, flask_client, mock_storage):
        """Test table-weekly-stats with valid parameters."""
        # Mock storage methods
        mock_storage.load_read_table_queries.return_value = []
        mock_storage.load_select_star_queries.return_value = []
        mock_storage.load_partition_candidates.return_value = []
        mock_storage.load_tables.return_value = []
        
        response = flask_client.get(
            '/table-weekly-stats?'
            'source_platform=bigquery&'
            'source_project=test-project&'
            'database=test_db&'
            'schema=test_schema&'
            'table=test_table'
        )
        
        assert response.status_code == 200
        data = response.get_json()
        assert data['success'] is True
        assert 'table_info' in data
        assert 'weekly_stats' in data
    
    def test_find_patterns_missing_params(self, flask_client):
        """Test find-patterns with missing parameters."""
        response = flask_client.post('/find-patterns')
        
        assert response.status_code == 400
        data = response.get_json()
        assert data['success'] is False
    
    def test_find_patterns_invalid_warehouse(self, flask_client):
        """Test find-patterns with invalid warehouse."""
        response = flask_client.post(
            '/find-patterns?'
            'source_platform=bigquery&'
            'source_project=test-project&'
            'database=test_db&'
            'schema=test_schema&'
            'table=test_table&'
            'target_warehouse=InvalidWarehouse'
        )
        
        assert response.status_code == 400
        data = response.get_json()
        assert data['success'] is False
        assert 'Invalid target_warehouse' in data['error']
    
    @patch('api.services.ai_query_service.query_ai_with_fallback')
    def test_find_patterns_success(self, mock_ai_query, flask_client, mock_storage):
        """Test find-patterns with valid parameters."""
        # Mock storage
        mock_storage.load_read_table_queries.return_value = []
        mock_storage.load_select_star_queries.return_value = []
        mock_storage.load_partition_candidates.return_value = []
        mock_storage.load_tables.return_value = []
        
        # Mock AI query
        mock_ai_query.return_value = ('AI response', {})
        
        response = flask_client.post(
            '/find-patterns?'
            'source_platform=bigquery&'
            'source_project=test-project&'
            'database=test_db&'
            'schema=test_schema&'
            'table=test_table'
        )
        
        assert response.status_code == 200
        data = response.get_json()
        assert data['success'] is True
        assert 'table_info' in data
        assert 'ai_suggestions' in data


class TestStatsRoutesErrorHandling:
    """Tests for error handling in stats routes."""
    
    @patch('api.services.ai_query_service.query_ai_with_fallback')
    def test_find_patterns_ai_error(self, mock_ai_query, flask_client, mock_storage):
        """Test find-patterns when AI service fails."""
        mock_storage.load_read_table_queries.return_value = []
        mock_storage.load_select_star_queries.return_value = []
        mock_storage.load_partition_candidates.return_value = []
        mock_storage.load_tables.return_value = []
        
        # Make AI query fail
        mock_ai_query.side_effect = Exception("AI service error")
        
        response = flask_client.post(
            '/find-patterns?'
            'source_platform=bigquery&'
            'source_project=test-project&'
            'database=test_db&'
            'schema=test_schema&'
            'table=test_table'
        )
        
        assert response.status_code == 500
        data = response.get_json()
        assert data['success'] is False
        assert 'error' in data

