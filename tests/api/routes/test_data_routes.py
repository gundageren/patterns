"""Tests for data routes."""

import pytest
from unittest.mock import patch, MagicMock


class TestDataRoutes:
    """Tests for data refresh endpoints."""
    
    @patch('api.services.refresh_service.refresh_tables')
    def test_refresh_tables(self, mock_refresh, flask_client, mock_storage):
        """Test refresh-tables endpoint."""
        mock_refresh.return_value = [{'table': 'test'}]
        
        response = flask_client.post('/refresh-tables')
        
        assert response.status_code == 200
        data = response.get_json()
        assert data['success'] is True
        assert data['tables_extracted'] == 1
    
    @patch('api.services.refresh_service.refresh_query_history')
    def test_refresh_query_history(self, mock_refresh, flask_client, mock_storage):
        """Test refresh-query-history endpoint."""
        mock_refresh.return_value = [{'query': 'test'}]
        
        response = flask_client.post('/refresh-query-history')
        
        assert response.status_code == 200
        data = response.get_json()
        assert data['success'] is True
        assert 'queries_extracted' in data
    
    @patch('api.services.refresh_service.refresh_all_data')
    def test_refresh_all(self, mock_refresh_all, flask_client, mock_storage):
        """Test refresh-query-history-and-tables endpoint."""
        mock_refresh_all.return_value = ([{'query': 'test'}], [{'table': 'test'}])
        
        response = flask_client.post('/refresh-query-history-and-tables')
        
        assert response.status_code == 200
        data = response.get_json()
        assert data['success'] is True
        assert 'queries_extracted' in data
        assert 'tables_extracted' in data
    
    @patch('api.services.refresh_service.run_analysis')
    def test_run_analysis_with_project(self, mock_analysis, flask_client, mock_storage):
        """Test run-analysis endpoint."""
        mock_storage.load_queries.return_value = [
            {'source_project': 'test-project'}
        ]
        mock_analysis.return_value = {'queries': 100}
        
        response = flask_client.post(
            '/run-analysis?'
            'source_platform=bigquery&'
            'source_project=test-project'
        )
        
        assert response.status_code == 200
        data = response.get_json()
        assert data['success'] is True
        assert 'results' in data
    
    def test_run_analysis_missing_project(self, flask_client, mock_storage):
        """Test run-analysis when no project found."""
        mock_storage.load_queries.return_value = []
        
        response = flask_client.post('/run-analysis')
        
        assert response.status_code == 400
        data = response.get_json()
        assert data['success'] is False

