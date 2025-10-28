"""Tests for info routes."""

import pytest
from unittest.mock import MagicMock


class TestInfoRoutes:
    """Tests for info endpoints."""
    
    def test_index_route(self, flask_client):
        """Test that index route returns 200."""
        response = flask_client.get('/')
        
        assert response.status_code == 200
    
    def test_list_tables_empty(self, flask_client, mock_storage):
        """Test list-tables with no data."""
        mock_storage.load_tables.return_value = []
        
        response = flask_client.get('/list-tables')
        
        assert response.status_code == 200
        data = response.get_json()
        assert data['success'] is True
        assert data['total_tables'] == 0
        assert data['tables'] == []
    
    def test_list_tables_with_data(self, flask_client, mock_storage, sample_table_data):
        """Test list-tables with sample data."""
        mock_storage.load_tables.return_value = [sample_table_data]
        
        response = flask_client.get('/list-tables')
        
        assert response.status_code == 200
        data = response.get_json()
        assert data['success'] is True
        assert data['total_tables'] == 1
        assert len(data['tables']) == 1
    
    def test_list_tables_with_filters(self, flask_client, mock_storage, sample_table_data):
        """Test list-tables with filtering."""
        mock_storage.load_tables.return_value = [sample_table_data]
        
        response = flask_client.get('/list-tables?source_platform=bigquery')
        
        assert response.status_code == 200
        data = response.get_json()
        assert data['success'] is True
        assert data['filters_applied']['source_platform'] == 'bigquery'
    
    def test_list_warehouses(self, flask_client):
        """Test list-warehouses endpoint."""
        response = flask_client.get('/list-warehouses')
        
        assert response.status_code == 200
        data = response.get_json()
        assert data['success'] is True
        assert 'warehouses' in data
        assert 'count' in data
        assert data['count'] > 0

