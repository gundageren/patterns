"""Tests for UI-related functionality."""

import pytest
from patterns_app import create_app, parse_cli_args


class TestUIConfig:
    """Test suite for UI configuration functionality."""

    def test_parse_cli_args_disable_ui_config_default(self, monkeypatch):
        """Test that disable_ui_config defaults to False."""
        monkeypatch.setattr('sys.argv', ['patterns_app.py'])
        args = parse_cli_args()
        assert args.disable_ui_config is False

    def test_parse_cli_args_with_disable_ui_config_flag(self, monkeypatch):
        """Test that --disable-ui-config flag is parsed correctly."""
        monkeypatch.setattr('sys.argv', ['patterns_app.py', '--disable-ui-config'])
        args = parse_cli_args()
        assert args.disable_ui_config is True

    def test_app_config_disabled(self, mock_storage):
        """Test app creation with disable_ui_config=True."""
        config = {
            "platform": "bigquery",
            "connection": {},
            "storage": {"type": "duckdb", "db_path": ":memory:"}
        }
        
        app = create_app(config, mock_storage, disable_ui_config=True)
        
        assert app.config['DISABLE_UI_CONFIG'] is True

    def test_app_config_enabled(self, mock_storage):
        """Test app creation with disable_ui_config=False."""
        config = {
            "platform": "bigquery",
            "connection": {},
            "storage": {"type": "duckdb", "db_path": ":memory:"}
        }
        
        app = create_app(config, mock_storage, disable_ui_config=False)
        
        assert app.config['DISABLE_UI_CONFIG'] is False

    def test_app_config_default(self, mock_storage):
        """Test app creation uses False as default."""
        config = {
            "platform": "bigquery",
            "connection": {},
            "storage": {"type": "duckdb", "db_path": ":memory:"}
        }
        
        app = create_app(config, mock_storage)
        
        assert app.config['DISABLE_UI_CONFIG'] is False

    def test_ui_config_endpoint_returns_disabled_status(self, mock_storage):
        """Test /ui-config endpoint returns correct status when disabled."""
        config = {
            "platform": "bigquery",
            "connection": {},
            "storage": {"type": "duckdb", "db_path": ":memory:"}
        }
        
        app = create_app(config, mock_storage, disable_ui_config=True)
        client = app.test_client()
        
        response = client.get('/ui-config')
        data = response.get_json()
        
        assert response.status_code == 200
        assert data['success'] is True
        assert data['disable_ui_config'] is True

    def test_ui_config_endpoint_returns_enabled_status(self, mock_storage):
        """Test /ui-config endpoint returns correct status when enabled."""
        config = {
            "platform": "bigquery",
            "connection": {},
            "storage": {"type": "duckdb", "db_path": ":memory:"}
        }
        
        app = create_app(config, mock_storage, disable_ui_config=False)
        client = app.test_client()
        
        response = client.get('/ui-config')
        data = response.get_json()
        
        assert response.status_code == 200
        assert data['success'] is True
        assert data['disable_ui_config'] is False

    def test_ui_config_endpoint_json_format(self, mock_storage):
        """Test /ui-config endpoint returns valid JSON."""
        config = {
            "platform": "bigquery",
            "connection": {},
            "storage": {"type": "duckdb", "db_path": ":memory:"}
        }
        
        app = create_app(config, mock_storage, disable_ui_config=True)
        client = app.test_client()
        
        response = client.get('/ui-config')
        
        assert response.content_type == 'application/json'
        assert response.status_code == 200

    def test_index_page_loads_with_disabled_config(self, mock_storage):
        """Test that index page loads when UI config is disabled."""
        config = {
            "platform": "bigquery",
            "connection": {},
            "storage": {"type": "duckdb", "db_path": ":memory:"}
        }
        
        app = create_app(config, mock_storage, disable_ui_config=True)
        client = app.test_client()
        
        response = client.get('/')
        
        assert response.status_code == 200
        assert b'<!DOCTYPE html>' in response.data

    def test_ui_javascript_disabling_present(self, mock_storage):
        """Test that JavaScript for disabling UI controls is present."""
        config = {
            "platform": "bigquery",
            "connection": {},
            "storage": {"type": "duckdb", "db_path": ":memory:"}
        }
        
        app = create_app(config, mock_storage, disable_ui_config=True)
        client = app.test_client()
        
        response = client.get('/')
        
        assert b'loadUIConfig' in response.data
        assert b'gear-icon-wrapper' in response.data

