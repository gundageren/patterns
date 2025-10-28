"""Pytest configuration and shared fixtures."""

import pytest
from unittest.mock import MagicMock, Mock
from datetime import datetime, timedelta


@pytest.fixture
def mock_storage():
    """Mock storage for testing."""
    storage = MagicMock()
    storage.load_queries.return_value = []
    storage.load_tables.return_value = []
    storage.load_read_table_queries.return_value = []
    storage.load_select_star_queries.return_value = []
    storage.load_partition_candidates.return_value = []
    return storage


@pytest.fixture
def sample_table_data():
    """Sample table data for testing."""
    return {
        'source_platform': 'bigquery',
        'source_project': 'test-project',
        'database': 'test_db',
        'schema': 'test_schema',
        'table': 'test_table',
        'size_bytes': 1024000,
        'row_count': 10000,
        'columns': [
            {'name': 'id', 'type': 'INTEGER'},
            {'name': 'name', 'type': 'STRING'},
            {'name': 'created_at', 'type': 'TIMESTAMP'}
        ]
    }


@pytest.fixture
def sample_query_data():
    """Sample query data for testing."""
    base_time = datetime.now()
    return [
        {
            'source_platform': 'bigquery',
            'source_project': 'test-project',
            'database': 'test_db',
            'schema': 'test_schema',
            'table': 'test_table',
            'start_time': (base_time - timedelta(days=i)).isoformat(),
            'count': 10 + i
        }
        for i in range(7)
    ]


@pytest.fixture
def sample_partition_candidates():
    """Sample partition candidate data for testing."""
    base_time = datetime.now()
    return [
        {
            'source_platform': 'bigquery',
            'source_project': 'test-project',
            'database': 'test_db',
            'schema': 'test_schema',
            'table': 'test_table',
            'column': 'created_at',
            'filter_type': 'range',
            'start_time': (base_time - timedelta(days=i)).isoformat(),
            'count': 5 + i
        }
        for i in range(7)
    ]


@pytest.fixture
def mock_bigquery_client():
    """Mock BigQuery client for testing."""
    client = MagicMock()
    client.project = 'test-project'
    return client


@pytest.fixture
def mock_snowflake_connection():
    """Mock Snowflake connection for testing."""
    conn = MagicMock()
    cursor = MagicMock()
    conn.cursor.return_value = cursor
    cursor.fetchall.return_value = []
    cursor.fetchone.return_value = None
    return conn


@pytest.fixture
def app_config():
    """Sample application configuration."""
    return {
        'source_platform': 'bigquery',
        'connection': {
            'project': 'test-project',
            'location': 'US'
        },
        'gemini': {
            'api_key': 'your-gemini-api-key',
            'model': 'gemini-2.5-flash'
        },
        'storage': {
            'type': 'duckdb',
            'db_path': ':memory:'
        }
    }


@pytest.fixture
def flask_app(app_config, mock_storage):
    """Flask app for testing routes."""
    from flask import Flask
    from api.routes.data_routes import create_data_routes
    from api.routes.stats_routes import create_stats_routes
    from api.routes.info_routes import create_info_routes
    from api.utils.decorators import require_app_initialized, handle_exceptions
    
    app = Flask("TestApp", template_folder="api/templates", static_folder="api/static")
    
    def require_init(f):
        return require_app_initialized(app_config, mock_storage)(f)
    
    app.register_blueprint(create_info_routes(app, mock_storage, require_init, handle_exceptions))
    app.register_blueprint(create_data_routes(app_config, mock_storage, require_init, handle_exceptions))
    app.register_blueprint(create_stats_routes(app_config, mock_storage, require_init, handle_exceptions))
    
    return app


@pytest.fixture
def flask_client(flask_app):
    """Flask test client."""
    return flask_app.test_client()

