"""
Patterns API - Analyze query patterns and optimize data warehouse tables

A Flask API that extracts query history, analyzes usage patterns, and provides
AI-powered optimization recommendations using Gemini.
"""

import os
import json
import logging
import argparse
from flask import Flask

# Import from organized API structure
from api.utils.decorators import require_app_initialized, handle_exceptions
from api.utils.config import get_source_platform, get_connection_config
from api.utils.dates import get_default_date_range
from api.services import refresh_service
from api.routes.data_routes import create_data_routes
from api.routes.stats_routes import create_stats_routes
from api.routes.info_routes import create_info_routes
from patterns.store.factory import get_storage

logging.basicConfig(
    format='[%(asctime)s] %(levelname)-8s %(message)s',
    level=logging.INFO
)
logger = logging.getLogger("patterns")

def initialize_storage(config: dict):
    """Initialize storage from configuration."""
    storage_conf = config.get("storage", {})
    storage_type = storage_conf.get("type", "duckdb")
    storage_config = {"db_path": storage_conf.get("db_path", "data/patterns.duckdb")}
    
    storage = get_storage(storage_type=storage_type, config=storage_config)
    logger.info("Storage initialized: %s", storage_type)
    return storage


def perform_initial_extraction(source_platform: str, connection: dict, storage):
    """Perform initial data extraction and analysis on startup."""
    start, end = get_default_date_range()
    queries, tables = refresh_service.refresh_all_data(source_platform, connection, storage, start, end)
    
    if queries and (source_project := queries[0].get("source_project")):
        refresh_service.run_analysis(source_platform, source_project, storage, start, end)
        logger.info("Initial extraction and analysis completed")
    else:
        logger.warning("No source_project found, skipping analysis")


def load_config_file(config_path: str) -> dict:
    """Load configuration from JSON file."""
    if not os.path.exists(config_path):
        raise FileNotFoundError(f"Config file not found: {config_path}")
    
    with open(config_path, "r") as f:
        return json.load(f)


def parse_cli_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description="Patterns API")
    parser.add_argument("--config", default="config.json", help="Config file path")
    parser.add_argument("--skip-initial-extraction", action="store_true",
                       help="Skip initial extraction on startup")
    parser.add_argument("--disable-ui-config", action="store_true",
                       help="Disable configuration controls in the UI")
    return parser.parse_args()


def create_app(config: dict, storage, disable_ui_config: bool = False) -> Flask:
    """Create and configure Flask application."""
    app = Flask("Patterns", template_folder="api/templates", static_folder="api/static")
    
    app.config['DISABLE_UI_CONFIG'] = disable_ui_config
    
    def require_init(f):
        return require_app_initialized(config, storage)(f)
    
    app.register_blueprint(create_info_routes(app, storage, require_init, handle_exceptions))
    app.register_blueprint(create_data_routes(config, storage, require_init, handle_exceptions))
    app.register_blueprint(create_stats_routes(config, storage, require_init, handle_exceptions))
    
    logger.info("Flask application configured with %d routes", len(app.url_map._rules))
    return app


if __name__ == '__main__':
    args = parse_cli_args()
    
    try:
        config = load_config_file(args.config)
        logger.info("Configuration loaded from %s", args.config)
    except (FileNotFoundError, json.JSONDecodeError) as e:
        logger.error("Configuration error: %s", e)
        exit(1)
    
    source_platform = get_source_platform(config)
    if not source_platform:
        logger.error("Platform must be specified in config")
        exit(1)
    
    try:
        app_storage = initialize_storage(config)
    except Exception as e:
        logger.error("Storage initialization failed: %s", e)
        exit(1)
    
    if not args.skip_initial_extraction:
        try:
            perform_initial_extraction(source_platform, get_connection_config(config), app_storage)
        except Exception as e:
            logger.error("Initial extraction failed: %s", e)
            exit(1)
    else:
        logger.info("Skipping initial extraction (use web UI to trigger)")
    
    app = create_app(config, app_storage, args.disable_ui_config)
    logger.info("Starting Flask application...")
    app.run(debug=True, use_reloader=False)
