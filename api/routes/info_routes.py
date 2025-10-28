"""Routes for informational endpoints."""

import json
from flask import Blueprint, request, render_template
from ..services import data_service
from ..utils.responses import json_response
from ..utils.constants import TOP_DATA_WAREHOUSES


def create_info_routes(app, app_storage, require_app_initialized, handle_exceptions):
    """Create blueprint with informational routes."""
    bp = Blueprint('info', __name__)
    
    @bp.route('/')
    def index():
        """Render main dashboard."""
        response = app.make_response(render_template('index.html'))
        # Prevent browser caching of the HTML to ensure users always get the latest version
        response.headers['Cache-Control'] = 'no-cache, no-store, must-revalidate'
        response.headers['Pragma'] = 'no-cache'
        response.headers['Expires'] = '0'
        return response
    
    @bp.route('/list-tables', methods=['GET'])
    @require_app_initialized
    @handle_exceptions
    def list_tables():
        """List tables with cascading filter options."""
        all_tables = app_storage.load_tables()
        
        filtered = data_service.filter_tables_by_criteria(
            all_tables,
            request.args.get('source_platform'),
            request.args.get('source_project'),
            request.args.get('database'),
            request.args.get('schema')
        )
        
        # Calculate available values for cascading dropdowns
        platform_filter = request.args.get('source_platform')
        platforms = sorted({t['source_platform'] for t in all_tables if t.get('source_platform')})
        
        if platform_filter:
            platform_tables = [t for t in all_tables if t.get('source_platform') == platform_filter]
            projects = sorted({t['source_project'] for t in platform_tables if t.get('source_project')})
        else:
            projects = sorted({t['source_project'] for t in all_tables if t.get('source_project')})
        
        databases = sorted({t['database'] for t in filtered if t.get('database')})
        schemas = sorted({t['schema'] for t in filtered if t.get('schema')})
        
        def get_column_count(columns_data):
            """Helper to safely get column count from various formats."""
            if not columns_data:
                return 0
            if isinstance(columns_data, str):
                try:
                    parsed = json.loads(columns_data) if columns_data else []
                    return len(parsed)
                except (json.JSONDecodeError, TypeError):
                    return 0
            elif isinstance(columns_data, list):
                return len(columns_data)
            return 0
        
        table_list = [
            {
                "database": t.get("database"),
                "schema": t.get("schema"),
                "table": t.get("table"),
                "size_bytes": t.get("size_bytes"),
                "column_count": get_column_count(t.get("columns")),
                "source_platform": t.get("source_platform"),
                "source_project": t.get("source_project"),
                "source_region": t.get("source_region")
            }
            for t in filtered
        ]
        
        return json_response(True, data={
            "filters_applied": {
                "source_platform": request.args.get('source_platform'),
                "source_project": request.args.get('source_project'),
                "database": request.args.get('database'),
                "schema": request.args.get('schema')
            },
            "available_values": {
                "source_platforms": platforms,
                "source_projects": projects,
                "databases": databases,
                "schemas": schemas
            },
            "total_tables": len(table_list),
            "tables": table_list
        })
    
    @bp.route('/list-warehouses', methods=['GET'])
    @require_app_initialized
    @handle_exceptions
    def list_warehouses():
        """List available data warehouses."""
        return json_response(True, data={
            "warehouses": TOP_DATA_WAREHOUSES,
            "count": len(TOP_DATA_WAREHOUSES)
        })
    
    @bp.route('/ui-config', methods=['GET'])
    def ui_config():
        """Get UI configuration settings."""
        return json_response(True, data={
            "disable_ui_config": app.config.get('DISABLE_UI_CONFIG', False)
        })
    
    return bp

