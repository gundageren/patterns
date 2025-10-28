"""Routes for data refresh and extraction."""

from flask import Blueprint, request
from ..services import refresh_service
from ..utils.responses import json_response
from ..utils.response_builders import build_analysis_data, build_refresh_response
from ..utils.validators import validate_platform_configured
from ..utils.dates import parse_date_params
from ..utils.config import get_source_platform, get_connection_config

def create_data_routes(app_config, app_storage, require_app_initialized, handle_exceptions):
    """Create blueprint with data refresh routes."""
    bp = Blueprint('data', __name__)
    
    def _build_analysis_response(queries, source_platform, storage, start, end):
        """Helper to build analysis response data."""
        if queries and (source_project := queries[0].get("source_project")):
            analysis_results = refresh_service.run_analysis(
                source_platform, source_project, storage, start, end
            )
            return build_analysis_data(True, source_project, analysis_results)
        else:
            return build_analysis_data(False, reason="No source_project found")
    
    @bp.route('/refresh-query-history-and-tables', methods=['POST'])
    @require_app_initialized
    @handle_exceptions
    def refresh_all():
        """Refresh both query history and tables."""
        start, end = parse_date_params(request.args.get('start_date'), request.args.get('end_date'))
        
        source_platform = get_source_platform(app_config)
        is_valid, error_response = validate_platform_configured(source_platform)
        if not is_valid:
            return error_response
        
        queries, tables = refresh_service.refresh_all_data(
            source_platform, get_connection_config(app_config), app_storage, start, end
        )
        
        analysis_data = _build_analysis_response(
            queries, source_platform, app_storage, start, end
        )
        
        return json_response(
            True,
            message="Data refreshed successfully",
            data=build_refresh_response(start, end, len(queries), len(tables), analysis_data)
        )
    
    @bp.route('/refresh-query-history', methods=['POST'])
    @require_app_initialized
    @handle_exceptions
    def refresh_queries():
        """Refresh query history only."""
        start, end = parse_date_params(request.args.get('start_date'), request.args.get('end_date'))
        run_analysis = request.args.get('run_analysis', 'true').lower() == 'true'
        
        source_platform = get_source_platform(app_config)
        is_valid, error_response = validate_platform_configured(source_platform)
        if not is_valid:
            return error_response
        
        queries = refresh_service.refresh_query_history(
            source_platform, get_connection_config(app_config), app_storage, start, end
        )
        
        if run_analysis:
            analysis_data = _build_analysis_response(
                queries, source_platform, app_storage, start, end
            )
        else:
            analysis_data = build_analysis_data(False, reason="Analysis not requested")
        
        return json_response(
            True, 
            message="Query history refreshed",
            data=build_refresh_response(start, end, queries_count=len(queries), analysis_data=analysis_data)
        )
    
    @bp.route('/refresh-tables', methods=['POST'])
    @require_app_initialized
    @handle_exceptions
    def refresh_tables_route():
        """Refresh table list only."""
        source_platform = get_source_platform(app_config)
        is_valid, error_response = validate_platform_configured(source_platform)
        if not is_valid:
            return error_response
        
        tables = refresh_service.refresh_tables(
            source_platform, get_connection_config(app_config), app_storage
        )
        
        return json_response(True, message="Tables refreshed", data={"tables_extracted": len(tables)})
    
    @bp.route('/run-analysis', methods=['POST'])
    @require_app_initialized
    @handle_exceptions
    def run_analysis_endpoint():
        """Run analysis on stored queries."""
        start, end = parse_date_params(request.args.get('start_date'), request.args.get('end_date'))
        source_platform = request.args.get('source_platform') or get_source_platform(app_config)
        source_project = request.args.get('source_project')
        
        is_valid, error_response = validate_platform_configured(source_platform)
        if not is_valid:
            return error_response
        
        # Get source_project if not provided
        if not source_project:
            queries = app_storage.load_queries(
                source_platform=source_platform, source_project=None, start_time=start, end_time=end
            )
            source_project = queries[0].get("source_project") if queries else None
        
        if not source_project:
            return json_response(False, error="No source_project found", status_code=400)
        
        results = refresh_service.run_analysis(source_platform, source_project, app_storage, start, end)
        
        return json_response(True, message="Analysis completed", data={
            "start_date": start,
            "end_date": end,
            "source_platform": source_platform,
            "source_project": source_project,
            "results": results
        })
    
    return bp

