"""Routes for statistics and AI pattern analysis."""

import logging
from flask import Blueprint, request
from ..services import data_service, ai_query_service, privacy_service
from ..utils.responses import json_response
from ..utils.response_builders import build_table_info_response
from ..utils.validators import validate_table_params, validate_target_warehouse, get_debug_flag
from ..utils.dates import parse_date_params
from ..utils.config import get_gemini_config
from ..utils.constants import TOP_DATA_WAREHOUSES

logger = logging.getLogger("patterns")


def create_stats_routes(app_config, app_storage, require_app_initialized, handle_exceptions):
    """Create blueprint with statistics and AI analysis routes."""
    bp = Blueprint('stats', __name__)
    
    @bp.route('/table-weekly-stats', methods=['GET'])
    @require_app_initialized
    @handle_exceptions
    def table_weekly_stats():
        """Get weekly statistics for a specific table."""
        source_platform = request.args.get('source_platform')
        source_project = request.args.get('source_project')
        database = request.args.get('database')
        schema = request.args.get('schema')
        table = request.args.get('table')
        start, end = parse_date_params(request.args.get('start_date'), request.args.get('end_date'))
        
        is_valid, error_response = validate_table_params(
            source_platform, source_project, database, schema, table
        )
        if not is_valid:
            return error_response
        
        stats = data_service.get_table_stats(
            app_storage, source_platform, source_project, database, schema, table, start, end
        )
        
        return json_response(True, data={
            "table_info": build_table_info_response(
                source_platform, source_project, database, schema, table
            ),
            "weekly_stats": stats['weekly_stats'],
            "partition_stats": stats['partition_stats'],
            "summary": stats['summary']
        })
    
    @bp.route('/find-patterns', methods=['POST'])
    @require_app_initialized
    @handle_exceptions
    def find_patterns():
        """Analyze query patterns and get AI-powered optimization suggestions."""
        source_platform = request.args.get('source_platform')
        source_project = request.args.get('source_project')
        database = request.args.get('database')
        schema = request.args.get('schema')
        table = request.args.get('table')
        target_warehouse = request.args.get('target_warehouse')
        debug = get_debug_flag(request.args.get('debug', ''))
        start, end = parse_date_params(request.args.get('start_date'), request.args.get('end_date'))
        
        is_valid, error_response = validate_table_params(
            source_platform, source_project, database, schema, table
        )
        if not is_valid:
            return error_response
        
        is_valid, error_response = validate_target_warehouse(target_warehouse)
        if not is_valid:
            return error_response
        
        stats = data_service.get_table_stats(
            app_storage, source_platform, source_project, database, schema, table, start, end, period='weekly'
        )
        
        stats_monthly = data_service.get_table_stats(
            app_storage, source_platform, source_project, database, schema, table, start, end, period='monthly'
        )
        stats['monthly_stats'] = stats_monthly.get('monthly_stats', [])
        
        logger.info("Querying Gemini AI for table: %s (debug=%s)", table, debug)
        gemini_config = get_gemini_config(app_config)
        
        try:
            ai_response, reverse_map = ai_query_service.query_ai_with_fallback(
                table, database, schema, source_platform, source_project,
                target_warehouse, stats, gemini_config, debug
            )
        except Exception as e:
            logger.error("AI query failed: %s", str(e))
            return json_response(False, error=str(e), status_code=500)
        
        ai_response_restored = privacy_service.restore_names_in_response(ai_response, reverse_map)
        
        period_stats_key = 'weekly_stats' if 'weekly_stats' in stats else 'monthly_stats'
        
        response_data = {
            "table_info": build_table_info_response(
                source_platform, source_project, database, schema, table,
                target_warehouse or "not specified", stats['table_metadata']
            ),
            "statistics": {
                period_stats_key: stats.get(period_stats_key, []),
                "partition_stats": stats['partition_stats'],
                "summary": stats['summary'],
                "period": stats.get('period', 'weekly')
            },
            "ai_suggestions": ai_response_restored,
            "available_warehouses": TOP_DATA_WAREHOUSES,
            "model_used": gemini_config.get("model", "gemini-2.5-flash")
        }
        
        if debug:
            prompt, _ = ai_query_service.anonymize_and_build_prompt(
                table, database, schema, source_platform, source_project,
                target_warehouse, stats, period='weekly'
            )
            response_data["debug"] = {
                "prompt": prompt,
                "prompt_length": len(prompt),
                "anonymization_map": reverse_map
            }
        
        return json_response(True, data=response_data)
    
    return bp

