"""Service for handling AI query operations with anonymization and retries."""

import logging
from typing import Dict, List, Optional, Tuple
from . import ai_service, privacy_service

logger = logging.getLogger("patterns")


def anonymize_and_build_prompt(table: str, database: str, schema: str,
                               source_platform: str, source_project: str,
                               target_warehouse: Optional[str],
                               stats_data: Dict, period: str = 'weekly') -> Tuple[str, Dict[str, str]]:
    """
    Anonymize data and build AI prompt for a given period.
    
    Args:
        table: Table name
        database: Database name
        schema: Schema name
        source_platform: Source platform
        source_project: Source project
        target_warehouse: Target warehouse
        stats_data: Statistics data including time_stats, partition_stats, table_metadata
        period: 'weekly' or 'monthly'
        
    Returns:
        Tuple of (prompt, reverse_mapping)
    """
    time_stats = stats_data.get('weekly_stats') if period == 'weekly' else stats_data.get('monthly_stats')
    
    logger.info("Anonymizing data for AI processing (period: %s)", period)
    forward_map, reverse_map = privacy_service.build_complete_anonymization_map(
        table, database, schema, source_platform, source_project,
        stats_data['partition_stats'], stats_data['table_metadata']
    )
    
    table_anon = forward_map[table]
    database_anon = forward_map[database]
    schema_anon = forward_map[schema]
    platform_anon = forward_map[source_platform]
    project_anon = forward_map[source_project]
    
    partition_anon = privacy_service.anonymize_data_for_ai(
        table, stats_data['partition_stats'], time_stats, stats_data['table_metadata']
    )[1]
    
    table_metadata_anon = privacy_service.anonymize_table_metadata(
        stats_data['table_metadata'], reverse_map
    )
    
    prompt = ai_service.build_ai_prompt(
        table_anon, platform_anon, project_anon, target_warehouse,
        time_stats, partition_anon, table_metadata_anon,
        database_anon, schema_anon, period=period
    )
    
    return prompt, reverse_map


def query_ai_with_fallback(table: str, database: str, schema: str,
                          source_platform: str, source_project: str,
                          target_warehouse: Optional[str],
                          stats: Dict, gemini_config: Dict,
                          debug: bool = False) -> Tuple[str, Dict[str, str]]:
    """
    Query AI with weekly data, falling back to monthly if it fails.
    
    Args:
        table: Table name
        database: Database name  
        schema: Schema name
        source_platform: Source platform
        source_project: Source project
        target_warehouse: Target warehouse
        stats: Statistics data
        gemini_config: Gemini configuration
        debug: Enable debug logging
        
    Returns:
        Tuple of (ai_response, reverse_mapping)
        
    Raises:
        ValueError: If both weekly and monthly attempts fail
    """
    prompt, reverse_map = anonymize_and_build_prompt(
        table, database, schema, source_platform, source_project,
        target_warehouse, stats, period='weekly'
    )
    
    logger.info("Weekly prompt size: %d characters, %d weekly stats entries",
               len(prompt), len(stats.get('weekly_stats', [])))
    
    if debug:
        _log_debug_prompt(prompt, "Weekly")
    
    try:
        logger.info("Attempting AI query with weekly stats")
        ai_response = ai_service.query_gemini_ai(
            prompt,
            target_warehouse,
            gemini_config.get("api_key"),
            gemini_config.get("model", "gemini-2.5-flash")
        )
        
        if ai_response:
            logger.info("AI response received with weekly stats (%d chars)", len(ai_response))
            return ai_response, reverse_map
        else:
            logger.warning("AI response is empty or None")
            raise ValueError("AI returned empty response")
            
    except Exception as e:
        logger.warning("Weekly stats attempt failed: %s. Retrying with monthly stats (last 2 months)...", str(e))
        
        monthly_stats = stats.get('monthly_stats', [])
        if len(monthly_stats) > 2:
            stats['monthly_stats'] = monthly_stats[-2:]
            logger.info("Reduced monthly stats from %d to 2 months", len(monthly_stats))
        
        partition_stats = stats.get('partition_stats', [])
        if len(partition_stats) > 2:
            stats['partition_stats'] = partition_stats[-2:]
            logger.info("Reduced partition stats from %d to 2 entries", len(partition_stats))
        
        prompt_monthly, reverse_map = anonymize_and_build_prompt(
            table, database, schema, source_platform, source_project,
            target_warehouse, stats, period='monthly'
        )
        
        logger.info("Retrying with monthly stats. Prompt: %d chars (was %d), %d monthly stats entries",
                   len(prompt_monthly), len(prompt), len(stats.get('monthly_stats', [])))
        
        if debug:
            _log_debug_prompt(prompt_monthly, "Monthly (Retry)")
        
        ai_response = ai_service.query_gemini_ai(
            prompt_monthly,
            target_warehouse,
            gemini_config.get("api_key"),
            gemini_config.get("model", "gemini-2.5-flash")
        )
        
        if ai_response:
            logger.info("AI response received with monthly stats (%d chars)", len(ai_response))
            return ai_response, reverse_map
        else:
            logger.warning("AI response is empty or None after retry with monthly stats")
            raise ValueError("AI returned empty response after retry")


def _log_debug_prompt(prompt: str, label: str) -> None:
    """Log debug prompt information."""
    logger.info("=" * 80)
    logger.info("DEBUG MODE: %s Gemini AI Prompt", label)
    logger.info("=" * 80)
    logger.info("\n%s\n", prompt)
    logger.info("=" * 80)
    logger.info("Prompt length: %d characters", len(prompt))
    logger.info("=" * 80)

