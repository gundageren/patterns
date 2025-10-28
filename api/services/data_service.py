"""Data loading, filtering, and statistics calculation service."""

import logging
from typing import List, Dict, Tuple, Optional, Literal
from collections import defaultdict
from ..utils.dates import parse_datetime, get_week_start

logger = logging.getLogger("patterns")


def filter_tables_by_criteria(tables: List[Dict], source_platform: str = None,
                              source_project: str = None, database: str = None,
                              schema: str = None) -> List[Dict]:
    """Filter tables by given criteria with case-insensitive matching."""
    filters = {
        'source_platform': source_platform,
        'source_project': source_project,
        'database': database,
        'schema': schema
    }
    filtered = tables
    for key, value in filters.items():
        if value:
            value_lower = value.lower() if isinstance(value, str) else value
            filtered = [t for t in filtered if
                       ((t.get(key) or '').lower() if isinstance(t.get(key), str) else t.get(key)) == value_lower]
    return filtered


def load_and_filter_query_data(storage, source_platform: str, source_project: str,
                               table: str, start: str, end: str) -> Dict[str, List[Dict]]:
    """
    Load and filter query data for a specific table.
    Returns dict with keys: 'read_queries', 'star_queries', 'partition_candidates'
    """
    logger.info("Loading query data for table: %s", table)
    
    read_queries = storage.load_read_table_queries(source_platform, source_project, start, end)
    star_queries = storage.load_select_star_queries(source_platform, source_project, start, end)
    partition_candidates = storage.load_partition_candidates(source_platform, source_project, start, end)
    
    table_lower = table.lower() if table else None
    filtered_read = [q for q in read_queries if (q.get('table') or '').lower() == table_lower]
    filtered_star = [q for q in star_queries if (q.get('table') or '').lower() == table_lower]
    filtered_parts = [p for p in partition_candidates if (p.get('table') or '').lower() == table_lower]
    
    logger.info("Filtered data: %d read queries, %d star queries, %d partition candidates",
                len(filtered_read), len(filtered_star), len(filtered_parts))
    
    return {
        'read_queries': filtered_read,
        'star_queries': filtered_star,
        'partition_candidates': filtered_parts
    }


def calculate_weekly_stats(read_queries: List[Dict],
                          star_queries: List[Dict]) -> List[Dict]:
    """Calculate weekly query statistics."""
    stats = defaultdict(lambda: {"total_queries": 0, "star_queries": 0})
    
    for query_list, stat_key in [(read_queries, "total_queries"),
                                  (star_queries, "star_queries")]:
        for q in query_list:
            if start_time := q.get('start_time'):
                week = get_week_start(parse_datetime(start_time))
                stats[week][stat_key] += q.get('count', 1)
    
    return [
        {"week_start": week, **values}
        for week, values in sorted(stats.items())
    ]


def calculate_weekly_partition_stats(candidates: List[Dict]) -> List[Dict]:
    """Calculate weekly partition candidate statistics by column and filter type."""
    stats = defaultdict(lambda: defaultdict(lambda: defaultdict(int)))
    
    for candidate in candidates:
        start_time = candidate.get('start_time')
        column = candidate.get('column')
        filter_type = candidate.get('filter_type')
        count = candidate.get('count', 0)
        
        if all([start_time, column, filter_type]):
            week = get_week_start(parse_datetime(start_time))
            stats[week][column][filter_type] += count
    
    result = []
    for week in sorted(stats.keys()):
        week_data = {
            "week_start": week,
            "columns": [
                {
                    "column": col,
                    "filter_types": [
                        {"filter_type": ft, "total_count": count}
                        for ft, count in sorted(col_stats.items())
                    ]
                }
                for col, col_stats in sorted(stats[week].items())
            ]
        }
        result.append(week_data)
    
    return result


def get_month_start(dt) -> str:
    """Get the start of the month for a given datetime (YYYY-MM format)."""
    return dt.strftime('%Y-%m')


def calculate_monthly_stats(read_queries: List[Dict],
                           star_queries: List[Dict]) -> List[Dict]:
    """Calculate monthly query statistics."""
    stats = defaultdict(lambda: {"total_queries": 0, "star_queries": 0})
    
    for query_list, stat_key in [(read_queries, "total_queries"),
                                  (star_queries, "star_queries")]:
        for q in query_list:
            if start_time := q.get('start_time'):
                month = get_month_start(parse_datetime(start_time))
                stats[month][stat_key] += q.get('count', 1)
    
    return [
        {"month_start": month, **values}
        for month, values in sorted(stats.items())
    ]


def calculate_monthly_partition_stats(candidates: List[Dict]) -> List[Dict]:
    """Calculate monthly partition candidate statistics by column and filter type."""
    stats = defaultdict(lambda: defaultdict(lambda: defaultdict(int)))
    
    for candidate in candidates:
        start_time = candidate.get('start_time')
        column = candidate.get('column')
        filter_type = candidate.get('filter_type')
        count = candidate.get('count', 0)
        
        if all([start_time, column, filter_type]):
            month = get_month_start(parse_datetime(start_time))
            stats[month][column][filter_type] += count
    
    result = []
    for month in sorted(stats.keys()):
        month_data = {
            "month_start": month,
            "columns": [
                {
                    "column": col,
                    "filter_types": [
                        {"filter_type": ft, "total_count": count}
                        for ft, count in sorted(col_stats.items())
                    ]
                }
                for col, col_stats in sorted(stats[month].items())
            ]
        }
        result.append(month_data)
    
    return result


def find_table_metadata(storage, source_platform: str, source_project: str,
                       database: str, schema: str, table: str) -> Optional[Dict]:
    """
    Find metadata for a specific table with case-insensitive matching.
    Returns table metadata dict or None if not found.
    """
    all_tables = storage.load_tables()
    
    # Prepare lowercase versions for comparison
    platform_lower = source_platform.lower() if source_platform else None
    project_lower = source_project.lower() if source_project else None
    database_lower = database.lower() if database else None
    schema_lower = schema.lower() if schema else None
    table_lower = table.lower() if table else None
    
    for tbl in all_tables:
        if ((tbl.get('source_platform') or '').lower() == platform_lower and
            (tbl.get('source_project') or '').lower() == project_lower and
            (tbl.get('database') or '').lower() == database_lower and
            (tbl.get('schema') or '').lower() == schema_lower and
            (tbl.get('table') or '').lower() == table_lower):
            return {
                'size_bytes': tbl.get('size_bytes', 'Unknown'),
                'row_count': tbl.get('row_count', 'Unknown'),
                'columns': tbl.get('columns', [])
            }
    
    logger.warning("Table metadata not found for: %s.%s.%s", database, schema, table)
    return None


def get_table_stats(storage, source_platform: str, source_project: str,
                   database: str, schema: str, table: str,
                   start: str, end: str, period: Literal['weekly', 'monthly'] = 'weekly') -> Dict:
    """
    Get complete statistics for a table (weekly or monthly stats and partition stats).
    This is the common logic used by both table_weekly_stats and find_patterns endpoints.
    
    Args:
        period: 'weekly' for weekly aggregation, 'monthly' for monthly aggregation
    """
    query_data = load_and_filter_query_data(storage, source_platform, source_project, table, start, end)
    
    if period == 'monthly':
        time_stats = calculate_monthly_stats(query_data['read_queries'], query_data['star_queries'])
        partition_stats = calculate_monthly_partition_stats(query_data['partition_candidates'])
        period_key = 'monthly_stats'
        period_count_key = 'total_months'
    else:
        time_stats = calculate_weekly_stats(query_data['read_queries'], query_data['star_queries'])
        partition_stats = calculate_weekly_partition_stats(query_data['partition_candidates'])
        period_key = 'weekly_stats'
        period_count_key = 'total_weeks'
    
    table_metadata = find_table_metadata(storage, source_platform, source_project, database, schema, table)
    
    return {
        period_key: time_stats,
        'partition_stats': partition_stats,
        'table_metadata': table_metadata,
        'period': period,
        'summary': {
            period_count_key: len(time_stats),
            'total_queries': sum(s["total_queries"] for s in time_stats),
            'total_star_queries': sum(s["star_queries"] for s in time_stats)
        }
    }

