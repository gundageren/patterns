"""Data extraction and refresh service."""

import logging
from typing import Dict, List, Tuple, Any
from patterns.extract.factory import get_extractor
from patterns.analyzer.factory import get_analyzer

logger = logging.getLogger("patterns")


def refresh_query_history(source_platform: str, connection: Dict, storage: Any,
                          start_time: str, end_time: str) -> List[Dict]:
    """Extract and store query history."""
    logger.info("Extracting query history from %s (%s to %s)", source_platform, start_time, end_time)
    
    extractor = get_extractor(platform=source_platform, connection=connection)
    queries = extractor.extract_query_history(start_time=start_time, end_time=end_time)
    storage.save_queries(queries)
    
    logger.info("Extracted and stored %d queries", len(queries))
    return queries


def refresh_tables(source_platform: str, connection: Dict, storage: Any) -> List[Dict]:
    """Extract and store table list."""
    logger.info("Extracting tables from %s", source_platform)
    
    extractor = get_extractor(platform=source_platform, connection=connection)
    tables = extractor.extract_tables()
    storage.save_tables(tables)
    
    logger.info("Extracted and stored %d tables", len(tables))
    return tables


def refresh_all_data(source_platform: str, connection: Dict, storage: Any,
                    start_time: str, end_time: str) -> Tuple[List, List]:
    """Extract and store both query history and tables."""
    queries = refresh_query_history(source_platform, connection, storage, start_time, end_time)
    tables = refresh_tables(source_platform, connection, storage)
    
    return queries, tables


def run_analysis(source_platform: str, source_project: str, storage: Any,
                start_time: str, end_time: str) -> Dict[str, int]:
    """Run complete analysis pipeline and return counts."""
    logger.info("Running analysis for %s/%s", source_platform, source_project)
    
    analyzer = get_analyzer(platform=source_platform, storage=storage)
    
    analyses = [
        ("read_table_queries", analyzer.find_read_table_queries, storage.save_read_table_queries),
        ("select_star_queries", analyzer.find_star_queries, storage.save_select_star_queries),
        ("partition_candidates", analyzer.extract_partition_cluster_candidates, storage.save_partition_candidates)
    ]
    
    results = {}
    for name, analyze_fn, save_fn in analyses:
        data = analyze_fn(
            source_platform=source_platform,
            source_project=source_project,
            start_time=start_time,
            end_time=end_time
        )
        save_fn(data)
        results[name] = len(data)
        logger.info("Completed %s: %d records", name, len(data))
    
    return results

