"""Response building utilities to reduce repetition."""

import json
import logging
from typing import Dict, Optional, List

logger = logging.getLogger("patterns")


def build_table_info_response(source_platform: str, source_project: str,
                              database: str, schema: str, table: str,
                              target_warehouse: Optional[str] = None,
                              table_metadata: Optional[Dict] = None) -> Dict:
    """
    Build standardized table info response.
    
    Args:
        source_platform: Source platform name
        source_project: Source project name
        database: Database name
        schema: Schema name
        table: Table name
        target_warehouse: Target warehouse (optional)
        table_metadata: Table metadata with size, rows, columns (optional)
        
    Returns:
        Formatted table info dictionary
    """
    table_info = {
        "source_platform": source_platform,
        "source_project": source_project,
        "database": database,
        "schema": schema,
        "table": table
    }
    
    if target_warehouse:
        table_info["target_warehouse"] = target_warehouse
    
    if table_metadata:
        # Parse columns properly - handle both JSON string and list formats
        columns_raw = table_metadata.get('columns') or []
        if isinstance(columns_raw, str):
            try:
                columns = json.loads(columns_raw) if columns_raw else []
            except (json.JSONDecodeError, TypeError) as e:
                logger.warning("Failed to parse columns JSON in build_table_info_response: %s", e)
                columns = []
        else:
            columns = columns_raw if columns_raw else []
        
        table_info.update({
            "size_bytes": table_metadata.get('size_bytes', 'Unknown'),
            "row_count": table_metadata.get('row_count', 'Unknown'),
            "column_count": len(columns),
            "columns": columns
        })
    
    return table_info


def build_analysis_data(analysis_completed: bool, 
                       source_project: Optional[str] = None,
                       analysis_results: Optional[Dict] = None,
                       reason: Optional[str] = None) -> Dict:
    """
    Build standardized analysis data response.
    
    Args:
        analysis_completed: Whether analysis was completed
        source_project: Source project name (if completed)
        analysis_results: Results dictionary (if completed)
        reason: Reason if not completed
        
    Returns:
        Formatted analysis data dictionary
    """
    if analysis_completed:
        return {
            "analysis_completed": True,
            "source_project": source_project,
            "analysis_results": analysis_results
        }
    else:
        return {
            "analysis_completed": False,
            "reason": reason or "Analysis not requested"
        }


def build_refresh_response(start_date: str, end_date: str,
                          queries_count: Optional[int] = None,
                          tables_count: Optional[int] = None,
                          analysis_data: Optional[Dict] = None) -> Dict:
    """
    Build standardized data refresh response.
    
    Returns:
        Formatted refresh data dictionary
    """
    data = {
        "start_date": start_date,
        "end_date": end_date
    }
    
    if queries_count is not None:
        data["queries_extracted"] = queries_count
    
    if tables_count is not None:
        data["tables_extracted"] = tables_count
    
    if analysis_data:
        data["analysis"] = analysis_data
    
    return data

