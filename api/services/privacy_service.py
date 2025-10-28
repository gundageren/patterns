"""Data anonymization and privacy protection service."""

import json
import re
import hashlib
import logging
from typing import List, Dict, Tuple, Optional

logger = logging.getLogger("patterns")


def anonymize_data_for_ai(table: str, partition_stats: List[Dict],
                         time_data: List[Dict], table_metadata: Optional[Dict] = None) -> Tuple[str, List[Dict], List[Dict], Dict[str, str]]:
    """
    Anonymize table and column names before sending to AI using unique identifiers.
    Uses hash-based approach to ensure anonymized names won't appear naturally in prompts or responses.
    
    Args:
        time_data: List of time-based stats (can be weekly with 'week_start' or monthly with 'month_start')
    
    Returns: (anonymized_table, anonymized_partition_stats, anonymized_time_data, reverse_mapping)
    """
    # Generate unique anonymized table name
    table_hash = hashlib.sha256(table.encode()).hexdigest()[:8].upper()
    anon_table = f"__TBL_{table_hash}__"
    reverse_mapping = {anon_table: table}
    
    # Collect unique column names from partition stats
    columns_from_stats = set()
    for period_stat in partition_stats:
        for col_data in period_stat.get("columns", []):
            col_name = col_data.get('column')
            if col_name:
                columns_from_stats.add(col_name)
    
    # Collect ALL column names from table metadata
    columns_from_metadata = set()
    if table_metadata:
        columns_raw = table_metadata.get('columns') or []
        # Handle both JSON string and already-parsed list/dict formats
        if isinstance(columns_raw, str):
            try:
                metadata_columns = json.loads(columns_raw) if columns_raw else []
            except (json.JSONDecodeError, TypeError) as e:
                logger.warning("Failed to parse columns JSON: %s", e)
                metadata_columns = []
        else:
            metadata_columns = columns_raw if columns_raw else []
        
        logger.debug("Raw columns type: %s, value: %s", type(columns_raw), columns_raw if isinstance(columns_raw, (list, dict)) else "STRING")
        
        for col in metadata_columns:
            if isinstance(col, dict):
                col_name = col.get('name', '')
                if col_name:
                    columns_from_metadata.add(col_name)
            elif col:  # If it's a string
                columns_from_metadata.add(str(col))
    
    # Combine all unique column names
    all_columns = columns_from_stats | columns_from_metadata
    
    # Log column collection for debugging
    logger.debug("Columns from partition stats: %d", len(columns_from_stats))
    logger.debug("Columns from metadata: %d", len(columns_from_metadata))
    logger.debug("Total unique columns: %d", len(all_columns))
    
    if columns_from_stats - columns_from_metadata:
        logger.debug("Columns in stats but not in metadata: %s", columns_from_stats - columns_from_metadata)
    if columns_from_metadata - columns_from_stats:
        logger.debug("Columns in metadata but not in stats: %s", columns_from_metadata - columns_from_stats)
    
    for col in sorted(all_columns):
        col_hash = hashlib.sha256(col.encode()).hexdigest()[:8].upper()
        anon_col = f"__COL_{col_hash}__"
        reverse_mapping[anon_col] = col
    
    name_mapping = {v: k for k, v in reverse_mapping.items()}
    
    anonymized_stats = []
    for period_stat in partition_stats:
        time_key = 'month_start' if 'month_start' in period_stat else 'week_start'
        
        anonymized_stat = {
            time_key: period_stat[time_key],
            "columns": [
                {
                    "column": name_mapping.get(col['column'], col['column']),
                    "filter_types": col.get("filter_types", [])
                }
                for col in period_stat.get("columns", [])
            ]
        }
        anonymized_stats.append(anonymized_stat)
    
    logger.info("Anonymized %d column names and 1 table name using hash-based identifiers", len(all_columns))
    return anon_table, anonymized_stats, time_data, reverse_mapping


def anonymize_table_metadata(table_metadata: Optional[Dict],
                             reverse_map: Dict[str, str]) -> Optional[Dict]:
    """Anonymize column names in table metadata."""
    if not table_metadata:
        return None
    
    name_mapping = {v: k for k, v in reverse_map.items()}
    
    table_metadata_anon = {
        'size_bytes': table_metadata['size_bytes'],
        'row_count': table_metadata['row_count'],
        'columns': []
    }
    
    columns_raw = table_metadata.get('columns') or []
    # Handle both JSON string and already-parsed list/dict formats
    if isinstance(columns_raw, str):
        try:
            columns = json.loads(columns_raw) if columns_raw else []
        except (json.JSONDecodeError, TypeError) as e:
            logger.warning("Failed to parse columns JSON in anonymize_table_metadata: %s", e)
            columns = []
    else:
        columns = columns_raw if columns_raw else []
    
    for col in columns:
        # Handle both string and dict column formats
        if isinstance(col, str):
            col_name = col
            col_type = 'unknown'
        elif isinstance(col, dict):
            col_name = col.get('name', '')
            col_type = col.get('type', 'unknown')
        else:
            continue
        
        # Use mapping if column was anonymized, otherwise use original name
        anon_name = name_mapping.get(col_name, col_name)
        table_metadata_anon['columns'].append({
            'name': anon_name,
            'type': col_type
        })
    
    return table_metadata_anon


def anonymize_entity(entity_name: str, prefix: str) -> Tuple[str, str]:
    """
    Anonymize a single entity (database, schema, platform, project) using hash.
    
    Args:
        entity_name: Original name to anonymize
        prefix: Prefix for anonymized name (e.g., 'DB', 'SCH', 'PLATFORM', 'PROJECT')
        
    Returns:
        Tuple of (anonymized_name, original_name)
    """
    entity_hash = hashlib.sha256(entity_name.encode()).hexdigest()[:8].upper()
    return f"__{prefix}_{entity_hash}__", entity_name


def anonymize_entities(database: str, schema: str, source_platform: str, 
                       source_project: str) -> Dict[str, str]:
    """
    Anonymize database, schema, platform, and project names.
    
    Returns:
        Dictionary mapping anonymized names to original names
    """
    entities = [
        (database, 'DB'),
        (schema, 'SCH'),
        (source_platform, 'PLATFORM'),
        (source_project, 'PROJECT')
    ]
    
    mapping = {}
    for entity, prefix in entities:
        anon_name, orig_name = anonymize_entity(entity, prefix)
        mapping[anon_name] = orig_name
    
    return mapping


def build_complete_anonymization_map(table: str, database: str, schema: str, 
                                     source_platform: str, source_project: str,
                                     partition_stats: List[Dict], 
                                     table_metadata: Optional[Dict]) -> Tuple[Dict[str, str], Dict[str, str]]:
    """
    Build complete anonymization mapping for all entities (table, columns, database, etc.).
    
    Returns:
        Tuple of (forward_mapping, reverse_mapping)
        forward_mapping: original -> anonymized
        reverse_mapping: anonymized -> original
    """
    anon_table, _, _, reverse_map = anonymize_data_for_ai(
        table, partition_stats, [], table_metadata
    )
    
    entity_map = anonymize_entities(database, schema, source_platform, source_project)
    reverse_map.update(entity_map)
    
    forward_map = {v: k for k, v in reverse_map.items()}
    
    logger.info("Built complete anonymization map with %d entities", len(reverse_map))
    return forward_map, reverse_map


def restore_names_in_response(ai_response: str, reverse_mapping: Dict[str, str]) -> str:
    """Restore original table and column names in AI response."""
    restored = ai_response
    
    # Sort by length (longest first) to avoid partial replacements
    for anon_name, orig_name in sorted(reverse_mapping.items(), key=lambda x: len(x[0]), reverse=True):
        patterns = [
            (rf'\b{re.escape(anon_name)}\b', orig_name),
            (rf'`{re.escape(anon_name)}`', f'`{orig_name}`'),
            (rf'"{re.escape(anon_name)}"', f'"{orig_name}"'),
            (rf"'{re.escape(anon_name)}'", f"'{orig_name}'"),
            (rf'(ON|BY|FROM|INTO|TABLE)\s+{re.escape(anon_name)}\b', rf'\1 {orig_name}'),
        ]
        for pattern, replacement in patterns:
            restored = re.sub(pattern, replacement, restored, flags=re.IGNORECASE)
    
    logger.info("Restored %d original names in AI response", len(reverse_mapping))
    return restored

