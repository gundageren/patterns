"""Request validation utilities."""

from typing import Optional, Tuple, Dict
from .responses import json_response
from .constants import TOP_DATA_WAREHOUSES


def validate_required_params(params: Dict[str, Optional[str]], param_names: list) -> Tuple[bool, Optional[tuple]]:
    """
    Validate that all required parameters are present.
    
    Args:
        params: Dictionary of parameter name to value
        param_names: List of required parameter names
        
    Returns:
        Tuple of (is_valid, error_response)
        If valid: (True, None)
        If invalid: (False, json_response with error)
    """
    missing = [name for name in param_names if not params.get(name)]
    
    if missing:
        return False, json_response(
            False,
            error=f"Missing required parameters: {', '.join(missing)}",
            status_code=400
        )
    
    return True, None


def validate_table_params(source_platform: str, source_project: str, 
                         database: str, schema: str, table: str) -> Tuple[bool, Optional[tuple]]:
    """
    Validate standard table identification parameters.
    
    Returns:
        Tuple of (is_valid, error_response)
    """
    return validate_required_params(
        {
            'source_platform': source_platform,
            'source_project': source_project,
            'database': database,
            'schema': schema,
            'table': table
        },
        ['source_platform', 'source_project', 'database', 'schema', 'table']
    )


def validate_target_warehouse(target_warehouse: Optional[str]) -> Tuple[bool, Optional[tuple]]:
    """
    Validate target warehouse parameter if provided.
    
    Returns:
        Tuple of (is_valid, error_response)
    """
    if target_warehouse and target_warehouse not in TOP_DATA_WAREHOUSES:
        return False, json_response(
            False,
            error=f"Invalid target_warehouse. Must be one of: {', '.join(TOP_DATA_WAREHOUSES)}",
            status_code=400
        )
    
    return True, None


def validate_platform_configured(source_platform: Optional[str]) -> Tuple[bool, Optional[tuple]]:
    """
    Validate that platform is configured.
    
    Returns:
        Tuple of (is_valid, error_response)
    """
    if not source_platform:
        return False, json_response(
            False, 
            error="Platform not configured", 
            status_code=400
        )
    
    return True, None


def get_debug_flag(debug_param: str) -> bool:
    """Parse debug parameter from request args."""
    return debug_param.lower() in ('true', '1', 'yes') if debug_param else False

