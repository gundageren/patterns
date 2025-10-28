"""Response formatting utilities."""

from flask import jsonify
from typing import Dict, Optional


def json_response(success: bool = True, message: str = None,
                 data: Dict = None, error: str = None, status_code: int = 200):
    """Create standardized JSON response."""
    response = {"success": success}
    if message:
        response["message"] = message
    if error:
        response["error"] = error
    if data:
        response.update(data)
    return jsonify(response), status_code

