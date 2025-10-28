"""Flask decorators for authentication and error handling."""

import logging
from functools import wraps
from .responses import json_response

logger = logging.getLogger("patterns")


def require_app_initialized(app_config, app_storage):
    """Decorator factory to check if app is properly initialized."""
    def decorator(f):
        @wraps(f)
        def decorated(*args, **kwargs):
            if not app_config or not app_storage:
                return json_response(False, error="Application not initialized", status_code=500)
            return f(*args, **kwargs)
        return decorated
    return decorator


def handle_exceptions(f):
    """Handle exceptions uniformly across endpoints."""
    @wraps(f)
    def decorated(*args, **kwargs):
        try:
            return f(*args, **kwargs)
        except Exception as e:
            logger.error("Error in %s: %s", f.__name__, e, exc_info=True)
            return json_response(False, error=str(e), status_code=500)
    return decorated

