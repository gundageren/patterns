"""Configuration access utilities."""

from typing import Optional, Dict, Any


def get_config_value(app_config: Dict, key: str, default=None) -> Any:
    """Get value from app config with optional default."""
    return app_config.get(key, default)


def get_source_platform(app_config: Dict) -> Optional[str]:
    """Get source platform from config."""
    return get_config_value(app_config, "source_platform")


def get_connection_config(app_config: Dict) -> Dict:
    """Get connection configuration."""
    return get_config_value(app_config, "connection", {})


def get_gemini_config(app_config: Dict) -> Dict:
    """Get Gemini API configuration."""
    return get_config_value(app_config, "gemini", {})

