"""Date and time utilities."""

from datetime import datetime, timedelta
from typing import Tuple, Optional


def get_default_date_range() -> Tuple[str, str]:
    """Returns (start_time, end_time) for last 30 days."""
    end = datetime.now().replace(microsecond=0)
    start = end - timedelta(days=30)
    return start.isoformat(), end.isoformat()


def parse_date_params(start: Optional[str], end: Optional[str]) -> Tuple[str, str]:
    """Parse date parameters with defaults."""
    if not start or not end:
        default_start, default_end = get_default_date_range()
        start = start or default_start
        end = end or default_end
    return start, end


def parse_datetime(dt_value) -> datetime:
    """Parse datetime from string or return datetime object."""
    if isinstance(dt_value, str):
        return datetime.fromisoformat(dt_value.replace('Z', '+00:00'))
    return dt_value


def get_week_start(dt: datetime) -> str:
    """Get Monday of the week for a datetime."""
    week_start = dt - timedelta(days=dt.weekday())
    return week_start.strftime('%Y-%m-%d')

