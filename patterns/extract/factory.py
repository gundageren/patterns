from typing import Optional, Dict, Any
from patterns.extract.base_extractor import BaseExtractor

def get_extractor(platform: str, connection: Optional[Dict[str, Any]] = None) -> BaseExtractor:
    platform = platform.lower()

    if platform == "snowflake":
        from patterns.extract.snowflake_extractor import SnowflakeExtractor
        return SnowflakeExtractor(connection)

    elif platform == "bigquery":
        from patterns.extract.bigquery_extractor import BigQueryExtractor
        return BigQueryExtractor(connection)

    else:
        raise ValueError(f"Unsupported platform: {platform}")
