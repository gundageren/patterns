from patterns.analyzer.base_analyzer import BaseAnalyzer
from patterns.analyzer.bigquery_analyzer import BigQueryAnalyzer
from patterns.analyzer.snowflake_analyzer import SnowflakeAnalyzer

def get_analyzer(platform: str, storage) -> BaseAnalyzer:
    platform = platform.lower()

    if platform == "snowflake":
        return SnowflakeAnalyzer(platform=platform, storage=storage)
    elif platform == "bigquery":
        return BigQueryAnalyzer(platform=platform, storage=storage)

    raise ValueError(f"No analyzer available for platform '{platform}'")
