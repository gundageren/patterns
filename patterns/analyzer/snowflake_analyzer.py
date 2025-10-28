import logging
from collections import defaultdict
from typing import List, Dict

from patterns.analyzer.base_analyzer import BaseAnalyzer

logger = logging.getLogger("patterns")


class SnowflakeAnalyzer(BaseAnalyzer):
    def recommend(self, column_filter_stats: Dict[str, int]) -> List[Dict]:
        filter_stats = self.get_column_filter_stats()
        if not filter_stats:
            logger.info("No filter stats found; no recommendations.")
            return []

        table_columns = defaultdict(lambda: defaultdict(int))
        for col, count in filter_stats.items():
            if "." in col:
                table, column = col.split(".", 1)
                table_columns[table][column] += count

        recommendations = []
        for table, cols in table_columns.items():
            sorted_cols = sorted(cols.items(), key=lambda x: x[1], reverse=True)
            if not sorted_cols:
                continue

            top_cols = [col for col, _ in sorted_cols[:3]]
            columns_list = ", ".join(top_cols)

            recommendations.append({
                "table": table,
                "recommendation": f"Consider defining CLUSTER BY keys on columns: {columns_list}",
                "reason": (
                    "These columns are frequently used in filters or joins. "
                    "Clustering improves pruning and query performance on large tables (>1TB). "
                    "Avoid clustering on columns with very high or very low cardinality or frequent updates."
                )
            })

        return recommendations
