import logging
from collections import defaultdict
from typing import List, Dict

from patterns.analyzer.base_analyzer import BaseAnalyzer

logger = logging.getLogger("patterns")


class BigQueryAnalyzer(BaseAnalyzer):
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

            partition_col = sorted_cols[0][0]
            cluster_cols = [col for col, _ in sorted_cols[1:4]]

            recommendations.append({
                "table": table,
                "recommendation": f"Consider PARTITION BY {partition_col}",
                "reason": (
                    "Partitioning the table on this frequently filtered column "
                    "can significantly reduce data scanned and query costs."
                )
            })

            if cluster_cols:
                recommendations.append({
                    "table": table,
                    "recommendation": f"Consider CLUSTER BY {', '.join(cluster_cols)}",
                    "reason": (
                        "Clustering by these additional frequently filtered columns "
                        "improves query performance by optimizing data locality within partitions."
                    )
                })

        return recommendations
