import logging
from abc import ABC, abstractmethod
from collections import Counter
from collections import defaultdict
from typing import List, Dict, Set, Optional

import sqlglot
from sqlglot.expressions import Expression
from sqlglot.expressions import Table, Star, CTE, Select, From, Alias, Subquery, Column, Join, Identifier, Merge
from sqlglot.optimizer import optimize

logger = logging.getLogger("patterns")
logging.getLogger("sqlglot").setLevel(logging.ERROR)


class BaseAnalyzer(ABC):
    def __init__(self, platform: str, storage):
        self.platform = platform
        self.storage = storage

    def get_column_filter_stats(self, source_platform: str, source_project: str, start_time: Optional[str] = None, end_time: Optional[str] = None):
        queries = self.storage.load_queries(source_platform=source_platform, source_project=source_project, start_time=start_time, end_time=end_time)
        counter = Counter()

        for q in queries:
            sql = q.get("query_text")
            if not sql:
                continue
            try:
                tree = sqlglot.parse_one(sql, read=self.platform)

                # Then, apply the optimizer to the parsed tree.
                # The 'optimize' function handles various optimizations, including qualify_tables.
                # If you have a schema, you can pass it like: optimize(tree, schema=your_schema)
                optimized_tree = optimize(tree)  # Apply optimization

                tables_in_query = self._extract_base_tables(optimized_tree)
                columns = self._extract_filter_columns(optimized_tree, tables_in_query)
                counter.update(columns)

            except Exception as e:
                logger.debug(f"Failed to parse or optimize query: {e} | Query: {sql}")
                continue
        return dict(counter)

    def _extract_base_tables(self, tree) -> Set[str]:
        """
        Extracts all unique base table names from the AST after optimization.
        'optimize' (which includes qualify_tables) should resolve aliases and CTEs
        to their underlying base tables.
        """
        tables = set()
        # Find all Table expressions in the optimized tree.
        # After optimization, `table.name` should generally represent the base table.
        for table_exp in tree.find_all(Table):
            # We want to ensure we're getting actual base tables, not just CTE aliases
            # that haven't been resolved to their source.
            # A common pattern is that `Table` expressions directly referencing a CTE
            # will still appear. We want the tables that the CTE *itself* reads from.

            # This check tries to filter out CTE names themselves when they are defined.
            # We are interested in tables referenced in SELECT, FROM, JOIN clauses, etc.
            # after the optimizer has done its work.
            if table_exp.this and table_exp.this.name:
                # Exclude CTEs defined in the 'WITH' clause itself, as they are not base tables.
                # The `optimize` function should ideally resolve references *to* CTEs
                # back to the base tables they query.
                if not (table_exp.parent and isinstance(table_exp.parent, sqlglot.exp.With)):
                    tables.add(table_exp.name.lower())

        return tables

    def _extract_filter_columns(self, tree, tables_in_query: Set[str]) -> List[str]:
        filter_columns = []

        # Iterate over all WHERE, ON (from JOINs), and HAVING clauses
        for clause in tree.find_all(sqlglot.exp.Where, sqlglot.exp.Join, sqlglot.exp.Having):
            condition_expression = None
            if isinstance(clause, sqlglot.exp.Where):
                condition_expression = clause.this
            elif isinstance(clause, sqlglot.exp.Join):
                condition_expression = clause.args.get("on")
            elif isinstance(clause, sqlglot.exp.Having):
                condition_expression = clause.this

            if condition_expression:
                for col in condition_expression.find_all(Column):
                    col_name = col.name
                    # After optimization, `col.table` should contain the resolved table name
                    # (either fully qualified or the base name).
                    table_name_for_col = col.table

                    if table_name_for_col:
                        # Ensure the table name is in our list of identified base tables
                        # (case-insensitive check)
                        if table_name_for_col.lower() in tables_in_query:
                            full_name = f"{table_name_for_col.lower()}.{col_name.lower()}"
                        else:
                            # If the table for the column isn't one of our identified base tables,
                            # it might be an alias or a CTE that wasn't fully resolved back to a base table,
                            # or perhaps a schema/db prefix that isn't in our simplified `tables_in_query`.
                            # For robustness, we can try to find a matching base table, or just use the column name.
                            # For this specific problem, let's prioritize getting the actual base table name.
                            # If `col.table` is not in `tables_in_query`, it's possible it's a qualified
                            # name that wasn't stripped down to just the table name.
                            # We'll just use the provided `col.table` as part of the full name if it exists.
                            full_name = f"{table_name_for_col.lower()}.{col_name.lower()}"
                    else:
                        # This scenario means the column's table wasn't resolved by SQLGlot.
                        # This might happen with extremely complex or malformed queries,
                        # or if SQLGlot's optimizer cannot fully determine the table.
                        # In such cases, we just report the column name.
                        full_name = col_name.lower()

                    filter_columns.append(full_name)
        return filter_columns



    def _resolve_table_info(self, table_expression):
        """
        Extracts catalog (outer DB), schema (middle), and table name from a sqlglot Table expression.
        Supports formats like: catalog.db.table, db.table, or just table.
        """
        catalog = table_expression.catalog  # e.g., PROD
        db = table_expression.db  # e.g., backend
        table_name = table_expression.this  # e.g., product_rate_plan_charge

        # Convert to string if it's an Identifier or Expression
        table_name = table_name.name if hasattr(table_name, "name") else str(table_name)
        db = db.name if hasattr(db, "name") else db
        catalog = catalog.name if hasattr(catalog, "name") else catalog

        return catalog, db, table_name

    def _find_select_star_in_expression(self, expression, cte_definitions=None):
        """
        Traverses an SQL expression to find direct SELECT * on tables.
        Returns a list of (database, schema, table_name) tuples.
        """
        if cte_definitions is None:
            cte_definitions = {}

        found_stars = defaultdict(int)

        # First pass to collect all CTE definitions for current scope
        for cte_exp in expression.find_all(CTE):
            cte_definitions[cte_exp.alias_or_name] = cte_exp.this

        # Now, traverse to find SELECT *
        for exp in expression.walk():
            if isinstance(exp, Select):
                has_star = False
                qualifier = None  # Stores the alias/table name if it's select alias.*

                for child in exp.expressions:
                    if isinstance(child, Star):
                        has_star = True
                        break  # Found a direct SELECT *
                    elif isinstance(child, Column) and isinstance(child.this, Star):
                        has_star = True
                        if child.expression and isinstance(child.expression, Identifier):
                            qualifier = child.expression.name  # Get the 'o' from o.*
                        break  # Found a SELECT alias.*

                if has_star:
                    # Find the FROM clause for this SELECT
                    from_exp = exp.find(From)
                    if not from_exp or not from_exp.this:
                        continue  # No FROM clause found for this SELECT with star

                    # Collect all direct table references in the FROM clause, handling joins
                    candidate_tables = {}  # alias_or_name: Table_expression

                    for from_child in from_exp.this.walk():
                        if isinstance(from_child, Table):
                            alias = from_child.alias_or_name
                            candidate_tables[alias] = from_child
                        elif isinstance(from_child, Alias) and isinstance(from_child.this, Table):
                            alias = from_child.alias_or_name
                            candidate_tables[alias] = from_child.this

                    for alias_or_name, table_expr in candidate_tables.items():
                        # If a specific alias was used (e.g., 'o' in 'o.*'), check if it matches
                        if qualifier and qualifier != alias_or_name:
                            continue  # This star is for a different alias/table

                        # If SELECT * is on a CTE, we ignore it as per requirements
                        if alias_or_name in cte_definitions:
                            continue

                        # If SELECT * is on a subquery result, we ignore it
                        if isinstance(table_expr,
                                      Subquery):  # This check is redundant if candidate_tables only contains Table expr
                            continue

                        # It's a direct SELECT * on an actual table (or alias of one)
                        db, schema, table_name = self._resolve_table_info(table_expr)
                        if table_name:
                            key = (db, schema, table_name)
                            found_stars[key] += 1
        return dict(found_stars)  # Convert defaultdict to dict

    def find_star_queries(self, source_platform: str, source_project: str, start_time: Optional[str] = None, end_time: Optional[str] = None):
        queries = self.storage.load_queries(source_platform=source_platform, source_project=source_project, start_time=start_time, end_time=end_time)
        results = []

        for q in queries:
            sql = q.get("query_text")
            query_id = q.get("query_id")
            start_time = q.get("start_time")
            source_platform = q.get("source_platform")
            source_project = q.get("source_project")

            base_meta = {
                "query": sql,
                "query_id": query_id,
                "start_time": start_time,
                "source_platform": source_platform,
                "source_project": source_project,
            }

            try:
                parsed_expression = sqlglot.parse_one(sql, read=source_platform)

                star_occurrences = self._find_select_star_in_expression(parsed_expression)

                if star_occurrences:  # Only add if we found any SELECT * on tables
                    for (db, schema, table), count in star_occurrences.items():
                        results.append({
                            "query": sql,
                            "query_id": query_id,
                            "source_platform": source_platform,
                            "source_project": source_project,
                            "start_time": start_time,
                            "database": db,
                            "schema": schema,
                            "table": table,
                            "count": count
                        })
                elif not star_occurrences and parsed_expression is not None:
                    # If no stars found, but query was valid, ensure it's not an error entry
                    pass  # Do nothing, this query simply doesn't match the criteria

            except Exception as e:
                # If parsing fails or any other unexpected error occurs
                results.append({
                    "query": sql,
                    "query_id": query_id,
                    "source_platform": source_platform,
                    "source_project": source_project,
                    "start_time": start_time,
                    "error": str(e)
                })
        return results



    def _find_read_tables_in_expression(self, expression, cte_definitions=None):
        """
        Finds all physical tables that are read from (in SELECTs, JOINs, subqueries, etc.).
        Includes tables inside CTE definitions, but not CTE aliases or write targets.
        Returns: dict of (db, schema, table_name) → count
        """
        if cte_definitions is None:
            cte_definitions = {}

        found_tables = defaultdict(int)

        # Step 1: Handle CTEs and recursively process them
        for cte in expression.find_all(CTE):
            cte_definitions[cte.alias_or_name] = cte.this
            cte_inner_tables = self._find_read_tables_in_expression(cte.this, cte_definitions)
            for key, count in cte_inner_tables.items():
                found_tables[key] += count

        # Helper to extract real table references from FROM / JOIN / USING
        def extract_tables_from_clause(clause):
            for subexp in clause.walk():
                if isinstance(subexp, Table):
                    alias = subexp.alias_or_name
                    if alias in cte_definitions:
                        continue  # skip references to CTEs
                    db, schema, table_name = self._resolve_table_info(subexp)
                    if table_name:
                        found_tables[(db, schema, table_name)] += 1

        # Step 2: Walk rest of the expression
        for exp in expression.walk():
            if isinstance(exp, (Select, Subquery)):
                from_clause = exp.args.get("from")
                if from_clause:
                    extract_tables_from_clause(from_clause.this)

            elif isinstance(exp, Join):
                extract_tables_from_clause(exp.this)

            elif isinstance(exp, Merge):
                using_clause = exp.args.get("using")
                if using_clause:
                    extract_tables_from_clause(using_clause)

            elif isinstance(exp, Expression):
                sub = exp.args.get("this")
                if isinstance(sub, Subquery):
                    extract_tables_from_clause(sub)

        return dict(found_tables)

    def find_read_table_queries(self, source_platform: str, source_project: str, start_time: Optional[str] = None, end_time: Optional[str] = None):
        """
        Loads queries and extracts read table metadata from each.
        Returns a list of dictionaries with query metadata and table access info.
        """
        queries = self.storage.load_queries(source_platform=source_platform, source_project=source_project, start_time=start_time, end_time=end_time)
        results = []

        for q in queries:
            sql = q.get("query_text")
            query_id = q.get("query_id")
            start_time = q.get("start_time")
            source_platform = q.get("source_platform")
            source_project = q.get("source_project")

            base_meta = {
                "query": sql,
                "query_id": query_id,
                "start_time": start_time,
                "source_platform": source_platform,
                "source_project": source_project,
            }

            try:
                parsed_expression = sqlglot.parse_one(sql, read=source_platform)
                read_tables = self._find_read_tables_in_expression(parsed_expression)

                for (db, schema, table), count in read_tables.items():
                    results.append({
                        **base_meta,
                        "database": db,
                        "schema": schema,
                        "table": table,
                        "count": count
                    })

            except Exception as e:
                results.append({
                    **base_meta,
                    "error": str(e)
                })

        return results

    def extract_partition_cluster_candidates(self, source_platform: str, source_project: str, start_time: Optional[str] = None, end_time: Optional[str] = None):
        """
        For each query, identify potential partition, cluster, and sort key candidates.
        Returns a list of dicts:
        {
            query_id, start_time, source_platform, source_project,
            database, schema, table,
            filter_type, column, count
        }
        filter_type: 'WHERE', 'JOIN', 'ORDER_BY', 'GROUP_BY'
        """
        queries = self.storage.load_queries(source_platform=source_platform, source_project=source_project, start_time=start_time, end_time=end_time)
        results = []

        for q in queries:
            sql = q.get("query_text")
            query_id = q.get("query_id")
            start_time = q.get("start_time")
            source_platform = q.get("source_platform")
            source_project = q.get("source_project")

            try:
                tree = sqlglot.parse_one(sql, read=source_platform)

                read_tables = self._find_read_tables_in_expression(tree)
                alias_map = {}

                for table_expr in tree.find_all(sqlglot.exp.Table):
                    alias = table_expr.alias_or_name
                    db, schema, table_name = self._resolve_table_info(table_expr)
                    alias_map[alias] = (db, schema, table_name)
                    # Also map the bare table name (no alias) to itself
                    alias_map[table_name] = (db, schema, table_name)

                # Store counts: (db, schema, table, filter_type, column) -> count
                filter_counts = defaultdict(int)

                # WHERE
                for where_exp in tree.find_all(sqlglot.exp.Where):
                    for col in where_exp.this.find_all(Column):
                        table_info = alias_map.get(col.table, (None, None, col.table))
                        filter_counts[(*table_info, "WHERE", col.name)] += 1

                # JOIN
                for join_exp in tree.find_all(sqlglot.exp.Join):
                    on_exp = join_exp.args.get("on")
                    if on_exp:
                        for col in on_exp.find_all(Column):
                            table_info = alias_map.get(col.table, (None, None, col.table))
                            filter_counts[(*table_info, "JOIN", col.name)] += 1

                # ORDER BY
                for order_exp in tree.find_all(sqlglot.exp.Order):
                    for col in order_exp.find_all(Column):
                        table_info = alias_map.get(col.table, (None, None, col.table))
                        filter_counts[(*table_info, "ORDER_BY", col.name)] += 1

                # GROUP BY
                for group_exp in tree.find_all(sqlglot.exp.Group):
                    for col in group_exp.find_all(Column):
                        table_info = alias_map.get(col.table, (None, None, col.table))
                        filter_counts[(*table_info, "GROUP_BY", col.name)] += 1

                # Emit results
                for (db, schema, table, filter_type, column), count in filter_counts.items():
                    results.append({
                        "query": sql,
                        "query_id": query_id,
                        "start_time": start_time,
                        "source_platform": source_platform,
                        "source_project": source_project,
                        "database": db,
                        "schema": schema,
                        "table": table,
                        "filter_type": filter_type,
                        "column": column,
                        "count": count
                    })

            except Exception as e:
                results.append({
                    "query": sql,
                    "query_id": query_id,
                    "start_time": start_time,
                    "source_platform": source_platform,
                    "source_project": source_project,
                    "error": str(e)
                })

        return results

    @abstractmethod
    def recommend(self, column_filter_stats: Dict[str, int]) -> List[Dict]:
        """
        Given column filter stats, return platform-specific optimization suggestions.

        Each dict contains:
        - table: str
        - recommendation: str
        - reason: str
        """
        pass