"""Tests for privacy service."""

import pytest
from api.services import privacy_service


class TestAnonymizeDataForAI:
    """Tests for anonymize_data_for_ai function."""
    
    def test_basic_anonymization(self):
        """Test basic anonymization of table and columns."""
        table = 'users'
        partition_stats = [
            {
                'week_start': '2024-01-01',
                'columns': [
                    {'column': 'user_id', 'filter_types': []},
                    {'column': 'created_at', 'filter_types': []}
                ]
            }
        ]
        time_data = [{'week_start': '2024-01-01', 'total_queries': 100}]
        table_metadata = {
            'size_bytes': 1024,
            'row_count': 100,
            'columns': [
                {'name': 'user_id', 'type': 'INTEGER'},
                {'name': 'created_at', 'type': 'TIMESTAMP'}
            ]
        }
        
        anon_table, anon_partition, anon_time, reverse_map = privacy_service.anonymize_data_for_ai(
            table, partition_stats, time_data, table_metadata
        )
        
        # Check that table is anonymized
        assert anon_table != table
        assert anon_table.startswith('__TBL_')
        assert reverse_map[anon_table] == table
        
        # Check that columns are anonymized
        assert 'user_id' in reverse_map.values()
        assert 'created_at' in reverse_map.values()
        
        # Check that partition stats are anonymized
        assert len(anon_partition) == 1
        assert anon_partition[0]['week_start'] == '2024-01-01'
    
    def test_monthly_period_anonymization(self):
        """Test anonymization with monthly data."""
        table = 'orders'
        partition_stats = [
            {
                'month_start': '2024-01',
                'columns': [
                    {'column': 'order_date', 'filter_types': []}
                ]
            }
        ]
        time_data = [{'month_start': '2024-01', 'total_queries': 50}]
        
        anon_table, anon_partition, anon_time, reverse_map = privacy_service.anonymize_data_for_ai(
            table, partition_stats, time_data, None
        )
        
        # Check monthly period is preserved
        assert anon_partition[0]['month_start'] == '2024-01'
        assert 'order_date' in reverse_map.values()


class TestAnonymizeEntity:
    """Tests for anonymize_entity function."""
    
    def test_anonymize_single_entity(self):
        """Test anonymizing a single entity."""
        anon_name, orig_name = privacy_service.anonymize_entity('my_database', 'DB')
        
        assert anon_name.startswith('__DB_')
        assert anon_name.endswith('__')
        assert orig_name == 'my_database'
    
    def test_different_prefixes(self):
        """Test that different prefixes produce different anonymized names."""
        db_anon, _ = privacy_service.anonymize_entity('test', 'DB')
        sch_anon, _ = privacy_service.anonymize_entity('test', 'SCH')
        
        assert db_anon.startswith('__DB_')
        assert sch_anon.startswith('__SCH_')
    
    def test_consistency(self):
        """Test that same input produces same anonymized name."""
        anon1, _ = privacy_service.anonymize_entity('database', 'DB')
        anon2, _ = privacy_service.anonymize_entity('database', 'DB')
        
        assert anon1 == anon2


class TestAnonymizeEntities:
    """Tests for anonymize_entities function."""
    
    def test_anonymize_all_entities(self):
        """Test anonymizing database, schema, platform, and project."""
        mapping = privacy_service.anonymize_entities(
            'my_database', 'my_schema', 'bigquery', 'my-project'
        )
        
        # Check all entities are in mapping
        assert 'my_database' in mapping.values()
        assert 'my_schema' in mapping.values()
        assert 'bigquery' in mapping.values()
        assert 'my-project' in mapping.values()
        
        # Check anonymized names have correct prefixes
        anon_names = list(mapping.keys())
        assert any('__DB_' in name for name in anon_names)
        assert any('__SCH_' in name for name in anon_names)
        assert any('__PLATFORM_' in name for name in anon_names)
        assert any('__PROJECT_' in name for name in anon_names)


class TestBuildCompleteAnonymizationMap:
    """Tests for build_complete_anonymization_map function."""
    
    def test_complete_mapping(self):
        """Test building complete anonymization map."""
        partition_stats = [
            {
                'week_start': '2024-01-01',
                'columns': [{'column': 'user_id', 'filter_types': []}]
            }
        ]
        table_metadata = {
            'size_bytes': 1024,
            'row_count': 100,
            'columns': [{'name': 'user_id', 'type': 'INTEGER'}]
        }
        
        forward_map, reverse_map = privacy_service.build_complete_anonymization_map(
            'users', 'database', 'schema', 'bigquery', 'project',
            partition_stats, table_metadata
        )
        
        # Check forward and reverse maps are consistent
        for orig, anon in forward_map.items():
            assert reverse_map[anon] == orig
        
        # Check all entities are mapped
        assert 'users' in forward_map
        assert 'database' in forward_map
        assert 'schema' in forward_map
        assert 'bigquery' in forward_map
        assert 'project' in forward_map
        assert 'user_id' in forward_map


class TestAnonymizeTableMetadata:
    """Tests for anonymize_table_metadata function."""
    
    def test_anonymize_metadata(self):
        """Test anonymizing table metadata columns."""
        table_metadata = {
            'size_bytes': 1024000,
            'row_count': 10000,
            'columns': [
                {'name': 'id', 'type': 'INTEGER'},
                {'name': 'name', 'type': 'STRING'}
            ]
        }
        
        # Create a simple reverse map
        reverse_map = {
            '__COL_ABCD1234__': 'id',
            '__COL_EFGH5678__': 'name'
        }
        
        anon_metadata = privacy_service.anonymize_table_metadata(table_metadata, reverse_map)
        
        assert anon_metadata['size_bytes'] == 1024000
        assert anon_metadata['row_count'] == 10000
        assert len(anon_metadata['columns']) == 2
        
        # Columns should be anonymized
        col_names = [col['name'] for col in anon_metadata['columns']]
        assert '__COL_ABCD1234__' in col_names or '__COL_EFGH5678__' in col_names
    
    def test_none_metadata(self):
        """Test that None metadata returns None."""
        result = privacy_service.anonymize_table_metadata(None, {})
        assert result is None


class TestRestoreNamesInResponse:
    """Tests for restore_names_in_response function."""
    
    def test_restore_table_name(self):
        """Test restoring table name in AI response."""
        ai_response = "The table __TBL_ABCD1234__ has high query volume."
        reverse_map = {'__TBL_ABCD1234__': 'users'}
        
        restored = privacy_service.restore_names_in_response(ai_response, reverse_map)
        
        assert 'users' in restored
        assert '__TBL_ABCD1234__' not in restored
    
    def test_restore_column_names(self):
        """Test restoring column names in AI response."""
        ai_response = "Consider partitioning on __COL_ABCD1234__ and __COL_EFGH5678__."
        reverse_map = {
            '__COL_ABCD1234__': 'created_at',
            '__COL_EFGH5678__': 'user_id'
        }
        
        restored = privacy_service.restore_names_in_response(ai_response, reverse_map)
        
        assert 'created_at' in restored
        assert 'user_id' in restored
        assert '__COL_ABCD1234__' not in restored
    
    def test_restore_multiple_occurrences(self):
        """Test restoring names that appear multiple times."""
        ai_response = "__TBL_TEST__ is used. Query __TBL_TEST__ frequently."
        reverse_map = {'__TBL_TEST__': 'orders'}
        
        restored = privacy_service.restore_names_in_response(ai_response, reverse_map)
        
        assert restored.count('orders') == 2
        assert '__TBL_TEST__' not in restored
    
    def test_restore_with_sql_keywords(self):
        """Test restoring in context of SQL keywords."""
        ai_response = "FROM __TBL_TEST__ WHERE __COL_TEST__ > 100"
        reverse_map = {
            '__TBL_TEST__': 'users',
            '__COL_TEST__': 'age'
        }
        
        restored = privacy_service.restore_names_in_response(ai_response, reverse_map)
        
        assert 'FROM users' in restored
        assert 'age >' in restored

