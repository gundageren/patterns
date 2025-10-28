"""Tests for API validators."""

import pytest
from api.utils.validators import (
    validate_required_params,
    validate_table_params,
    validate_target_warehouse,
    validate_platform_configured,
    get_debug_flag
)


class TestValidateRequiredParams:
    """Tests for validate_required_params function."""
    
    def test_all_params_present(self):
        """Test validation passes when all required params are present."""
        params = {'param1': 'value1', 'param2': 'value2'}
        is_valid, error_response = validate_required_params(params, ['param1', 'param2'])
        
        assert is_valid is True
        assert error_response is None
    
    def test_missing_params(self, flask_app):
        """Test validation fails when required params are missing."""
        with flask_app.app_context():
            params = {'param1': 'value1'}
            is_valid, error_response = validate_required_params(params, ['param1', 'param2'])
            
            assert is_valid is False
            assert error_response is not None
            response, status_code = error_response
            assert status_code == 400
    
    def test_empty_string_treated_as_missing(self, flask_app):
        """Test that empty strings are treated as missing params."""
        with flask_app.app_context():
            params = {'param1': 'value1', 'param2': ''}
            is_valid, error_response = validate_required_params(params, ['param1', 'param2'])
            
            assert is_valid is False
            assert error_response is not None
    
    def test_none_treated_as_missing(self, flask_app):
        """Test that None values are treated as missing params."""
        with flask_app.app_context():
            params = {'param1': 'value1', 'param2': None}
            is_valid, error_response = validate_required_params(params, ['param1', 'param2'])
            
            assert is_valid is False
            assert error_response is not None


class TestValidateTableParams:
    """Tests for validate_table_params function."""
    
    def test_valid_table_params(self):
        """Test validation passes with all table params."""
        is_valid, error_response = validate_table_params(
            'bigquery', 'my-project', 'database', 'schema', 'table'
        )
        
        assert is_valid is True
        assert error_response is None
    
    def test_missing_source_platform(self, flask_app):
        """Test validation fails with missing source_platform."""
        with flask_app.app_context():
            is_valid, error_response = validate_table_params(
                None, 'my-project', 'database', 'schema', 'table'
            )
            
            assert is_valid is False
            assert error_response is not None
    
    def test_missing_multiple_params(self, flask_app):
        """Test validation fails with multiple missing params."""
        with flask_app.app_context():
            is_valid, error_response = validate_table_params(
                'bigquery', None, None, 'schema', 'table'
            )
            
            assert is_valid is False
            response, status_code = error_response
            assert status_code == 400


class TestValidateTargetWarehouse:
    """Tests for validate_target_warehouse function."""
    
    def test_valid_warehouse(self):
        """Test validation passes for valid warehouse."""
        is_valid, error_response = validate_target_warehouse('Snowflake')
        
        assert is_valid is True
        assert error_response is None
    
    def test_none_warehouse_is_valid(self):
        """Test that None warehouse is valid (optional param)."""
        is_valid, error_response = validate_target_warehouse(None)
        
        assert is_valid is True
        assert error_response is None
    
    def test_invalid_warehouse(self, flask_app):
        """Test validation fails for invalid warehouse."""
        with flask_app.app_context():
            is_valid, error_response = validate_target_warehouse('InvalidWarehouse')
            
            assert is_valid is False
            assert error_response is not None
            response, status_code = error_response
            assert status_code == 400


class TestValidatePlatformConfigured:
    """Tests for validate_platform_configured function."""
    
    def test_configured_platform(self):
        """Test validation passes when platform is configured."""
        is_valid, error_response = validate_platform_configured('bigquery')
        
        assert is_valid is True
        assert error_response is None
    
    def test_none_platform(self, flask_app):
        """Test validation fails when platform is None."""
        with flask_app.app_context():
            is_valid, error_response = validate_platform_configured(None)
            
            assert is_valid is False
            assert error_response is not None
            response, status_code = error_response
            assert status_code == 400
    
    def test_empty_platform(self, flask_app):
        """Test validation fails when platform is empty string."""
        with flask_app.app_context():
            is_valid, error_response = validate_platform_configured('')
            
            assert is_valid is False
            assert error_response is not None
            response, status_code = error_response
            assert status_code == 400


class TestGetDebugFlag:
    """Tests for get_debug_flag function."""
    
    def test_true_values(self):
        """Test that various true values are recognized."""
        assert get_debug_flag('true') is True
        assert get_debug_flag('True') is True
        assert get_debug_flag('TRUE') is True
        assert get_debug_flag('1') is True
        assert get_debug_flag('yes') is True
        assert get_debug_flag('YES') is True
    
    def test_false_values(self):
        """Test that false and other values return False."""
        assert get_debug_flag('false') is False
        assert get_debug_flag('0') is False
        assert get_debug_flag('no') is False
        assert get_debug_flag('') is False
        assert get_debug_flag('random') is False
    
    def test_none_value(self):
        """Test that None returns False."""
        assert get_debug_flag(None) is False

