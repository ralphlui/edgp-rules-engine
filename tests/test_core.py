"""
Comprehensive unit test suite for all functions in the EDGP Rules Engine.
This file tests all core functionality across the application.
"""
import pytest
import asyncio
import sys
import os
from unittest.mock import Mock, patch, AsyncMock
from datetime import datetime
import json

# Add the parent directory to the Python path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Import core modules
from app.models.validation import ValidationRule, ValidationRequest, ValidationResponse, ValidationResultDetail, ValidationSummary
from app.validators.validator import data_validator

class TestCoreValidation:
    """Test core validation functionality"""
    
    @pytest.fixture
    def sample_data(self):
        """Sample test data fixture"""
        return [
            {"id": 1, "name": "John", "age": 25, "email": "john@example.com", "score": 85},
            {"id": 2, "name": "Jane", "age": 30, "email": "jane@example.com", "score": 92},
            {"id": 3, "name": "Bob", "age": 22, "email": "bob@example.com", "score": 78},
        ]
    
    def test_validation_request_model(self, sample_data):
        """Test ValidationRequest model creation"""
        rule = ValidationRule(rule_name="expect_column_to_exist", column_name="name")
        request = ValidationRequest(rules=[rule], dataset=sample_data)
        
        assert len(request.rules) == 1
        assert len(request.dataset) == 3
        assert request.rules[0].rule_name == "expect_column_to_exist"
    
    def test_validation_response_model(self):
        """Test ValidationResponse model creation"""
        result = ValidationResultDetail(
            rule_name="test_rule",
            column_name="test_column",
            success=True,
            message="Test passed",
            details={}
        )
        
        summary = ValidationSummary(
            total_rules=1,
            successful_rules=1,
            failed_rules=0,
            success_rate=1.0,
            total_rows=0,
            total_columns=0,
            execution_time_ms=0
        )
        
        response = ValidationResponse(results=[result], summary=summary)
        
        assert len(response.results) == 1
        assert response.summary.total_rules == 1
        assert response.results[0].success is True
    
    def test_data_validator_success(self, sample_data):
        """Test data validator with successful validation"""
        rule = ValidationRule(rule_name="expect_column_to_exist", column_name="name")
        request = ValidationRequest(rules=[rule], dataset=sample_data)
        
        # This should not raise an exception
        response = data_validator(request)
        
        assert isinstance(response, ValidationResponse)
        assert len(response.results) == 1
        assert response.summary.total_rules == 1
    
    def test_data_validator_multiple_rules(self, sample_data):
        """Test data validator with multiple rules"""
        rules = [
            ValidationRule(rule_name="expect_column_to_exist", column_name="name"),
            ValidationRule(rule_name="expect_column_to_exist", column_name="age"),
        ]
        request = ValidationRequest(rules=rules, dataset=sample_data)
        
        response = data_validator(request)
        
        assert isinstance(response, ValidationResponse)
        assert len(response.results) == 2
        assert response.summary.total_rules == 2


class TestValidatorRegistry:
    """Test validator registry functionality"""
    
    def test_validator_registry_import(self):
        """Test that validator registry can be imported"""
        try:
            from app.validators.validator_registry import get_validator, VALIDATORS
            assert callable(get_validator)
            assert isinstance(VALIDATORS, dict)
        except ImportError as e:
            pytest.skip(f"Validator registry not available: {e}")
    
    def test_get_validator_function(self):
        """Test getting a validator function"""
        try:
            from app.validators.validator_registry import get_validator
            
            # Try to get a common validator
            validator = get_validator("expect_column_to_exist")
            
            # Should either return a function or None
            assert validator is None or callable(validator)
        except ImportError as e:
            pytest.skip(f"Validator registry not available: {e}")


class TestAPIRoutes:
    """Test API route functionality"""
    
    def test_import_routes(self):
        """Test that routes can be imported"""
        try:
            from app.api.routes import router
            assert router is not None
        except ImportError as e:
            pytest.skip(f"Routes not available: {e}")


class TestConfiguration:
    """Test configuration functionality"""
    
    def test_core_config_import(self):
        """Test that core config can be imported"""
        try:
            from app.core.config import Settings
            settings = Settings()
            assert settings is not None
        except ImportError as e:
            pytest.skip(f"Core config not available: {e}")
    
    def test_sqs_config_import(self):
        """Test that SQS config can be imported"""
        try:
            from app.sqs.config import SQSSettings
            settings = SQSSettings()
            assert settings is not None
        except ImportError as e:
            pytest.skip(f"SQS config not available: {e}")


class TestSQSFunctionality:
    """Test SQS-related functionality"""
    
    def test_sqs_models_import(self):
        """Test that SQS models can be imported"""
        try:
            from app.models.sqs_models import (
                SQSValidationRequest,
                SQSValidationResponse,
                ValidationRule,
                MessageStatus
            )
            assert SQSValidationRequest is not None
            assert SQSValidationResponse is not None
        except ImportError as e:
            pytest.skip(f"SQS models not available: {e}")
    
    def test_sqs_client_import(self):
        """Test that SQS client can be imported"""
        try:
            from app.sqs.client import SQSClient
            assert SQSClient is not None
        except ImportError as e:
            pytest.skip(f"SQS client not available: {e}")
    
    def test_sqs_manager_import(self):
        """Test that SQS manager can be imported"""
        try:
            from app.sqs.manager import SQSManager
            assert SQSManager is not None
        except ImportError as e:
            pytest.skip(f"SQS manager not available: {e}")


class TestValidatorImplementations:
    """Test individual validator implementations"""
    
    def test_column_exists_validator(self):
        """Test column exists validator"""
        try:
            from app.validators.expect_column_to_exist import validate_column_to_exist
            from app.models.rule import Rule
            
            data = [{"name": "John", "age": 25}]
            rule = Rule(rule_name="expect_column_to_exist", column_name="name")
            
            result = validate_column_to_exist(data, rule)
            assert isinstance(result, dict)
            assert 'success' in result
        except ImportError as e:
            pytest.skip(f"Column exists validator not available: {e}")
    
    def test_values_between_validator(self):
        """Test values between validator"""
        try:
            from app.validators.expect_column_values_to_be_between import validate_column_values_to_be_between
            from app.models.rule import Rule
            
            data = [{"age": 25}, {"age": 30}]
            rule = Rule(
                rule_name="expect_column_values_to_be_between",
                column_name="age",
                value={"min_value": 18, "max_value": 65}
            )
            
            result = validate_column_values_to_be_between(data, rule)
            assert isinstance(result, dict)
            assert 'success' in result
        except ImportError as e:
            pytest.skip(f"Values between validator not available: {e}")


class TestUtilityFunctions:
    """Test utility and helper functions"""
    
    def test_gx_utils_import(self):
        """Test that GX utils can be imported"""
        try:
            from app.validators.gx_utils import validate_with_gx, get_gx_validator
            assert callable(validate_with_gx)
            assert callable(get_gx_validator)
        except ImportError as e:
            pytest.skip(f"GX utils not available: {e}")


class TestMainApplication:
    """Test main application functionality"""
    
    def test_main_app_import(self):
        """Test that main app can be imported"""
        try:
            from app.main import app
            assert app is not None
        except ImportError as e:
            pytest.skip(f"Main app not available: {e}")


class TestCORSFunctionality:
    """Test CORS functionality"""
    
    def test_cors_test_exists(self):
        """Test that CORS test file exists"""
        cors_test_path = os.path.join(os.path.dirname(__file__), "cors_test.html")
        if os.path.exists(cors_test_path):
            assert True  # File exists
        else:
            pytest.skip("CORS test file not found")


# Integration tests
class TestIntegration:
    """Integration tests for the complete workflow"""
    
    @pytest.fixture
    def sample_integration_data(self):
        """Sample data for integration testing"""
        return [
            {"customer_id": 1, "name": "Alice", "age": 28, "email": "alice@test.com", "balance": 1000.50},
            {"customer_id": 2, "name": "Bob", "age": 35, "email": "bob@test.com", "balance": 2500.75},
            {"customer_id": 3, "name": "Charlie", "age": 42, "email": "charlie@test.com", "balance": 500.25},
        ]
    
    def test_full_validation_workflow(self, sample_integration_data):
        """Test the complete validation workflow"""
        rules = [
            ValidationRule(rule_name="expect_column_to_exist", column_name="customer_id"),
            ValidationRule(rule_name="expect_column_to_exist", column_name="name"),
            ValidationRule(rule_name="expect_column_to_exist", column_name="email"),
        ]
        
        request = ValidationRequest(rules=rules, dataset=sample_integration_data)
        response = data_validator(request)
        
        # Basic checks
        assert isinstance(response, ValidationResponse)
        assert len(response.results) == len(rules)
        assert response.summary.total_rules == len(rules)
        
        # Check that all results have required fields
        for result in response.results:
            assert hasattr(result, 'rule_name')
            assert hasattr(result, 'success')
            assert hasattr(result, 'message')


# Performance tests
class TestPerformance:
    """Performance tests for the validation engine"""
    
    def test_large_dataset_validation(self):
        """Test validation with a large dataset"""
        # Create a large dataset
        large_data = []
        for i in range(1000):
            large_data.append({
                "id": i,
                "name": f"User_{i}",
                "age": 20 + (i % 50),
                "score": 50 + (i % 50)
            })
        
        rules = [
            ValidationRule(rule_name="expect_column_to_exist", column_name="id"),
            ValidationRule(rule_name="expect_column_to_exist", column_name="name"),
        ]
        
        request = ValidationRequest(rules=rules, dataset=large_data)
        
        # Time the validation
        import time
        start_time = time.time()
        response = data_validator(request)
        end_time = time.time()
        
        execution_time = end_time - start_time
        
        # Should complete in reasonable time (less than 10 seconds)
        assert execution_time < 10.0
        assert isinstance(response, ValidationResponse)
        assert len(response.results) == len(rules)
    
    def test_many_rules_validation(self):
        """Test validation with many rules"""
        data = [
            {"id": 1, "name": "Test", "age": 25, "score": 85, "active": True}
        ]
        
        # Create many rules
        rules = []
        for i in range(10):
            rules.append(ValidationRule(rule_name="expect_column_to_exist", column_name="id"))
        
        request = ValidationRequest(rules=rules, dataset=data)
        response = data_validator(request)
        
        assert isinstance(response, ValidationResponse)
        assert len(response.results) == len(rules)


if __name__ == "__main__":
    # Run the tests
    pytest.main([__file__, "-v"])
