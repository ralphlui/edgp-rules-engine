"""
Comprehensive test suite for rules and models modules.
"""
import pytest
from datetime import datetime
from typing import List, Dict, Any
from unittest.mock import Mock, patch

from app.models.rule import Rule
from app.models.validation_request import ValidationRequest
from app.models.validation_response import ValidationResponse, ValidationResult, ValidationSummary
from app.models.validation import ValidationRule, DataType, MessageStatus
from app.models.sqs_models import SQSValidationRequest, SQSValidationResponse, FailedValidation
from app.rules.expectation_rules import get_all_expectation_rules


class TestRuleModelComprehensive:
    """Comprehensive tests for Rule model"""
    
    def test_rule_creation_basic(self):
        """Test basic rule creation"""
        rule = Rule(rule_name="expect_column_to_exist", column_name="test_column")
        
        assert rule.rule_name == "expect_column_to_exist"
        assert rule.column_name == "test_column"
    
    def test_rule_creation_with_parameters(self):
        """Test rule creation with various parameters"""
        rule = Rule(
            rule_name="expect_column_values_to_be_between",
            column_name="age",
            value={"min_value": 0, "max_value": 120}
        )
        
        assert rule.rule_name == "expect_column_values_to_be_between"
        assert rule.column_name == "age"
        assert rule.value == {"min_value": 0, "max_value": 120}
    
    def test_rule_creation_with_list_parameters(self):
        """Test rule creation with list parameters"""
        rule = Rule(
            rule_name="expect_column_values_to_be_in_set",
            column_name="status",
            value=["active", "inactive", "pending"]
        )
        
        assert rule.value == ["active", "inactive", "pending"]
        assert rule.column_name == "status"
    
    def test_rule_serialization(self):
        """Test rule serialization to dict"""
        rule = Rule(
            rule_name="expect_column_to_exist",
            column_name="test_column",
            min_value=10
        )
        
        rule_dict = rule.dict()
        assert "rule_name" in rule_dict
        assert "column_name" in rule_dict
        assert rule_dict["rule_name"] == "expect_column_to_exist"
    
    def test_rule_json_serialization(self):
        """Test rule JSON serialization"""
        rule = Rule(rule_name="expect_column_to_exist", column_name="test")
        
        json_str = rule.json()
        assert isinstance(json_str, str)
        assert "expect_column_to_exist" in json_str
    
    def test_rule_validation_required_fields(self):
        """Test rule validation for required fields"""
        # Should require rule_name
        with pytest.raises(Exception):  # ValidationError or similar
            Rule(column_name="test")
    
    def test_rule_optional_fields(self):
        """Test rule with optional fields"""
        rule = Rule(rule_name="expect_column_to_exist", column_name="test")
        
        # Optional fields should be None or have defaults
        assert rule.value is None
        assert rule.column_name == "test"
        assert rule.rule_name == "expect_column_to_exist"


class TestValidationRequestComprehensive:
    """Comprehensive tests for ValidationRequest model"""
    
    def test_validation_request_creation(self):
        """Test validation request creation"""
        request = ValidationRequest(
            rules=[Rule(rule_name="expect_column_to_exist", column_name="name")],
            dataset=[{"name": "John", "age": 25}]
        )
        
        assert len(request.rules) == 1
        assert len(request.dataset) == 1
        assert request.rules[0].rule_name == "expect_column_to_exist"
    
    def test_validation_request_multiple_rules(self):
        """Test validation request with multiple rules"""
        rules = [
            Rule(rule_name="expect_column_to_exist", column_name="name"),
            Rule(rule_name="expect_column_to_exist", column_name="age"),
            Rule(rule_name="expect_column_values_to_be_positive", column_name="age")
        ]
        
        request = ValidationRequest(
            rules=rules,
            dataset=[{"name": "John", "age": 25}]
        )
        
        assert len(request.rules) == 3
        assert all(isinstance(rule, Rule) for rule in request.rules)
    
    def test_validation_request_large_dataset(self):
        """Test validation request with large dataset"""
        large_dataset = [
            {"id": i, "name": f"person_{i}", "age": 20 + (i % 50)}
            for i in range(1000)
        ]
        
        request = ValidationRequest(
            rules=[Rule(rule_name="expect_column_to_exist", column_name="id")],
            dataset=large_dataset
        )
        
        assert len(request.dataset) == 1000
    
    def test_validation_request_empty_dataset(self):
        """Test validation request with empty dataset"""
        request = ValidationRequest(
            rules=[Rule(rule_name="expect_column_to_exist", column_name="name")],
            dataset=[]
        )
        
        assert len(request.dataset) == 0
    
    def test_validation_request_serialization(self):
        """Test validation request serialization"""
        request = ValidationRequest(
            rules=[Rule(rule_name="expect_column_to_exist", column_name="name")],
            dataset=[{"name": "John"}]
        )
        
        request_dict = request.dict()
        assert "rules" in request_dict
        assert "dataset" in request_dict
        assert isinstance(request_dict["rules"], list)
        assert isinstance(request_dict["dataset"], list)
    
    def test_validation_request_complex_data_types(self):
        """Test validation request with complex data types"""
        dataset = [
            {
                "id": 1,
                "name": "John",
                "scores": [95, 87, 92],
                "metadata": {"department": "engineering", "level": "senior"},
                "active": True,
                "salary": 75000.50
            }
        ]
        
        request = ValidationRequest(
            rules=[Rule(rule_name="expect_column_to_exist", column_name="id")],
            dataset=dataset
        )
        
        assert request.dataset[0]["scores"] == [95, 87, 92]
        assert request.dataset[0]["metadata"]["department"] == "engineering"


class TestValidationResponseComprehensive:
    """Comprehensive tests for ValidationResponse model"""
    
    def test_validation_result_creation(self):
        """Test validation result creation"""
        result = ValidationResult(
            rule="expect_column_to_exist",
            column="name",
            success=True,
            message="Column 'name' exists",
            details={}
        )
        
        assert result.rule == "expect_column_to_exist"
        assert result.success is True
        assert "name" in result.message
    
    def test_validation_result_with_details(self):
        """Test validation result with additional details"""
        result = ValidationResult(
            rule="expect_column_values_to_be_between",
            column="age",
            success=False,
            message="Values outside range",
            details={"expected_range": [0, 120], "violations": [150, -5]}
        )
        
        assert result.details is not None
        assert "expected_range" in result.details
        assert "violations" in result.details
    
    def test_validation_summary_creation(self):
        """Test validation summary creation"""
        summary = ValidationSummary(
            total_rules=5,
            passed=3,
            failed=2
        )
        
        assert summary.total_rules == 5
        assert summary.passed == 3
        assert summary.failed == 2
    
    def test_validation_response_creation(self):
        """Test validation response creation"""
        results = [
            ValidationResult(
                rule="expect_column_to_exist",
                column="name",
                success=True,
                message="Success",
                details={}
            ),
            ValidationResult(
                rule="expect_column_to_exist",
                column="age",
                success=False,
                message="Column missing",
                details={}
            )
        ]
        
        summary = ValidationSummary(
            total_rules=2,
            passed=1,
            failed=1
        )
        
        response = ValidationResponse(results=results, summary=summary)
        
        assert len(response.results) == 2
        assert response.summary.total_rules == 2
    
    def test_validation_response_serialization(self):
        """Test validation response serialization"""
        result = ValidationResult(
            rule="test_rule",
            column="test_column",
            success=True,
            message="Test message",
            details={}
        )
        
        summary = ValidationSummary(
            total_rules=1,
            passed=1,
            failed=0
        )
        
        response = ValidationResponse(results=[result], summary=summary)
        
        response_dict = response.model_dump()
        assert "results" in response_dict
        assert "summary" in response_dict
        assert len(response_dict["results"]) == 1
    
    def test_validation_response_json(self):
        """Test validation response JSON serialization"""
        result = ValidationResult(
            rule="test_rule",
            column="test_column",
            success=True,
            message="Test message",
            details={}
        )
        
        summary = ValidationSummary(
            total_rules=1,
            passed=1,
            failed=0
        )
        
        response = ValidationResponse(results=[result], summary=summary)
        
        json_str = response.model_dump_json()
        assert isinstance(json_str, str)
        assert "test_rule" in json_str


class TestValidationModelsComprehensive:
    """Comprehensive tests for validation models"""
    
    def test_validation_rule_creation(self):
        """Test validation rule creation"""
        rule = ValidationRule(
            rule_name="expect_column_to_exist",
            column_name="test_column"
        )
        
        # Should have reasonable defaults
        assert rule.rule_name == "expect_column_to_exist"
        assert rule.column_name == "test_column"
    
    def test_data_type_enum(self):
        """Test DataType enum"""
        assert DataType.TABULAR == "tabular"
        assert DataType.JSON == "json"
        assert DataType.CSV == "csv"
        
    def test_message_status_enum(self):
        """Test MessageStatus enum"""
        assert MessageStatus.PENDING == "pending"
        assert MessageStatus.SUCCESS == "success"
        assert MessageStatus.FAILED == "failed"


class TestSQSModelsComprehensive:
    """Comprehensive tests for SQS models"""
    
    def test_sqs_validation_request_creation(self):
        """Test SQS validation request creation"""
        from app.models.sqs_models import DataEntry, DataType, ValidationRule
        
        data_entry = DataEntry(
            data_type=DataType.TABULAR,
            domain_name="test_domain",
            file_id="test-file-123",
            policy_id="test-policy-456",
            data={"name": "John"},
            validation_rules=[ValidationRule(rule_name="expect_column_to_exist", column_name="name")]
        )
        
        request = SQSValidationRequest(
            data_entry=data_entry
        )
        
        assert request.data_entry.file_id == "test-file-123"
        assert request.data_entry.policy_id == "test-policy-456"
        assert len(request.data_entry.validation_rules) == 1
    
    def test_failed_validation_creation(self):
        """Test failed validation model creation"""
        failed = FailedValidation(
            rule_name="expect_column_to_exist",
            column_name="missing_column",
            error_message="Column not found"
        )
        
        assert failed.rule_name == "expect_column_to_exist"
        assert failed.column_name == "missing_column"
        assert "not found" in failed.error_message
        assert failed.status == "fail"
    
    def test_sqs_validation_response_creation(self):
        """Test SQS validation response creation"""
        failed_validations = [
            FailedValidation(
                rule_name="test_rule",
                column_name="test_column",
                error_message="Test error"
            )
        ]
        
        response = SQSValidationResponse(
            file_id="test-file-123",
            policy_id="test-policy-456",
            data_type="tabular",
            status="fail",
            domain_name="test_domain",
            data={"id": "test-data-id"},
            failed_validations=failed_validations
        )
        
        assert response.file_id == "test-file-123"
        assert response.status == "fail"
        assert response.data_type == "tabular"
        assert len(response.failed_validations) == 1
    
    def test_sqs_models_serialization(self):
        """Test SQS models serialization"""
        from app.models.sqs_models import DataEntry, DataType, ValidationRule
        
        data_entry = DataEntry(
            data_type=DataType.TABULAR,
            domain_name="test_domain",
            file_id="test-file-123",
            policy_id="test-policy-456",
            data={"name": "John"},
            validation_rules=[ValidationRule(rule_name="expect_column_to_exist", column_name="name")]
        )
        
        request = SQSValidationRequest(
            data_entry=data_entry
        )
        
        request_dict = request.model_dump()
        assert "data_entry" in request_dict
        assert "file_id" in request_dict["data_entry"]
        assert "validation_rules" in request_dict["data_entry"]
    
    def test_sqs_models_json_serialization(self):
        """Test SQS models JSON serialization"""
        failed = FailedValidation(
            rule_name="test_rule",
            column_name="test_column",
            error_message="Test error"
        )
        
        json_str = failed.model_dump_json()
        assert isinstance(json_str, str)
        assert "test_rule" in json_str


class TestExpectationRulesComprehensive:
    """Comprehensive tests for expectation rules module"""
    
    def test_get_all_expectation_rules(self):
        """Test getting all expectation rules"""
        rules = get_all_expectation_rules()
        
        assert isinstance(rules, list)
        assert len(rules) > 0
        
        # Check that rules are dictionaries (actual implementation returns dicts)
        for rule in rules:
            assert isinstance(rule, dict)
            assert "rule_name" in rule
    
    def test_expectation_rules_structure(self):
        """Test structure of expectation rules"""
        rules = get_all_expectation_rules()
        
        for rule in rules:
            assert isinstance(rule, dict)
            assert "rule_name" in rule
            assert isinstance(rule["rule_name"], str)
    
    def test_expectation_rules_variety(self):
        """Test variety of expectation rules"""
        rules = get_all_expectation_rules()
        
        rule_names = [rule["rule_name"] for rule in rules]
        
        # Should have multiple different rule types
        assert len(set(rule_names)) > 5  # At least 5 different rule types
    
    def test_rules_module_imports(self):
        """Test that rules module imports work"""
        from app.rules import expectation_rules
        
        assert hasattr(expectation_rules, 'get_all_expectation_rules')
    
    def test_rules_module_functions_callable(self):
        """Test that rules module functions are callable"""
        assert callable(get_all_expectation_rules)


class TestModelsIntegration:
    """Integration tests for models working together"""
    
    def test_rule_to_validation_request(self):
        """Test rule integration with validation request"""
        rule = Rule(rule_name="expect_column_to_exist", column_name="name")
        
        request = ValidationRequest(
            rules=[rule],
            dataset=[{"name": "John", "age": 25}]
        )
        
        assert request.rules[0].rule_name == rule.rule_name
        assert request.rules[0].column_name == rule.column_name
    
    def test_validation_request_to_response_flow(self):
        """Test flow from request to response"""
        # Create request
        request = ValidationRequest(
            rules=[Rule(rule_name="expect_column_to_exist", column_name="name")],
            dataset=[{"name": "John"}]
        )
        
        # Create response (as would be done by validator)
        result = ValidationResult(
            rule="expect_column_to_exist",
            column="name",
            success=True,
            message="Column exists",
            details={}
        )
        
        summary = ValidationSummary(
            total_rules=1,
            passed=1,
            failed=0
        )
        
        response = ValidationResponse(results=[result], summary=summary)
        
        # Verify integration
        assert response.results[0].rule == request.rules[0].rule_name
        assert response.summary.total_rules == len(request.rules)
    
    def test_sqs_integration_flow(self):
        """Test SQS models integration"""
        from app.models.sqs_models import DataEntry, DataType, ValidationRule
        
        # Create SQS request
        data_entry = DataEntry(
            data_type=DataType.TABULAR,
            domain_name="test_domain",
            file_id="test-file-123",
            policy_id="test-policy-456",
            data={"name": "John"},
            validation_rules=[ValidationRule(rule_name="expect_column_to_exist", column_name="name")]
        )
        
        sqs_request = SQSValidationRequest(
            data_entry=data_entry
        )
        
        # Create SQS response
        sqs_response = SQSValidationResponse(
            file_id=sqs_request.data_entry.file_id,
            policy_id=sqs_request.data_entry.policy_id,
            data_type=sqs_request.data_entry.data_type.value,
            status="success",
            domain_name=sqs_request.data_entry.domain_name,
            data={"id": "test-id"},
            failed_validations=[]
        )
        
        # Verify integration
        assert sqs_response.file_id == sqs_request.data_entry.file_id
        assert sqs_response.status == "success"
    
    def test_model_error_handling(self):
        """Test model error handling"""
        # Test invalid rule creation
        with pytest.raises(Exception):
            Rule()  # Missing required fields
        
        # Test invalid validation request
        with pytest.raises(Exception):
            ValidationRequest(rules=[], dataset=None)  # Invalid dataset
