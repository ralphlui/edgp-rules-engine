"""
Tests for SQS functionality with new message format.
"""
import pytest
import json
import asyncio
from unittest.mock import Mock, patch, AsyncMock
from datetime import datetime

from app.models.sqs_models import (
    SQSValidationRequest, 
    SQSValidationResponse, 
    ValidationRule,
    MessageStatus,
    DataEntry,
    DataType,
    ValidationResultDetail,
    ValidationSummary,
    SQSMessageWrapper,
    ValidationResult,
    get_dataset_from_request,
    get_validation_rules_from_request,
    create_response_from_request_and_results
)
from app.sqs.config import SQSSettings

# Custom JSON encoder to handle datetime
class DateTimeEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime):
            return obj.isoformat()
        return super().default(obj)

def test_validation_rule_model():
    """Test ValidationRule model with enhanced fields"""
    rule = ValidationRule(
        rule_name="expect_column_values_to_be_between",
        column_name="age",
        value={"min_value": 18, "max_value": 65},
        rule_description="Age must be between 18 and 65",
        severity="error",
        expectation_type="expect_column_values_to_be_between",
        kwargs={"min_value": 18, "max_value": 65}
    )
    
    assert rule.rule_name == "expect_column_values_to_be_between"
    assert rule.column_name == "age"
    assert rule.value["min_value"] == 18
    assert rule.value["max_value"] == 65
    assert rule.expectation_type == "expect_column_values_to_be_between"
    assert rule.kwargs["min_value"] == 18

def test_validation_rule_auto_expectation_type():
    """Test ValidationRule automatically sets expectation_type from rule_name"""
    rule = ValidationRule(
        rule_name="expect_column_to_exist",
        column_name="name"
    )
    
    assert rule.expectation_type == "expect_column_to_exist"

def test_validation_rule_auto_kwargs():
    """Test ValidationRule automatically syncs kwargs with value"""
    rule = ValidationRule(
        rule_name="expect_column_values_to_be_between",
        column_name="age",
        value={"min_value": 18, "max_value": 65}
    )
    
    assert rule.kwargs == {"min_value": 18, "max_value": 65}

def test_data_entry_model():
    """Test DataEntry model with new structure"""
    data_entry = DataEntry(
        data_type=DataType.TABULAR,
        domain_name="Customer",
        file_id="file-123-uuid",
        policy_id="policy-456-uuid",
        data={"name": "John", "age": 30},
        validation_rules=[
            ValidationRule(
                rule_name="expect_column_to_exist",
                column_name="name"
            )
        ]
    )
    
    assert data_entry.data_type == DataType.TABULAR
    assert data_entry.domain_name == "Customer"
    assert data_entry.file_id == "file-123-uuid"
    assert data_entry.policy_id == "policy-456-uuid"
    assert len(data_entry.validation_rules) == 1

def test_data_entry_validation():
    """Test DataEntry validation"""
    
    # Test empty data validation
    with pytest.raises(ValueError, match="Data cannot be empty"):
        DataEntry(
            data_type=DataType.TABULAR,
            domain_name="Customer",
            file_id="file-123-uuid",
            policy_id="policy-456-uuid",
            data={},  # This should raise an error
            validation_rules=[
                ValidationRule(
                    rule_name="expect_column_to_exist",
                    column_name="name"
                )
            ]
        )
    
    # Test empty rules validation
    with pytest.raises(ValueError, match="Validation rules cannot be empty"):
        DataEntry(
            data_type=DataType.TABULAR,
        domain_name="TestDomain",
            file_id="file-123-uuid",
            policy_id="policy-456-uuid",
            data={"name": "John"},
            validation_rules=[]  # This should raise an error
        )

def test_sqs_validation_request_model():
    """Test SQSValidationRequest model with new structure"""
    data_entry = DataEntry(
        data_type=DataType.TABULAR,
        domain_name="TestDomain",
        file_id="file-123-uuid",
        policy_id="policy-456-uuid",
        data={"name": "John", "age": 30},
        validation_rules=[
            ValidationRule(
                rule_name="expect_column_to_exist",
                column_name="name"
            ),
            ValidationRule(
                rule_name="expect_column_values_to_be_between",
                column_name="age",
                value={"min_value": 0, "max_value": 120}
            )
        ]
    )
    
    request = SQSValidationRequest(data_entry=data_entry)
    
    assert request.data_entry.file_id == "file-123-uuid"
    assert request.data_entry.policy_id == "policy-456-uuid"
    assert len(request.data_entry.validation_rules) == 2

def test_helper_functions():
    """Test helper functions for backward compatibility"""
    
    # Create test request
    data_entry = DataEntry(
        data_type=DataType.TABULAR,
        domain_name="TestDomain",
        file_id="file-123-uuid",
        policy_id="policy-456-uuid",
        data={"name": "John", "age": 30},
        validation_rules=[
            ValidationRule(
                rule_name="expect_column_to_exist",
                column_name="name"
            )
        ]
    )
    
    request = SQSValidationRequest(data_entry=data_entry)
    
    # Test get_dataset_from_request
    dataset = get_dataset_from_request(request)
    assert isinstance(dataset, list)
    assert len(dataset) == 1
    assert dataset[0]["name"] == "John"
    
    # Test get_validation_rules_from_request
    rules = get_validation_rules_from_request(request)
    assert len(rules) == 1
    assert rules[0].rule_name == "expect_column_to_exist"

def test_response_creation():
    """Test response creation helper function"""
    
    # Create test request
    data_entry = DataEntry(
        data_type=DataType.TABULAR,
        domain_name="TestDomain",
        file_id="file-123-uuid",
        policy_id="policy-456-uuid",
        data={"name": "John", "age": 30},
        validation_rules=[
            ValidationRule(
                rule_name="expect_column_to_exist",
                column_name="name"
            )
        ]
    )
    
    request = SQSValidationRequest(data_entry=data_entry)
    
    # Create test results
    validation_results = [
        ValidationResultDetail(
            rule_name="expect_column_to_exist",
            column_name="name",
            success=True,
            message="Column exists",
            element_count=1,
            unexpected_count=0,
            unexpected_percent=0.0
        )
    ]
    
    summary = ValidationSummary(
        total_rules=1,
        successful_rules=1,
        failed_rules=0,
        success_rate=1.0,
        total_rows=1,
        total_columns=2,
        execution_time_ms=100,
        validation_engine="great_expectations"
    )
    
    # Test response creation
    response = create_response_from_request_and_results(
        request=request,
        validation_results=validation_results,
        summary=summary,
        processing_time_ms=100,
        status=MessageStatus.SUCCESS
    )

    assert response.file_id == "file-123-uuid"
    assert response.policy_id == "policy-456-uuid"
    assert response.status == "success"
    assert response.domain_name == "TestDomain"

def test_validation_result_detail():
    """Test ValidationResultDetail model"""
    result = ValidationResultDetail(
        rule_name="expect_column_to_exist",
        column_name="name",
        success=True,
        message="Column 'name' exists",
        expected="column exists",
        actual="column found",
        details={"checked_columns": ["name", "age"]},
        element_count=100,
        unexpected_count=0,
        unexpected_percent=0.0
    )
    
    assert result.rule_name == "expect_column_to_exist"
    assert result.success is True
    assert result.unexpected_percent == 0.0
    assert result.details["checked_columns"] == ["name", "age"]

def test_validation_summary():
    """Test ValidationSummary model"""
    summary = ValidationSummary(
        total_rules=5,
        successful_rules=4,
        failed_rules=1,
        success_rate=0.8,
        total_rows=1000,
        total_columns=10,
        execution_time_ms=2500,
        validation_engine="great_expectations"
    )
    
    assert summary.total_rules == 5
    assert summary.success_rate == 0.8
    assert summary.validation_engine == "great_expectations"

def test_sqs_validation_response():
    """Test SQSValidationResponse model"""
    
    # Create data entry
    data_entry = DataEntry(
        data_type=DataType.TABULAR,
        domain_name="TestDomain",
        file_id="file-123-uuid",
        policy_id="policy-456-uuid",
        data={"name": "John", "age": 30},
        validation_rules=[
            ValidationRule(
                rule_name="expect_column_to_exist",
                column_name="name"
            )
        ]
    )
    
    # Create validation result
    validation_result = ValidationResult(
        validation_results=[
            ValidationResultDetail(
                rule_name="expect_column_to_exist",
                column_name="name",
                success=True,
                message="Column exists"
            )
        ],
        summary=ValidationSummary(
            total_rules=1,
            successful_rules=1,
            failed_rules=0,
            success_rate=1.0,
            total_rows=1,
            total_columns=2,
            execution_time_ms=100
        ),
        processing_time_ms=100,
        status=MessageStatus.SUCCESS
    )
    
    # Create response with new model structure
    response = SQSValidationResponse(
        file_id="file-123-uuid",
        policy_id="policy-456-uuid",
        data_type="tabular",
        status="success",
        domain_name="TestDomain",
        data={"id": "test-data-id"},
        failed_validations=None
    )
    
    assert response.file_id == "file-123-uuid"
    assert response.policy_id == "policy-456-uuid"
    assert response.status == "success"
    assert response.domain_name == "TestDomain"

def test_sqs_message_wrapper():
    """Test SQSMessageWrapper model"""
    
    # Create request
    data_entry = DataEntry(
        data_type=DataType.TABULAR,
        domain_name="TestDomain",
        file_id="file-123-uuid",
        policy_id="policy-456-uuid",
        data={"name": "John", "age": 30},
        validation_rules=[
            ValidationRule(
                rule_name="expect_column_to_exist",
                column_name="name"
            )
        ]
    )
    
    request = SQSValidationRequest(data_entry=data_entry)
    
    # Create wrapper
    wrapper = SQSMessageWrapper(
        receipt_handle="receipt-123",
        message_id="msg-123",
        body=request,
        attempts=1,
        attributes={"ApproximateReceiveCount": "1"}
    )
    
    assert wrapper.receipt_handle == "receipt-123"
    assert wrapper.message_id == "msg-123"
    assert wrapper.body.data_entry.file_id == "file-123-uuid"
    assert wrapper.attempts == 1

def test_message_status_enum():
    """Test MessageStatus enum"""
    assert MessageStatus.PENDING == "pending"
    assert MessageStatus.PROCESSING == "processing"
    assert MessageStatus.SUCCESS == "success"
    assert MessageStatus.FAILED == "failed"
    assert MessageStatus.RETRY == "retry"
    assert MessageStatus.DLQ == "dlq"

def test_data_type_enum():
    """Test DataType enum"""
    assert DataType.TABULAR == "tabular"
    assert DataType.JSON == "json"
    assert DataType.CSV == "csv"
    assert DataType.PARQUET == "parquet"
    assert DataType.DATABASE == "database"

def test_json_serialization():
    """Test JSON serialization of models"""
    
    # Create a complete request
    data_entry = DataEntry(
        data_type=DataType.TABULAR,
        domain_name="TestDomain",
        file_id="file-123-uuid",
        policy_id="policy-456-uuid",
        data={"name": "John", "age": 30},
        validation_rules=[
            ValidationRule(
                rule_name="expect_column_to_exist",
                column_name="name",
                rule_description="Name column must exist",
                severity="error"
            )
        ]
    )
    
    request = SQSValidationRequest(data_entry=data_entry)
    
    # Test serialization
    json_str = request.model_dump_json()
    assert "file-123-uuid" in json_str
    assert "policy-456-uuid" in json_str
    assert "expect_column_to_exist" in json_str
    
    # Test deserialization
    request_copy = SQSValidationRequest.model_validate_json(json_str)
    assert request_copy.data_entry.file_id == "file-123-uuid"
    assert request_copy.data_entry.policy_id == "policy-456-uuid"
    assert len(request_copy.data_entry.validation_rules) == 1
