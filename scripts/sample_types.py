#!/usr/bin/env python3
"""
Sample Input/Output Types for the Rules Engine

This script demonstrates the unified validation types and their usage
across API and SQS interfaces.
"""

import json
from datetime import datetime
from typing import Dict, Any, List

# Import unified validation models
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.models.validation import (
    ValidationRequest,
    ValidationResponse,
    ValidationRule,
    ValidationResultDetail,
    ValidationSummary,
    SQSValidationRequest,
    SQSValidationResponse,
    DataEntry,
    DataType,
    MessageStatus,
    RuleSeverity
)

def create_sample_dataset() -> List[Dict[str, Any]]:
    """Create a sample dataset for validation"""
    return [
        {"id": 1, "name": "Alice", "age": 25, "email": "alice@example.com", "status": "active"},
        {"id": 2, "name": "Bob", "age": 30, "email": "bob@example.com", "status": "active"},
        {"id": 3, "name": "Charlie", "age": 35, "email": "charlie@example.com", "status": "inactive"},
        {"id": 4, "name": "Diana", "age": 28, "email": "diana@example.com", "status": "active"},
        {"id": 5, "name": "Eve", "age": 22, "email": "eve@example.com", "status": "active"}
    ]

def create_sample_rules() -> List[ValidationRule]:
    """Create sample validation rules"""
    return [
        # Column existence rule
        ValidationRule(
            rule_name="expect_column_to_exist",
            column_name="name",
            rule_description="Ensure name column exists"
        ),
        
        # Range validation rule
        ValidationRule(
            rule_name="expect_column_values_to_be_between",
            column_name="age",
            value={"min_value": 18, "max_value": 65},
            rule_description="Age should be between 18 and 65",
            severity=RuleSeverity.ERROR
        ),
        
        # Set membership rule
        ValidationRule(
            rule_name="expect_column_values_to_be_in_set",
            column_name="status",
            value={"value_set": ["active", "inactive", "suspended"]},
            rule_description="Status should be valid",
            severity=RuleSeverity.WARNING
        ),
        
        # Value type rule
        ValidationRule(
            rule_name="expect_column_values_to_be_of_type",
            column_name="id",
            value={"type_": "int"},
            rule_description="ID should be integer"
        )
    ]

def create_api_input_sample() -> Dict[str, Any]:
    """Create sample API input (ValidationRequest)"""
    
    request = ValidationRequest(
        dataset=create_sample_dataset(),
        rules=create_sample_rules(),
        data_key="sample-api-dataset-001",
        data_type=DataType.TABULAR,
        source="api_sample"
    )
    
    return {
        "description": "API Input Sample - ValidationRequest",
        "usage": "POST /api/rules/validate",
        "content_type": "application/json",
        "data": request.model_dump()
    }

def create_api_output_sample() -> Dict[str, Any]:
    """Create sample API output (ValidationResponse)"""
    
    # Sample validation results
    results = [
        ValidationResultDetail(
            rule_name="expect_column_to_exist",
            column_name="name",
            success=True,
            message="Column 'name' exists",
            details={"column_count": 1}
        ),
        ValidationResultDetail(
            rule_name="expect_column_values_to_be_between",
            column_name="age",
            success=True,
            message="All age values are within range",
            details={"min_value": 18, "max_value": 65},
            element_count=5,
            unexpected_count=0,
            unexpected_percent=0.0
        ),
        ValidationResultDetail(
            rule_name="expect_column_values_to_be_in_set",
            column_name="status",
            success=True,
            message="All status values are valid",
            details={"value_set": ["active", "inactive", "suspended"]},
            element_count=5,
            unexpected_count=0,
            unexpected_percent=0.0
        ),
        ValidationResultDetail(
            rule_name="expect_column_values_to_be_of_type",
            column_name="id",
            success=True,
            message="All ID values are integers",
            details={"type_": "int"},
            element_count=5,
            unexpected_count=0,
            unexpected_percent=0.0
        )
    ]
    
    summary = ValidationSummary(
        total_rules=4,
        successful_rules=4,
        failed_rules=0,
        success_rate=1.0,
        total_rows=5,
        total_columns=5,
        execution_time_ms=150,
        validation_engine="great_expectations"
    )
    
    response = ValidationResponse(
        results=results,
        summary=summary,
        execution_time_ms=150
    )
    
    return {
        "description": "API Output Sample - ValidationResponse",
        "usage": "Response from POST /api/rules/validate",
        "content_type": "application/json",
        "data": response.model_dump()
    }

def create_sqs_input_sample() -> Dict[str, Any]:
    """Create sample SQS input message (SQSValidationRequest)"""
    
    # Enhanced data entry
    data_entry = DataEntry(
        data_type=DataType.TABULAR,
        data_key="batch-001-dataset-user-profiles",
        columns=["id", "name", "age", "email", "status"],
        data=create_sample_dataset(),
        source="user_profile_system",
        schema_version="2.0"
    )
    
    request = SQSValidationRequest(
        message_id="msg-20250805-001",
        correlation_id="batch-001",
        source="user_validation_service",
        data_entry=data_entry,
        validation_rules=create_sample_rules(),
        batch_id="batch-001",
        priority=3,
        callback_url="https://api.example.com/validation/callback",
        max_retries=3
    )
    
    return {
        "description": "SQS Input Sample - SQSValidationRequest",
        "usage": "Message sent to SQS input queue",
        "queue": "validation-input-queue",
        "data": request.model_dump()
    }

def create_sqs_output_sample() -> Dict[str, Any]:
    """Create sample SQS output message (SQSValidationResponse)"""
    
    # Sample detailed results
    results = [
        ValidationResultDetail(
            rule_name="expect_column_to_exist",
            column_name="name",
            success=True,
            message="Column 'name' exists",
            details={"column_count": 1}
        ),
        ValidationResultDetail(
            rule_name="expect_column_values_to_be_between",
            column_name="age",
            success=False,
            message="1 value(s) outside expected range",
            expected={"min_value": 18, "max_value": 65},
            actual={"min_value": 16, "max_value": 70},
            details={"min_value": 18, "max_value": 65, "unexpected_values": [16, 70]},
            element_count=5,
            unexpected_count=2,
            unexpected_percent=40.0
        ),
        ValidationResultDetail(
            rule_name="expect_column_values_to_be_in_set",
            column_name="status",
            success=True,
            message="All status values are valid",
            details={"value_set": ["active", "inactive", "suspended"]},
            element_count=5,
            unexpected_count=0,
            unexpected_percent=0.0
        )
    ]
    
    summary = ValidationSummary(
        total_rules=3,
        successful_rules=2,
        failed_rules=1,
        success_rate=0.67,
        total_rows=5,
        total_columns=5,
        execution_time_ms=275,
        validation_engine="great_expectations"
    )
    
    response = SQSValidationResponse(
        message_id="msg-20250805-001",
        correlation_id="batch-001",
        processing_time_ms=275,
        status=MessageStatus.SUCCESS,
        worker_id="worker-abc123",
        data_key="batch-001-dataset-user-profiles",
        data_type=DataType.TABULAR,
        validation_results=results,
        summary=summary,
        batch_id="batch-001",
        source="user_validation_service",
        schema_version="1.0"
    )
    
    return {
        "description": "SQS Output Sample - SQSValidationResponse",
        "usage": "Message sent to SQS output queue",
        "queue": "validation-output-queue",
        "data": response.model_dump()
    }

def create_legacy_compatibility_samples() -> Dict[str, Any]:
    """Create samples showing legacy format compatibility"""
    
    # Legacy API format (still supported)
    legacy_api_request = {
        "dataset": create_sample_dataset(),
        "rules": [
            {
                "expectation_type": "expect_column_to_exist",
                "kwargs": {"column": "name"}
            },
            {
                "expectation_type": "expect_column_values_to_be_between",
                "kwargs": {"column": "age", "min_value": 18, "max_value": 65}
            }
        ]
    }
    
    # Legacy SQS format (still supported)
    legacy_sqs_message = {
        "message_id": "legacy-msg-001",
        "data": create_sample_dataset(),
        "rules": [
            {
                "rule_name": "expect_column_to_exist",
                "column_name": "name"
            },
            {
                "rule_name": "expect_column_values_to_be_between", 
                "column_name": "age",
                "value": {"min_value": 18, "max_value": 65}
            }
        ]
    }
    
    return {
        "description": "Legacy Compatibility Samples",
        "note": "These formats are still supported for backward compatibility",
        "legacy_api_request": legacy_api_request,
        "legacy_sqs_message": legacy_sqs_message,
        "migration_note": "New applications should use the unified formats shown in other samples"
    }

def main():
    """Generate and display all sample types"""
    
    print("=" * 80)
    print("UNIFIED VALIDATION TYPES - INPUT/OUTPUT SAMPLES")
    print("=" * 80)
    print()
    
    # API Samples
    print("1. API INPUT SAMPLE")
    print("-" * 40)
    api_input = create_api_input_sample()
    print(f"Description: {api_input['description']}")
    print(f"Usage: {api_input['usage']}")
    print(f"Content-Type: {api_input['content_type']}")
    print("Sample JSON:")
    print(json.dumps(api_input['data'], indent=2, default=str))
    print()
    
    print("2. API OUTPUT SAMPLE")
    print("-" * 40)
    api_output = create_api_output_sample()
    print(f"Description: {api_output['description']}")
    print(f"Usage: {api_output['usage']}")
    print(f"Content-Type: {api_output['content_type']}")
    print("Sample JSON:")
    print(json.dumps(api_output['data'], indent=2, default=str))
    print()
    
    # SQS Samples
    print("3. SQS INPUT SAMPLE")
    print("-" * 40)
    sqs_input = create_sqs_input_sample()
    print(f"Description: {sqs_input['description']}")
    print(f"Usage: {sqs_input['usage']}")
    print(f"Queue: {sqs_input['queue']}")
    print("Sample JSON:")
    print(json.dumps(sqs_input['data'], indent=2, default=str))
    print()
    
    print("4. SQS OUTPUT SAMPLE")
    print("-" * 40)
    sqs_output = create_sqs_output_sample()
    print(f"Description: {sqs_output['description']}")
    print(f"Usage: {sqs_output['usage']}")
    print(f"Queue: {sqs_output['queue']}")
    print("Sample JSON:")
    print(json.dumps(sqs_output['data'], indent=2, default=str))
    print()
    
    # Legacy Compatibility
    print("5. LEGACY COMPATIBILITY")
    print("-" * 40)
    legacy = create_legacy_compatibility_samples()
    print(f"Description: {legacy['description']}")
    print(f"Note: {legacy['note']}")
    print()
    print("Legacy API Request:")
    print(json.dumps(legacy['legacy_api_request'], indent=2, default=str))
    print()
    print("Legacy SQS Message:")
    print(json.dumps(legacy['legacy_sqs_message'], indent=2, default=str))
    print()
    print(f"Migration Note: {legacy['migration_note']}")
    print()
    
    print("=" * 80)
    print("VALIDATION TYPE ALIGNMENT SUMMARY")
    print("=" * 80)
    print("✅ Unified models in app/models/validation.py")
    print("✅ Consistent ValidationRule across API and SQS")
    print("✅ Detailed ValidationResultDetail with statistics")
    print("✅ Enhanced ValidationSummary with metadata")
    print("✅ Backward compatibility for legacy formats")
    print("✅ Type safety with Pydantic validation")
    print("✅ Clear separation between API and SQS variants")
    print("✅ Processor aligned with unified models")
    print()
    print("Files Updated:")
    print("- app/models/validation.py (new unified models)")
    print("- app/api/routes.py (updated to use unified models)")  
    print("- app/sqs/processor.py (updated to use unified models)")
    print("- scripts/sample_types.py (this file with samples)")
    print()

if __name__ == "__main__":
    main()
