"""
SQS Message Format Examples
Demonstrates the current SQS input/output queue message format.
"""
import json
import sys
import os

# Add the parent directory to the path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.models.sqs_models import (
    SQSValidationRequest,
    DataEntry,
    DataType,
    ValidationRule
)

def create_basic_example():
    """Create a basic example of the SQS message format"""
    
    # Create validation rules
    validation_rules = [
        ValidationRule(
            rule_name="expect_column_to_exist",
            column_name="name",
            rule_description="Name column must exist in the dataset",
            severity="error"
        ),
        ValidationRule(
            rule_name="expect_column_values_to_be_between",
            column_name="age",
            value={"min_value": 18, "max_value": 65},
            rule_description="Age must be between 18 and 65",
            severity="error"
        )
    ]
    
    # Create data entry
    data_entry = DataEntry(
        data_type=DataType.TABULAR,
        domain_name="Customer",
        file_id="f47ac10b-58cc-4372-a567-0e02b2c3d479",
        policy_id="p47ac10b-58cc-4372-a567-0e02b2c3d480",
        data={
            "records": [
                {"name": "John Doe", "age": 30, "status": "active"},
                {"name": "Jane Smith", "age": 25, "status": "inactive"}
            ]
        },
        validation_rules=validation_rules
    )
    
    # Create SQS request
    return SQSValidationRequest(data_entry=data_entry)

def create_advanced_example():
    """Create a more complex example with multiple rule types"""
    
    validation_rules = [
        ValidationRule(
            rule_name="expect_column_to_exist",
            column_name="email",
            rule_description="Email column must exist",
            severity="error"
        ),
        ValidationRule(
            rule_name="expect_column_values_to_be_in_set",
            column_name="department",
            value={"value_set": ["engineering", "sales", "marketing", "hr"]},
            rule_description="Department must be one of the allowed values",
            severity="warning"
        ),
        ValidationRule(
            rule_name="expect_column_values_to_be_positive",
            column_name="salary",
            rule_description="Salary must be positive",
            severity="error"
        )
    ]
    
    data_entry = DataEntry(
        data_type=DataType.JSON,
        domain_name="Employee",
        file_id="employee-data-2025-08-09",
        policy_id="hr-validation-policy-v1",
        data={
            "employees": [
                {
                    "email": "john@company.com",
                    "department": "engineering",
                    "salary": 75000
                },
                {
                    "email": "jane@company.com", 
                    "department": "sales",
                    "salary": 65000
                }
            ]
        },
        validation_rules=validation_rules
    )
    
    return SQSValidationRequest(data_entry=data_entry)

def show_automatic_field_population():
    """Demonstrate automatic field population features"""
    
    print("=== Automatic Field Population ===")
    
    # Create a rule and show how fields are auto-populated
    rule = ValidationRule(
        rule_name="expect_column_values_to_be_between",
        column_name="age",
        value={"min_value": 18, "max_value": 65}
    )
    
    print(f"Rule name: {rule.rule_name}")
    print(f"Expectation type (auto-set): {rule.expectation_type}")
    print(f"Kwargs (auto-sync): {rule.kwargs}")
    print(f"Severity (default): {rule.severity}")

def main():
    """Main demonstration function"""
    
    print("SQS Message Format Examples")
    print("=" * 50)
    
    # Basic example
    print("\n1. Basic Example:")
    basic_request = create_basic_example()
    print(json.dumps(basic_request.model_dump(), indent=2))
    
    # Advanced example  
    print("\n2. Advanced Example:")
    advanced_request = create_advanced_example()
    print(json.dumps(advanced_request.model_dump(), indent=2))
    
    # Show automatic features
    print("\n3. Automatic Field Population:")
    show_automatic_field_population()
    
    # Test helper functions
    print("\n4. Helper Functions:")
    from app.models.sqs_models import get_dataset_from_request, get_validation_rules_from_request
    
    dataset = get_dataset_from_request(basic_request)
    rules = get_validation_rules_from_request(basic_request)
    
    print(f"Extracted dataset: {len(dataset)} records")
    print(f"Extracted rules: {len(rules)} validation rules")
    
    print("\n✅ Examples completed successfully!")

if __name__ == "__main__":
    main()
