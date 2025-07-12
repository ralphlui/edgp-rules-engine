"""
Test script to validate the new validator system
"""
import sys
import os

# Add the parent directory to the Python path so we can import app modules
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.models.rule import Rule
from app.models.validation_request import ValidationRequest
from app.validators.validator import data_validator

# Sample test data
test_data = [
    {"id": 1, "name": "John", "age": 25, "email": "john@example.com", "score": 85},
    {"id": 2, "name": "Jane", "age": 30, "email": "jane@example.com", "score": 92},
    {"id": 3, "name": "Bob", "age": 22, "email": "bob@example.com", "score": 78},
    {"id": 4, "name": "Alice", "age": 28, "email": "alice@example.com", "score": 88},
    {"id": 5, "name": "Charlie", "age": 35, "email": "invalid-email", "score": 95}
]

# Test rules
test_rules = [
    Rule(rule_name="ExpectColumnToExist", column_name="id"),
    Rule(rule_name="ExpectColumnValuesToBeUnique", column_name="id"),
    Rule(rule_name="ExpectColumnValuesToNotBeNone", column_name="name"),
    Rule(rule_name="ExpectColumnValuesToBeBetween", column_name="age", value={"min_value": 18, "max_value": 65}),
    Rule(rule_name="ExpectColumnValuesToBeValidEmail", column_name="email"),
    Rule(rule_name="ExpectColumnValuesToBePositive", column_name="score"),
    Rule(rule_name="ExpectTableRowCountToBeBetween", value={"min_value": 3, "max_value": 10})
]

if __name__ == "__main__":
    # Create validation request
    request = ValidationRequest(rules=test_rules, dataset=test_data)
    
    # Run validation
    response = data_validator(request)
    
    # Print results
    print("Validation Results:")
    print("=" * 50)
    
    for i, result in enumerate(response.result, 1):
        print(f"{i}. Rule: {result['rule_name']}")
        print(f"   Column: {result.get('column_name', 'N/A')}")
        print(f"   Success: {result['success']}")
        
        if not result['success']:
            print(f"   Error: {result.get('error', 'N/A')}")
        else:
            print(f"   Result: {result.get('result', {})}")
        
        print("-" * 30)
