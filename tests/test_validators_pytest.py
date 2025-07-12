"""
Pytest test cases for the validator system
"""
import sys
import os
import pytest

# Add the parent directory to the Python path so we can import app modules
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.models.rule import Rule
from app.models.validation_request import ValidationRequest
from app.validators.validator import data_validator


class TestValidators:
    """Test class for validator functions"""
    
    @pytest.fixture
    def sample_data(self):
        """Sample test data fixture"""
        return [
            {"id": 1, "name": "John", "age": 25, "email": "john@example.com", "score": 85},
            {"id": 2, "name": "Jane", "age": 30, "email": "jane@example.com", "score": 92},
            {"id": 3, "name": "Bob", "age": 22, "email": "bob@example.com", "score": 78},
            {"id": 4, "name": "Alice", "age": 28, "email": "alice@example.com", "score": 88},
            {"id": 5, "name": "Charlie", "age": 35, "email": "valid@email.com", "score": 95}
        ]
    
    def test_column_exists(self, sample_data):
        """Test that column existence validation works"""
        rule = Rule(rule_name="ExpectColumnToExist", column_name="id")
        request = ValidationRequest(rules=[rule], dataset=sample_data)
        response = data_validator(request)
        
        assert len(response.result) == 1
        assert response.result[0]["success"] is True
        assert response.result[0]["rule_name"] == "ExpectColumnToExist"
    
    def test_column_unique(self, sample_data):
        """Test that column uniqueness validation works"""
        rule = Rule(rule_name="ExpectColumnValuesToBeUnique", column_name="id")
        request = ValidationRequest(rules=[rule], dataset=sample_data)
        response = data_validator(request)
        
        assert len(response.result) == 1
        assert response.result[0]["success"] is True
        assert response.result[0]["result"]["unexpected_count"] == 0
    
    def test_column_not_none(self, sample_data):
        """Test that not-null validation works"""
        rule = Rule(rule_name="ExpectColumnValuesToNotBeNone", column_name="name")
        request = ValidationRequest(rules=[rule], dataset=sample_data)
        response = data_validator(request)
        
        assert len(response.result) == 1
        assert response.result[0]["success"] == True
        assert response.result[0]["result"]["unexpected_count"] == 0
    
    def test_values_between(self, sample_data):
        """Test that range validation works"""
        rule = Rule(
            rule_name="ExpectColumnValuesToBeBetween", 
            column_name="age", 
            value={"min_value": 18, "max_value": 65}
        )
        request = ValidationRequest(rules=[rule], dataset=sample_data)
        response = data_validator(request)
        
        assert len(response.result) == 1
        assert response.result[0]["success"] is True
        assert response.result[0]["result"]["unexpected_count"] == 0
    
    def test_email_validation(self, sample_data):
        """Test that email validation works"""
        rule = Rule(rule_name="ExpectColumnValuesToBeValidEmail", column_name="email")
        request = ValidationRequest(rules=[rule], dataset=sample_data)
        response = data_validator(request)
        
        assert len(response.result) == 1
        assert response.result[0]["success"] is True  # All emails should be valid now
        assert response.result[0]["result"]["unexpected_count"] == 0
    
    def test_positive_values(self, sample_data):
        """Test that positive value validation works"""
        rule = Rule(rule_name="ExpectColumnValuesToBePositive", column_name="score")
        request = ValidationRequest(rules=[rule], dataset=sample_data)
        response = data_validator(request)
        
        assert len(response.result) == 1
        assert response.result[0]["success"] is True
        assert response.result[0]["result"]["unexpected_count"] == 0
    
    def test_row_count_between(self, sample_data):
        """Test that row count validation works"""
        rule = Rule(
            rule_name="ExpectTableRowCountToBeBetween", 
            value={"min_value": 3, "max_value": 10}
        )
        request = ValidationRequest(rules=[rule], dataset=sample_data)
        response = data_validator(request)
        
        assert len(response.result) == 1
        assert response.result[0]["success"] is True
        assert response.result[0]["result"]["observed_value"] == 5
    
    def test_missing_column_error(self, sample_data):
        """Test that missing column validation fails correctly"""
        rule = Rule(rule_name="ExpectColumnToExist", column_name="nonexistent")
        request = ValidationRequest(rules=[rule], dataset=sample_data)
        response = data_validator(request)
        
        assert len(response.result) == 1
        assert response.result[0]["success"] is False
        assert "does not exist" in response.result[0]["error"]
    
    def test_invalid_email_data(self):
        """Test that invalid email validation fails correctly"""
        data_with_invalid_email = [
            {"id": 1, "email": "invalid-email"},
            {"id": 2, "email": "valid@email.com"}
        ]
        rule = Rule(rule_name="ExpectColumnValuesToBeValidEmail", column_name="email")
        request = ValidationRequest(rules=[rule], dataset=data_with_invalid_email)
        response = data_validator(request)
        
        assert len(response.result) == 1
        assert response.result[0]["success"] is False
        assert response.result[0]["result"]["unexpected_count"] == 1
