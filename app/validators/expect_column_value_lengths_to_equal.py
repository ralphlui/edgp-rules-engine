from typing import List, Dict, Any
from app.models.rule import Rule
from app.validators.gx_utils import validate_with_gx


def validate_column_value_lengths_to_equal(data: List[Dict[str, Any]], rule: Rule) -> Dict[str, Any]:
    """
    Validate that column value lengths equal a specific value using Great Expectations.
    
    Args:
        data: List of dictionaries representing the data
        rule: Rule object containing rule_name, column_name, and expected length
    
    Returns:
        Dictionary with validation result
    """
    try:
        # Extract the expected length
        expected_length = None
        
        if hasattr(rule, 'value'):
            if isinstance(rule.value, dict):
                expected_length = rule.value.get('value')
            else:
                expected_length = rule.value
        
        if expected_length is None:
            return {
                "success": False,
                "rule_name": rule.rule_name,
                "column_name": rule.column_name,
                "error": "value parameter is required"
            }
        
        # Use Great Expectations validation
        result = validate_with_gx(
            data=data,
            expectation_type="expect_column_value_lengths_to_equal",
            column=rule.column_name,
            value=expected_length
        )
        
        return {
            "success": result["success"],
            "rule_name": rule.rule_name,
            "column_name": rule.column_name,
            "message": result["message"] if result["success"] else None,
            "error": result["error"] if not result["success"] else None
        }
        
    except Exception as e:
        return {
            "success": False,
            "rule_name": rule.rule_name,
            "column_name": rule.column_name,
            "error": str(e)
        }
