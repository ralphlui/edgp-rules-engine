from typing import List, Dict, Any
from app.models.rule import Rule
from app.validators.gx_utils import validate_with_gx


def validate_column_min_to_be_between(data: List[Dict[str, Any]], rule: Rule) -> Dict[str, Any]:
    """
    Validate that column minimum value is between min and max values using Great Expectations.
    
    Args:
        data: List of dictionaries representing the data
        rule: Rule object containing rule_name, column_name, and range values
    
    Returns:
        Dictionary with validation result
    """
    try:
        # Extract min and max values from rule
        min_value = None
        max_value = None
        
        if hasattr(rule, 'min_value') and hasattr(rule, 'max_value'):
            min_value = rule.min_value
            max_value = rule.max_value
        elif hasattr(rule, 'value') and isinstance(rule.value, dict):
            min_value = rule.value.get('min_value')
            max_value = rule.value.get('max_value')
        
        if min_value is None or max_value is None:
            return {
                "success": False,
                "rule_name": rule.rule_name,
                "column_name": rule.column_name,
                "error": "Both min_value and max_value parameters are required"
            }
        
        # Use Great Expectations validation
        result = validate_with_gx(
            data=data,
            expectation_type="expect_column_min_to_be_between",
            column=rule.column_name,
            min_value=min_value,
            max_value=max_value
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
