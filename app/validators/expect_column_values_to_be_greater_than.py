from typing import List, Dict, Any
from app.models.rule import Rule
from app.validators.gx_utils import validate_with_gx


def validate_column_values_to_be_greater_than(data: List[Dict[str, Any]], rule: Rule) -> Dict[str, Any]:
    """
    Validate that column values are greater than a specified value using Great Expectations.
    
    Args:
        data: List of dictionaries representing the data
        rule: Rule object containing rule_name, column_name, and threshold value
    
    Returns:
        Dictionary with validation result
    """
    try:
        # Extract the threshold value
        threshold_value = None
        
        if hasattr(rule, 'min_value') and rule.min_value is not None:
            threshold_value = rule.min_value
        elif hasattr(rule, 'value'):
            if isinstance(rule.value, dict):
                threshold_value = rule.value.get('value') or rule.value.get('min_value')
            else:
                threshold_value = rule.value
        
        if threshold_value is None:
            return {
                "success": False,
                "rule_name": rule.rule_name,
                "column_name": rule.column_name,
                "error": "threshold value parameter is required"
            }
        
        # Use Great Expectations validation
        result = validate_with_gx(
            data=data,
            expectation_type="expect_column_values_to_be_between",
            column=rule.column_name,
            min_value=threshold_value,
            max_value=None,
            strict_min=True  # values must be > threshold_value, not >=
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
