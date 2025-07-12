from typing import List, Dict, Any
from app.models.rule import Rule
from app.validators.gx_utils import validate_with_gx


def validate_column_values_to_be_before(data: List[Dict[str, Any]], rule: Rule) -> Dict[str, Any]:
    """
    Validate that column values are before a specified date using Great Expectations.
    
    Args:
        data: List of dictionaries representing the data
        rule: Rule object containing rule_name, column_name, and max_date
    
    Returns:
        Dictionary with validation result
    """
    try:
        # Extract the max_date
        max_date = None
        
        if hasattr(rule, 'value') and isinstance(rule.value, dict):
            max_date = rule.value.get('max_date')
        
        if not max_date:
            return {
                "success": False,
                "rule_name": rule.rule_name,
                "column_name": rule.column_name,
                "error": "max_date parameter is required"
            }
        
        return validate_with_gx(
            data=data,
            expectation_type="expect_column_values_to_be_before",
            column=rule.column_name,
            rule=rule,
            max_value=max_date
        )
        
    except Exception as e:
        return {
            "success": False,
            "rule_name": rule.rule_name,
            "column_name": rule.column_name,
            "error": str(e)
        }
