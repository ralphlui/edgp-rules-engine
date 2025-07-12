from typing import List, Dict, Any
from app.models.rule import Rule
from app.validators.gx_utils import validate_with_gx


def validate_column_values_to_be_after(data: List[Dict[str, Any]], rule: Rule) -> Dict[str, Any]:
    """
    Validate that column values are after a specified date using Great Expectations.
    
    Args:
        data: List of dictionaries representing the data
        rule: Rule object containing rule_name, column_name, and min_date
    
    Returns:
        Dictionary with validation result
    """
    try:
        # Extract the min_date
        min_date = None
        
        if hasattr(rule, 'value') and isinstance(rule.value, dict):
            min_date = rule.value.get('min_date')
        
        if not min_date:
            return {
                "success": False,
                "rule_name": rule.rule_name,
                "column_name": rule.column_name,
                "error": "min_date parameter is required"
            }
        
        return validate_with_gx(
            data=data,
            expectation_type="expect_column_values_to_be_after",
            column=rule.column_name,
            rule=rule,
            min_value=min_date
        )
        
    except Exception as e:
        return {
            "success": False,
            "rule_name": rule.rule_name,
            "column_name": rule.column_name,
            "error": str(e)
        }
