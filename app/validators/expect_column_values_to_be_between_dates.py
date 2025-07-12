from typing import List, Dict, Any
from app.models.rule import Rule
from app.validators.gx_utils import validate_with_gx


def validate_column_values_to_be_between_dates(data: List[Dict[str, Any]], rule: Rule) -> Dict[str, Any]:
    """
    Validate that column values are between two specified dates using Great Expectations.
    
    Args:
        data: List of dictionaries representing the data
        rule: Rule object containing rule_name, column_name, min_date, and max_date
    
    Returns:
        Dictionary with validation result
    """
    try:
        # Extract the min_date and max_date
        min_date = None
        max_date = None
        
        if hasattr(rule, 'value') and isinstance(rule.value, dict):
            min_date = rule.value.get('min_date')
            max_date = rule.value.get('max_date')
        
        if not min_date or not max_date:
            return {
                "success": False,
                "rule_name": rule.rule_name,
                "column_name": rule.column_name,
                "error": "Both min_date and max_date parameters are required"
            }
        
        return validate_with_gx(
            data=data,
            expectation_type="expect_column_values_to_be_between",
            column=rule.column_name,
            rule=rule,
            min_value=min_date,
            max_value=max_date
        )
        
    except Exception as e:
        return {
            "success": False,
            "rule_name": rule.rule_name,
            "column_name": rule.column_name,
            "error": str(e)
        }
