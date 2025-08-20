from typing import List, Dict, Any
from app.models.rule import Rule
from app.validators.gx_utils import validate_with_gx


def validate_column_values_to_match_strftime_format(data: List[Dict[str, Any]], rule: Rule) -> Dict[str, Any]:
    """
    Validate that column values match a strftime format using Great Expectations.
    
    Args:
        data: List of dictionaries representing the data
        rule: Rule object containing rule_name, column_name, and strftime_format
    
    Returns:
        Dictionary with validation result
    """
    try:
        # Extract the strftime format
        strftime_format = None
        
        if hasattr(rule, 'strftime_format') and rule.strftime_format:
            strftime_format = rule.strftime_format
        elif hasattr(rule, 'value') and isinstance(rule.value, dict):
            strftime_format = rule.value.get('strftime_format')
        
        if not strftime_format:
            return {
                "success": False,
                "rule_name": rule.rule_name,
                "column_name": rule.column_name,
                "error": "strftime_format parameter is required"
            }
        
        # Use Great Expectations validation
        result = validate_with_gx(
            data=data,
            expectation_type="expect_column_values_to_match_strftime_format",
            column=rule.column_name,
            strftime_format=strftime_format
        )
        
        return {
            "success": result["success"],
            "rule_name": rule.rule_name,
            "column_name": rule.column_name,
            "message": result["message"],
            "error": result["error"] if not result["success"] else None
        }
        
    except Exception as e:
        return {
            "success": False,
            "rule_name": rule.rule_name,
            "column_name": rule.column_name,
            "error": str(e)
        }
