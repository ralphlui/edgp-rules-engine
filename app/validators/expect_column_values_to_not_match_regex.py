from typing import List, Dict, Any
from app.models.rule import Rule
from app.validators.gx_utils import validate_with_gx


def validate_column_values_to_not_match_regex(data: List[Dict[str, Any]], rule: Rule) -> Dict[str, Any]:
    """
    Validate that column values do not match a regex pattern using Great Expectations.
    
    Args:
        data: List of dictionaries representing the data
        rule: Rule object containing rule_name, column_name, and regex pattern
    
    Returns:
        Dictionary with validation result
    """
    try:
        # Extract the regex pattern
        regex_pattern = None
        
        if hasattr(rule, 'regex') and rule.regex:
            regex_pattern = rule.regex
        elif hasattr(rule, 'value') and isinstance(rule.value, dict):
            regex_pattern = rule.value.get('regex')
        
        if not regex_pattern:
            return {
                "success": False,
                "rule_name": rule.rule_name,
                "column_name": rule.column_name,
                "error": "regex parameter is required"
            }
        
        # Use Great Expectations validation
        result = validate_with_gx(
            data=data,
            expectation_type="expect_column_values_to_not_match_regex",
            column=rule.column_name,
            regex=regex_pattern
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
