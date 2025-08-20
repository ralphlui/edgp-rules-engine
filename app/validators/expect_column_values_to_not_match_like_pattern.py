from typing import List, Dict, Any
from app.models.rule import Rule
from app.validators.gx_utils import validate_with_gx


def validate_column_values_to_not_match_like_pattern(data: List[Dict[str, Any]], rule: Rule) -> Dict[str, Any]:
    """
    Validate that column values do not match a LIKE pattern using Great Expectations.
    
    Args:
        data: List of dictionaries representing the data
        rule: Rule object containing rule_name, column_name, and like_pattern
    
    Returns:
        Dictionary with validation result
    """
    try:
        # Extract the like pattern
        like_pattern = None
        
        if hasattr(rule, 'like_pattern') and rule.like_pattern:
            like_pattern = rule.like_pattern
        elif hasattr(rule, 'value') and isinstance(rule.value, dict):
            like_pattern = rule.value.get('like_pattern')
        
        if not like_pattern:
            return {
                "success": False,
                "rule_name": rule.rule_name,
                "column_name": rule.column_name,
                "error": "like_pattern parameter is required"
            }
        
        # Use Great Expectations validation
        result = validate_with_gx(
            data=data,
            expectation_type="expect_column_values_to_not_match_like_pattern",
            column=rule.column_name,
            like_pattern=like_pattern
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
