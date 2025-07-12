from typing import List, Dict, Any
from app.models.rule import Rule
from app.validators.gx_utils import validate_with_gx


def validate_table_columns_to_match_ordered_list(data: List[Dict[str, Any]], rule: Rule) -> Dict[str, Any]:
    """
    Validate that table columns match an ordered list using Great Expectations.
    
    Args:
        data: List of dictionaries representing the data
        rule: Rule object containing rule_name and column_list
    
    Returns:
        Dictionary with validation result
    """
    try:
        # Extract the column list
        column_list = None
        
        if hasattr(rule, 'value') and isinstance(rule.value, dict):
            column_list = rule.value.get('column_list')
        
        if not column_list:
            return {
                "success": False,
                "rule_name": rule.rule_name,
                "column_name": rule.column_name,
                "error": "column_list parameter is required"
            }
        
        # Use Great Expectations validation
        result = validate_with_gx(
            data=data,
            expectation_type="expect_table_columns_to_match_ordered_list",
            column=None,  # Table-level expectation
            column_list=column_list
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
