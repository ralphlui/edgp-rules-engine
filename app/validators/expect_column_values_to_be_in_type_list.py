from typing import List, Dict, Any
from app.models.rule import Rule
from app.validators.gx_utils import validate_with_gx


def validate_column_values_to_be_in_type_list(data: List[Dict[str, Any]], rule: Rule) -> Dict[str, Any]:
    """
    Validate that column values are in a list of allowed types using Great Expectations.
    
    Args:
        data: List of dictionaries representing the data
        rule: Rule object containing rule_name, column_name, and type_list
    
    Returns:
        Dictionary with validation result
    """
    try:
        # Extract the type list
        type_list = None
        
        if hasattr(rule, 'type_list') and rule.type_list:
            type_list = rule.type_list
        elif hasattr(rule, 'value') and isinstance(rule.value, dict):
            type_list = rule.value.get('type_list')
        
        if not type_list:
            return {
                "success": False,
                "rule_name": rule.rule_name,
                "column_name": rule.column_name,
                "error": "type_list parameter is required"
            }
        
        # Use Great Expectations validation
        result = validate_with_gx(
            data=data,
            expectation_type="expect_column_values_to_be_in_type_list",
            column=rule.column_name,
            type_list=type_list
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
