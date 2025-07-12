from typing import List, Dict, Any
from app.models.rule import Rule
from app.validators.gx_utils import validate_with_gx


def validate_column_to_exist(data: List[Dict[str, Any]], rule: Rule) -> Dict[str, Any]:
    """
    Validate that a specified column exists in the dataset using Great Expectations.
    
    Args:
        data: List of dictionaries representing the dataset
        rule: Rule object containing column_name
        
    Returns:
        Dict containing validation results
    """
    column_name = rule.column_name
    
    if not column_name:
        return {
            "rule_name": rule.rule_name,
            "column_name": column_name,
            "success": False,
            "error": "Column name must be specified"
        }
    
    try:
        # Use Great Expectations to validate
        result = validate_with_gx(
            data=data,
            expectation_type="expect_column_to_exist",
            column=column_name
        )
        
        return {
            "success": result["success"],
            "rule_name": rule.rule_name,
            "column_name": column_name,
            "message": result["message"] if result["success"] else None,
            "error": result["error"] if not result["success"] else None
        }
        
    except Exception as e:
        return {
            "rule_name": rule.rule_name,
            "column_name": column_name,
            "success": False,
            "error": f"Great Expectations validation error: {str(e)}"
        }
