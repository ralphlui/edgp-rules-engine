from typing import List, Dict, Any
from app.models.rule import Rule
from app.validators.gx_utils import validate_with_gx


def validate_column_pair_values_to_be_equal(data: List[Dict[str, Any]], rule: Rule) -> Dict[str, Any]:
    """
    Validate that column A values equal column B values using Great Expectations.
    
    Args:
        data: List of dictionaries representing the data
        rule: Rule object containing rule_name and column pair information
    
    Returns:
        Dictionary with validation result
    """
    try:
        # Extract column A and B names
        column_a = None
        column_b = None
        
        if hasattr(rule, 'value') and isinstance(rule.value, dict):
            column_a = rule.value.get('column_A')
            column_b = rule.value.get('column_B')
        
        if not column_a or not column_b:
            return {
                "success": False,
                "rule_name": rule.rule_name,
                "column_name": rule.column_name,
                "error": "Both column_A and column_B parameters are required"
            }
        
        # Use Great Expectations validation
        result = validate_with_gx(
            data=data,
            expectation_type="expect_column_pair_values_to_be_equal",
            column=None,  # Not applicable for pair validation
            column_A=column_a,
            column_B=column_b
        )
        
        return {
            "success": result["success"],
            "rule_name": rule.rule_name,
            "column_name": f"{column_a} = {column_b}",
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
