from typing import List, Dict, Any
from app.models.rule import Rule
from app.validators.gx_utils import validate_with_gx


def validate_column_pair_values_a_to_be_greater_than_b(data: List[Dict[str, Any]], rule: Rule) -> Dict[str, Any]:
    """
    Validate that column A values are greater than column B values using Great Expectations.
    
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
        or_equal = False
        
        if hasattr(rule, 'value') and isinstance(rule.value, dict):
            column_a = rule.value.get('column_A')
            column_b = rule.value.get('column_B')
            or_equal = rule.value.get('or_equal', False)
        
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
            expectation_type="expect_column_pair_values_A_to_be_greater_than_B",
            column=None,  # Not applicable for pair validation
            column_A=column_a,
            column_B=column_b,
            or_equal=or_equal
        )
        
        return {
            "success": result["success"],
            "rule_name": rule.rule_name,
            "column_name": f"{column_a} > {column_b}",
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
