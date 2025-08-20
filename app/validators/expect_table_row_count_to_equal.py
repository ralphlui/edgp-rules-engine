from typing import List, Dict, Any
from app.models.rule import Rule
from app.validators.gx_utils import validate_with_gx


def validate_table_row_count_to_equal(data: List[Dict[str, Any]], rule: Rule) -> Dict[str, Any]:
    """
    Validate that table row count equals a specific value using Great Expectations.
    
    Args:
        data: List of dictionaries representing the data
        rule: Rule object containing rule_name and expected row count
    
    Returns:
        Dictionary with validation result
    """
    try:
        # Extract the expected row count
        expected_count = None
        
        if hasattr(rule, 'value'):
            if isinstance(rule.value, dict):
                expected_count = rule.value.get('value')
            else:
                expected_count = rule.value
        
        if expected_count is None:
            return {
                "success": False,
                "rule_name": rule.rule_name,
                "column_name": rule.column_name,
                "error": "value parameter is required"
            }
        
        # Use Great Expectations validation
        result = validate_with_gx(
            data=data,
            expectation_type="expect_table_row_count_to_equal",
            column=None,  # Table-level expectation
            value=expected_count
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
