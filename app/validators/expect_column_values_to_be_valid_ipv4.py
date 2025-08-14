from typing import List, Dict, Any
from app.models.rule import Rule
from app.validators.gx_utils import validate_with_gx


def validate_column_values_to_be_valid_ipv4(data: List[Dict[str, Any]], rule: Rule) -> Dict[str, Any]:
    """
    Validate that column values are valid IPv4 addresses using Great Expectations.
    
    Args:
        data: List of dictionaries representing the data
        rule: Rule object containing rule_name and column_name
    
    Returns:
        Dictionary with validation result
    """
    result = validate_with_gx(
        data=data,
        expectation_type="expect_column_values_to_be_valid_ipv4",
        column=rule.column_name,
        rule=rule
    )
    
    # Add rule_name and column_name to the result
    result["rule_name"] = rule.rule_name
    result["column_name"] = rule.column_name
    
    return result
