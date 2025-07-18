from typing import List, Dict, Any
from app.models.rule import Rule
from app.validators.gx_utils import validate_with_gx


def validate_column_values_to_be_valid_credit_card_number(data: List[Dict[str, Any]], rule: Rule) -> Dict[str, Any]:
    """
    Validate that column values are valid credit card numbers using Great Expectations.
    
    Args:
        data: List of dictionaries representing the data
        rule: Rule object containing rule_name and column_name
    
    Returns:
        Dictionary with validation result
    """
    return validate_with_gx(
        data=data,
        expectation_type="expect_column_values_to_be_valid_credit_card_number",
        column=rule.column_name,
        rule=rule
    )
