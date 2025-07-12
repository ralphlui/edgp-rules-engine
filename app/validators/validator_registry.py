from typing import Dict, Callable, List, Any
from app.models.rule import Rule

# Import all validator functions
from app.validators.expect_column_distinct_values_to_be_in_set import validate_column_distinct_values_to_be_in_set
from app.validators.expect_column_values_to_be_in_set import validate_column_values_to_be_in_set
from app.validators.expect_column_values_to_not_be_in_set import validate_column_values_to_not_be_in_set
from app.validators.expect_column_values_to_be_between import validate_column_values_to_be_between
from app.validators.expect_column_value_lengths_to_be_between import validate_column_value_lengths_to_be_between
from app.validators.expect_column_values_to_match_regex import validate_column_values_to_match_regex
from app.validators.expect_column_values_to_not_match_regex import validate_column_values_to_not_match_regex
from app.validators.expect_column_values_to_match_strftime_format import validate_column_values_to_match_strftime_format
from app.validators.expect_column_values_to_be_unique import validate_column_values_to_be_unique
from app.validators.expect_compound_columns_to_be_unique import validate_compound_columns_to_be_unique
from app.validators.expect_column_mean_to_be_between import validate_column_mean_to_be_between
from app.validators.expect_column_median_to_be_between import validate_column_median_to_be_between
from app.validators.expect_column_sum_to_be_between import validate_column_sum_to_be_between
from app.validators.expect_column_min_to_be_between import validate_column_min_to_be_between
from app.validators.expect_column_max_to_be_between import validate_column_max_to_be_between
from app.validators.expect_column_proportion_of_unique_values_to_be_between import validate_column_proportion_of_unique_values_to_be_between
from app.validators.expect_column_values_to_be_of_type import validate_column_values_to_be_of_type
from app.validators.expect_column_values_to_be_in_type_list import validate_column_values_to_be_in_type_list
from app.validators.expect_column_values_to_be_dateutil_parseable import validate_column_values_to_be_dateutil_parseable
from app.validators.expect_column_values_to_match_like_pattern import validate_column_values_to_match_like_pattern
from app.validators.expect_column_values_to_not_match_like_pattern import validate_column_values_to_not_match_like_pattern
from app.validators.expect_column_values_to_be_boolean import validate_column_values_to_be_boolean
from app.validators.expect_column_values_to_be_none import validate_column_values_to_be_none
from app.validators.expect_column_values_to_not_be_none import validate_column_values_to_not_be_none
from app.validators.expect_column_value_lengths_to_equal import validate_column_value_lengths_to_equal
from app.validators.expect_column_values_to_be_positive import validate_column_values_to_be_positive
from app.validators.expect_column_values_to_be_less_than import validate_column_values_to_be_less_than
from app.validators.expect_column_values_to_be_greater_than import validate_column_values_to_be_greater_than
from app.validators.expect_column_values_to_be_increasing import validate_column_values_to_be_increasing
from app.validators.expect_column_values_to_be_decreasing import validate_column_values_to_be_decreasing
from app.validators.expect_column_pair_values_a_to_be_greater_than_b import validate_column_pair_values_a_to_be_greater_than_b
from app.validators.expect_column_pair_values_to_be_equal import validate_column_pair_values_to_be_equal
from app.validators.expect_table_columns_to_match_ordered_list import validate_table_columns_to_match_ordered_list
from app.validators.expect_table_column_count_to_be_between import validate_table_column_count_to_be_between
from app.validators.expect_table_row_count_to_equal import validate_table_row_count_to_equal
from app.validators.expect_table_row_count_to_be_between import validate_table_row_count_to_be_between
from app.validators.expect_table_custom_query_to_return_no_rows import validate_table_custom_query_to_return_no_rows
from app.validators.expect_column_to_exist import validate_column_to_exist
from app.validators.expect_column_values_to_be_valid_email import validate_column_values_to_be_valid_email
from app.validators.expect_column_values_to_be_valid_url import validate_column_values_to_be_valid_url
from app.validators.expect_column_values_to_be_valid_ipv4 import validate_column_values_to_be_valid_ipv4
from app.validators.expect_column_values_to_be_valid_credit_card_number import validate_column_values_to_be_valid_credit_card_number
from app.validators.expect_column_values_to_be_after import validate_column_values_to_be_after
from app.validators.expect_column_values_to_be_before import validate_column_values_to_be_before
from app.validators.expect_column_values_to_be_between_dates import validate_column_values_to_be_between_dates


# Registry mapping rule names to their validation functions
VALIDATOR_REGISTRY: Dict[str, Callable[[List[Dict[str, Any]], Rule], Dict[str, Any]]] = {
    "ExpectColumnDistinctValuesToBeInSet": validate_column_distinct_values_to_be_in_set,
    "ExpectColumnValuesToBeInSet": validate_column_values_to_be_in_set,
    "ExpectColumnValuesToNotBeInSet": validate_column_values_to_not_be_in_set,
    "ExpectColumnValuesToBeBetween": validate_column_values_to_be_between,
    "ExpectColumnValueLengthsToBeBetween": validate_column_value_lengths_to_be_between,
    "ExpectColumnValuesToMatchRegex": validate_column_values_to_match_regex,
    "ExpectColumnValuesToNotMatchRegex": validate_column_values_to_not_match_regex,
    "ExpectColumnValuesToMatchStrftimeFormat": validate_column_values_to_match_strftime_format,
    "ExpectColumnValuesToBeUnique": validate_column_values_to_be_unique,
    "ExpectCompoundColumnsToBeUnique": validate_compound_columns_to_be_unique,
    "ExpectColumnMeanToBeBetween": validate_column_mean_to_be_between,
    "ExpectColumnMedianToBeBetween": validate_column_median_to_be_between,
    "ExpectColumnSumToBeBetween": validate_column_sum_to_be_between,
    "ExpectColumnMinToBeBetween": validate_column_min_to_be_between,
    "ExpectColumnMaxToBeBetween": validate_column_max_to_be_between,
    "ExpectColumnProportionOfUniqueValuesToBeBetween": validate_column_proportion_of_unique_values_to_be_between,
    "ExpectColumnValuesToBeOfType": validate_column_values_to_be_of_type,
    "ExpectColumnValuesToBeInTypeList": validate_column_values_to_be_in_type_list,
    "ExpectColumnValuesToBeDateutilParseable": validate_column_values_to_be_dateutil_parseable,
    "ExpectColumnValuesToMatchLikePattern": validate_column_values_to_match_like_pattern,
    "ExpectColumnValuesToNotMatchLikePattern": validate_column_values_to_not_match_like_pattern,
    "ExpectColumnValuesToBeBoolean": validate_column_values_to_be_boolean,
    "ExpectColumnValuesToBeNone": validate_column_values_to_be_none,
    "ExpectColumnValuesToNotBeNone": validate_column_values_to_not_be_none,
    "ExpectColumnValueLengthsToEqual": validate_column_value_lengths_to_equal,
    "ExpectColumnValuesToBePositive": validate_column_values_to_be_positive,
    "ExpectColumnValuesToBeLessThan": validate_column_values_to_be_less_than,
    "ExpectColumnValuesToBeGreaterThan": validate_column_values_to_be_greater_than,
    "ExpectColumnValuesToBeIncreasing": validate_column_values_to_be_increasing,
    "ExpectColumnValuesToBeDecreasing": validate_column_values_to_be_decreasing,
    "ExpectColumnPairValuesAToBeGreaterThanB": validate_column_pair_values_a_to_be_greater_than_b,
    "ExpectColumnPairValuesToBeEqual": validate_column_pair_values_to_be_equal,
    "ExpectTableColumnsToMatchOrderedList": validate_table_columns_to_match_ordered_list,
    "ExpectTableColumnCountToBeBetween": validate_table_column_count_to_be_between,
    "ExpectTableRowCountToEqual": validate_table_row_count_to_equal,
    "ExpectTableRowCountToBeBetween": validate_table_row_count_to_be_between,
    "ExpectTableCustomQueryToReturnNoRows": validate_table_custom_query_to_return_no_rows,
    "ExpectColumnToExist": validate_column_to_exist,
    "ExpectColumnValuesToBeValidEmail": validate_column_values_to_be_valid_email,
    "ExpectColumnValuesToBeValidUrl": validate_column_values_to_be_valid_url,
    "ExpectColumnValuesToBeValidIPv4": validate_column_values_to_be_valid_ipv4,
    "ExpectColumnValuesToBeValidCreditCardNumber": validate_column_values_to_be_valid_credit_card_number,
    "ExpectColumnValuesToBeAfter": validate_column_values_to_be_after,
    "ExpectColumnValuesToBeBefore": validate_column_values_to_be_before,
    "ExpectColumnValuesToBeBetweenDates": validate_column_values_to_be_between_dates,
}


def get_validator(rule_name: str) -> Callable[[List[Dict[str, Any]], Rule], Dict[str, Any]]:
    """
    Get the validator function for a given rule name.
    
    Args:
        rule_name: Name of the expectation rule
        
    Returns:
        Validator function for the rule
        
    Raises:
        ValueError: If no validator is found for the rule name
    """
    if rule_name not in VALIDATOR_REGISTRY:
        raise ValueError(f"No validator found for rule: {rule_name}")
    
    return VALIDATOR_REGISTRY[rule_name]


def get_available_validators() -> List[str]:
    """
    Get a list of all available validator rule names.
    
    Returns:
        List of available rule names
    """
    return list(VALIDATOR_REGISTRY.keys())


def validate_rule(data: List[Dict[str, Any]], rule: Rule) -> Dict[str, Any]:
    """
    Validate data against a single rule.
    
    Args:
        data: List of dictionaries representing the dataset
        rule: Rule object to validate against
        
    Returns:
        Dict containing validation results
    """
    try:
        validator = get_validator(rule.rule_name)
        return validator(data, rule)
    except ValueError as e:
        return {
            "rule_name": rule.rule_name,
            "column_name": rule.column_name,
            "success": False,
            "error": str(e)
        }
    except Exception as e:
        return {
            "rule_name": rule.rule_name,
            "column_name": rule.column_name,
            "success": False,
            "error": f"Unexpected error during validation: {str(e)}"
        }
