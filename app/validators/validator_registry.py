from typing import Dict, Callable, List, Any
from app.models.rule import Rule
import importlib

# Lazy import mapping - validators are imported only when needed
VALIDATOR_MAPPING = {
    "expect_column_distinct_values_to_be_in_set": ("app.validators.expect_column_distinct_values_to_be_in_set", "validate_column_distinct_values_to_be_in_set"),
    "expect_column_values_to_be_in_set": ("app.validators.expect_column_values_to_be_in_set", "validate_column_values_to_be_in_set"),
    "expect_column_values_to_not_be_in_set": ("app.validators.expect_column_values_to_not_be_in_set", "validate_column_values_to_not_be_in_set"),
    "expect_column_values_to_be_between": ("app.validators.expect_column_values_to_be_between", "validate_column_values_to_be_between"),
    "expect_column_value_lengths_to_be_between": ("app.validators.expect_column_value_lengths_to_be_between", "validate_column_value_lengths_to_be_between"),
    "expect_column_values_to_match_regex": ("app.validators.expect_column_values_to_match_regex", "validate_column_values_to_match_regex"),
    "expect_column_values_to_not_match_regex": ("app.validators.expect_column_values_to_not_match_regex", "validate_column_values_to_not_match_regex"),
    "expect_column_values_to_match_strftime_format": ("app.validators.expect_column_values_to_match_strftime_format", "validate_column_values_to_match_strftime_format"),
    "expect_column_values_to_be_unique": ("app.validators.expect_column_values_to_be_unique", "validate_column_values_to_be_unique"),
    "expect_compound_columns_to_be_unique": ("app.validators.expect_compound_columns_to_be_unique", "validate_compound_columns_to_be_unique"),
    "expect_column_mean_to_be_between": ("app.validators.expect_column_mean_to_be_between", "validate_column_mean_to_be_between"),
    "expect_column_median_to_be_between": ("app.validators.expect_column_median_to_be_between", "validate_column_median_to_be_between"),
    "expect_column_sum_to_be_between": ("app.validators.expect_column_sum_to_be_between", "validate_column_sum_to_be_between"),
    "expect_column_min_to_be_between": ("app.validators.expect_column_min_to_be_between", "validate_column_min_to_be_between"),
    "expect_column_max_to_be_between": ("app.validators.expect_column_max_to_be_between", "validate_column_max_to_be_between"),
    "expect_column_proportion_of_unique_values_to_be_between": ("app.validators.expect_column_proportion_of_unique_values_to_be_between", "validate_column_proportion_of_unique_values_to_be_between"),
    "expect_column_values_to_be_of_type": ("app.validators.expect_column_values_to_be_of_type", "validate_column_values_to_be_of_type"),
    "expect_column_values_to_be_in_type_list": ("app.validators.expect_column_values_to_be_in_type_list", "validate_column_values_to_be_in_type_list"),
    "expect_column_values_to_be_dateutil_parseable": ("app.validators.expect_column_values_to_be_dateutil_parseable", "validate_column_values_to_be_dateutil_parseable"),
    "expect_column_values_to_match_like_pattern": ("app.validators.expect_column_values_to_match_like_pattern", "validate_column_values_to_match_like_pattern"),
    "expect_column_values_to_not_match_like_pattern": ("app.validators.expect_column_values_to_not_match_like_pattern", "validate_column_values_to_not_match_like_pattern"),
    "expect_column_values_to_be_boolean": ("app.validators.expect_column_values_to_be_boolean", "validate_column_values_to_be_boolean"),
    "expect_column_values_to_be_none": ("app.validators.expect_column_values_to_be_none", "validate_column_values_to_be_none"),
    "expect_column_values_to_not_be_none": ("app.validators.expect_column_values_to_not_be_none", "validate_column_values_to_not_be_none"),
    "expect_column_value_lengths_to_equal": ("app.validators.expect_column_value_lengths_to_equal", "validate_column_value_lengths_to_equal"),
    "expect_column_values_to_be_positive": ("app.validators.expect_column_values_to_be_positive", "validate_column_values_to_be_positive"),
    "expect_column_values_to_be_less_than": ("app.validators.expect_column_values_to_be_less_than", "validate_column_values_to_be_less_than"),
    "expect_column_values_to_be_greater_than": ("app.validators.expect_column_values_to_be_greater_than", "validate_column_values_to_be_greater_than"),
    "expect_column_values_to_be_increasing": ("app.validators.expect_column_values_to_be_increasing", "validate_column_values_to_be_increasing"),
    "expect_column_values_to_be_decreasing": ("app.validators.expect_column_values_to_be_decreasing", "validate_column_values_to_be_decreasing"),
    "expect_column_pair_values_a_to_be_greater_than_b": ("app.validators.expect_column_pair_values_a_to_be_greater_than_b", "validate_column_pair_values_a_to_be_greater_than_b"),
    "expect_column_pair_values_to_be_equal": ("app.validators.expect_column_pair_values_to_be_equal", "validate_column_pair_values_to_be_equal"),
    "expect_table_columns_to_match_ordered_list": ("app.validators.expect_table_columns_to_match_ordered_list", "validate_table_columns_to_match_ordered_list"),
    "expect_table_column_count_to_be_between": ("app.validators.expect_table_column_count_to_be_between", "validate_table_column_count_to_be_between"),
    "expect_table_row_count_to_equal": ("app.validators.expect_table_row_count_to_equal", "validate_table_row_count_to_equal"),
    "expect_table_row_count_to_be_between": ("app.validators.expect_table_row_count_to_be_between", "validate_table_row_count_to_be_between"),
    "expect_table_custom_query_to_return_no_rows": ("app.validators.expect_table_custom_query_to_return_no_rows", "validate_table_custom_query_to_return_no_rows"),
    "expect_column_to_exist": ("app.validators.expect_column_to_exist", "validate_column_to_exist"),
    "expect_column_values_to_be_valid_email": ("app.validators.expect_column_values_to_be_valid_email", "validate_column_values_to_be_valid_email"),
    "expect_column_values_to_be_valid_url": ("app.validators.expect_column_values_to_be_valid_url", "validate_column_values_to_be_valid_url"),
    "expect_column_values_to_be_valid_ipv4": ("app.validators.expect_column_values_to_be_valid_ipv4", "validate_column_values_to_be_valid_ipv4"),
    "expect_column_values_to_be_valid_credit_card_number": ("app.validators.expect_column_values_to_be_valid_credit_card_number", "validate_column_values_to_be_valid_credit_card_number"),
    "expect_column_values_to_be_after": ("app.validators.expect_column_values_to_be_after", "validate_column_values_to_be_after"),
    "expect_column_values_to_be_before": ("app.validators.expect_column_values_to_be_before", "validate_column_values_to_be_before"),
    "expect_column_values_to_be_between_dates": ("app.validators.expect_column_values_to_be_between_dates", "validate_column_values_to_be_between_dates"),
}

def _get_validator_function(rule_name: str) -> Callable:
    """Lazy-load validator function by rule name"""
    if rule_name not in VALIDATOR_MAPPING:
        raise ValueError(f"Unknown validation rule: {rule_name}")
    
    module_name, function_name = VALIDATOR_MAPPING[rule_name]
    try:
        module = importlib.import_module(module_name)
        return getattr(module, function_name)
    except ImportError as e:
        raise ImportError(f"Could not import validator {rule_name}: {e}")
    except AttributeError as e:
        raise AttributeError(f"Validator function {function_name} not found in {module_name}: {e}")


# Mapping old-style rule names to new-style rule names for backward compatibility
LEGACY_RULE_MAPPING = {
    "ExpectColumnDistinctValuesToBeInSet": "expect_column_distinct_values_to_be_in_set",
    "ExpectColumnValuesToBeInSet": "expect_column_values_to_be_in_set",
    "ExpectColumnValuesToNotBeInSet": "expect_column_values_to_not_be_in_set",
    "ExpectColumnValuesToBeBetween": "expect_column_values_to_be_between",
    "ExpectColumnValueLengthsToBeBetween": "expect_column_value_lengths_to_be_between",
    "ExpectColumnValuesToMatchRegex": "expect_column_values_to_match_regex",
    "ExpectColumnValuesToNotMatchRegex": "expect_column_values_to_not_match_regex",
    "ExpectColumnValuesToMatchStrftimeFormat": "expect_column_values_to_match_strftime_format",
    "ExpectColumnValuesToBeUnique": "expect_column_values_to_be_unique",
    "ExpectCompoundColumnsToBeUnique": "expect_compound_columns_to_be_unique",
    "ExpectColumnMeanToBeBetween": "expect_column_mean_to_be_between",
    "ExpectColumnMedianToBeBetween": "expect_column_median_to_be_between",
    "ExpectColumnSumToBeBetween": "expect_column_sum_to_be_between",
    "ExpectColumnMinToBeBetween": "expect_column_min_to_be_between",
    "ExpectColumnMaxToBeBetween": "expect_column_max_to_be_between",
    "ExpectColumnProportionOfUniqueValuesToBeBetween": "expect_column_proportion_of_unique_values_to_be_between",
    "ExpectColumnValuesToBeOfType": "expect_column_values_to_be_of_type",
    "ExpectColumnValuesToBeInTypeList": "expect_column_values_to_be_in_type_list",
    "ExpectColumnValuesToBeDateutilParseable": "expect_column_values_to_be_dateutil_parseable",
    "ExpectColumnValuesToMatchLikePattern": "expect_column_values_to_match_like_pattern",
    "ExpectColumnValuesToNotMatchLikePattern": "expect_column_values_to_not_match_like_pattern",
    "ExpectColumnValuesToBeBoolean": "expect_column_values_to_be_boolean",
    "ExpectColumnValuesToBeNone": "expect_column_values_to_be_none",
    "ExpectColumnValuesToNotBeNone": "expect_column_values_to_not_be_none",
    "ExpectColumnValueLengthsToEqual": "expect_column_value_lengths_to_equal",
    "ExpectColumnValuesToBePositive": "expect_column_values_to_be_positive",
    "ExpectColumnValuesToBeLessThan": "expect_column_values_to_be_less_than",
    "ExpectColumnValuesToBeGreaterThan": "expect_column_values_to_be_greater_than",
    "ExpectColumnValuesToBeIncreasing": "expect_column_values_to_be_increasing",
    "ExpectColumnValuesToBeDecreasing": "expect_column_values_to_be_decreasing",
    "ExpectColumnPairValuesAToBeGreaterThanB": "expect_column_pair_values_a_to_be_greater_than_b",
    "ExpectColumnPairValuesToBeEqual": "expect_column_pair_values_to_be_equal",
    "ExpectTableColumnsToMatchOrderedList": "expect_table_columns_to_match_ordered_list",
    "ExpectTableColumnCountToBeBetween": "expect_table_column_count_to_be_between",
    "ExpectTableRowCountToEqual": "expect_table_row_count_to_equal",
    "ExpectTableRowCountToBeBetween": "expect_table_row_count_to_be_between",
    "ExpectTableCustomQueryToReturnNoRows": "expect_table_custom_query_to_return_no_rows",
    "ExpectColumnToExist": "expect_column_to_exist",
    "ExpectColumnValuesToBeValidEmail": "expect_column_values_to_be_valid_email",
    "ExpectColumnValuesToBeValidUrl": "expect_column_values_to_be_valid_url",
    "ExpectColumnValuesToBeValidIPv4": "expect_column_values_to_be_valid_ipv4",
    "ExpectColumnValuesToBeValidCreditCardNumber": "expect_column_values_to_be_valid_credit_card_number",
    "ExpectColumnValuesToBeAfter": "expect_column_values_to_be_after",
    "ExpectColumnValuesToBeBefore": "expect_column_values_to_be_before",
    "ExpectColumnValuesToBeBetweenDates": "expect_column_values_to_be_between_dates",
}

# Cache for loaded validators to avoid repeated imports
_validator_cache = {}

def get_validator(rule_name: str) -> Callable[[List[Dict[str, Any]], Rule], Dict[str, Any]]:
    """
    Get the validator function for a given rule name with lazy loading.
    
    Args:
        rule_name: Name of the expectation rule
        
    Returns:
        Validator function for the rule
        
    Raises:
        ValueError: If no validator is found for the rule name
    """
    # Check if already cached
    if rule_name in _validator_cache:
        return _validator_cache[rule_name]
    
    # Handle legacy rule names
    normalized_rule_name = LEGACY_RULE_MAPPING.get(rule_name, rule_name)
    
    # Check cache again with normalized name
    if normalized_rule_name in _validator_cache:
        _validator_cache[rule_name] = _validator_cache[normalized_rule_name]  # Cache both names
        return _validator_cache[rule_name]
    
    # Load the validator function
    try:
        validator_func = _get_validator_function(normalized_rule_name)
        _validator_cache[rule_name] = validator_func
        _validator_cache[normalized_rule_name] = validator_func
        return validator_func
    except (ImportError, AttributeError, ValueError) as e:
        raise ValueError(f"No validator found for rule: {rule_name}. Error: {e}")


def get_available_validators() -> List[str]:
    """
    Get a list of all available validator rule names.
    
    Returns:
        List of available rule names
    """
    # Return both legacy and new-style rule names
    available = list(VALIDATOR_MAPPING.keys()) + list(LEGACY_RULE_MAPPING.keys())
    return sorted(set(available))


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
