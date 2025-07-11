from typing import List, Dict, Any
from app.models.rule import Rule

# Sample data for expectation rules
expectation_rules_data = [
    {
        "rule_name": "ExpectColumnDistinctValuesToBeInSet",
        "column_name": "test",
        "value": [1, 2, 3, 4, 5]
    },
    {
        "rule_name": "ExpectColumnValuesToBeInSet",
        "column_name": "test",
        "value": ["value1", "value2"]
    },
    {
        "rule_name": "ExpectColumnValuesToNotBeInSet",
        "column_name": "test",
        "value": ["invalid1", "invalid2"]
    },
    {
        "rule_name": "ExpectColumnValuesToBeBetween",
        "column_name": "age",
        "value": {"min_value": 0, "max_value": 100}
    },
    {
        "rule_name": "ExpectColumnValueLengthsToBeBetween",
        "column_name": "description",
        "value": {"min_value": 5, "max_value": 500}
    },
    {
        "rule_name": "ExpectColumnValuesToMatchRegex",
        "column_name": "phone",
        "value": {"regex": "^\\d{3}-\\d{3}-\\d{4}$"}
    },
    {
        "rule_name": "ExpectColumnValuesToNotMatchRegex",
        "column_name": "ssn",
        "value": {"regex": "\\d{3}-\\d{2}-\\d{4}"}
    },
    {
        "rule_name": "ExpectColumnValuesToMatchStrftimeFormat",
        "column_name": "date",
        "value": {"strftime_format": "%Y-%m-%d"}
    },
    {
        "rule_name": "ExpectColumnValuesToBeUnique",
        "column_name": "id"
    },
    {
        "rule_name": "ExpectCompoundColumnsToBeUnique",
        "column_name": None,
        "value": {"column_list": ["first_name", "last_name", "dob"]}
    },
    {
        "rule_name": "ExpectColumnMeanToBeBetween",
        "column_name": "salary",
        "value": {"min_value": 50000, "max_value": 100000}
    },
    {
        "rule_name": "ExpectColumnMedianToBeBetween",
        "column_name": "price",
        "value": {"min_value": 20, "max_value": 50}
    },
    {
        "rule_name": "ExpectColumnSumToBeBetween",
        "column_name": "inventory",
        "value": {"min_value": 1000, "max_value": 2000}
    },
    {
        "rule_name": "ExpectColumnMinToBeBetween",
        "column_name": "temperature",
        "value": {"min_value": -10, "max_value": 0}
    },
    {
        "rule_name": "ExpectColumnMaxToBeBetween",
        "column_name": "score",
        "value": {"min_value": 90, "max_value": 100}
    },
    {
        "rule_name": "ExpectColumnProportionOfUniqueValuesToBeBetween",
        "column_name": "user_id",
        "value": {"min_value": 0.95, "max_value": 1.0}
    },
    {
        "rule_name": "ExpectColumnValuesToBeOfType",
        "column_name": "count",
        "value": {"type_": "INTEGER"}
    },
    {
        "rule_name": "ExpectColumnValuesToBeInTypeList",
        "column_name": "comment",
        "value": {"type_list": ["VARCHAR", "TEXT"]}
    },
    {
        "rule_name": "ExpectColumnValuesToBeDateutilParseable",
        "column_name": "event_time"
    },
    {
        "rule_name": "ExpectColumnValuesToMatchLikePattern",
        "column_name": "product_code",
        "value": {"like_pattern": "PROD_%"}
    },
    {
        "rule_name": "ExpectColumnValuesToNotMatchLikePattern",
        "column_name": "internal_code",
        "value": {"like_pattern": "TEST_%"}
    },
    {
        "rule_name": "ExpectColumnValuesToBeBoolean",
        "column_name": "is_active"
    },
    {
        "rule_name": "ExpectColumnValuesToBeNone",
        "column_name": "optional_field"
    },
    {
        "rule_name": "ExpectColumnValuesToNotBeNone",
        "column_name": "required_field"
    },
    {
        "rule_name": "ExpectColumnValueLengthsToEqual",
        "column_name": "zip_code",
        "value": {"value": 5}
    },
    {
        "rule_name": "ExpectColumnValuesToBePositive",
        "column_name": "quantity"
    },
    {
        "rule_name": "ExpectColumnValuesToBeLessThan",
        "column_name": "age",
        "value": {"value": 120}
    },
    {
        "rule_name": "ExpectColumnValuesToBeGreaterThan",
        "column_name": "salary",
        "value": {"value": 0}
    },
    {
        "rule_name": "ExpectColumnValuesToBeIncreasing",
        "column_name": "timestamp"
    },
    {
        "rule_name": "ExpectColumnValuesToBeDecreasing",
        "column_name": "depreciation"
    },
    {
        "rule_name": "ExpectColumnPairValuesAToBeGreaterThanB",
        "column_name": None,
        "value": {"column_A": "revenue", "column_B": "cost", "or_equal": True}
    },
    {
        "rule_name": "ExpectColumnPairValuesToBeEqual",
        "column_name": None,
        "value": {"column_A": "id", "column_B": "reference_id"}
    },
    {
        "rule_name": "ExpectTableColumnsToMatchOrderedList",
        "column_name": None,
        "value": {"column_list": ["id", "name", "date"]}
    },
    {
        "rule_name": "ExpectTableColumnCountToBeBetween",
        "column_name": None,
        "value": {"min_value": 5, "max_value": 10}
    },
    {
        "rule_name": "ExpectTableRowCountToEqual",
        "column_name": None,
        "value": {"value": 10000}
    },
    {
        "rule_name": "ExpectTableRowCountToBeBetween",
        "column_name": None,
        "value": {"min_value": 9000, "max_value": 11000}
    },
    {
        "rule_name": "ExpectTableCustomQueryToReturnNoRows",
        "column_name": None,
        "value": {"query": "SELECT * FROM table WHERE total < 0"}
    },
    {
        "rule_name": "ExpectColumnToExist",
        "column_name": "required_column"
    },
    {
        "rule_name": "ExpectColumnValuesToBeValidEmail",
        "column_name": "email"
    },
    {
        "rule_name": "ExpectColumnValuesToBeValidUrl",
        "column_name": "website"
    },
    {
        "rule_name": "ExpectColumnValuesToBeValidIPv4",
        "column_name": "ip_address"
    },
    {
        "rule_name": "ExpectColumnValuesToBeValidCreditCardNumber",
        "column_name": "credit_card"
    },
    {
        "rule_name": "ExpectColumnValuesToBeAfter",
        "column_name": "event_date",
        "value": {"min_date": "2023-01-01"}
    },
    {
        "rule_name": "ExpectColumnValuesToBeBefore",
        "column_name": "expiry_date",
        "value": {"max_date": "2025-12-31"}
    },
    {
        "rule_name": "ExpectColumnValuesToBeBetweenDates",
        "column_name": "order_date",
        "value": {"min_date": "2023-01-01", "max_date": "2023-12-31"}
    }
]

def get_all_expectation_rules() -> List[Rule]:
    return expectation_rules_data