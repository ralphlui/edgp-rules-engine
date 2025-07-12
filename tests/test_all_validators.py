"""
Comprehensive test cases for all validators in the rules engine.
Tests both success and failure scenarios for each validator.
"""
import sys
import os
sys.path.insert(0, os.path.abspath('.'))

from app.models.rule import Rule
from app.validators.validator_registry import get_validator

def test_validator(rule_name, test_data, rule_params, expected_success=True, description=""):
    """Helper function to test a validator"""
    print(f"\n--- Testing {rule_name} ---")
    print(f"Description: {description}")
    
    try:
        # Create rule
        rule = Rule(rule_name=rule_name, **rule_params)
        
        # Get validator
        validator = get_validator(rule_name)
        
        # Run validation
        result = validator(test_data, rule)
        
        print(f"Expected Success: {expected_success}")
        print(f"Actual Success: {result['success']}")
        print(f"Rule: {result.get('rule_name', 'N/A')}")
        print(f"Column: {result.get('column_name', 'N/A')}")
        
        if result['success']:
            print(f"✅ Message: {result.get('message', 'Validation passed')}")
        else:
            print(f"❌ Error: {result.get('error', 'Validation failed')}")
        
        # Check if result matches expectation
        if result['success'] == expected_success:
            print("✅ Test PASSED")
        else:
            print("❌ Test FAILED - unexpected result")
            
    except Exception as e:
        print(f"❌ Test FAILED with exception: {e}")

def run_all_tests():
    """Run comprehensive tests for all validators"""
    
    print("="*80)
    print("COMPREHENSIVE VALIDATOR TESTS")
    print("="*80)
    
    # Test data sets
    basic_data = [
        {'id': 1, 'name': 'John', 'age': 25, 'email': 'john@example.com', 'active': True},
        {'id': 2, 'name': 'Jane', 'age': 30, 'email': 'jane@example.com', 'active': False},
        {'id': 3, 'name': 'Bob', 'age': 35, 'email': 'bob@example.com', 'active': True}
    ]
    
    numeric_data = [
        {'score': 85, 'temperature': 22.5, 'count': 100},
        {'score': 92, 'temperature': 24.0, 'count': 150},
        {'score': 78, 'temperature': 21.8, 'count': 120}
    ]
    
    string_data = [
        {'code': 'ABC123', 'description': 'Product description here', 'date': '2023-01-15'},
        {'code': 'DEF456', 'description': 'Another product description', 'date': '2023-02-20'},
        {'code': 'GHI789', 'description': 'Third product description', 'date': '2023-03-10'}
    ]
    
    # 1. Test ExpectColumnToExist
    test_validator("ExpectColumnToExist", basic_data, 
                  {"column_name": "id"}, True, "Column exists")
    test_validator("ExpectColumnToExist", basic_data, 
                  {"column_name": "missing_col"}, False, "Column does not exist")
    
    # 2. Test ExpectColumnValuesToBeInSet
    test_validator("ExpectColumnValuesToBeInSet", basic_data,
                  {"column_name": "name", "value": ["John", "Jane", "Bob"]}, True, "All names in allowed set")
    test_validator("ExpectColumnValuesToBeInSet", basic_data,
                  {"column_name": "name", "value": ["John", "Jane"]}, False, "Bob not in allowed set")
    
    # 3. Test ExpectColumnValuesToNotBeInSet
    test_validator("ExpectColumnValuesToNotBeInSet", basic_data,
                  {"column_name": "name", "value": ["Alice", "Charlie"]}, True, "No forbidden names found")
    test_validator("ExpectColumnValuesToNotBeInSet", basic_data,
                  {"column_name": "name", "value": ["John", "Alice"]}, False, "John is forbidden")
    
    # 4. Test ExpectColumnValuesToBeBetween
    test_validator("ExpectColumnValuesToBeBetween", basic_data,
                  {"column_name": "age", "value": {"min_value": 20, "max_value": 40}}, True, "Ages in range")
    test_validator("ExpectColumnValuesToBeBetween", basic_data,
                  {"column_name": "age", "value": {"min_value": 30, "max_value": 40}}, False, "Some ages below 30")
    
    # 5. Test ExpectColumnValuesToBeGreaterThan
    test_validator("ExpectColumnValuesToBeGreaterThan", basic_data,
                  {"column_name": "age", "value": {"value": 20}}, True, "All ages > 20")
    test_validator("ExpectColumnValuesToBeGreaterThan", basic_data,
                  {"column_name": "age", "value": {"value": 30}}, False, "Some ages <= 30")
    
    # 6. Test ExpectColumnValuesToBeLessThan
    test_validator("ExpectColumnValuesToBeLessThan", basic_data,
                  {"column_name": "age", "value": {"value": 40}}, True, "All ages < 40")
    test_validator("ExpectColumnValuesToBeLessThan", basic_data,
                  {"column_name": "age", "value": {"value": 30}}, False, "Some ages >= 30")
    
    # 7. Test ExpectColumnValuesToBePositive
    test_validator("ExpectColumnValuesToBePositive", basic_data,
                  {"column_name": "age"}, True, "All ages positive")
    
    # 8. Test ExpectColumnValuesToBeUnique
    test_validator("ExpectColumnValuesToBeUnique", basic_data,
                  {"column_name": "id"}, True, "All IDs unique")
    test_validator("ExpectColumnValuesToBeUnique", 
                  [{'id': 1}, {'id': 1}, {'id': 2}],
                  {"column_name": "id"}, False, "Duplicate IDs")
    
    # 9. Test ExpectColumnValuesToBeValidEmail
    test_validator("ExpectColumnValuesToBeValidEmail", basic_data,
                  {"column_name": "email"}, True, "All emails valid")
    test_validator("ExpectColumnValuesToBeValidEmail",
                  [{'email': 'invalid-email'}, {'email': 'test@example.com'}],
                  {"column_name": "email"}, False, "Some emails invalid")
    
    # 10. Test ExpectColumnValuesToBeBoolean  
    test_validator("ExpectColumnValuesToBeBoolean", basic_data,
                  {"column_name": "active"}, True, "All values boolean")
    
    # 11. Test ExpectColumnMeanToBeBetween
    test_validator("ExpectColumnMeanToBeBetween", numeric_data,
                  {"column_name": "score", "value": {"min_value": 80, "max_value": 90}}, True, "Mean in range")
    test_validator("ExpectColumnMeanToBeBetween", numeric_data,
                  {"column_name": "score", "value": {"min_value": 90, "max_value": 100}}, False, "Mean below range")
    
    # 12. Test ExpectColumnMaxToBeBetween
    test_validator("ExpectColumnMaxToBeBetween", numeric_data,
                  {"column_name": "score", "value": {"min_value": 90, "max_value": 100}}, True, "Max in range")
    
    # 13. Test ExpectColumnMinToBeBetween
    test_validator("ExpectColumnMinToBeBetween", numeric_data,
                  {"column_name": "score", "value": {"min_value": 70, "max_value": 80}}, True, "Min in range")
    
    # 14. Test ExpectColumnSumToBeBetween
    test_validator("ExpectColumnSumToBeBetween", numeric_data,
                  {"column_name": "count", "value": {"min_value": 350, "max_value": 400}}, True, "Sum in range")
    
    # 15. Test ExpectColumnValueLengthsToBeBetween
    test_validator("ExpectColumnValueLengthsToBeBetween", string_data,
                  {"column_name": "description", "value": {"min_value": 10, "max_value": 50}}, True, "Description lengths OK")
    
    # 16. Test ExpectColumnValueLengthsToEqual
    test_validator("ExpectColumnValueLengthsToEqual", string_data,
                  {"column_name": "code", "value": {"value": 6}}, True, "All codes 6 chars")
    
    # 17. Test ExpectTableRowCountToBeBetween
    test_validator("ExpectTableRowCountToBeBetween", basic_data,
                  {"column_name": None, "value": {"min_value": 2, "max_value": 5}}, True, "Row count in range")
    
    # 18. Test ExpectTableRowCountToEqual
    test_validator("ExpectTableRowCountToEqual", basic_data,
                  {"column_name": None, "value": {"value": 3}}, True, "Exactly 3 rows")
    test_validator("ExpectTableRowCountToEqual", basic_data,
                  {"column_name": None, "value": {"value": 5}}, False, "Not 5 rows")
    
    # 19. Test ExpectColumnValuesToMatchRegex
    test_validator("ExpectColumnValuesToMatchRegex", string_data,
                  {"column_name": "code", "value": {"regex": "^[A-Z]{3}[0-9]{3}$"}}, True, "Codes match pattern")
    test_validator("ExpectColumnValuesToMatchRegex", basic_data,
                  {"column_name": "name", "value": {"regex": "^[0-9]+$"}}, False, "Names don't match number pattern")
    
    # 20. Test ExpectColumnValuesToNotMatchRegex
    test_validator("ExpectColumnValuesToNotMatchRegex", basic_data,
                  {"column_name": "name", "value": {"regex": "^[0-9]+$"}}, True, "Names don't match number pattern")
    test_validator("ExpectColumnValuesToNotMatchRegex", string_data,
                  {"column_name": "code", "value": {"regex": "^[A-Z]{3}[0-9]{3}$"}}, False, "Codes do match pattern")
    
    # 21. Test ExpectColumnValuesToMatchStrftimeFormat
    test_validator("ExpectColumnValuesToMatchStrftimeFormat", string_data,
                  {"column_name": "date", "value": {"strftime_format": "%Y-%m-%d"}}, True, "Dates match format")
    
    # 22. Test ExpectColumnDistinctValuesToBeInSet
    test_validator("ExpectColumnDistinctValuesToBeInSet", basic_data,
                  {"column_name": "name", "value": ["John", "Jane", "Bob", "Alice"]}, True, "All distinct names in set")
    test_validator("ExpectColumnDistinctValuesToBeInSet", basic_data,
                  {"column_name": "name", "value": ["John", "Jane"]}, False, "Bob not in allowed set")
    
    # 23. Test ExpectColumnValuesToBeOfType
    test_validator("ExpectColumnValuesToBeOfType", basic_data,
                  {"column_name": "age", "value": {"type_": "int"}}, True, "Ages are integers")
    test_validator("ExpectColumnValuesToBeOfType", basic_data,
                  {"column_name": "name", "value": {"type_": "int"}}, False, "Names are not integers")
    
    # 24. Test ExpectColumnValuesToBeInTypeList
    test_validator("ExpectColumnValuesToBeInTypeList", basic_data,
                  {"column_name": "name", "value": {"type_list": ["str", "object"]}}, True, "Names are strings")
    
    # 25. Test ExpectColumnValuesToNotBeNone
    test_validator("ExpectColumnValuesToNotBeNone", basic_data,
                  {"column_name": "name"}, True, "No null names")
    test_validator("ExpectColumnValuesToNotBeNone", 
                  [{'name': 'John'}, {'name': None}, {'name': 'Jane'}],
                  {"column_name": "name"}, False, "Contains null name")
    
    # 26. Test ExpectColumnValuesToBeNone
    test_validator("ExpectColumnValuesToBeNone", 
                  [{'optional': None}, {'optional': None}, {'optional': None}],
                  {"column_name": "optional"}, True, "All values are null")
    test_validator("ExpectColumnValuesToBeNone", basic_data,
                  {"column_name": "name"}, False, "Names are not null")
    
    # 27. Test ExpectCompoundColumnsToBeUnique
    test_validator("ExpectCompoundColumnsToBeUnique", 
                  [{'first': 'John', 'last': 'Doe'}, {'first': 'Jane', 'last': 'Smith'}, {'first': 'John', 'last': 'Smith'}],
                  {"column_name": None, "value": {"column_list": ["first", "last"]}}, True, "Compound keys unique")
    test_validator("ExpectCompoundColumnsToBeUnique", 
                  [{'first': 'John', 'last': 'Doe'}, {'first': 'John', 'last': 'Doe'}, {'first': 'Jane', 'last': 'Smith'}],
                  {"column_name": None, "value": {"column_list": ["first", "last"]}}, False, "Duplicate compound keys")
    
    # 28. Test ExpectColumnMedianToBeBetween
    test_validator("ExpectColumnMedianToBeBetween", numeric_data,
                  {"column_name": "score", "value": {"min_value": 80, "max_value": 90}}, True, "Median in range")
    test_validator("ExpectColumnMedianToBeBetween", numeric_data,
                  {"column_name": "score", "value": {"min_value": 90, "max_value": 100}}, False, "Median below range")
    
    # 29. Test ExpectColumnProportionOfUniqueValuesToBeBetween
    test_validator("ExpectColumnProportionOfUniqueValuesToBeBetween", basic_data,
                  {"column_name": "id", "value": {"min_value": 0.9, "max_value": 1.0}}, True, "High uniqueness")
    test_validator("ExpectColumnProportionOfUniqueValuesToBeBetween", 
                  [{'category': 'A'}, {'category': 'A'}, {'category': 'B'}],
                  {"column_name": "category", "value": {"min_value": 0.8, "max_value": 1.0}}, False, "Low uniqueness")
    
    # 30. Test ExpectColumnValuesToBeDateutilParseable
    test_validator("ExpectColumnValuesToBeDateutilParseable", string_data,
                  {"column_name": "date"}, True, "Dates are parseable")
    test_validator("ExpectColumnValuesToBeDateutilParseable", 
                  [{'bad_date': 'not-a-date'}, {'bad_date': '2023-99-99'}],
                  {"column_name": "bad_date"}, False, "Dates not parseable")
    
    # 31. Test ExpectColumnValuesToMatchLikePattern
    test_validator("ExpectColumnValuesToMatchLikePattern", string_data,
                  {"column_name": "code", "value": {"like_pattern": "___123"}}, False, "Codes don't match LIKE pattern")
    test_validator("ExpectColumnValuesToMatchLikePattern", 
                  [{'product': 'PROD_001'}, {'product': 'PROD_002'}, {'product': 'PROD_003'}],
                  {"column_name": "product", "value": {"like_pattern": "PROD_%"}}, True, "Products match LIKE pattern")
    
    # 32. Test ExpectColumnValuesToNotMatchLikePattern
    test_validator("ExpectColumnValuesToNotMatchLikePattern", basic_data,
                  {"column_name": "name", "value": {"like_pattern": "TEST_%"}}, True, "Names don't match test pattern")
    test_validator("ExpectColumnValuesToNotMatchLikePattern", 
                  [{'internal': 'TEST_001'}, {'internal': 'TEST_002'}],
                  {"column_name": "internal", "value": {"like_pattern": "TEST_%"}}, False, "Internals match test pattern")
    
    # 33. Test ExpectColumnValuesToBeIncreasing
    test_validator("ExpectColumnValuesToBeIncreasing", 
                  [{'seq': 1}, {'seq': 2}, {'seq': 3}],
                  {"column_name": "seq"}, True, "Sequence is increasing")
    test_validator("ExpectColumnValuesToBeIncreasing", 
                  [{'seq': 3}, {'seq': 1}, {'seq': 2}],
                  {"column_name": "seq"}, False, "Sequence not increasing")
    
    # 34. Test ExpectColumnValuesToBeDecreasing
    test_validator("ExpectColumnValuesToBeDecreasing", 
                  [{'seq': 3}, {'seq': 2}, {'seq': 1}],
                  {"column_name": "seq"}, True, "Sequence is decreasing")
    test_validator("ExpectColumnValuesToBeDecreasing", 
                  [{'seq': 1}, {'seq': 2}, {'seq': 3}],
                  {"column_name": "seq"}, False, "Sequence not decreasing")
    
    # 35. Test ExpectColumnPairValuesAToBeGreaterThanB
    test_validator("ExpectColumnPairValuesAToBeGreaterThanB", 
                  [{'revenue': 100, 'cost': 80}, {'revenue': 200, 'cost': 150}],
                  {"column_name": None, "value": {"column_A": "revenue", "column_B": "cost"}}, True, "Revenue > Cost")
    test_validator("ExpectColumnPairValuesAToBeGreaterThanB", 
                  [{'revenue': 80, 'cost': 100}, {'revenue': 200, 'cost': 150}],
                  {"column_name": None, "value": {"column_A": "revenue", "column_B": "cost"}}, False, "Some revenue <= cost")
    
    # 36. Test ExpectColumnPairValuesToBeEqual
    test_validator("ExpectColumnPairValuesToBeEqual", 
                  [{'id': 1, 'ref_id': 1}, {'id': 2, 'ref_id': 2}],
                  {"column_name": None, "value": {"column_A": "id", "column_B": "ref_id"}}, True, "IDs match references")
    test_validator("ExpectColumnPairValuesToBeEqual", 
                  [{'id': 1, 'ref_id': 2}, {'id': 2, 'ref_id': 1}],
                  {"column_name": None, "value": {"column_A": "id", "column_B": "ref_id"}}, False, "IDs don't match references")
    
    # 37. Test ExpectTableColumnsToMatchOrderedList
    test_validator("ExpectTableColumnsToMatchOrderedList", basic_data,
                  {"column_name": None, "value": {"column_list": ["id", "name", "age", "email", "active"]}}, True, "Columns match order")
    test_validator("ExpectTableColumnsToMatchOrderedList", basic_data,
                  {"column_name": None, "value": {"column_list": ["name", "id", "age"]}}, False, "Columns don't match order")
    
    # 38. Test ExpectTableColumnCountToBeBetween
    test_validator("ExpectTableColumnCountToBeBetween", basic_data,
                  {"column_name": None, "value": {"min_value": 4, "max_value": 6}}, True, "Column count in range")
    test_validator("ExpectTableColumnCountToBeBetween", basic_data,
                  {"column_name": None, "value": {"min_value": 10, "max_value": 15}}, False, "Column count not in range")
    
    # 39. Test ExpectTableCustomQueryToReturnNoRows
    test_validator("ExpectTableCustomQueryToReturnNoRows", basic_data,
                  {"column_name": None, "value": {"query": "SELECT * FROM table WHERE age < 0"}}, True, "Query returns no rows")
    
    # 40. Test ExpectColumnValuesToBeValidUrl
    test_validator("ExpectColumnValuesToBeValidUrl", 
                  [{'website': 'https://example.com'}, {'website': 'http://test.org'}],
                  {"column_name": "website"}, True, "Valid URLs")
    test_validator("ExpectColumnValuesToBeValidUrl", 
                  [{'website': 'not-a-url'}, {'website': 'invalid://url'}],
                  {"column_name": "website"}, False, "Invalid URLs")
    
    # 41. Test ExpectColumnValuesToBeValidIPv4
    test_validator("ExpectColumnValuesToBeValidIPv4", 
                  [{'ip': '192.168.1.1'}, {'ip': '10.0.0.1'}],
                  {"column_name": "ip"}, True, "Valid IPv4 addresses")
    test_validator("ExpectColumnValuesToBeValidIPv4", 
                  [{'ip': '999.999.999.999'}, {'ip': 'not-an-ip'}],
                  {"column_name": "ip"}, False, "Invalid IPv4 addresses")
    
    # 42. Test ExpectColumnValuesToBeValidCreditCardNumber
    test_validator("ExpectColumnValuesToBeValidCreditCardNumber", 
                  [{'card': '4111111111111111'}, {'card': '5555555555554444'}],
                  {"column_name": "card"}, True, "Valid credit card numbers")
    test_validator("ExpectColumnValuesToBeValidCreditCardNumber", 
                  [{'card': '1234567890123456'}, {'card': 'not-a-card'}],
                  {"column_name": "card"}, False, "Invalid credit card numbers")
    
    # 43. Test ExpectColumnValuesToBeAfter
    test_validator("ExpectColumnValuesToBeAfter", 
                  [{'event_date': '2024-01-15'}, {'event_date': '2024-02-20'}],
                  {"column_name": "event_date", "value": {"min_date": "2023-12-31"}}, True, "Dates after minimum")
    test_validator("ExpectColumnValuesToBeAfter", 
                  [{'event_date': '2022-01-15'}, {'event_date': '2024-02-20'}],
                  {"column_name": "event_date", "value": {"min_date": "2023-01-01"}}, False, "Some dates before minimum")
    
    # 44. Test ExpectColumnValuesToBeBefore
    test_validator("ExpectColumnValuesToBeBefore", 
                  [{'expiry_date': '2024-01-15'}, {'expiry_date': '2024-02-20'}],
                  {"column_name": "expiry_date", "value": {"max_date": "2025-12-31"}}, True, "Dates before maximum")
    test_validator("ExpectColumnValuesToBeBefore", 
                  [{'expiry_date': '2026-01-15'}, {'expiry_date': '2024-02-20'}],
                  {"column_name": "expiry_date", "value": {"max_date": "2025-12-31"}}, False, "Some dates after maximum")
    
    # 45. Test ExpectColumnValuesToBeBetweenDates
    test_validator("ExpectColumnValuesToBeBetweenDates", 
                  [{'order_date': '2023-06-15'}, {'order_date': '2023-08-20'}],
                  {"column_name": "order_date", "value": {"min_date": "2023-01-01", "max_date": "2023-12-31"}}, True, "Dates in range")
    test_validator("ExpectColumnValuesToBeBetweenDates", 
                  [{'order_date': '2022-06-15'}, {'order_date': '2024-08-20'}],
                  {"column_name": "order_date", "value": {"min_date": "2023-01-01", "max_date": "2023-12-31"}}, False, "Dates outside range")
    
    print("\n" + "="*80)
    print(f"TEST SUMMARY COMPLETE - Tested all 45 validators!")
    print("="*80)

if __name__ == "__main__":
    run_all_tests()
