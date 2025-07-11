import pytest
from app.models.rule import Rule

def test_rule_with_all_fields():
    rule = Rule(
        rule_name="ExpectColumnValuesToBeInSet",
        column_name="status",
        value=["active", "inactive"]
    )
    assert rule.rule_name == "ExpectColumnValuesToBeInSet"
    assert rule.column_name == "status"
    assert rule.value == ["active", "inactive"]

def test_rule_with_optional_fields_missing():
    rule = Rule(
        rule_name="ExpectColumnValuesToBeUnique"
    )
    assert rule.rule_name == "ExpectColumnValuesToBeUnique"
    assert rule.column_name is None
    assert rule.value is None

def test_rule_with_various_value_types():
    # Test with bool
    rule_bool = Rule(rule_name="TestBool", value=True)
    assert rule_bool.value is True

    # Test with set
    rule_set = Rule(rule_name="TestSet", value={1, 2, 3})
    assert rule_set.value == {1, 2, 3}

    # Test with dict
    rule_dict = Rule(rule_name="TestDict", value={"min": 1, "max": 10})
    assert rule_dict.value == {"min": 1, "max": 10}

    # Test with int
    rule_int = Rule(rule_name="TestInt", value=42)
    assert rule_int.value == 42

    # Test with float
    rule_float = Rule(rule_name="TestFloat", value=3.14)
    assert rule_float.value == 3.14

    # Test with string
    rule_str = Rule(rule_name="TestStr", value="test")
    assert rule_str.value == "test"