from typing import List, Dict, Any
import pandas as pd
from app.models.rule import Rule


def validate_column_values_to_be_of_type(data: List[Dict[str, Any]], rule: Rule) -> Dict[str, Any]:
    """
    Validate that all values in a column are of a specified type.
    
    Args:
        data: List of dictionaries representing the dataset
        rule: Rule object containing column_name and value (dict with type_)
        
    Returns:
        Dict containing validation results
    """
    df = pd.DataFrame(data)
    column_name = rule.column_name
    
    if column_name not in df.columns:
        return {
            "rule_name": rule.rule_name,
            "column_name": column_name,
            "success": False,
            "error": f"Column '{column_name}' not found in dataset"
        }
    
    if not rule.value or not isinstance(rule.value, dict) or "type_" not in rule.value:
        return {
            "rule_name": rule.rule_name,
            "column_name": column_name,
            "success": False,
            "error": "Rule value must contain type_ specification"
        }
    
    expected_type = rule.value["type_"].upper()
    
    # Type mapping for common types
    type_checkers = {
        "INTEGER": lambda x: pd.api.types.is_integer_dtype(pd.Series([x])),
        "FLOAT": lambda x: pd.api.types.is_float_dtype(pd.Series([x])),
        "STRING": lambda x: isinstance(x, str),
        "VARCHAR": lambda x: isinstance(x, str),
        "TEXT": lambda x: isinstance(x, str),
        "BOOLEAN": lambda x: isinstance(x, bool),
        "DATETIME": lambda x: pd.api.types.is_datetime64_any_dtype(pd.Series([x])),
        "DATE": lambda x: pd.api.types.is_datetime64_any_dtype(pd.Series([x]))
    }
    
    if expected_type not in type_checkers:
        return {
            "rule_name": rule.rule_name,
            "column_name": column_name,
            "success": False,
            "error": f"Unsupported type: {expected_type}"
        }
    
    try:
        # Get non-null values
        non_null_values = df[column_name].dropna()
        type_checker = type_checkers[expected_type]
        unexpected_values = []
        
        for value in non_null_values:
            try:
                if not type_checker(value):
                    unexpected_values.append(value)
            except:
                unexpected_values.append(value)
        
        success = len(unexpected_values) == 0
        unexpected_count = len(unexpected_values)
        total_count = len(non_null_values)
        
        result = {
            "rule_name": rule.rule_name,
            "column_name": column_name,
            "success": success,
            "result": {
                "element_count": total_count,
                "unexpected_count": unexpected_count,
                "unexpected_percent": (unexpected_count / total_count * 100) if total_count > 0 else 0,
                "partial_unexpected_list": unexpected_values[:20],  # Limit to first 20 values
                "expected_type": expected_type
            }
        }
        
        if not success:
            result["error"] = f"Found {unexpected_count} values not of type {expected_type}"
            
        return result
        
    except Exception as e:
        return {
            "rule_name": rule.rule_name,
            "column_name": column_name,
            "success": False,
            "error": f"Validation error: {str(e)}"
        }
