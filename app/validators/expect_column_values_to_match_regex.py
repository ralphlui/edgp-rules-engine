from typing import List, Dict, Any
import pandas as pd
import re
from app.models.rule import Rule


def validate_column_values_to_match_regex(data: List[Dict[str, Any]], rule: Rule) -> Dict[str, Any]:
    """
    Validate that all values in a column match a specified regex pattern.
    
    Args:
        data: List of dictionaries representing the dataset
        rule: Rule object containing column_name and value (dict with regex pattern)
        
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
    
    if not rule.value or not isinstance(rule.value, dict) or "regex" not in rule.value:
        return {
            "rule_name": rule.rule_name,
            "column_name": column_name,
            "success": False,
            "error": "Rule value must contain regex pattern"
        }
    
    regex_pattern = rule.value["regex"]
    
    try:
        # Compile regex pattern
        compiled_pattern = re.compile(regex_pattern)
        
        # Get non-null values and convert to string
        non_null_values = df[column_name].dropna().astype(str)
        unexpected_values = []
        
        for value in non_null_values:
            if not compiled_pattern.match(value):
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
                "regex": regex_pattern
            }
        }
        
        if not success:
            result["error"] = f"Found {unexpected_count} values that don't match regex pattern"
            
        return result
        
    except re.error as e:
        return {
            "rule_name": rule.rule_name,
            "column_name": column_name,
            "success": False,
            "error": f"Invalid regex pattern: {str(e)}"
        }
    except Exception as e:
        return {
            "rule_name": rule.rule_name,
            "column_name": column_name,
            "success": False,
            "error": f"Validation error: {str(e)}"
        }
