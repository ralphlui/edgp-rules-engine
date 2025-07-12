from typing import List, Dict, Any
import pandas as pd
from app.models.rule import Rule


def validate_table_row_count_to_be_between(data: List[Dict[str, Any]], rule: Rule) -> Dict[str, Any]:
    """
    Validate that the row count of the table is between specified min and max values.
    
    Args:
        data: List of dictionaries representing the dataset
        rule: Rule object containing value (dict with min_value and max_value)
        
    Returns:
        Dict containing validation results
    """
    df = pd.DataFrame(data)
    
    if not rule.value or not isinstance(rule.value, dict):
        return {
            "rule_name": rule.rule_name,
            "column_name": None,
            "success": False,
            "error": "Rule value must contain min_value and max_value"
        }
    
    min_value = rule.value.get("min_value")
    max_value = rule.value.get("max_value")
    
    if min_value is None or max_value is None:
        return {
            "rule_name": rule.rule_name,
            "column_name": None,
            "success": False,
            "error": "Both min_value and max_value must be specified"
        }
    
    try:
        observed_row_count = len(df)
        success = min_value <= observed_row_count <= max_value
        
        result = {
            "rule_name": rule.rule_name,
            "column_name": None,
            "success": success,
            "result": {
                "observed_value": observed_row_count,
                "min_value": min_value,
                "max_value": max_value
            }
        }
        
        if not success:
            result["error"] = f"Row count {observed_row_count} is not between {min_value} and {max_value}"
            
        return result
        
    except Exception as e:
        return {
            "rule_name": rule.rule_name,
            "column_name": None,
            "success": False,
            "error": f"Validation error: {str(e)}"
        }
