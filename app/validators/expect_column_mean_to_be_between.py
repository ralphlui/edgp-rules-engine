from typing import List, Dict, Any
import pandas as pd
from app.models.rule import Rule


def validate_column_mean_to_be_between(data: List[Dict[str, Any]], rule: Rule) -> Dict[str, Any]:
    """
    Validate that the mean of a column is between specified min and max values.
    
    Args:
        data: List of dictionaries representing the dataset
        rule: Rule object containing column_name and value (dict with min_value and max_value)
        
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
    
    if not rule.value or not isinstance(rule.value, dict):
        return {
            "rule_name": rule.rule_name,
            "column_name": column_name,
            "success": False,
            "error": "Rule value must contain min_value and max_value"
        }
    
    min_value = rule.value.get("min_value")
    max_value = rule.value.get("max_value")
    
    if min_value is None or max_value is None:
        return {
            "rule_name": rule.rule_name,
            "column_name": column_name,
            "success": False,
            "error": "Both min_value and max_value must be specified"
        }
    
    try:
        # Get non-null values and convert to numeric
        numeric_values = pd.to_numeric(df[column_name], errors='coerce').dropna()
        
        if len(numeric_values) == 0:
            return {
                "rule_name": rule.rule_name,
                "column_name": column_name,
                "success": False,
                "error": "No numeric values found in column"
            }
        
        observed_mean = numeric_values.mean()
        success = min_value <= observed_mean <= max_value
        
        result = {
            "rule_name": rule.rule_name,
            "column_name": column_name,
            "success": success,
            "result": {
                "observed_value": observed_mean,
                "element_count": len(numeric_values),
                "min_value": min_value,
                "max_value": max_value
            }
        }
        
        if not success:
            result["error"] = f"Mean {observed_mean} is not between {min_value} and {max_value}"
            
        return result
        
    except Exception as e:
        return {
            "rule_name": rule.rule_name,
            "column_name": column_name,
            "success": False,
            "error": f"Validation error: {str(e)}"
        }
