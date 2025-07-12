from typing import List, Dict, Any
import pandas as pd
import re
from app.models.rule import Rule


def validate_column_values_to_be_valid_email(data: List[Dict[str, Any]], rule: Rule) -> Dict[str, Any]:
    """
    Validate that all values in a column are valid email addresses.
    
    Args:
        data: List of dictionaries representing the dataset
        rule: Rule object containing column_name
        
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
    
    # Basic email regex pattern
    email_pattern = re.compile(r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$')
    
    try:
        # Get non-null values and convert to string
        non_null_values = df[column_name].dropna().astype(str)
        unexpected_values = []
        
        for value in non_null_values:
            if not email_pattern.match(value.strip()):
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
                "partial_unexpected_list": unexpected_values[:20]  # Limit to first 20 values
            }
        }
        
        if not success:
            result["error"] = f"Found {unexpected_count} invalid email addresses"
            
        return result
        
    except Exception as e:
        return {
            "rule_name": rule.rule_name,
            "column_name": column_name,
            "success": False,
            "error": f"Validation error: {str(e)}"
        }
