from typing import List, Dict, Any
import pandas as pd
from app.models.rule import Rule


def validate_column_values_to_not_be_none(data: List[Dict[str, Any]], rule: Rule) -> Dict[str, Any]:
    """
    Validate that no values in a column are None/null.
    
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
    
    try:
        total_count = len(df)
        null_count = df[column_name].isnull().sum()
        
        success = null_count == 0
        
        result = {
            "rule_name": rule.rule_name,
            "column_name": column_name,
            "success": success,
            "result": {
                "element_count": total_count,
                "unexpected_count": null_count,
                "unexpected_percent": (null_count / total_count * 100) if total_count > 0 else 0
            }
        }
        
        if not success:
            result["error"] = f"Found {null_count} null values"
            
        return result
        
    except Exception as e:
        return {
            "rule_name": rule.rule_name,
            "column_name": column_name,
            "success": False,
            "error": f"Validation error: {str(e)}"
        }
