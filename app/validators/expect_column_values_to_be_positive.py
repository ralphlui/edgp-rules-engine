from typing import List, Dict, Any
import pandas as pd
from app.models.rule import Rule


def validate_column_values_to_be_positive(data: List[Dict[str, Any]], rule: Rule) -> Dict[str, Any]:
    """
    Validate that all values in a column are positive (> 0).
    
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
        # Get non-null values and convert to numeric
        non_null_values = df[column_name].dropna()
        numeric_values = pd.to_numeric(non_null_values, errors='coerce').dropna()
        
        # Find non-positive values
        non_positive_values = numeric_values[numeric_values <= 0].tolist()
        
        success = len(non_positive_values) == 0
        unexpected_count = len(non_positive_values)
        total_count = len(numeric_values)
        
        result = {
            "rule_name": rule.rule_name,
            "column_name": column_name,
            "success": success,
            "result": {
                "element_count": total_count,
                "unexpected_count": unexpected_count,
                "unexpected_percent": (unexpected_count / total_count * 100) if total_count > 0 else 0,
                "partial_unexpected_list": non_positive_values[:20]  # Limit to first 20 values
            }
        }
        
        if not success:
            result["error"] = f"Found {unexpected_count} non-positive values"
            
        return result
        
    except Exception as e:
        return {
            "rule_name": rule.rule_name,
            "column_name": column_name,
            "success": False,
            "error": f"Validation error: {str(e)}"
        }
