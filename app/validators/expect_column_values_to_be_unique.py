from typing import List, Dict, Any
import pandas as pd
from app.models.rule import Rule


def validate_column_values_to_be_unique(data: List[Dict[str, Any]], rule: Rule) -> Dict[str, Any]:
    """
    Validate that all values in a column are unique.
    
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
        # Get non-null values
        non_null_values = df[column_name].dropna()
        total_count = len(non_null_values)
        unique_count = len(non_null_values.unique())
        
        # Find duplicate values
        duplicates = non_null_values[non_null_values.duplicated(keep=False)]
        unexpected_count = len(duplicates)
        
        success = unexpected_count == 0
        
        result = {
            "rule_name": rule.rule_name,
            "column_name": column_name,
            "success": success,
            "result": {
                "element_count": total_count,
                "unique_count": unique_count,
                "unexpected_count": unexpected_count,
                "unexpected_percent": (unexpected_count / total_count * 100) if total_count > 0 else 0,
                "partial_unexpected_list": duplicates.unique().tolist()[:20]  # Limit to first 20 unique duplicates
            }
        }
        
        if not success:
            result["error"] = f"Found {unexpected_count} non-unique values"
            
        return result
        
    except Exception as e:
        return {
            "rule_name": rule.rule_name,
            "column_name": column_name,
            "success": False,
            "error": f"Validation error: {str(e)}"
        }
