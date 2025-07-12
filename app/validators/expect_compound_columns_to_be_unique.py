from typing import List, Dict, Any
import pandas as pd
from app.models.rule import Rule


def validate_compound_columns_to_be_unique(data: List[Dict[str, Any]], rule: Rule) -> Dict[str, Any]:
    """
    Validate that the combination of multiple columns creates unique rows.
    
    Args:
        data: List of dictionaries representing the dataset
        rule: Rule object containing value (dict with column_list)
        
    Returns:
        Dict containing validation results
    """
    df = pd.DataFrame(data)
    
    if not rule.value or not isinstance(rule.value, dict) or "column_list" not in rule.value:
        return {
            "rule_name": rule.rule_name,
            "column_name": None,
            "success": False,
            "error": "Rule value must contain column_list"
        }
    
    column_list = rule.value["column_list"]
    
    if not isinstance(column_list, list) or len(column_list) == 0:
        return {
            "rule_name": rule.rule_name,
            "column_name": None,
            "success": False,
            "error": "column_list must be a non-empty list"
        }
    
    # Check if all columns exist
    missing_columns = [col for col in column_list if col not in df.columns]
    if missing_columns:
        return {
            "rule_name": rule.rule_name,
            "column_name": None,
            "success": False,
            "error": f"Columns not found: {missing_columns}"
        }
    
    try:
        # Check for duplicates in the combination of columns
        subset_df = df[column_list].dropna()
        total_count = len(subset_df)
        unique_count = len(subset_df.drop_duplicates())
        duplicate_count = total_count - unique_count
        
        success = duplicate_count == 0
        
        # Get duplicate rows for reporting
        duplicates = subset_df[subset_df.duplicated(keep=False)]
        
        result = {
            "rule_name": rule.rule_name,
            "column_name": None,
            "success": success,
            "result": {
                "element_count": total_count,
                "unique_count": unique_count,
                "unexpected_count": duplicate_count,
                "unexpected_percent": (duplicate_count / total_count * 100) if total_count > 0 else 0,
                "column_list": column_list,
                "partial_unexpected_list": duplicates.head(20).to_dict('records')  # First 20 duplicate combinations
            }
        }
        
        if not success:
            result["error"] = f"Found {duplicate_count} duplicate combinations in columns {column_list}"
            
        return result
        
    except Exception as e:
        return {
            "rule_name": rule.rule_name,
            "column_name": None,
            "success": False,
            "error": f"Validation error: {str(e)}"
        }
