from typing import List, Dict, Any
import pandas as pd
from app.models.rule import Rule
from app.validators.gx_utils import validate_with_gx


def validate_column_distinct_values_to_be_in_set(data: List[Dict[str, Any]], rule: Rule) -> Dict[str, Any]:
    """
    Validate that the distinct values in a column are within a specified set using Great Expectations.
    
    Args:
        data: List of dictionaries representing the dataset
        rule: Rule object containing column_name and value (list of allowed values)
        
    Returns:
        Dict containing validation results
    """
    df = pd.DataFrame(data)
    column_name = rule.column_name
    allowed_values = rule.value if rule.value else []
    
    if column_name not in df.columns:
        return {
            "rule_name": rule.rule_name,
            "column_name": column_name,
            "success": False,
            "error": f"Column '{column_name}' not found in dataset"
        }
    
    try:
        # Use Great Expectations to validate
        gx_result = validate_with_gx(
            df=df,
            expectation_method="expect_column_distinct_values_to_be_in_set",
            column=column_name,
            value_set=allowed_values
        )
        
        # Format the result to match our expected structure
        success = gx_result["success"]
        
        formatted_result = {
            "rule_name": rule.rule_name,
            "column_name": column_name,
            "success": success,
            "result": {
                "observed_value": gx_result["result"].get("observed_value", []),
                "element_count": gx_result["result"].get("element_count", 0),
                "missing_count": gx_result["result"].get("missing_count", 0),
                "missing_percent": gx_result["result"].get("missing_percent", 0),
                "unexpected_count": gx_result["result"].get("unexpected_count", 0),
                "unexpected_percent": gx_result["result"].get("unexpected_percent", 0),
                "unexpected_values": gx_result["result"].get("partial_unexpected_list", [])
            }
        }
        
        if not success:
            unexpected_count = gx_result["result"].get("unexpected_count", 0)
            formatted_result["error"] = f"Great Expectations validation failed: {unexpected_count} unexpected distinct values found"
            
        return formatted_result
        
    except Exception as e:
        return {
            "rule_name": rule.rule_name,
            "column_name": column_name,
            "success": False,
            "error": f"Great Expectations validation error: {str(e)}"
        }
