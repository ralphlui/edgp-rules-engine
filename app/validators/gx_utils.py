"""
Great Expectations utilities for the validation system
"""
try:
    import great_expectations as gx
    GX_AVAILABLE = True
except ImportError:
    gx = None
    GX_AVAILABLE = False

import pandas as pd
from typing import Dict, Any, List


class GXValidator:
    """Great Expectations validator utility class"""
    
    def __init__(self):
        """Initialize the Great Expectations context"""
        self.context = None
        self.datasource = None
        self.data_asset = None
        if GX_AVAILABLE:
            self._setup_context()
        else:
            print("⚠️ Great Expectations not available - using fallback validation")
    
    def _setup_context(self):
        """Set up Great Expectations context with minimal configuration"""
        if not GX_AVAILABLE:
            return
            
        try:
            # Create ephemeral context (in-memory, no file system)
            self.context = gx.get_context(mode='ephemeral')
            
            # Add pandas datasource
            self.datasource = self.context.sources.add_pandas("pandas_datasource")
            
            # Add dataframe asset
            self.data_asset = self.datasource.add_dataframe_asset("validation_data")
            
        except Exception as e:
            raise RuntimeError(f"Failed to setup Great Expectations context: {str(e)}")
    
    def get_validator(self, df: pd.DataFrame, expectation_suite_name: str = "validation_suite"):
        """
        Get a Great Expectations validator for the given DataFrame
        
        Args:
            df: pandas DataFrame to validate
            expectation_suite_name: name of the expectation suite
            
        Returns:
            Great Expectations validator
        """
        try:
            # Create or get expectation suite
            try:
                suite = self.context.get_expectation_suite(expectation_suite_name)
            except Exception:
                suite = self.context.add_expectation_suite(expectation_suite_name)
            
            # Create batch request with dataframe
            batch_request = self.data_asset.build_batch_request(dataframe=df)
            
            # Get validator
            validator = self.context.get_validator(
                batch_request=batch_request,
                expectation_suite=suite
            )
            
            return validator
            
        except Exception as e:
            raise RuntimeError(f"Failed to create validator: {str(e)}")


# Global validator instance
_gx_validator = None


def get_gx_validator() -> GXValidator:
    """Get the global Great Expectations validator instance"""
    if not GX_AVAILABLE:
        raise RuntimeError("Great Expectations is not available")
        
    global _gx_validator
    if _gx_validator is None:
        _gx_validator = GXValidator()
    return _gx_validator


def validate_with_gx(data: List[Dict[str, Any]], expectation_type: str, column: str, **kwargs) -> Dict[str, Any]:
    """
    Generic function to validate data using Great Expectations
    
    Args:
        data: List of dictionaries to validate
        expectation_type: name of the expectation method to call
        column: column name to validate
        **kwargs: arguments to pass to the expectation method
        
    Returns:
        Dict containing validation results
    """
    if not GX_AVAILABLE:
        return {
            "success": False,
            "message": f"Great Expectations not available for '{expectation_type}' validation",
            "error": f"Great Expectations not available (Python 3.13 compatibility issue). Expectation '{expectation_type}' not supported.",
            "result": {},
            "meta": {}
        }
        
    try:
        # Convert data to DataFrame
        df = pd.DataFrame(data)
        
        # Get validator
        validator = get_gx_validator().get_validator(df)
        
        # Get the expectation method
        expectation_func = getattr(validator, expectation_type)
        
        # Run the expectation with column as first argument
        result = expectation_func(column=column, **kwargs)
        
        # Create response based on result
        if result.success:
            return {
                "success": True,
                "message": f"Validation passed for column '{column}'",
                "error": None,
                "result": result.result,
                "meta": result.meta
            }
        else:
            return {
                "success": False,
                "message": f"Validation failed for column '{column}'",
                "error": f"Column '{column}' validation failed",
                "result": result.result,
                "meta": result.meta
            }
        
    except Exception as e:
        return {
            "success": False,
            "message": f"Validation error for column '{column}': {str(e)}",
            "error": f"Great Expectations validation error: {str(e)}",
            "result": {},
            "meta": {}
        }
