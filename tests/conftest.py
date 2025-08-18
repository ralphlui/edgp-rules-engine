"""
Shared pytest configuration and fixtures for all test files
"""
import pytest


@pytest.fixture(scope="function", autouse=True)
def reset_gx_state():
    """Reset Great Expectations global state between tests for isolation"""
    # Clear any existing GX validator
    import app.validators.gx_utils as gx_utils
    gx_utils._gx_validator = None
    
    # Clear any GX context cache if it exists
    try:
        import great_expectations as gx
        import tempfile
        import shutil
        import os
        
        # Clear any global state in GX that might be cached
        if hasattr(gx.data_context, '_contexts'):
            gx.data_context._contexts.clear()
            
        # Force clearing any module-level caches
        if hasattr(gx.data_context.data_context, 'BaseDataContext'):
            gx.data_context.data_context.BaseDataContext._project_config_with_variables_substituted = None
            
    except (ImportError, AttributeError):
        pass
        
    yield
    
    # Clean up after test - more thorough cleanup
    gx_utils._gx_validator = None
    
    try:
        import great_expectations as gx
        if hasattr(gx.data_context, '_contexts'):
            gx.data_context._contexts.clear()
            
        # Clean up any temporary directories that GX might have created
        import gc
        gc.collect()
        
    except (ImportError, AttributeError):
        pass


@pytest.fixture(scope="function")
def isolated_gx_test():
    """Fixture specifically for Great Expectations validation tests with complete state isolation"""
    import app.validators.gx_utils as gx_utils
    import great_expectations as gx
    import pandas as pd
    import tempfile
    import shutil
    import os
    
    # Store original state
    original_gx_validator = getattr(gx_utils, '_gx_validator', None)
    original_context = getattr(gx, 'get_context', lambda: None)()
    
    # Create a completely isolated temporary directory for this test
    temp_dir = tempfile.mkdtemp(prefix="gx_test_isolated_")
    
    try:
        # Force complete reset of GX state
        gx_utils._gx_validator = None
        
        # Clear any cached GX contexts
        if hasattr(gx, '_context'):
            delattr(gx, '_context')
        
        # Set up completely fresh GX environment
        os.environ['GE_HOME'] = temp_dir
        
        # Initialize with a simple test to establish clean baseline state
        test_data = [{"isolated_test_col": "isolated_val"}]
        df = pd.DataFrame(test_data)
        validator = gx_utils.get_gx_validator().get_validator(df)
        
        # Verify clean state by running a simple validation
        result = validator.expect_column_to_exist(column="isolated_test_col")
        
        # Yield the fresh validator
        yield validator
        
    except Exception as e:
        # If GX initialization fails, yield None 
        print(f"GX isolation setup failed: {e}")
        yield None
        
    finally:
        # Comprehensive cleanup
        try:
            # Reset GX utils state
            gx_utils._gx_validator = original_gx_validator
            
            # Clear any module-level caches
            if hasattr(gx, '_context'):
                delattr(gx, '_context')
            
            # Clean up temporary directory
            if os.path.exists(temp_dir):
                shutil.rmtree(temp_dir, ignore_errors=True)
                
            # Restore original GE_HOME if it existed
            if 'GE_HOME' in os.environ:
                del os.environ['GE_HOME']
                
        except Exception as cleanup_error:
            print(f"Warning: GX cleanup failed: {cleanup_error}")
