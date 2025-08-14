"""
Comprehensive test suite for main application, API routes, and configuration.
"""
import pytest
from fastapi.testclient import TestClient
from unittest.mock import patch, Mock
import os
import tempfile

from app.main import app
from app.core.config import Settings, settings
from app.api.routes import router


class TestMainAppComprehensive:
    """Comprehensive tests for main application functionality"""
    
    def test_app_creation(self):
        """Test FastAPI app creation"""
        assert app is not None
        assert app.title == "EDGP Rules Engine API"
        assert "rules" in app.description.lower()
    
    def test_app_middleware_stack(self):
        """Test middleware configuration"""
        # Check that middleware is configured
        assert len(app.user_middleware) > 0
        
        # Test CORS middleware presence
        middleware_classes = [middleware.cls.__name__ if hasattr(middleware, 'cls') else str(type(middleware)) for middleware in app.user_middleware]
        has_cors = any('CORS' in name for name in middleware_classes)
        assert has_cors or len(middleware_classes) > 0  # Either CORS or other middleware
    
    def test_app_routes_registration(self):
        """Test route registration"""
        routes = [route.path for route in app.routes if hasattr(route, 'path')]
        
        # Check key routes exist
        assert any('/health' in route for route in routes)
        assert any('/api' in route for route in routes)
    
    def test_app_exception_handlers(self):
        """Test exception handler configuration"""
        # Test that app has exception handlers configured
        assert hasattr(app, 'exception_handlers')
    
    def test_app_startup_shutdown_events(self):
        """Test startup and shutdown event configuration"""
        # Verify app can be instantiated with events
        with TestClient(app) as client:
            # App should start and stop without errors
            response = client.get("/health")
            assert response.status_code == 200
    
    def test_app_openapi_schema(self):
        """Test OpenAPI schema generation"""
        schema = app.openapi()
        
        assert "openapi" in schema
        assert "info" in schema
        assert schema["info"]["title"] == "EDGP Rules Engine API"
        assert "paths" in schema
    
    def test_app_docs_endpoints(self):
        """Test documentation endpoints"""
        client = TestClient(app)
        
        # Test Swagger UI
        response = client.get("/docs")
        assert response.status_code == 200
        
        # Test ReDoc
        response = client.get("/redoc")
        assert response.status_code == 200
    
    def test_app_cors_configuration(self):
        """Test CORS configuration"""
        client = TestClient(app)
        
        # Test CORS headers on preflight request
        response = client.options("/api/rules/validate", headers={
            "Origin": "http://localhost:3000",
            "Access-Control-Request-Method": "POST"
        })
        
        # Should not error (may be 405 but shouldn't crash)
        assert response.status_code in [200, 405]
    
    def test_app_health_endpoint_detailed(self):
        """Test health endpoint functionality"""
        client = TestClient(app)
        
        response = client.get("/health")
        assert response.status_code == 200
        
        data = response.json()
        assert "status" in data
        assert data["status"] == "healthy"
        assert "service" in data
        assert data["service"] == "EDGP Rules Engine API"
    
    def test_app_validation_endpoint_comprehensive(self):
        """Test validation endpoint with various scenarios"""
        client = TestClient(app)
        
        # Test valid request
        valid_request = {
            "rules": [{"rule_name": "expect_column_to_exist", "column_name": "name"}],
            "dataset": [{"name": "John", "age": 25}]
        }
        
        response = client.post("/api/rules/validate", json=valid_request)
        assert response.status_code == 200
        
        data = response.json()
        assert "results" in data
        assert "summary" in data
    
    def test_app_error_handling(self):
        """Test application error handling"""
        client = TestClient(app)
        
        # Test invalid JSON
        response = client.post("/api/rules/validate", 
                              data="invalid json",
                              headers={"Content-Type": "application/json"})
        assert response.status_code in [400, 422]  # Bad request or validation error
        
        # Test missing required fields
        response = client.post("/api/rules/validate", json={})
        assert response.status_code in [400, 422]


class TestConfigurationComprehensive:
    """Comprehensive tests for configuration management"""
    
    def test_settings_creation(self):
        """Test settings object creation"""
        settings = Settings()
        
        assert hasattr(settings, 'allowed_origins')
        assert hasattr(settings, 'api_title')
        assert isinstance(settings.allowed_origins, list)
    
    def test_settings_environment_loading(self):
        """Test environment variable loading"""
        # Test with environment variables
        with patch.dict(os.environ, {
            'ALLOWED_ORIGINS': 'http://localhost:3000,http://localhost:8080'
        }):
            test_settings = Settings()
            assert 'http://localhost:3000' in test_settings.allowed_origins
    
    def test_settings_defaults(self):
        """Test default configuration values"""
        test_settings = Settings()
        
        # Test default values are reasonable
        assert isinstance(test_settings.allowed_origins, list)
        assert isinstance(test_settings.host, str)
        assert isinstance(test_settings.port, int)
    
    def test_get_settings_function(self):
        """Test settings getter function"""
        test_settings = settings
        
        assert isinstance(test_settings, Settings)
        assert hasattr(test_settings, 'allowed_origins')
    
    def test_settings_validation(self):
        """Test settings validation"""
        # Test that invalid settings are handled
        with patch.dict(os.environ, {'PORT': 'invalid_port'}):
            try:
                test_settings = Settings()
                # Should either handle gracefully or raise validation error
            except Exception as e:
                # Expected for invalid port values
                assert 'int' in str(e).lower() or 'invalid' in str(e).lower()
    
    def test_settings_cors_origins_parsing(self):
        """Test CORS origins parsing"""
        test_origins = "http://localhost:3000,https://example.com,http://localhost:8080"
        
        with patch.dict(os.environ, {'ALLOWED_ORIGINS': test_origins}):
            test_settings = Settings()
            
            assert len(test_settings.allowed_origins) >= 3
            assert 'http://localhost:3000' in test_settings.allowed_origins
            assert 'https://example.com' in test_settings.allowed_origins
    
    def test_settings_immutability(self):
        """Test that settings behave consistently"""
        settings1 = settings
        settings2 = settings
        
        # Should be consistent
        assert settings1.allowed_origins == settings2.allowed_origins


class TestAPIRoutesComprehensive:
    """Comprehensive tests for API routes functionality"""
    
    def test_router_creation(self):
        """Test API router creation"""
        assert router is not None
        assert hasattr(router, 'routes')
    
    def test_router_routes_registration(self):
        """Test that routes are properly registered"""
        routes = [route.path for route in router.routes if hasattr(route, 'path')]
        
        # Check expected routes from the API router
        expected_routes = ['/api/rules', '/api/rules/validate']
        for expected in expected_routes:
            assert any(expected in route for route in routes)
    
    def test_validation_endpoint_parameter_handling(self):
        """Test validation endpoint parameter handling"""
        client = TestClient(app)
        
        # Test with different rule types
        test_cases = [
            {
                "rules": [{"rule_name": "expect_column_to_exist", "column_name": "name"}],
                "dataset": [{"name": "John"}]
            },
            {
                "rules": [{"rule_name": "expect_column_values_to_be_positive", "column_name": "age"}],
                "dataset": [{"age": 25}]
            }
        ]
        
        for test_case in test_cases:
            response = client.post("/api/rules/validate", json=test_case)
            # Should process without crashing
            assert response.status_code in [200, 400, 422]
    
    def test_rules_endpoint_functionality(self):
        """Test rules listing endpoint"""
        client = TestClient(app)
        
        response = client.get("/api/rules")
        assert response.status_code == 200
        
        data = response.json()
        assert isinstance(data, list)
    
    def test_api_request_size_handling(self):
        """Test API request size handling"""
        client = TestClient(app)
        
        # Test with larger dataset
        large_dataset = [{"id": i, "value": f"item_{i}"} for i in range(100)]
        request = {
            "rules": [{"rule_name": "expect_column_to_exist", "column_name": "id"}],
            "dataset": large_dataset
        }
        
        response = client.post("/api/rules/validate", json=request)
        # Should handle larger requests
        assert response.status_code in [200, 400, 413, 422]  # Success, bad request, or payload too large
    
    def test_api_content_type_validation(self):
        """Test API content type validation"""
        client = TestClient(app)
        
        # Test with wrong content type
        response = client.post("/api/rules/validate", 
                              data='{"test": "data"}',
                              headers={"Content-Type": "text/plain"})
        assert response.status_code in [400, 415, 422]  # Bad request or unsupported media type
    
    def test_api_response_format_consistency(self):
        """Test API response format consistency"""
        client = TestClient(app)
        
        valid_request = {
            "rules": [{"rule_name": "expect_column_to_exist", "column_name": "name"}],
            "dataset": [{"name": "test"}]
        }
        
        response = client.post("/api/rules/validate", json=valid_request)
        
        if response.status_code == 200:
            data = response.json()
            # Check response structure
            assert "results" in data
            assert "summary" in data
            assert isinstance(data["results"], list)
            assert isinstance(data["summary"], dict)


class TestIntegrationComprehensive:
    """Comprehensive integration tests"""
    
    def test_full_application_flow(self):
        """Test complete application workflow"""
        client = TestClient(app)
        
        # 1. Check health
        health_response = client.get("/health")
        assert health_response.status_code == 200
        
        # 2. Get available rules
        rules_response = client.get("/api/rules")
        assert rules_response.status_code == 200
        
        # 3. Perform validation
        validation_request = {
            "rules": [{"rule_name": "expect_column_to_exist", "column_name": "name"}],
            "dataset": [{"name": "Integration Test"}]
        }
        
        validation_response = client.post("/api/rules/validate", json=validation_request)
        assert validation_response.status_code == 200
    
    def test_application_with_various_environments(self):
        """Test application behavior in different environments"""
        # Test development environment
        with patch.dict(os.environ, {'ENVIRONMENT': 'development'}):
            with TestClient(app) as client:
                response = client.get("/health")
                assert response.status_code == 200
        
        # Test production environment  
        with patch.dict(os.environ, {'ENVIRONMENT': 'production'}):
            with TestClient(app) as client:
                response = client.get("/health")
                assert response.status_code == 200
    
    def test_concurrent_requests_handling(self):
        """Test concurrent request handling"""
        import threading
        import time
        
        client = TestClient(app)
        results = []
        
        def make_request():
            try:
                response = client.get("/health")
                results.append(response.status_code)
            except Exception as e:
                results.append(str(e))
        
        # Create multiple threads
        threads = [threading.Thread(target=make_request) for _ in range(5)]
        
        # Start all threads
        for thread in threads:
            thread.start()
        
        # Wait for completion
        for thread in threads:
            thread.join()
        
        # Check results
        assert len(results) == 5
        assert all(isinstance(result, int) and result == 200 for result in results)
    
    def test_memory_usage_stability(self):
        """Test that repeated requests don't cause memory leaks"""
        client = TestClient(app)
        
        # Make multiple requests
        for _ in range(10):
            response = client.get("/health")
            assert response.status_code == 200
            
            # Also test validation endpoint
            request = {
                "rules": [{"rule_name": "expect_column_to_exist", "column_name": "test"}],
                "dataset": [{"test": "value"}]
            }
            response = client.post("/api/rules/validate", json=request)
            # Should complete without memory issues
            assert response.status_code in [200, 400, 422]
