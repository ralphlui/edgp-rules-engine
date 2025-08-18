"""
Comprehensive unit tests for app.core.config module
Tests application configuration settings and environment handling
"""
import pytest
import os
import tempfile
from pathlib import Path
from unittest.mock import patch, MagicMock
from pydantic import ValidationError
import json

from app.core.config import Settings, get_env_file_path


class TestGetEnvFilePath:
    """Test get_env_file_path function"""
    
    def test_get_env_file_path_no_app_env(self):
        """Test env file path selection with no APP_ENV set"""
        with patch.dict(os.environ, {}, clear=True):
            if 'APP_ENV' in os.environ:
                del os.environ['APP_ENV']
            
            with patch('app.core.config.Path') as mock_path:
                mock_project_root = MagicMock()
                mock_path.return_value.parent.parent.parent = mock_project_root
                
                # Mock .env exists
                mock_env_file = MagicMock()
                mock_env_file.exists.return_value = True
                mock_project_root.__truediv__ = lambda self, other: mock_env_file
                
                result = get_env_file_path()
                
                # Should return path to .env file
                assert result == str(mock_env_file)
    
    def test_get_env_file_path_with_sit_env(self):
        """Test env file path selection with APP_ENV=SIT"""
        with patch.dict(os.environ, {'APP_ENV': 'SIT'}):
            with patch('app.core.config.Path') as mock_path:
                mock_project_root = MagicMock()
                mock_path.return_value.parent.parent.parent = mock_project_root
                
                # Mock .env.development exists
                mock_env_dev = MagicMock()
                mock_env_dev.exists.return_value = True
                
                def mock_truediv(self, other):
                    if other == ".env.development":
                        return mock_env_dev
                    return MagicMock()
                
                mock_project_root.__truediv__ = mock_truediv
                
                result = get_env_file_path()
                
                # Should return path to .env.development
                assert result == str(mock_env_dev)
    
    def test_get_env_file_path_with_production_env(self):
        """Test env file path selection with APP_ENV=PRODUCTION"""
        with patch.dict(os.environ, {'APP_ENV': 'PRODUCTION'}):
            with patch('app.core.config.Path') as mock_path:
                mock_project_root = MagicMock()
                mock_path.return_value.parent.parent.parent = mock_project_root
                
                # Mock .env.production exists
                mock_env_prod = MagicMock()
                mock_env_prod.exists.return_value = True
                
                def mock_truediv(self, other):
                    if other == ".env.production":
                        return mock_env_prod
                    return MagicMock()
                
                mock_project_root.__truediv__ = mock_truediv
                
                result = get_env_file_path()
                
                # Should return path to .env.production
                assert result == str(mock_env_prod)
    
    def test_get_env_file_path_fallback_logic(self):
        """Test fallback logic when specific env file doesn't exist"""
        with patch.dict(os.environ, {'APP_ENV': 'PRD'}):
            with patch('app.core.config.Path') as mock_path:
                mock_project_root = MagicMock()
                mock_path.return_value.parent.parent.parent = mock_project_root
                
                # Mock .env.development exists as fallback
                mock_env_dev = MagicMock()
                mock_env_dev.exists.return_value = True
                
                def mock_truediv(self, other):
                    if other == ".env.development":
                        return mock_env_dev
                    elif other == ".env.production":
                        mock_file = MagicMock()
                        mock_file.exists.return_value = False
                        return mock_file
                    return MagicMock()
                
                mock_project_root.__truediv__ = mock_truediv
                
                result = get_env_file_path()
                
                # Should fallback to .env.development
                assert result == str(mock_env_dev)
    
    def test_get_env_file_path_all_env_mapping(self):
        """Test all environment mappings"""
        env_mapping = {
            "SIT": ".env.development",
            "DEV": ".env.development", 
            "DEVELOPMENT": ".env.development",
            "PRD": ".env.production",
            "PROD": ".env.production",
            "PRODUCTION": ".env.production"
        }
        
        for app_env, expected_file in env_mapping.items():
            with patch.dict(os.environ, {'APP_ENV': app_env}):
                with patch('app.core.config.Path') as mock_path:
                    mock_project_root = MagicMock()
                    mock_path.return_value.parent.parent.parent = mock_project_root
                    
                    mock_target_file = MagicMock()
                    mock_target_file.exists.return_value = True
                    
                    def mock_truediv(self, other):
                        if other == expected_file:
                            return mock_target_file
                        return MagicMock()
                    
                    mock_project_root.__truediv__ = mock_truediv
                    
                    result = get_env_file_path()
                    
                    assert result == str(mock_target_file)


class TestSettings:
    """Test Settings class"""
    
    def test_settings_creation_with_defaults(self):
        """Test Settings creation with default values"""
        settings = Settings()
        
        # Test default values
        assert settings.host == "localhost"  # From .env file
        assert settings.port == 8008
        assert settings.environment == "development"
        assert settings.api_title == "EDGP Rules Engine API"
        assert settings.api_version == "1.0.0"
        assert settings.api_description == "Data Quality Validation API using Great Expectations rules"
        # Check that allowed_origins contains the expected URLs and "*"
        expected_origins = [
            "http://localhost:3000",
            "http://localhost:3001", 
            "http://localhost:8080",
            "http://localhost:4200",
            "http://localhost:5173",
            "http://127.0.0.1:3000",
            "http://127.0.0.1:8080",
            "http://127.0.0.1:4200",
            "http://127.0.0.1:5173",
            "*"
        ]
        assert settings.allowed_origins == expected_origins
    
    def test_settings_creation_with_custom_env_vars(self):
        """Test Settings creation with custom environment variables"""
        with patch.dict(os.environ, {
            'HOST': '0.0.0.0',
            'PORT': '9000',
            'ENVIRONMENT': 'production',
            'API_TITLE': 'Custom API',
            'API_VERSION': '2.0.0',
            'API_DESCRIPTION': 'Custom description',
            'ALLOWED_ORIGINS': '["http://localhost:3000", "https://example.com"]'
        }):
            settings = Settings()
            
            assert settings.host == '0.0.0.0'
            assert settings.port == 9000
            assert settings.environment == 'production'
            assert settings.api_title == 'Custom API'
            assert settings.api_version == '2.0.0'
            assert settings.api_description == 'Custom description'
            assert settings.allowed_origins == ["http://localhost:3000", "https://example.com"]
    
    def test_settings_port_validation(self):
        """Test port handling"""
        # Test valid port
        with patch.dict(os.environ, {'PORT': '8080'}):
            settings = Settings()
            assert settings.port == 8080
        
        # Test port 0 (no validation currently exists, so it should work)
        with patch.dict(os.environ, {'PORT': '0'}):
            settings = Settings()
            assert settings.port == 0  # Currently no validation prevents this
        
        # Test high port (no validation currently exists)
        with patch.dict(os.environ, {'PORT': '70000'}):
            settings = Settings()
            assert settings.port == 70000  # Currently no validation prevents this
    
    def test_settings_allowed_origins_validation(self):
        """Test allowed_origins validation and parsing"""
        # Test valid JSON list
        with patch.dict(os.environ, {'ALLOWED_ORIGINS': '["http://localhost:3000", "https://api.example.com"]'}):
            settings = Settings()
            assert settings.allowed_origins == ["http://localhost:3000", "https://api.example.com"]
        
        # Test single origin as string (should be converted to list)
        with patch.dict(os.environ, {'ALLOWED_ORIGINS': 'http://localhost:3000'}):
            settings = Settings()
            assert settings.allowed_origins == ["http://localhost:3000"]
        
        # Test wildcard
        with patch.dict(os.environ, {'ALLOWED_ORIGINS': '["*"]'}):
            settings = Settings()
            assert settings.allowed_origins == ["*"]
        
        # Test invalid JSON (should use default)
        with patch.dict(os.environ, {'ALLOWED_ORIGINS': 'invalid json'}):
            settings = Settings()
            # Should fallback to treating as single string
            assert settings.allowed_origins == ["invalid json"]
    
    def test_settings_allowed_origins_edge_cases(self):
        """Test allowed_origins edge cases"""
        # Empty string
        with patch.dict(os.environ, {'ALLOWED_ORIGINS': ''}):
            settings = Settings()
            assert settings.allowed_origins == [""]
        
        # Empty JSON array
        with patch.dict(os.environ, {'ALLOWED_ORIGINS': '[]'}):
            settings = Settings()
            assert settings.allowed_origins == []
        
        # Mixed types in JSON (should cause validation error)
        with patch.dict(os.environ, {'ALLOWED_ORIGINS': '[true, 123, "http://test.com"]'}):
            with pytest.raises(ValidationError):
                Settings()
    
    def test_settings_serialization(self):
        """Test Settings serialization"""
        settings = Settings()
        
        settings_dict = settings.model_dump()
        
        # Check all fields are present
        expected_fields = [
            "host", "port", "environment", "api_title", 
            "api_version", "api_description", "allowed_origins"
        ]
        
        for field in expected_fields:
            assert field in settings_dict
        
        # Check types
        assert isinstance(settings_dict["host"], str)
        assert isinstance(settings_dict["port"], int)
        assert isinstance(settings_dict["environment"], str)
        assert isinstance(settings_dict["api_title"], str)
        assert isinstance(settings_dict["api_version"], str)
        assert isinstance(settings_dict["api_description"], str)
        assert isinstance(settings_dict["allowed_origins"], list)
    
    def test_settings_env_file_loading(self):
        """Test settings loading from environment variables (simulating env file)"""
        # Instead of mocking env file path, directly set environment variables
        test_env_vars = {
            "HOST": "test.example.com",
            "PORT": "8000", 
            "ENVIRONMENT": "testing",
            "API_TITLE": "Test API",
            "ALLOWED_ORIGINS": '["http://test1.com", "http://test2.com"]'
        }
        
        with patch.dict(os.environ, test_env_vars, clear=False):
            settings = Settings()
            
            # Should load values from environment
            assert settings.host == "test.example.com"
            assert settings.port == 8000
            assert settings.environment == "testing" 
            assert settings.api_title == "Test API"
            assert settings.allowed_origins == ["http://test1.com", "http://test2.com"]


class TestSettingsIntegration:
    """Test Settings integration scenarios"""
    
    def test_settings_environment_precedence(self):
        """Test environment variable precedence and defaults"""
        # Test with environment variables set
        with patch.dict(os.environ, {
            'HOST': 'env.example.com',
            'PORT': '9000',
            'API_TITLE': 'Environment API Title'
        }, clear=False):
            settings = Settings()
            
            # Environment variables should be used
            assert settings.host == "env.example.com"
            assert settings.port == 9000
            assert settings.api_title == "Environment API Title"
            
        # Test defaults when environment variables are cleared
        excluded_vars = {'HOST', 'PORT', 'API_TITLE'}
        clean_env = {k: v for k, v in os.environ.items() if k not in excluded_vars}
        with patch.dict(os.environ, clean_env, clear=True):
            settings = Settings()
            
            # Should use values from env file or code defaults
            assert settings.host == "localhost"  # from .env file
            assert settings.port == 8008  # actual default
    
    def test_settings_cors_configuration_scenarios(self):
        """Test various CORS configuration scenarios"""
        cors_test_cases = [
            # Development - allow all
            ('["*"]', ["*"]),
            
            # Production - specific domains
            ('["https://app.example.com", "https://api.example.com"]', 
             ["https://app.example.com", "https://api.example.com"]),
            
            # Mixed HTTP/HTTPS for development
            ('["http://localhost:3000", "https://localhost:3000", "http://127.0.0.1:3000"]',
             ["http://localhost:3000", "https://localhost:3000", "http://127.0.0.1:3000"]),
            
            # Single domain
            ('"https://single.example.com"', ["https://single.example.com"]),
            
            # Common frontend development ports
            ('["http://localhost:3000", "http://localhost:8080", "http://localhost:4200"]',
             ["http://localhost:3000", "http://localhost:8080", "http://localhost:4200"])
        ]
        
        for origins_str, expected in cors_test_cases:
            with patch.dict(os.environ, {'ALLOWED_ORIGINS': origins_str}):
                settings = Settings()
                assert settings.allowed_origins == expected
    
    def test_settings_production_security_best_practices(self):
        """Test production security configuration"""
        # Production-like configuration
        with patch.dict(os.environ, {
            'ENVIRONMENT': 'production',
            'HOST': '0.0.0.0',  # Listen on all interfaces
            'PORT': '443',  # HTTPS port
            'ALLOWED_ORIGINS': '["https://app.example.com", "https://dashboard.example.com"]'
        }):
            settings = Settings()
            
            assert settings.environment == 'production'
            assert settings.host == '0.0.0.0'
            assert settings.port == 443
            
            # Should not allow wildcard in production
            assert "*" not in settings.allowed_origins
            assert all(origin.startswith('https://') for origin in settings.allowed_origins)
    
    def test_settings_development_convenience_features(self):
        """Test development-focused convenience features"""
        # Development-like configuration
        with patch.dict(os.environ, {
            'ENVIRONMENT': 'development',
            'HOST': 'localhost',
            'PORT': '8008',
            'ALLOWED_ORIGINS': '["*"]'  # Allow all for development ease
        }):
            settings = Settings()
            
            assert settings.environment == 'development'
            assert settings.host == 'localhost'
            assert settings.port == 8008
            assert settings.allowed_origins == ["*"]


class TestSettingsEdgeCases:
    """Test edge cases and error conditions"""
    
    def test_settings_with_malformed_json_origins(self):
        """Test handling of malformed JSON in ALLOWED_ORIGINS"""
        # Cases that should raise validation errors (valid JSON but wrong types)
        validation_error_cases = [
            '{"not": "a list"}',  # Valid JSON dict but not a list
            'true',               # Valid JSON boolean
        ]
        
        for malformed in validation_error_cases:
            with patch.dict(os.environ, {'ALLOWED_ORIGINS': malformed}):
                # Should raise validation errors for invalid types
                with pytest.raises(ValidationError):
                    Settings()
                    
        # Cases that work (fallback behavior)
        # null JSON returns None, which falls back to default/env file value
        with patch.dict(os.environ, {'ALLOWED_ORIGINS': 'null'}):
            settings = Settings()
            # Should fall back to default values (from .env file)
            assert isinstance(settings.allowed_origins, list)
            assert len(settings.allowed_origins) > 0  # has default values
                    
        # Invalid JSON that falls back to comma-separated parsing
        with patch.dict(os.environ, {'ALLOWED_ORIGINS': '[unterminated'}):
            settings = Settings()
            # Invalid JSON should fall back to comma-separated string parsing
            assert settings.allowed_origins == ['[unterminated']
    
    def test_settings_extreme_values(self):
        """Test settings with extreme values"""
        # Maximum valid port
        with patch.dict(os.environ, {'PORT': '65535'}):
            settings = Settings()
            assert settings.port == 65535
        
        # Minimum valid port
        with patch.dict(os.environ, {'PORT': '1'}):
            settings = Settings()
            assert settings.port == 1
        
        # Very long string values
        long_title = "A" * 1000
        with patch.dict(os.environ, {'API_TITLE': long_title}):
            settings = Settings()
            assert settings.api_title == long_title
            assert len(settings.api_title) == 1000
        
        # Empty string values
        with patch.dict(os.environ, {
            'HOST': '',
            'API_TITLE': '',
            'API_DESCRIPTION': ''
        }):
            settings = Settings()
            assert settings.host == ''
            assert settings.api_title == ''
            assert settings.api_description == ''
    
    def test_settings_unicode_and_special_characters(self):
        """Test settings with unicode and special characters"""
        with patch.dict(os.environ, {
            'API_TITLE': 'EDGP 规则引擎 API',  # Chinese characters
            'API_DESCRIPTION': 'Validación de datos con símbolos especiales: @#$%^&*()',  # Spanish + symbols
            'HOST': 'münchen.example.com',  # German umlaut
        }):
            settings = Settings()
            
            assert 'EDGP 规则引擎 API' == settings.api_title
            assert 'Validación de datos' in settings.api_description
            assert settings.host == 'münchen.example.com'
    
    def test_settings_case_sensitivity(self):
        """Test environment variable case sensitivity"""
        # Settings is configured with case_sensitive=False, so both should work
        with patch.dict(os.environ, {
            'host': 'lowercase.example.com',  # lowercase
            'PORT': '8080',  # uppercase
        }):
            settings = Settings()
            
            # Both lowercase and uppercase should work (case_sensitive=False)
            assert settings.host == 'lowercase.example.com'  # lowercase works
            assert settings.port == 8080  # uppercase works
            
        # Test mixed case
        with patch.dict(os.environ, {
            'Host': 'mixedcase.example.com',  # mixed case
            'port': '9090',  # lowercase 
        }):
            settings = Settings()
            
            assert settings.host == 'mixedcase.example.com'  # mixed case works
            assert settings.port == 9090  # lowercase works
    
    def test_settings_type_coercion(self):
        """Test automatic type coercion"""
        # String numbers should be converted
        with patch.dict(os.environ, {
            'PORT': '8080',  # String that should become int
        }):
            settings = Settings()
            assert isinstance(settings.port, int)
            assert settings.port == 8080
        
        # Boolean-like strings
        with patch.dict(os.environ, {
            'ENVIRONMENT': 'true',  # Should remain string, not become boolean
        }):
            settings = Settings()
            assert isinstance(settings.environment, str)
            assert settings.environment == 'true'


class TestSettingsRealWorldScenarios:
    """Test real-world deployment scenarios"""
    
    def test_docker_deployment_scenario(self):
        """Test typical Docker deployment configuration"""
        with patch.dict(os.environ, {
            'HOST': '0.0.0.0',
            'PORT': '8000',
            'ENVIRONMENT': 'production',
            'API_TITLE': 'Production EDGP API',
            'ALLOWED_ORIGINS': '["https://dashboard.company.com", "https://api.company.com"]'
        }):
            settings = Settings()
            
            # Typical Docker setup
            assert settings.host == '0.0.0.0'  # Listen on all interfaces
            assert settings.port == 8000
            assert settings.environment == 'production'
            assert len(settings.allowed_origins) == 2
            assert all('https://' in origin for origin in settings.allowed_origins)
    
    def test_kubernetes_deployment_scenario(self):
        """Test typical Kubernetes deployment configuration"""
        with patch.dict(os.environ, {
            'HOST': '0.0.0.0',
            'PORT': '8080',  # Common k8s port
            'ENVIRONMENT': 'production',
            'API_TITLE': 'K8s EDGP Rules Engine',
            'API_VERSION': '1.2.3',
            'ALLOWED_ORIGINS': '["https://ingress.k8s.cluster.local"]'
        }):
            settings = Settings()
            
            assert settings.host == '0.0.0.0'
            assert settings.port == 8080
            assert settings.api_version == '1.2.3'
            assert 'k8s.cluster.local' in settings.allowed_origins[0]
    
    def test_local_development_scenario(self):
        """Test typical local development configuration"""
        with patch.dict(os.environ, {
            'HOST': 'localhost',
            'PORT': '8008',
            'ENVIRONMENT': 'development',
            'ALLOWED_ORIGINS': '["http://localhost:3000", "http://localhost:8080", "http://127.0.0.1:3000"]'
        }):
            settings = Settings()
            
            assert settings.host == 'localhost'
            assert settings.port == 8008
            assert settings.environment == 'development'
            # Should allow common dev server ports
            dev_ports = ['3000', '8080']
            origins_str = str(settings.allowed_origins)
            for port in dev_ports:
                assert port in origins_str


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
