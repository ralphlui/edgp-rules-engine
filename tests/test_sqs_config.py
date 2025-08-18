"""
Comprehensive unit tests for app.sqs.config module
Tests SQS configuration settings and URL resolution functionality
"""
import pytest
import os
import tempfile
from pathlib import Path
from unittest.mock import patch, MagicMock
from pydantic import ValidationError

from app.sqs.config import SQSSettings, get_sqs_env_file_path


class TestGetSqsEnvFilePath:
    """Test get_sqs_env_file_path function"""
    
    def test_get_sqs_env_file_path_no_app_env(self):
        """Test env file path selection with no APP_ENV set"""
        with patch.dict(os.environ, {}, clear=True):
            if 'APP_ENV' in os.environ:
                del os.environ['APP_ENV']
            
            with patch('app.sqs.config.Path') as mock_path:
                mock_project_root = MagicMock()
                mock_path.return_value.parent.parent.parent = mock_project_root
                
                # Mock .env exists
                mock_env_file = MagicMock()
                mock_env_file.exists.return_value = True
                mock_project_root.__truediv__ = lambda self, other: mock_env_file
                
                result = get_sqs_env_file_path()
                
                # Should return path to .env file
                assert result == str(mock_env_file)
    
    def test_get_sqs_env_file_path_with_sit_env(self):
        """Test env file path selection with APP_ENV=SIT"""
        with patch.dict(os.environ, {'APP_ENV': 'SIT'}):
            with patch('app.sqs.config.Path') as mock_path:
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
                
                result = get_sqs_env_file_path()
                
                # Should return path to .env.development
                assert result == str(mock_env_dev)
    
    def test_get_sqs_env_file_path_with_prod_env(self):
        """Test env file path selection with APP_ENV=PROD"""
        with patch.dict(os.environ, {'APP_ENV': 'PROD'}):
            with patch('app.sqs.config.Path') as mock_path:
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
                
                result = get_sqs_env_file_path()
                
                # Should return path to .env.production
                assert result == str(mock_env_prod)
    
    def test_get_sqs_env_file_path_fallback_to_development(self):
        """Test fallback to .env.development when specific env file doesn't exist"""
        with patch.dict(os.environ, {'APP_ENV': 'SIT'}):
            with patch('app.sqs.config.Path') as mock_path:
                mock_project_root = MagicMock()
                mock_path.return_value.parent.parent.parent = mock_project_root
                
                # Mock specific env file doesn't exist, but .env.development does
                mock_env_dev = MagicMock()
                mock_env_dev.exists.return_value = True
                
                def mock_truediv(self, other):
                    if other == ".env.development":
                        return mock_env_dev
                    mock_file = MagicMock()
                    mock_file.exists.return_value = False
                    return mock_file
                
                mock_project_root.__truediv__ = mock_truediv
                
                result = get_sqs_env_file_path()
                
                # Should fallback to .env.development
                assert result == str(mock_env_dev)
    
    def test_get_sqs_env_file_path_default_when_no_files_exist(self):
        """Test default path when no env files exist"""
        with patch.dict(os.environ, {'APP_ENV': 'TEST'}):
            with patch('app.sqs.config.Path') as mock_path:
                mock_project_root = MagicMock()
                mock_path.return_value.parent.parent.parent = mock_project_root
                
                # Mock default .env path
                mock_default = MagicMock()
                
                def mock_truediv(self, other):
                    if other == ".env":
                        return mock_default
                    mock_file = MagicMock()
                    mock_file.exists.return_value = False
                    return mock_file
                
                mock_project_root.__truediv__ = mock_truediv
                
                result = get_sqs_env_file_path()
                
                # Should return default .env path
                assert result == str(mock_default)


class TestSQSSettings:
    """Test SQSSettings class"""
    
    def test_sqs_settings_creation_with_defaults(self):
        """Test SQSSettings creation with default values from .env file"""
        settings = SQSSettings()
        
        # Test values loaded from .env file (with placeholder values)
        assert settings.aws_access_key_id is not None  # From .env file
        assert settings.aws_secret_access_key is not None  # From .env file
        assert settings.aws_region is not None  # From .env file (actual value)
        assert settings.aws_session_token is None
        
        # Queue URLs from env with template format
        assert "SQS_INBOUND_QUEUE" == settings.input_queue_url
        assert "SQS_OUTBOUND_QUEUE" == settings.output_queue_url  
        
        assert settings.max_messages_per_poll == 10
        assert settings.visibility_timeout == 300
        assert settings.wait_time_seconds == 20  # Updated from actual default
        assert settings.poll_interval == 5
        assert settings.max_retry_delay == 300
        
        assert settings.worker_count == 4
        assert settings.auto_start_workers == True  # Actual default value
        assert settings.max_retries == 3
        assert settings.retry_delay == 30
        
        assert settings.processing_timeout == 120
        assert settings.batch_processing == True
        assert settings.health_check_interval == 60
    
    def test_sqs_settings_creation_with_custom_values(self):
        """Test SQSSettings creation with custom values"""
        with patch.dict(os.environ, {
            'SQS_AWS_ACCESS_KEY_ID': 'test_key',
            'SQS_AWS_SECRET_ACCESS_KEY': 'test_secret',
            'SQS_AWS_REGION': 'eu-west-1',
            'SQS_INPUT_QUEUE_URL': 'https://sqs.eu-west-1.amazonaws.com/123456789012/input',
            'SQS_OUTPUT_QUEUE_URL': 'https://sqs.eu-west-1.amazonaws.com/123456789012/output',
            'SQS_DLQ_URL': 'https://sqs.eu-west-1.amazonaws.com/123456789012/dlq',
            'SQS_MAX_MESSAGES_PER_POLL': '5',
            'SQS_WORKER_COUNT': '8',
            'SQS_AUTO_START_WORKERS': 'true',
            'SQS_PROCESSING_TIMEOUT': '180'
        }):
            settings = SQSSettings()
            
            assert settings.aws_access_key_id == 'test_key'
            assert settings.aws_secret_access_key == 'test_secret'
            assert settings.aws_region == 'eu-west-1'
            assert settings.input_queue_url == 'https://sqs.eu-west-1.amazonaws.com/123456789012/input'
            assert settings.output_queue_url == 'https://sqs.eu-west-1.amazonaws.com/123456789012/output'
            assert settings.dlq_url == 'https://sqs.eu-west-1.amazonaws.com/123456789012/dlq'
            assert settings.max_messages_per_poll == 5
            assert settings.worker_count == 8
            assert settings.auto_start_workers == True
            assert settings.processing_timeout == 180
    
    def test_sqs_settings_validation_constraints(self):
        """Test SQSSettings validation constraints"""
        # Test invalid max_messages_per_poll (> 10)
        with patch.dict(os.environ, {'SQS_MAX_MESSAGES_PER_POLL': '15'}):
            with pytest.raises(ValidationError) as exc_info:
                SQSSettings()
            assert "Input should be less than or equal to 10" in str(exc_info.value)  # Pydantic v2 message
        
        # Test invalid wait_time_seconds (> 20)
        with patch.dict(os.environ, {'SQS_WAIT_TIME_SECONDS': '25'}):
            with pytest.raises(ValidationError) as exc_info:
                SQSSettings()
            assert "Input should be less than or equal to 20" in str(exc_info.value)  # Pydantic v2 message
        
        # Test invalid worker_count (< 1)
        with patch.dict(os.environ, {'SQS_WORKER_COUNT': '0'}):
            with pytest.raises(ValidationError) as exc_info:
                SQSSettings()
            assert "Input should be greater than or equal to 1" in str(exc_info.value)  # Pydantic v2 message
    
    def test_sqs_settings_legacy_support(self):
        """Test legacy queue URL support in model_post_init"""
        with patch.dict(os.environ, {
            'SQS_SQS_QUEUE_URL': 'https://sqs.ap-southeast-1.amazonaws.com/123456789012/legacy_input',
            'SQS_SQS_DLQ_URL': 'https://sqs.ap-southeast-1.amazonaws.com/123456789012/legacy_dlq',
            # Clear primary fields to test legacy mapping
            'SQS_INPUT_QUEUE_URL': '',
            'SQS_DLQ_URL': ''
        }):
            settings = SQSSettings()
            
            # Legacy URLs should be mapped to new fields
            assert settings.input_queue_url == 'https://sqs.ap-southeast-1.amazonaws.com/123456789012/legacy_input'
            assert settings.dlq_url == 'https://sqs.ap-southeast-1.amazonaws.com/123456789012/legacy_dlq'
    
    def test_sqs_settings_has_output_queue(self):
        """Test has_output_queue property"""
        # With default output queue from .env file
        settings = SQSSettings()
        assert settings.has_output_queue == True  # .env sets OUTPUT_QUEUE/sqs_queue_output
        
        # With explicit output queue
        with patch.dict(os.environ, {'SQS_OUTPUT_QUEUE_URL': 'https://test.queue'}):
            settings = SQSSettings()
            assert settings.has_output_queue == True

class TestSQSSettingsURLResolution:
    """Test SQS settings URL resolution methods"""
    
    def test_build_queue_url_with_full_url(self):
        """Test build_queue_url with full URL (should return as-is)"""
        settings = SQSSettings()
        full_url = 'https://sqs.ap-southeast-1.amazonaws.com/123456789012/my-queue'
        
        result = settings.build_queue_url(full_url)
        assert result == full_url
    
    def test_build_queue_url_with_template_and_env_account(self):
        """Test build_queue_url with template and account ID from environment"""
        with patch.dict(os.environ, {'AWS_ACCOUNT_ID': '123456789012'}):
            settings = SQSSettings()
            settings.aws_region = 'us-west-2'
            
            result = settings.build_queue_url('INPUT_QUEUE/my-input')
            expected = 'https://sqs.us-west-2.amazonaws.com/123456789012/my-input'
            assert result == expected
    
    def test_build_queue_url_with_template_and_runtime_params(self):
        """Test build_queue_url with template and runtime parameters"""
        settings = SQSSettings()
        
        result = settings.build_queue_url(
            'OUTPUT_QUEUE/my-output',
            account_id='999999999999',
            region='eu-central-1'
        )
        expected = 'https://sqs.eu-central-1.amazonaws.com/999999999999/my-output'
        assert result == expected
    
    def test_build_queue_url_with_queue_base_name_override(self):
        """Test build_queue_url with queue base name override"""
        settings = SQSSettings()
        
        result = settings.build_queue_url(
            'INPUT_QUEUE/old-name',
            queue_base_name='new-name',
            account_id='123456789012',
            region='ap-southeast-1'
        )
        expected = 'https://sqs.ap-southeast-1.amazonaws.com/123456789012/new-name'
        assert result == expected
    
    def test_build_queue_url_extract_account_from_existing_url(self):
        """Test build_queue_url extracting account ID from existing URL"""
        with patch.dict(os.environ, {
            'SQS_INPUT_QUEUE_URL': 'https://sqs.ap-southeast-1.amazonaws.com/555555555555/existing-queue'
        }):
            settings = SQSSettings()
            
            result = settings.build_queue_url('OUTPUT_QUEUE/new-queue')
            expected = 'https://sqs.ap-southeast-1.amazonaws.com/555555555555/new-queue'
            assert result == expected
    
    def test_build_queue_url_no_account_id_available(self):
        """Test build_queue_url when no account ID is available"""
        with patch.dict(os.environ, {}, clear=True):
            settings = SQSSettings()
            
            result = settings.build_queue_url('INPUT_QUEUE/my-queue')
            # Should return template as-is when account ID unavailable
            assert result == 'INPUT_QUEUE/my-queue'
    
    def test_build_queue_url_simple_queue_name(self):
        """Test build_queue_url with simple queue name (no template prefix)"""
        settings = SQSSettings()
        
        result = settings.build_queue_url(
            'simple-queue',
            account_id='123456789012',
            region='ap-southeast-1'
        )
        expected = 'https://sqs.ap-southeast-1.amazonaws.com/123456789012/simple-queue'
        assert result == expected
    
    def test_build_queue_url_none_template(self):
        """Test build_queue_url with None template"""
        settings = SQSSettings()
        
        result = settings.build_queue_url(None)
        assert result is None
    
    def test_get_input_queue_url(self):
        """Test get_input_queue_url method"""
        with patch.dict(os.environ, {'SQS_INPUT_QUEUE_URL': 'INPUT_QUEUE/test-input'}):
            settings = SQSSettings()
            
            # Test with account ID and region
            result = settings.get_input_queue_url(account_id='123456789012', region='us-west-2')
            
            # Should call build_queue_url internally and return a constructed URL
            assert 'https://sqs.us-west-2.amazonaws.com/123456789012/' in result
            assert 'test-input' in result
    
    def test_get_output_queue_url(self):
        """Test get_output_queue_url method"""
        with patch.dict(os.environ, {'SQS_OUTPUT_QUEUE_URL': 'OUTPUT_QUEUE/test-output'}):
            settings = SQSSettings()
            
            # Test with account ID and region
            result = settings.get_output_queue_url(account_id='456789012345', region='eu-west-1')
            
            # Should call build_queue_url internally and return a constructed URL
            assert 'https://sqs.eu-west-1.amazonaws.com/456789012345/' in result
            assert 'test-output' in result

    def test_get_dlq_url(self):
        """Test get_dlq_url method"""
        with patch.dict(os.environ, {'SQS_DLQ_URL': 'DLQ_QUEUE/test-dlq'}):
            settings = SQSSettings()
            
            # Test with account ID and region
            result = settings.get_dlq_url(account_id='789012345678', region='ap-northeast-1')
            
            # Should call build_queue_url internally and return a constructed URL
            assert 'https://sqs.ap-northeast-1.amazonaws.com/789012345678/' in result
            assert 'test-dlq' in result


class TestSQSSettingsEdgeCases:
    """Test edge cases and error conditions"""
    
    def test_build_queue_url_invalid_existing_url_format(self):
        """Test build_queue_url with invalid existing URL format"""
        with patch.dict(os.environ, {
            'SQS_INPUT_QUEUE_URL': 'invalid-url-format'
        }):
            settings = SQSSettings()
            
            # Should handle gracefully and return template as-is
            result = settings.build_queue_url('OUTPUT_QUEUE/new-queue')
            assert result == 'OUTPUT_QUEUE/new-queue'
    
    def test_build_queue_url_malformed_existing_url(self):
        """Test build_queue_url with malformed existing URL"""
        with patch.dict(os.environ, {
            'SQS_OUTPUT_QUEUE_URL': 'https://sqs.region.amazonaws.com/malformed'
        }):
            settings = SQSSettings()
            
            # Should extract "malformed" as account ID and build URL with it
            result = settings.build_queue_url('INPUT_QUEUE/test')
            assert result == 'https://sqs.ap-southeast-1.amazonaws.com/malformed/test'
    
    def test_complex_template_formats(self):
        """Test various template formats"""
        settings = SQSSettings()
        test_cases = [
            ('INPUT_QUEUE/simple', 'simple'),
            ('OUTPUT_QUEUE/with-dashes', 'with-dashes'),
            ('DLQ_QUEUE/with_underscores', 'with_underscores'),
            ('CUSTOM_QUEUE/custom.dots', 'custom.dots'),
            ('just-a-name', 'just-a-name'),
            ('', ''),  # Edge case: empty template
        ]
        
        for template, expected_queue_name in test_cases:
            if template:  # Skip empty template test
                result = settings.build_queue_url(
                    template,
                    account_id='123456789012',
                    region='ap-southeast-1'
                )
                if '/' in template:
                    queue_name = template.split('/', 1)[1]
                else:
                    queue_name = template
                expected = f'https://sqs.ap-southeast-1.amazonaws.com/123456789012/{queue_name}'
                assert result == expected
    
    def test_environment_variable_precedence(self):
        """Test environment variable precedence for account ID"""
        with patch.dict(os.environ, {
            'AWS_ACCOUNT_ID': '111111111111',
            'SQS_ACCOUNT_ID': '222222222222'
        }):
            settings = SQSSettings()
            
            # AWS_ACCOUNT_ID should take precedence
            result = settings.build_queue_url('INPUT_QUEUE/test')
            expected = 'https://sqs.ap-southeast-1.amazonaws.com/111111111111/test'
            assert result == expected
    
    def test_region_parameter_override(self):
        """Test region parameter overriding configured region"""
        settings = SQSSettings()
        settings.aws_region = 'ap-southeast-1'
        
        # Runtime region should override configured region
        result = settings.build_queue_url(
            'INPUT_QUEUE/test',
            account_id='123456789012',
            region='eu-west-1'  # Override
        )
        expected = 'https://sqs.eu-west-1.amazonaws.com/123456789012/test'
        assert result == expected
    
    def test_multiple_slashes_in_template(self):
        """Test template with multiple slashes"""
        settings = SQSSettings()
        
        # Should only split on first slash
        result = settings.build_queue_url(
            'INPUT_QUEUE/path/with/slashes',
            account_id='123456789012',
            region='ap-southeast-1'
        )
        expected = 'https://sqs.ap-southeast-1.amazonaws.com/123456789012/path/with/slashes'
        assert result == expected


class TestSQSSettingsRealWorldScenarios:
    """Test real-world usage scenarios"""
    
    def test_development_environment_setup(self):
        """Test typical development environment setup"""
        with patch.dict(os.environ, {
            'AWS_ACCOUNT_ID': '123456789012',
            'SQS_AWS_REGION': 'ap-southeast-1',
            'SQS_INPUT_QUEUE_URL': 'INPUT_QUEUE/dev_validation_input',
            'SQS_OUTPUT_QUEUE_URL': 'OUTPUT_QUEUE/dev_validation_output',
            'SQS_DLQ_URL': 'DLQ_QUEUE/dev_validation_dlq',
            'SQS_WORKER_COUNT': '2',
            'SQS_AUTO_START_WORKERS': 'true'
        }):
            settings = SQSSettings()
            
            # Test resolved URLs
            input_url = settings.get_input_queue_url()
            output_url = settings.get_output_queue_url()
            dlq_url = settings.get_dlq_url()
            
            assert input_url == 'https://sqs.ap-southeast-1.amazonaws.com/123456789012/dev_validation_input'
            assert output_url == 'https://sqs.ap-southeast-1.amazonaws.com/123456789012/dev_validation_output'
            assert dlq_url == 'https://sqs.ap-southeast-1.amazonaws.com/123456789012/dev_validation_dlq'
            
            # Test other settings
            assert settings.worker_count == 2
            assert settings.auto_start_workers == True
            assert settings.has_output_queue == True
            assert settings.has_dlq == True
    
    def test_production_environment_setup(self):
        """Test typical production environment setup"""
        with patch.dict(os.environ, {
            'AWS_ACCOUNT_ID': '999999999999',
            'SQS_AWS_REGION': 'ap-southeast-1',
            'SQS_INPUT_QUEUE_URL': 'INPUT_QUEUE/prod_validation_input',
            'SQS_OUTPUT_QUEUE_URL': 'OUTPUT_QUEUE/prod_validation_output',
            'SQS_DLQ_URL': 'DLQ_QUEUE/prod_validation_dlq',
            'SQS_WORKER_COUNT': '8',
            'SQS_MAX_RETRIES': '5',
            'SQS_PROCESSING_TIMEOUT': '300'
        }):
            settings = SQSSettings()
            
            # Test resolved URLs
            input_url = settings.get_input_queue_url()
            output_url = settings.get_output_queue_url()
            
            assert input_url == 'https://sqs.ap-southeast-1.amazonaws.com/999999999999/prod_validation_input'
            assert output_url == 'https://sqs.ap-southeast-1.amazonaws.com/999999999999/prod_validation_output'
            
            # Test production settings
            assert settings.worker_count == 8
            assert settings.max_retries == 5
            assert settings.processing_timeout == 300
    
    def test_migration_from_legacy_urls(self):
        """Test migration scenario from legacy full URLs"""
        with patch.dict(os.environ, {
            'SQS_SQS_QUEUE_URL': 'https://sqs.ap-southeast-1.amazonaws.com/555555555555/legacy_input',
            'SQS_SQS_DLQ_URL': 'https://sqs.ap-southeast-1.amazonaws.com/555555555555/legacy_dlq',
            # Clear primary fields to test legacy mapping
            'SQS_INPUT_QUEUE_URL': '',
            'SQS_DLQ_URL': ''
        }):
            settings = SQSSettings()
            
            # Legacy URLs should be available through new interface
            assert settings.input_queue_url == 'https://sqs.ap-southeast-1.amazonaws.com/555555555555/legacy_input'
            assert settings.dlq_url == 'https://sqs.ap-southeast-1.amazonaws.com/555555555555/legacy_dlq'
            
            # Should work with get methods too
            input_url = settings.get_input_queue_url()
            dlq_url = settings.get_dlq_url()
            
            assert input_url == 'https://sqs.ap-southeast-1.amazonaws.com/555555555555/legacy_input'
            assert dlq_url == 'https://sqs.ap-southeast-1.amazonaws.com/555555555555/legacy_dlq'


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
