"""
SQS Configuration settings for AWS integration.
"""
from pydantic_settings import BaseSettings
from pydantic import Field, ConfigDict
from typing import Optional
import os
from pathlib import Path

def get_sqs_env_file_path() -> str:
    """
    Get the same environment file path as the main config.
    This ensures SQS config uses the same environment file selection logic.
    """
    app_env = os.getenv("APP_ENV", "").upper()
    
    # Get the project root directory (where .env files are located)
    project_root = Path(__file__).parent.parent.parent
    
    # Environment mapping (same as main config)
    env_mapping = {
        "SIT": ".env.development",
        "DEV": ".env.development", 
        "DEVELOPMENT": ".env.development",
        "PRD": ".env.production",
        "PROD": ".env.production",
        "PRODUCTION": ".env.production"
    }
    
    if app_env in env_mapping:
        env_file = project_root / env_mapping[app_env]
        if env_file.exists():
            return str(env_file)
    
    # Fallback logic - only use .env if no specific environment was requested
    if not app_env:
        fallback_files = [".env", ".env.development"]
    else:
        fallback_files = [".env.development"]
        
    for fallback in fallback_files:
        fallback_path = project_root / fallback
        if fallback_path.exists():
            return str(fallback_path)
    
    # If no env files exist, return default path
    default_path = project_root / ".env"
    return str(default_path)

class SQSSettings(BaseSettings):
    """SQS-specific configuration settings"""
    
    # AWS Configuration
    app_env: Optional[str] = Field(default=None, description="Application environment")
    aws_access_key_id: Optional[str] = Field(default=None, description="AWS Access Key ID")
    aws_secret_access_key: Optional[str] = Field(default=None, description="AWS Secret Access Key")
    aws_region: str = Field(default="us-east-1", description="AWS Region")
    aws_session_token: Optional[str] = Field(default=None, description="AWS Session Token for temporary credentials")
    
    # SQS Queue Configuration
    input_queue_url: Optional[str] = Field(default=None, description="Input SQS Queue URL for validation requests")
    output_queue_url: Optional[str] = Field(default=None, description="Output SQS Queue URL for validation results")
    
    # Legacy support (can be removed later)
    sqs_queue_url: Optional[str] = Field(default=None, description="Legacy queue URL (use input_queue_url instead)")
    sqs_dlq_url: Optional[str] = Field(default=None, description="Legacy DLQ URL (use dlq_url instead)")
    dlq_url: Optional[str] = Field(default=None, description="Dead Letter Queue URL for failed messages")
    
    # Processing Configuration
    max_messages_per_poll: int = Field(default=10, ge=1, le=10, description="Maximum messages to retrieve per poll (1-10)")
    visibility_timeout: int = Field(default=300, ge=0, description="Message visibility timeout in seconds")
    wait_time_seconds: int = Field(default=5, ge=0, le=20, description="Long polling wait time (0-20 seconds)")
    poll_interval: int = Field(default=5, ge=1, description="Polling interval when no messages available")
    max_retry_delay: int = Field(default=300, ge=0, description="Maximum retry delay in seconds")
    
    # Worker Configuration
    worker_count: int = Field(default=4, ge=1, description="Number of concurrent worker processes")
    auto_start_workers: bool = Field(default=False, description="Auto-start workers on application startup")
    max_retries: int = Field(default=3, ge=0, description="Maximum retry attempts for failed messages")
    retry_delay: int = Field(default=30, ge=0, description="Delay between retries in seconds")
    
    # Processing Limits
    processing_timeout: int = Field(default=120, ge=1, description="Maximum time to process a single message")
    batch_processing: bool = Field(default=True, description="Enable batch processing for better performance")
    
    # Health Check
    health_check_interval: int = Field(default=60, ge=10, description="Health check interval in seconds")
    
    model_config = ConfigDict(
        env_file=get_sqs_env_file_path(),
        env_prefix="SQS_",
        case_sensitive=False,
        extra="ignore"
    )

    def model_post_init(self, __context) -> None:
        """Handle runtime environment variable overrides and backward compatibility"""
        # Override with runtime environment variables if they exist (SQS_ prefixed)
        if os.getenv("SQS_APP_ENV"):
            self.app_env = os.getenv("SQS_APP_ENV")
        if os.getenv("SQS_AWS_ACCESS_KEY_ID"):
            self.aws_access_key_id = os.getenv("SQS_AWS_ACCESS_KEY_ID")
        if os.getenv("SQS_AWS_SECRET_ACCESS_KEY"):
            self.aws_secret_access_key = os.getenv("SQS_AWS_SECRET_ACCESS_KEY")
        if os.getenv("SQS_AWS_REGION"):
            self.aws_region = os.getenv("SQS_AWS_REGION")
        if os.getenv("SQS_INPUT_QUEUE_URL"):
            self.input_queue_url = os.getenv("SQS_INPUT_QUEUE_URL")
        if os.getenv("SQS_OUTPUT_QUEUE_URL"):
            self.output_queue_url = os.getenv("SQS_OUTPUT_QUEUE_URL")
        
        # Handle runtime environment variables passed directly (non-prefixed)
        # This allows passing AWS_REGION, AWS_ACCESS_KEY_ID, etc. at runtime
        # Check if current values are references to environment variable names
        if self.app_env and self.app_env in os.environ:
            self.app_env = os.getenv(self.app_env)
        if self.aws_access_key_id and self.aws_access_key_id in os.environ:
            self.aws_access_key_id = os.getenv(self.aws_access_key_id)
        if self.aws_secret_access_key and self.aws_secret_access_key in os.environ:
            self.aws_secret_access_key = os.getenv(self.aws_secret_access_key)
        if self.aws_region and self.aws_region in os.environ:
            self.aws_region = os.getenv(self.aws_region)
        if self.input_queue_url and self.input_queue_url in os.environ:
            self.input_queue_url = os.getenv(self.input_queue_url)
        if self.output_queue_url and self.output_queue_url in os.environ:
            self.output_queue_url = os.getenv(self.output_queue_url)
        
        # Handle legacy queue URL mapping
        if not self.input_queue_url and self.sqs_queue_url:
            self.input_queue_url = self.sqs_queue_url
        
        # Handle legacy DLQ URL mapping
        if not self.dlq_url and self.sqs_dlq_url:
            self.dlq_url = self.sqs_dlq_url
    
    def build_queue_url(self, url_template: Optional[str], queue_base_name: Optional[str] = None, 
                       account_id: Optional[str] = None, region: Optional[str] = None) -> Optional[str]:
        """
        Build a full SQS queue URL from a template and runtime parameters.
        
        Supports templates like:
        - INPUT_QUEUE/my_queue -> https://sqs.{region}.amazonaws.com/{account_id}/my_queue
        - OUTPUT_QUEUE/my_output -> https://sqs.{region}.amazonaws.com/{account_id}/my_output
        - https://full.url/path -> returns as-is (already full URL)
        
        Args:
            url_template: Template string or full URL
            queue_base_name: Override queue name from template
            account_id: AWS account ID (from environment or parameter)
            region: AWS region (defaults to configured region)
            
        Returns:
            Full SQS queue URL or None if template is invalid
        """
        if not url_template:
            return None
            
        # If already a full URL (starts with https://), return as-is
        if url_template.startswith('https://'):
            return url_template
            
        # Get runtime parameters from environment if not provided
        account_id = account_id or os.getenv('AWS_ACCOUNT_ID') or os.getenv('SQS_ACCOUNT_ID')
        region = region or self.aws_region
        
        if not account_id:
            # Try to extract account ID from existing full URLs in environment
            for env_var in ['SQS_INPUT_QUEUE_URL', 'SQS_OUTPUT_QUEUE_URL', 'SQS_DLQ_URL']:
                existing_url = os.getenv(env_var, '')
                if 'amazonaws.com/' in existing_url:
                    try:
                        account_id = existing_url.split('amazonaws.com/')[1].split('/')[0]
                        break
                    except IndexError:
                        continue
        
        if not account_id:
            # Cannot build URL without account ID
            return url_template  # Return template as-is, let AWS SDK handle the error
            
        # Parse template (format: TEMPLATE_NAME/queue_name)
        if '/' in url_template:
            template_type, queue_name = url_template.split('/', 1)
            queue_name = queue_base_name or queue_name
        else:
            # Assume it's just a queue name
            queue_name = queue_base_name or url_template
            
        # Build full URL
        return f"https://sqs.{region}.amazonaws.com/{account_id}/{queue_name}"
    
    def get_input_queue_url(self, account_id: Optional[str] = None, region: Optional[str] = None) -> Optional[str]:
        """Get the resolved input queue URL"""
        return self.build_queue_url(self.input_queue_url, account_id=account_id, region=region)
    
    def get_output_queue_url(self, account_id: Optional[str] = None, region: Optional[str] = None) -> Optional[str]:
        """Get the resolved output queue URL"""
        return self.build_queue_url(self.output_queue_url, account_id=account_id, region=region)
    
    def get_dlq_url(self, account_id: Optional[str] = None, region: Optional[str] = None) -> Optional[str]:
        """Get the resolved DLQ URL"""
        return self.build_queue_url(self.dlq_url, account_id=account_id, region=region)
    
    @property
    def has_output_queue(self) -> bool:
        """Check if output queue is configured"""
        return bool(self.output_queue_url)
    
    @property 
    def has_dlq(self) -> bool:
        """Check if DLQ is configured"""
        return bool(self.dlq_url)

# Global SQS settings instance
sqs_settings = SQSSettings()
