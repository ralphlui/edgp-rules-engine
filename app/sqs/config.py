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
    aws_access_key_id: Optional[str] = Field(default=None, description="AWS Access Key ID")
    aws_secret_access_key: Optional[str] = Field(default=None, description="AWS Secret Access Key")
    aws_region: str = Field(default="us-east-1", description="AWS Region")
    aws_session_token: Optional[str] = Field(default=None, description="AWS Session Token for temporary credentials")
    
    # SQS Queue Configuration
    input_queue_url: Optional[str] = Field(default=None, description="Input SQS Queue URL for validation requests")
    output_queue_url: Optional[str] = Field(default=None, description="Output SQS Queue URL for validation results")
    dlq_url: Optional[str] = Field(default=None, description="Dead Letter Queue URL for failed messages")
    
    # Legacy support (can be removed later)
    sqs_queue_url: Optional[str] = Field(default=None, description="Legacy queue URL (use input_queue_url instead)")
    sqs_dlq_url: Optional[str] = Field(default=None, description="Legacy DLQ URL (use dlq_url instead)")
    
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
        """Handle backward compatibility and validation"""
        # Handle legacy queue URL
        if not self.input_queue_url and self.sqs_queue_url:
            self.input_queue_url = self.sqs_queue_url
        
        # Handle legacy DLQ URL  
        if not self.dlq_url and self.sqs_dlq_url:
            self.dlq_url = self.sqs_dlq_url
    
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
