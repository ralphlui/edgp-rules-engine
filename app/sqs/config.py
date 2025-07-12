"""
SQS Configuration settings for AWS integration.
"""
from pydantic import BaseSettings, Field
from typing import Optional
import os

class SQSSettings(BaseSettings):
    """SQS-specific configuration settings"""
    
    # AWS Configuration
    aws_access_key_id: Optional[str] = Field(default=None, description="AWS Access Key ID")
    aws_secret_access_key: Optional[str] = Field(default=None, description="AWS Secret Access Key")
    aws_region: str = Field(default="us-east-1", description="AWS Region")
    aws_session_token: Optional[str] = Field(default=None, description="AWS Session Token for temporary credentials")
    
    # SQS Configuration
    sqs_queue_url: str = Field(..., description="SQS Queue URL for validation requests")
    sqs_dlq_url: Optional[str] = Field(default=None, description="Dead Letter Queue URL for failed messages")
    
    # Processing Configuration
    max_messages_per_poll: int = Field(default=10, ge=1, le=10, description="Maximum messages to retrieve per poll (1-10)")
    visibility_timeout: int = Field(default=300, ge=0, description="Message visibility timeout in seconds")
    wait_time_seconds: int = Field(default=20, ge=0, le=20, description="Long polling wait time (0-20 seconds)")
    
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
    
    class Config:
        env_file = ".env"
        env_prefix = "SQS_"
        case_sensitive = False

# Global SQS settings instance
sqs_settings = SQSSettings()
