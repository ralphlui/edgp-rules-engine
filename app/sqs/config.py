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
    
    # SQS Queue Configuration
    input_queue_url: str = Field(..., description="Input SQS Queue URL for validation requests")
    output_queue_url: Optional[str] = Field(default=None, description="Output SQS Queue URL for validation results")
    dlq_url: Optional[str] = Field(default=None, description="Dead Letter Queue URL for failed messages")
    
    # Legacy support (can be removed later)
    sqs_queue_url: Optional[str] = Field(default=None, description="Legacy queue URL (use input_queue_url instead)")
    sqs_dlq_url: Optional[str] = Field(default=None, description="Legacy DLQ URL (use dlq_url instead)")
    
    # Processing Configuration
    max_messages_per_poll: int = Field(default=10, ge=1, le=10, description="Maximum messages to retrieve per poll (1-10)")
    visibility_timeout: int = Field(default=300, ge=0, description="Message visibility timeout in seconds")
    wait_time_seconds: int = Field(default=20, ge=0, le=20, description="Long polling wait time (0-20 seconds)")
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
    
    class Config:
        env_file = ".env"
        env_prefix = "SQS_"
        case_sensitive = False
    
    def __post_init__(self):
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
