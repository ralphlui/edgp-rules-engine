"""
SQS Message models for validation requests and responses.
"""
from pydantic import BaseModel, Field, validator
from typing import List, Dict, Any, Optional, Union
from datetime import datetime
from enum import Enum

class MessageStatus(str, Enum):
    """Message processing status"""
    PENDING = "pending"
    PROCESSING = "processing"
    SUCCESS = "success"
    FAILED = "failed"
    RETRY = "retry"
    DLQ = "dlq"

class ValidationRule(BaseModel):
    """Validation rule model for SQS messages"""
    rule_name: str = Field(..., description="Name of the validation rule")
    column_name: Optional[str] = Field(default=None, description="Target column name")
    value: Optional[Union[Dict[str, Any], List[Any], str, int, float, bool]] = Field(
        default=None, 
        description="Rule parameters (min_value, max_value, regex, etc.)"
    )

class SQSValidationRequest(BaseModel):
    """SQS message model for validation requests"""
    
    # Message Metadata
    message_id: str = Field(..., description="Unique message identifier")
    correlation_id: Optional[str] = Field(default=None, description="Correlation ID for tracking")
    timestamp: datetime = Field(default_factory=datetime.utcnow, description="Message timestamp")
    source: Optional[str] = Field(default=None, description="Source system or application")
    
    # Validation Data
    data: List[Dict[str, Any]] = Field(..., description="Dataset to validate")
    rules: List[ValidationRule] = Field(..., description="Validation rules to apply")
    
    # Processing Options
    batch_id: Optional[str] = Field(default=None, description="Batch identifier for grouped processing")
    priority: int = Field(default=5, ge=1, le=10, description="Processing priority (1=highest, 10=lowest)")
    callback_url: Optional[str] = Field(default=None, description="Webhook URL for result notification")
    
    # Retry Configuration
    max_retries: Optional[int] = Field(default=3, ge=0, description="Maximum retry attempts")
    retry_count: int = Field(default=0, ge=0, description="Current retry count")
    
    @validator('data')
    def validate_data_not_empty(cls, v):
        if not v:
            raise ValueError("Data cannot be empty")
        return v
    
    @validator('rules')
    def validate_rules_not_empty(cls, v):
        if not v:
            raise ValueError("Rules cannot be empty")
        return v

class SQSValidationResponse(BaseModel):
    """SQS message response model for validation results"""
    
    # Message Metadata
    message_id: str = Field(..., description="Original message identifier")
    correlation_id: Optional[str] = Field(default=None, description="Correlation ID")
    processed_at: datetime = Field(default_factory=datetime.utcnow, description="Processing completion time")
    processing_time_ms: int = Field(..., description="Processing time in milliseconds")
    
    # Processing Status
    status: MessageStatus = Field(..., description="Processing status")
    worker_id: Optional[str] = Field(default=None, description="Worker that processed the message")
    
    # Validation Results
    validation_results: List[Dict[str, Any]] = Field(default=[], description="Detailed validation results")
    summary: Dict[str, Any] = Field(default={}, description="Validation summary statistics")
    
    # Error Information
    error_message: Optional[str] = Field(default=None, description="Error message if processing failed")
    error_code: Optional[str] = Field(default=None, description="Error code for categorization")
    
    # Metadata
    total_rules: int = Field(default=0, description="Total number of rules processed")
    successful_rules: int = Field(default=0, description="Number of successful validations")
    failed_rules: int = Field(default=0, description="Number of failed validations")

class SQSMessageWrapper(BaseModel):
    """Wrapper for SQS message with metadata"""
    
    # SQS Message Info
    receipt_handle: str = Field(..., description="SQS receipt handle for message deletion")
    message_id: str = Field(..., description="SQS message ID")
    
    # Message Content
    body: SQSValidationRequest = Field(..., description="Parsed message body")
    
    # Processing Metadata
    received_at: datetime = Field(default_factory=datetime.utcnow, description="Message received timestamp")
    attempts: int = Field(default=1, description="Processing attempt count")
    
    # SQS Attributes
    attributes: Dict[str, Any] = Field(default={}, description="SQS message attributes")
    
class ProcessingResult(BaseModel):
    """Result of message processing"""
    
    success: bool = Field(..., description="Whether processing was successful")
    message_id: str = Field(..., description="Message identifier")
    processing_time_ms: int = Field(..., description="Processing time in milliseconds")
    
    # Results
    response: Optional[SQSValidationResponse] = Field(default=None, description="Processing response")
    error: Optional[str] = Field(default=None, description="Error message if failed")
    
    # Actions
    should_delete: bool = Field(default=True, description="Whether to delete message from queue")
    should_retry: bool = Field(default=False, description="Whether to retry processing")
    send_to_dlq: bool = Field(default=False, description="Whether to send to dead letter queue")
