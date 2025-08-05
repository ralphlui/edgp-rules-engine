"""
SQS Message models for validation requests and responses.
Standardized data types for SQS input and output queues.
"""
from pydantic import BaseModel, Field, field_validator
from typing import List, Dict, Any, Optional, Union
from datetime import datetime, timezone
from enum import Enum

class MessageStatus(str, Enum):
    """Message processing status"""
    PENDING = "pending"
    PROCESSING = "processing"
    SUCCESS = "success"
    FAILED = "failed"
    RETRY = "retry"
    DLQ = "dlq"

class DataType(str, Enum):
    """Supported data types for validation"""
    TABULAR = "tabular"         # Pandas DataFrame-like data
    JSON = "json"               # JSON object data
    CSV = "csv"                 # CSV format data
    PARQUET = "parquet"         # Parquet format data
    DATABASE = "database"       # Database query result

class ValidationRule(BaseModel):
    """
    Validation rule model following Great Expectations format.
    
    Examples:
    - expect_column_to_exist: {"rule_name": "expect_column_to_exist", "column_name": "age"}
    - expect_column_values_to_be_between: {"rule_name": "expect_column_values_to_be_between", "column_name": "age", "value": {"min_value": 18, "max_value": 65}}
    """
    rule_name: str = Field(..., description="Great Expectations rule name (e.g., 'expect_column_to_exist')")
    column_name: Optional[str] = Field(default=None, description="Target column name for validation")
    value: Optional[Union[Dict[str, Any], List[Any], str, int, float, bool]] = Field(
        default=None, 
        description="Rule parameters (e.g., {'min_value': 18, 'max_value': 65} for range validation)"
    )
    
    # Additional metadata
    rule_description: Optional[str] = Field(default=None, description="Human-readable description of the rule")
    severity: Optional[str] = Field(default="error", description="Rule severity: 'error', 'warning', 'info'")

class DataEntry(BaseModel):
    """
    Standardized data entry for SQS input queue.
    Represents a dataset with metadata and column information.
    """
    data_type: DataType = Field(..., description="Type of data being validated")
    data_key: str = Field(..., description="Unique identifier for this dataset")
    
    # Data Content
    columns: List[str] = Field(..., description="List of column names in the dataset")
    data: List[Dict[str, Any]] = Field(..., description="Actual data rows as list of dictionaries")
    
    # Metadata
    source: Optional[str] = Field(default=None, description="Data source (file path, table name, etc.)")
    schema_version: Optional[str] = Field(default="1.0", description="Data schema version")
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc), description="Data creation timestamp")
    
    @field_validator('data')
    @classmethod
    def validate_data_not_empty(cls, v):
        if not v:
            raise ValueError("Data cannot be empty")
        return v
    
    @field_validator('columns')
    @classmethod
    def validate_columns_not_empty(cls, v):
        if not v:
            raise ValueError("Columns list cannot be empty")
        return v

class SQSValidationRequest(BaseModel):
    """
    Standardized SQS input queue message format.
    Contains data entry and validation rules to be processed.
    """
    
    # Message Metadata
    message_id: str = Field(..., description="Unique message identifier")
    correlation_id: Optional[str] = Field(default=None, description="Correlation ID for tracking related messages")
    timestamp: datetime = Field(default_factory=lambda: datetime.now(timezone.utc), description="Message creation timestamp")
    source: Optional[str] = Field(default=None, description="Source system or application")
    
    # Data and Validation
    data_entry: DataEntry = Field(..., description="Dataset to be validated")
    validation_rules: List[ValidationRule] = Field(..., description="List of validation rules to apply")
    
    # Processing Options
    batch_id: Optional[str] = Field(default=None, description="Batch identifier for grouped processing")
    priority: int = Field(default=5, ge=1, le=10, description="Processing priority (1=highest, 10=lowest)")
    callback_url: Optional[str] = Field(default=None, description="Webhook URL for result notification")
    
    # Retry Configuration
    max_retries: Optional[int] = Field(default=3, ge=0, description="Maximum retry attempts")
    retry_count: int = Field(default=0, ge=0, description="Current retry count")
    
    # Legacy support (for backward compatibility)
    data: Optional[List[Dict[str, Any]]] = Field(default=None, description="Legacy data field (deprecated)")
    rules: Optional[List[ValidationRule]] = Field(default=None, description="Legacy rules field (deprecated)")
    
    @field_validator('validation_rules')
    @classmethod
    def validate_rules_not_empty(cls, v):
        if not v:
            raise ValueError("Validation rules cannot be empty")
        return v

class ValidationResultDetail(BaseModel):
    """Detailed result for a single validation rule"""
    rule_name: str = Field(..., description="Name of the validation rule")
    column_name: Optional[str] = Field(default=None, description="Column that was validated")
    success: bool = Field(..., description="Whether the validation passed")
    message: str = Field(..., description="Human-readable validation result message")
    
    # Detailed information
    expected: Optional[Any] = Field(default=None, description="Expected value or condition")
    actual: Optional[Any] = Field(default=None, description="Actual value or result")
    details: Dict[str, Any] = Field(default={}, description="Additional validation details")
    
    # Statistics
    element_count: Optional[int] = Field(default=None, description="Number of elements validated")
    unexpected_count: Optional[int] = Field(default=None, description="Number of elements that failed validation")
    unexpected_percent: Optional[float] = Field(default=None, description="Percentage of elements that failed")

class ValidationSummary(BaseModel):
    """Summary statistics for validation results"""
    total_rules: int = Field(..., description="Total number of rules executed")
    successful_rules: int = Field(..., description="Number of rules that passed")
    failed_rules: int = Field(..., description="Number of rules that failed")
    success_rate: float = Field(..., description="Overall success rate (0.0 to 1.0)")
    
    # Data statistics
    total_rows: int = Field(..., description="Total number of data rows validated")
    total_columns: int = Field(..., description="Total number of columns in dataset")
    
    # Processing metadata
    execution_time_ms: int = Field(..., description="Total execution time in milliseconds")
    validation_engine: str = Field(default="great_expectations", description="Validation engine used")

class SQSValidationResponse(BaseModel):
    """
    Standardized SQS output queue message format.
    Contains validation results and processing metadata.
    """
    
    # Message Metadata
    message_id: str = Field(..., description="Original message identifier")
    correlation_id: Optional[str] = Field(default=None, description="Correlation ID from request")
    processed_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc), description="Processing completion timestamp")
    processing_time_ms: int = Field(..., description="Total processing time in milliseconds")
    
    # Processing Status
    status: MessageStatus = Field(..., description="Overall processing status")
    worker_id: Optional[str] = Field(default=None, description="ID of worker that processed the message")
    
    # Data Information
    data_key: str = Field(..., description="Data key from original request")
    data_type: DataType = Field(..., description="Data type from original request")
    
    # Validation Results
    validation_results: List[ValidationResultDetail] = Field(default=[], description="Detailed validation results")
    summary: ValidationSummary = Field(..., description="Validation summary statistics")
    
    # Error Information (if applicable)
    error_message: Optional[str] = Field(default=None, description="Error message if processing failed")
    error_code: Optional[str] = Field(default=None, description="Error code for categorization")
    error_details: Optional[Dict[str, Any]] = Field(default=None, description="Additional error context")
    
    # Metadata
    batch_id: Optional[str] = Field(default=None, description="Batch identifier from request")
    source: Optional[str] = Field(default=None, description="Source system from request")
    schema_version: str = Field(default="1.0", description="Response schema version")
    
    # Legacy fields (for backward compatibility)
    total_rules: Optional[int] = Field(default=None, description="Legacy: use summary.total_rules instead")
    successful_rules: Optional[int] = Field(default=None, description="Legacy: use summary.successful_rules instead")
    failed_rules: Optional[int] = Field(default=None, description="Legacy: use summary.failed_rules instead")

class SQSMessageWrapper(BaseModel):
    """Wrapper for SQS message with metadata"""
    
    # SQS Message Info
    receipt_handle: str = Field(..., description="SQS receipt handle for message deletion")
    message_id: str = Field(..., description="SQS message ID")
    
    # Message Content
    body: SQSValidationRequest = Field(..., description="Parsed message body")
    
    # Processing Metadata
    received_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc), description="Message received timestamp")
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
from pydantic import BaseModel, Field, field_validator
from typing import List, Dict, Any, Optional, Union
from datetime import datetime, timezone
from enum import Enum

class MessageStatus(str, Enum):
    """Message processing status"""
    PENDING = "pending"
    PROCESSING = "processing"
    SUCCESS = "success"
    FAILED = "failed"
    RETRY = "retry"
    DLQ = "dlq"

class DataType(str, Enum):
    """Supported data types for validation"""
    TABULAR = "tabular"         # Pandas DataFrame-like data
    JSON = "json"               # JSON object data
    CSV = "csv"                 # CSV format data
    PARQUET = "parquet"         # Parquet format data
    DATABASE = "database"       # Database query result

class ValidationRule(BaseModel):
    """
    Validation rule model following Great Expectations format.
    
    Examples:
    - expect_column_to_exist: {"rule_name": "expect_column_to_exist", "column_name": "age"}
    - expect_column_values_to_be_between: {"rule_name": "expect_column_values_to_be_between", "column_name": "age", "value": {"min_value": 18, "max_value": 65}}
    """
    rule_name: str = Field(..., description="Great Expectations rule name (e.g., 'expect_column_to_exist')")
    column_name: Optional[str] = Field(default=None, description="Target column name for validation")
    value: Optional[Union[Dict[str, Any], List[Any], str, int, float, bool]] = Field(
        default=None, 
        description="Rule parameters (e.g., {'min_value': 18, 'max_value': 65} for range validation)"
    )
    
    # Additional metadata
    rule_description: Optional[str] = Field(default=None, description="Human-readable description of the rule")
    severity: Optional[str] = Field(default="error", description="Rule severity: 'error', 'warning', 'info'")

class DataEntry(BaseModel):
    """
    Standardized data entry for SQS input queue.
    Represents a dataset with metadata and column information.
    """
    data_type: DataType = Field(..., description="Type of data being validated")
    data_key: str = Field(..., description="Unique identifier for this dataset")
    
    # Data Content
    columns: List[str] = Field(..., description="List of column names in the dataset")
    data: List[Dict[str, Any]] = Field(..., description="Actual data rows as list of dictionaries")
    
    # Metadata
    source: Optional[str] = Field(default=None, description="Data source (file path, table name, etc.)")
    schema_version: Optional[str] = Field(default="1.0", description="Data schema version")
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc), description="Data creation timestamp")
    
    @field_validator('data')
    @classmethod
    def validate_data_not_empty(cls, v):
        if not v:
            raise ValueError("Data cannot be empty")
        return v
    
    @field_validator('columns')
    @classmethod
    def validate_columns_not_empty(cls, v):
        if not v:
            raise ValueError("Columns list cannot be empty")
        return v

class SQSValidationRequest(BaseModel):
    """
    Standardized SQS input queue message format.
    Contains data entry and validation rules to be processed.
    """
    
    # Message Metadata
    message_id: str = Field(..., description="Unique message identifier")
    correlation_id: Optional[str] = Field(default=None, description="Correlation ID for tracking related messages")
    timestamp: datetime = Field(default_factory=lambda: datetime.now(timezone.utc), description="Message creation timestamp")
    source: Optional[str] = Field(default=None, description="Source system or application")
    
    # Data and Validation
    data_entry: DataEntry = Field(..., description="Dataset to be validated")
    validation_rules: List[ValidationRule] = Field(..., description="List of validation rules to apply")
    
    # Processing Options
    batch_id: Optional[str] = Field(default=None, description="Batch identifier for grouped processing")
    priority: int = Field(default=5, ge=1, le=10, description="Processing priority (1=highest, 10=lowest)")
    callback_url: Optional[str] = Field(default=None, description="Webhook URL for result notification")
    
    # Retry Configuration
    max_retries: Optional[int] = Field(default=3, ge=0, description="Maximum retry attempts")
    retry_count: int = Field(default=0, ge=0, description="Current retry count")
    
    # Legacy support (for backward compatibility)
    data: Optional[List[Dict[str, Any]]] = Field(default=None, description="Legacy data field (deprecated)")
    rules: Optional[List[ValidationRule]] = Field(default=None, description="Legacy rules field (deprecated)")
    
    @field_validator('validation_rules')
    @classmethod
    def validate_rules_not_empty(cls, v):
        if not v:
            raise ValueError("Validation rules cannot be empty")
        return v

class ValidationResultDetail(BaseModel):
    """Detailed result for a single validation rule"""
    rule_name: str = Field(..., description="Name of the validation rule")
    column_name: Optional[str] = Field(default=None, description="Column that was validated")
    success: bool = Field(..., description="Whether the validation passed")
    message: str = Field(..., description="Human-readable validation result message")
    
    # Detailed information
    expected: Optional[Any] = Field(default=None, description="Expected value or condition")
    actual: Optional[Any] = Field(default=None, description="Actual value or result")
    details: Dict[str, Any] = Field(default={}, description="Additional validation details")
    
    # Statistics
    element_count: Optional[int] = Field(default=None, description="Number of elements validated")
    unexpected_count: Optional[int] = Field(default=None, description="Number of elements that failed validation")
    unexpected_percent: Optional[float] = Field(default=None, description="Percentage of elements that failed")

class ValidationSummary(BaseModel):
    """Summary statistics for validation results"""
    total_rules: int = Field(..., description="Total number of rules executed")
    successful_rules: int = Field(..., description="Number of rules that passed")
    failed_rules: int = Field(..., description="Number of rules that failed")
    success_rate: float = Field(..., description="Overall success rate (0.0 to 1.0)")
    
    # Data statistics
    total_rows: int = Field(..., description="Total number of data rows validated")
    total_columns: int = Field(..., description="Total number of columns in dataset")
    
    # Processing metadata
    execution_time_ms: int = Field(..., description="Total execution time in milliseconds")
    validation_engine: str = Field(default="great_expectations", description="Validation engine used")

class SQSValidationResponse(BaseModel):
    """
    Standardized SQS output queue message format.
    Contains validation results and processing metadata.
    """
    
    # Message Metadata
    message_id: str = Field(..., description="Original message identifier")
    correlation_id: Optional[str] = Field(default=None, description="Correlation ID from request")
    processed_at: datetime = Field(default_factory=datetime.utcnow, description="Processing completion timestamp")
    processing_time_ms: int = Field(..., description="Total processing time in milliseconds")
    
    # Processing Status
    status: MessageStatus = Field(..., description="Overall processing status")
    worker_id: Optional[str] = Field(default=None, description="ID of worker that processed the message")
    
    # Data Information
    data_key: str = Field(..., description="Data key from original request")
    data_type: DataType = Field(..., description="Data type from original request")
    
    # Validation Results
    validation_results: List[ValidationResultDetail] = Field(default=[], description="Detailed validation results")
    summary: ValidationSummary = Field(..., description="Validation summary statistics")
    
    # Error Information (if applicable)
    error_message: Optional[str] = Field(default=None, description="Error message if processing failed")
    error_code: Optional[str] = Field(default=None, description="Error code for categorization")
    error_details: Optional[Dict[str, Any]] = Field(default=None, description="Additional error context")
    
    # Metadata
    batch_id: Optional[str] = Field(default=None, description="Batch identifier from request")
    source: Optional[str] = Field(default=None, description="Source system from request")
    schema_version: str = Field(default="1.0", description="Response schema version")

class ValidationResultDetail(BaseModel):
    """Detailed result for a single validation rule"""
    rule_name: str = Field(..., description="Name of the validation rule")
    column_name: Optional[str] = Field(default=None, description="Column that was validated")
    success: bool = Field(..., description="Whether the validation passed")
    message: str = Field(..., description="Human-readable validation result message")
    
    # Detailed information
    expected: Optional[Any] = Field(default=None, description="Expected value or condition")
    actual: Optional[Any] = Field(default=None, description="Actual value or result")
    details: Dict[str, Any] = Field(default={}, description="Additional validation details")
    
    # Statistics
    element_count: Optional[int] = Field(default=None, description="Number of elements validated")
    unexpected_count: Optional[int] = Field(default=None, description="Number of elements that failed validation")
    unexpected_percent: Optional[float] = Field(default=None, description="Percentage of elements that failed")

class ValidationSummary(BaseModel):
    """Summary statistics for validation results"""
    total_rules: int = Field(..., description="Total number of rules executed")
    successful_rules: int = Field(..., description="Number of rules that passed")
    failed_rules: int = Field(..., description="Number of rules that failed")
    success_rate: float = Field(..., description="Overall success rate (0.0 to 1.0)")
    
    # Data statistics
    total_rows: int = Field(..., description="Total number of data rows validated")
    total_columns: int = Field(..., description="Total number of columns in dataset")
    
    # Processing metadata
    execution_time_ms: int = Field(..., description="Total execution time in milliseconds")
    validation_engine: str = Field(default="great_expectations", description="Validation engine used")

class SQSValidationResponse(BaseModel):
    """
    Standardized SQS output queue message format.
    Contains validation results and processing metadata.
    """
    
    # Message Metadata
    message_id: str = Field(..., description="Original message identifier")
    correlation_id: Optional[str] = Field(default=None, description="Correlation ID from request")
    processed_at: datetime = Field(default_factory=datetime.utcnow, description="Processing completion timestamp")
    processing_time_ms: int = Field(..., description="Total processing time in milliseconds")
    
    # Processing Status
    status: MessageStatus = Field(..., description="Overall processing status")
    worker_id: Optional[str] = Field(default=None, description="ID of worker that processed the message")
    
    # Data Information
    data_key: str = Field(..., description="Data key from original request")
    data_type: DataType = Field(..., description="Data type from original request")
    
    # Validation Results
    validation_results: List[ValidationResultDetail] = Field(default=[], description="Detailed validation results")
    summary: ValidationSummary = Field(..., description="Validation summary statistics")
    
    # Error Information (if applicable)
    error_message: Optional[str] = Field(default=None, description="Error message if processing failed")
    error_code: Optional[str] = Field(default=None, description="Error code for categorization")
    error_details: Optional[Dict[str, Any]] = Field(default=None, description="Additional error context")
    
    # Metadata
    batch_id: Optional[str] = Field(default=None, description="Batch identifier from request")
    source: Optional[str] = Field(default=None, description="Source system from request")
    schema_version: str = Field(default="1.0", description="Response schema version")
    
    # Legacy fields (for backward compatibility)
    total_rules: Optional[int] = Field(default=None, description="Legacy: use summary.total_rules instead")
    successful_rules: Optional[int] = Field(default=None, description="Legacy: use summary.successful_rules instead")
    failed_rules: Optional[int] = Field(default=None, description="Legacy: use summary.failed_rules instead")

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
