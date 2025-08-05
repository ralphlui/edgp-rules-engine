"""
Unified validation models for the rules engine.
This module provides consistent input/output types across API and SQS interfaces.
"""
from pydantic import BaseModel, Field, validator
from typing import List, Dict, Any, Optional, Union
from datetime import datetime
from enum import Enum

# ============================================================================
# ENUMS AND CONSTANTS
# ============================================================================

class DataType(str, Enum):
    """Supported data types for validation"""
    TABULAR = "tabular"         # Pandas DataFrame-like data
    JSON = "json"               # JSON object data
    CSV = "csv"                 # CSV format data
    PARQUET = "parquet"         # Parquet format data
    DATABASE = "database"       # Database query result

class MessageStatus(str, Enum):
    """Message processing status"""
    PENDING = "pending"
    PROCESSING = "processing"
    SUCCESS = "success"
    FAILED = "failed"
    RETRY = "retry"
    DLQ = "dlq"

class RuleSeverity(str, Enum):
    """Validation rule severity levels"""
    ERROR = "error"
    WARNING = "warning"
    INFO = "info"

# ============================================================================
# CORE VALIDATION MODELS
# ============================================================================

class ValidationRule(BaseModel):
    """
    Unified validation rule model following Great Expectations format.
    Used consistently across API and SQS interfaces.
    
    Examples:
    - Column existence: {"rule_name": "expect_column_to_exist", "column_name": "age"}
    - Range validation: {"rule_name": "expect_column_values_to_be_between", "column_name": "age", "value": {"min_value": 18, "max_value": 65}}
    - Set membership: {"rule_name": "expect_column_values_to_be_in_set", "column_name": "status", "value": {"value_set": ["active", "inactive"]}}
    """
    rule_name: str = Field(..., description="Great Expectations rule name (e.g., 'expect_column_to_exist')")
    column_name: Optional[str] = Field(default=None, description="Target column name for validation")
    value: Optional[Union[Dict[str, Any], List[Any], str, int, float, bool]] = Field(
        default=None, 
        description="Rule parameters (e.g., {'min_value': 18, 'max_value': 65} for range validation)"
    )
    
    # Additional metadata
    rule_description: Optional[str] = Field(default=None, description="Human-readable description of the rule")
    severity: RuleSeverity = Field(default=RuleSeverity.ERROR, description="Rule severity level")
    
    # Legacy compatibility
    expectation_type: Optional[str] = Field(default=None, description="Legacy field, use rule_name instead")
    kwargs: Optional[Dict[str, Any]] = Field(default=None, description="Legacy field, use value instead")
    
    @validator('expectation_type', pre=True, always=True)
    def sync_expectation_type(cls, v, values):
        """Sync legacy expectation_type with rule_name"""
        if v and not values.get('rule_name'):
            values['rule_name'] = v
        elif values.get('rule_name') and not v:
            return values['rule_name']
        return v
    
    @validator('kwargs', pre=True, always=True)
    def sync_kwargs(cls, v, values):
        """Sync legacy kwargs with value"""
        if v and not values.get('value'):
            values['value'] = v
        elif values.get('value') and not v:
            return values['value']
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
    details: Dict[str, Any] = Field(default_factory=dict, description="Additional validation details")
    
    # Statistics
    element_count: Optional[int] = Field(default=None, description="Number of elements validated")
    unexpected_count: Optional[int] = Field(default=None, description="Number of elements that failed validation")
    unexpected_percent: Optional[float] = Field(default=None, description="Percentage of elements that failed")
    
    # Legacy compatibility fields
    rule: Optional[str] = Field(default=None, description="Legacy field, use rule_name instead")
    column: Optional[str] = Field(default=None, description="Legacy field, use column_name instead")
    
    @validator('rule', pre=True, always=True)
    def sync_rule(cls, v, values):
        """Sync legacy rule with rule_name"""
        if values.get('rule_name') and not v:
            return values['rule_name']
        elif v and not values.get('rule_name'):
            values['rule_name'] = v
        return v
    
    @validator('column', pre=True, always=True)
    def sync_column(cls, v, values):
        """Sync legacy column with column_name"""
        if values.get('column_name') and not v:
            return values['column_name']
        elif v and not values.get('column_name'):
            values['column_name'] = v
        return v

class ValidationSummary(BaseModel):
    """Summary statistics for validation results"""
    total_rules: int = Field(..., description="Total number of rules executed")
    successful_rules: int = Field(..., description="Number of rules that passed")
    failed_rules: int = Field(..., description="Number of rules that failed")
    success_rate: float = Field(..., description="Overall success rate (0.0 to 1.0)")
    
    # Data statistics
    total_rows: int = Field(default=0, description="Total number of data rows validated")
    total_columns: int = Field(default=0, description="Total number of columns in dataset")
    
    # Processing metadata
    execution_time_ms: int = Field(default=0, description="Total execution time in milliseconds")
    validation_engine: str = Field(default="great_expectations", description="Validation engine used")
    
    # Legacy compatibility fields
    passed: Optional[int] = Field(default=None, description="Legacy field, use successful_rules instead")
    failed: Optional[int] = Field(default=None, description="Legacy field, use failed_rules instead")
    
    @validator('passed', pre=True, always=True)
    def sync_passed(cls, v, values):
        """Sync legacy passed with successful_rules"""
        if values.get('successful_rules') is not None and v is None:
            return values['successful_rules']
        elif v is not None and values.get('successful_rules') is None:
            values['successful_rules'] = v
        return v
    
    @validator('failed', pre=True, always=True)
    def sync_failed(cls, v, values):
        """Sync legacy failed with failed_rules"""
        if values.get('failed_rules') is not None and v is None:
            return values['failed_rules']
        elif v is not None and values.get('failed_rules') is None:
            values['failed_rules'] = v
        return v

# ============================================================================
# INPUT MODELS
# ============================================================================

class ValidationRequest(BaseModel):
    """
    Core validation request model.
    Used for API endpoints and as base for SQS messages.
    """
    dataset: List[Dict[str, Any]] = Field(..., description="Data to validate as list of dictionaries")
    rules: List[ValidationRule] = Field(..., description="Validation rules to apply")
    
    # Optional metadata
    data_key: Optional[str] = Field(default=None, description="Unique identifier for this dataset")
    data_type: DataType = Field(default=DataType.TABULAR, description="Type of data being validated")
    source: Optional[str] = Field(default=None, description="Data source identifier")
    
    @validator('dataset')
    def validate_dataset_not_empty(cls, v):
        if not v:
            raise ValueError("Dataset cannot be empty")
        return v
    
    @validator('rules')
    def validate_rules_not_empty(cls, v):
        if not v:
            raise ValueError("Rules cannot be empty")
        return v

class DataEntry(BaseModel):
    """
    Enhanced data entry for structured data with metadata.
    Used in SQS messages for more detailed data description.
    """
    data_type: DataType = Field(..., description="Type of data being validated")
    data_key: str = Field(..., description="Unique identifier for this dataset")
    
    # Data Content
    columns: List[str] = Field(..., description="List of column names in the dataset")
    data: List[Dict[str, Any]] = Field(..., description="Actual data rows as list of dictionaries")
    
    # Metadata
    source: Optional[str] = Field(default=None, description="Data source (file path, table name, etc.)")
    schema_version: str = Field(default="1.0", description="Data schema version")
    created_at: datetime = Field(default_factory=datetime.utcnow, description="Data creation timestamp")
    
    @validator('data')
    def validate_data_not_empty(cls, v):
        if not v:
            raise ValueError("Data cannot be empty")
        return v
    
    @validator('columns')
    def validate_columns_not_empty(cls, v):
        if not v:
            raise ValueError("Columns list cannot be empty")
        return v

class SQSValidationRequest(BaseModel):
    """
    Enhanced SQS input queue message format.
    Extends ValidationRequest with messaging and processing metadata.
    """
    
    # Message Metadata
    message_id: str = Field(..., description="Unique message identifier")
    correlation_id: Optional[str] = Field(default=None, description="Correlation ID for tracking related messages")
    timestamp: datetime = Field(default_factory=datetime.utcnow, description="Message creation timestamp")
    source: Optional[str] = Field(default=None, description="Source system or application")
    
    # Primary validation content (using enhanced model)
    data_entry: Optional[DataEntry] = Field(default=None, description="Enhanced dataset with metadata")
    validation_rules: List[ValidationRule] = Field(..., description="List of validation rules to apply")
    
    # Legacy support for backward compatibility
    data: Optional[List[Dict[str, Any]]] = Field(default=None, description="Legacy data field (use data_entry instead)")
    rules: Optional[List[ValidationRule]] = Field(default=None, description="Legacy rules field (use validation_rules instead)")
    dataset: Optional[List[Dict[str, Any]]] = Field(default=None, description="Legacy dataset field (use data_entry instead)")
    
    # Processing Options
    batch_id: Optional[str] = Field(default=None, description="Batch identifier for grouped processing")
    priority: int = Field(default=5, ge=1, le=10, description="Processing priority (1=highest, 10=lowest)")
    callback_url: Optional[str] = Field(default=None, description="Webhook URL for result notification")
    
    # Retry Configuration
    max_retries: int = Field(default=3, ge=0, description="Maximum retry attempts")
    retry_count: int = Field(default=0, ge=0, description="Current retry count")
    
    @validator('validation_rules', pre=True, always=True)
    def ensure_validation_rules(cls, v, values):
        """Ensure validation_rules is populated from legacy fields if needed"""
        if not v:
            # Try to get from legacy rules field
            legacy_rules = values.get('rules')
            if legacy_rules:
                return legacy_rules
            else:
                raise ValueError("validation_rules cannot be empty")
        return v
    
    def get_dataset(self) -> List[Dict[str, Any]]:
        """Get dataset from enhanced or legacy format"""
        if self.data_entry and self.data_entry.data:
            return self.data_entry.data
        elif self.data:
            return self.data
        elif self.dataset:
            return self.dataset
        else:
            raise ValueError("No dataset found in message")
    
    def get_data_key(self) -> str:
        """Get data key from enhanced or legacy format"""
        if self.data_entry and self.data_entry.data_key:
            return self.data_entry.data_key
        else:
            return f"legacy-{self.message_id}"
    
    def get_data_type(self) -> DataType:
        """Get data type from enhanced or legacy format"""
        if self.data_entry and self.data_entry.data_type:
            return self.data_entry.data_type
        else:
            return DataType.TABULAR

# ============================================================================
# OUTPUT MODELS
# ============================================================================

class ValidationResponse(BaseModel):
    """
    Core validation response model.
    Used for API endpoints and as base for SQS responses.
    """
    results: List[ValidationResultDetail] = Field(..., description="Detailed validation results")
    summary: ValidationSummary = Field(..., description="Validation summary statistics")
    
    # Processing metadata
    processed_at: datetime = Field(default_factory=datetime.utcnow, description="Processing completion timestamp")
    execution_time_ms: int = Field(default=0, description="Total execution time in milliseconds")

class SQSValidationResponse(BaseModel):
    """
    Enhanced SQS output queue message format.
    Extends ValidationResponse with messaging and processing metadata.
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
    
    # Validation Results (using unified models)
    validation_results: List[ValidationResultDetail] = Field(default_factory=list, description="Detailed validation results")
    summary: ValidationSummary = Field(..., description="Validation summary statistics")
    
    # Error Information (if applicable)
    error_message: Optional[str] = Field(default=None, description="Error message if processing failed")
    error_code: Optional[str] = Field(default=None, description="Error code for categorization")
    error_details: Optional[Dict[str, Any]] = Field(default=None, description="Additional error context")
    
    # Metadata
    batch_id: Optional[str] = Field(default=None, description="Batch identifier from request")
    source: Optional[str] = Field(default=None, description="Source system from request")
    schema_version: str = Field(default="1.0", description="Response schema version")
    
    # Legacy compatibility fields
    results: Optional[List[ValidationResultDetail]] = Field(default=None, description="Legacy field, use validation_results instead")
    total_rules: Optional[int] = Field(default=None, description="Legacy field, use summary.total_rules instead")
    successful_rules: Optional[int] = Field(default=None, description="Legacy field, use summary.successful_rules instead")
    failed_rules: Optional[int] = Field(default=None, description="Legacy field, use summary.failed_rules instead")
    
    @validator('results', pre=True, always=True)
    def sync_results(cls, v, values):
        """Sync legacy results with validation_results"""
        if values.get('validation_results') and not v:
            return values['validation_results']
        elif v and not values.get('validation_results'):
            values['validation_results'] = v
        return v

# ============================================================================
# PROCESSING MODELS
# ============================================================================

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
    attributes: Dict[str, Any] = Field(default_factory=dict, description="SQS message attributes")

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

# ============================================================================
# UTILITY FUNCTIONS
# ============================================================================

def convert_legacy_rule(rule_dict: Dict[str, Any]) -> ValidationRule:
    """
    Convert legacy rule format to unified ValidationRule format.
    
    Args:
        rule_dict: Legacy rule dictionary
        
    Returns:
        ValidationRule instance
    """
    # Handle different legacy formats
    if 'expectation_type' in rule_dict:
        # Great Expectations format
        return ValidationRule(
            rule_name=rule_dict['expectation_type'],
            column_name=rule_dict.get('kwargs', {}).get('column'),
            value=rule_dict.get('kwargs', {}),
            expectation_type=rule_dict['expectation_type'],
            kwargs=rule_dict.get('kwargs', {})
        )
    elif 'rule_name' in rule_dict:
        # New unified format
        return ValidationRule(**rule_dict)
    else:
        # Try to infer format
        return ValidationRule(
            rule_name=rule_dict.get('rule_name', 'unknown'),
            column_name=rule_dict.get('column_name'),
            value=rule_dict.get('value')
        )

def convert_legacy_validation_request(request_dict: Dict[str, Any]) -> ValidationRequest:
    """
    Convert legacy validation request to unified format.
    
    Args:
        request_dict: Legacy request dictionary
        
    Returns:
        ValidationRequest instance
    """
    # Convert rules
    rules = []
    for rule in request_dict.get('rules', []):
        if isinstance(rule, dict):
            rules.append(convert_legacy_rule(rule))
        else:
            rules.append(rule)
    
    return ValidationRequest(
        dataset=request_dict.get('dataset', request_dict.get('data', [])),
        rules=rules,
        data_key=request_dict.get('data_key'),
        data_type=request_dict.get('data_type', DataType.TABULAR),
        source=request_dict.get('source')
    )
