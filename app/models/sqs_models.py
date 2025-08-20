"""
SQS Message models for validation requests and responses.
Updated format to match the new input/output queue structure.
"""
from pydantic import BaseModel, Field, field_validator, model_validator
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
    Enhanced validation rule model following Great Expectations format.
    Now includes additional metadata fields for better rule management.
    """
    rule_name: str = Field(..., description="Great Expectations rule name (e.g., 'expect_column_to_exist')")
    column_name: Optional[str] = Field(default=None, description="Target column name for validation")
    value: Optional[Union[Dict[str, Any], List[Any], str, int, float, bool]] = Field(
        default=None, 
        description="Rule parameters (e.g., {'min_value': 18, 'max_value': 65} for range validation)"
    )
    
    @field_validator('column_name', mode='before')
    @classmethod
    def normalize_column_name(cls, v):
        """Convert single-item list to string for backward compatibility"""
        if isinstance(v, list) and len(v) == 1:
            return v[0]
        return v
    
    # Enhanced metadata fields
    rule_description: Optional[str] = Field(default=None, description="Human-readable description of the rule")
    severity: str = Field(default="error", description="Rule severity: 'error', 'warning', 'info'")
    expectation_type: Optional[str] = Field(default=None, description="Great Expectations expectation type (often same as rule_name)")
    kwargs: Optional[Dict[str, Any]] = Field(default=None, description="Additional keyword arguments for the rule")
    
    @field_validator('expectation_type', mode='before')
    @classmethod
    def set_default_expectation_type(cls, v, info):
        """Set expectation_type to rule_name if not provided"""
        if v is None and info.data and 'rule_name' in info.data:
            return info.data['rule_name']
        return v
    
    @field_validator('kwargs', mode='before')  
    @classmethod
    def sync_kwargs_with_value(cls, v, info):
        """Sync kwargs with value field if not provided"""
        if v is None and info.data and 'value' in info.data and info.data['value'] is not None:
            if isinstance(info.data['value'], dict):
                return info.data['value']
        return v
    
    @model_validator(mode='after')
    def set_defaults_after_validation(self):
        """Set defaults after all fields are validated"""
        if self.expectation_type is None:
            self.expectation_type = self.rule_name
        if self.kwargs is None and self.value is not None and isinstance(self.value, dict):
            self.kwargs = self.value
        return self

class DataEntry(BaseModel):
    """
    Updated data entry model to match the new SQS input format.
    Contains data type, file/policy IDs, actual data, and validation rules.
    """
    data_type: DataType = Field(..., description="Type of data being validated")
    domain_name: str = Field(..., description="Domain name for the data")
    file_id: str = Field(..., description="Unique file identifier (UUID)")
    policy_id: str = Field(..., description="Policy identifier (UUID)")
    data: Dict[str, Any] = Field(..., description="The actual data object to be validated")
    validation_rules: List[ValidationRule] = Field(..., description="List of validation rules to apply")
    
    @field_validator('data')
    @classmethod
    def validate_data_not_empty(cls, v):
        if not v:
            raise ValueError("Data cannot be empty")
        return v
    
    @field_validator('validation_rules')
    @classmethod
    def validate_rules_not_empty(cls, v):
        if not v:
            raise ValueError("Validation rules cannot be empty")
        return v

class SQSValidationRequest(BaseModel):
    """
    Updated SQS input queue message format.
    Main wrapper containing the data_entry object.
    Supports backward compatibility with old format.
    """
    data_entry: DataEntry = Field(..., description="Complete data entry with validation rules")
    
    @model_validator(mode='before')
    @classmethod
    def handle_legacy_format(cls, data):
        """Handle backward compatibility with old SQS message format"""
        if isinstance(data, dict):
            # Check if this is the old format (has 'data' and 'rules' fields)
            if 'data' in data and 'rules' in data and 'data_entry' not in data:
                # Convert old format to new format
                legacy_data = data.get('data', [])
                legacy_rules = data.get('rules', [])
                
                # Create DataEntry from legacy fields
                data_entry = {
                    'data_type': 'tabular',  # Default assumption
                    'domain_name': 'Legacy',  # Default domain
                    'file_id': data.get('message_id', 'legacy-file-id'),
                    'policy_id': data.get('correlation_id', 'legacy-policy-id'),
                    'data': {'records': legacy_data} if isinstance(legacy_data, list) else legacy_data,
                    'validation_rules': legacy_rules
                }
                
                return {'data_entry': data_entry}
            
            # Check if 'validation_rules' is at top level (another old format)
            elif 'validation_rules' in data and 'data_entry' not in data:
                # Move validation_rules into data_entry structure
                validation_rules = data.pop('validation_rules')
                
                # Create basic data_entry if missing
                if 'data_entry' not in data:
                    data['data_entry'] = {
                        'data_type': 'tabular',
                        'domain_name': 'Legacy',
                        'file_id': 'legacy-file-id', 
                        'policy_id': 'legacy-policy-id',
                        'data': {},
                        'validation_rules': validation_rules
                    }
                
        return data

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

class ValidationResult(BaseModel):
    """Container for complete validation results"""
    validation_results: List[ValidationResultDetail] = Field(default=[], description="Detailed validation results")
    summary: ValidationSummary = Field(..., description="Validation summary statistics")
    
    # Processing metadata
    processed_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc), description="Processing completion timestamp")
    processing_time_ms: int = Field(..., description="Total processing time in milliseconds")
    status: MessageStatus = Field(..., description="Overall processing status")

class FailedValidation(BaseModel):
    """Failed validation details"""
    rule_name: str = Field(..., description="Name of the failed rule")
    column_name: str = Field(..., description="Column name that failed validation")
    error_message: str = Field(..., description="Error message for this validation failure")
    status: str = Field(default="fail", description="Status of the validation")

class SQSValidationResponse(BaseModel):
    """
    Updated SQS output queue message format to match the required outbound format.
    """
    file_id: str = Field(..., description="File identifier")
    policy_id: str = Field(..., description="Policy identifier")
    data_type: str = Field(..., description="Type of data")
    status: str = Field(..., description="Overall validation status: 'success' or 'fail'")
    domain_name: str = Field(..., description="Domain name")
    data: Dict[str, Any] = Field(..., description="Data with ID")
    failed_validations: Optional[List[FailedValidation]] = Field(default=None, description="Failed validations if any")

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

# Helper functions for backward compatibility and easy access
def get_dataset_from_request(request: SQSValidationRequest) -> List[Dict[str, Any]]:
    """Extract dataset in list format from the new request structure"""
    data = request.data_entry.data
    if isinstance(data, dict):
        # Convert single data object to list format expected by validators
        return [data]
    elif isinstance(data, list):
        return data
    else:
        raise ValueError("Data must be a dictionary or list of dictionaries")

def get_validation_rules_from_request(request: SQSValidationRequest) -> List[ValidationRule]:
    """Extract validation rules from the new request structure"""
    return request.data_entry.validation_rules

def create_response_from_request_and_results(
    request: SQSValidationRequest, 
    validation_results: List[ValidationResultDetail],
    summary: ValidationSummary,
    processing_time_ms: int,
    status: MessageStatus = MessageStatus.SUCCESS
) -> SQSValidationResponse:
    """Create a response from request and validation results in the new outbound format"""
    
    # Determine overall status
    overall_status = "success" if summary.failed_rules == 0 else "fail"
    
    # Create failed validations if there are any failures
    failed_validations = None
    if summary.failed_rules > 0:
        failed_validations = []
        
        # Create one FailedValidation for each failed result
        for result in validation_results:
            if not result.success:
                failed_validation = FailedValidation(
                    rule_name=result.rule_name,
                    column_name=result.column_name or "unknown",
                    error_message=result.message or f"Validation failed for rule {result.rule_name}",
                    status="fail"
                )
                failed_validations.append(failed_validation)
    
    return SQSValidationResponse(
        file_id=request.data_entry.file_id,
        policy_id=request.data_entry.policy_id,
        data_type=request.data_entry.data_type.value,
        status=overall_status,
        domain_name=request.data_entry.domain_name,
        data={"id": request.data_entry.data.get("id", "unknown")},
        failed_validations=failed_validations
    )
