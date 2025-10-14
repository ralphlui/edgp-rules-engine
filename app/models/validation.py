"""
Simplified validation models for the rules engine.
This module provides consistent input/output types across API and SQS interfaces.
"""
from pydantic import BaseModel, Field, field_validator, model_validator
from typing import List, Dict, Any, Optional, Union
from datetime import datetime, timezone
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
    """
    rule_name: str = Field(..., description="Great Expectations rule name")
    column_name: Optional[str] = Field(default=None, description="Target column name for validation")
    value: Optional[Union[Dict[str, Any], List[Any], str, int, float, bool]] = Field(
        default=None, 
        description="Rule parameters"
    )
    
    # Additional metadata
    rule_description: Optional[str] = Field(default=None, description="Human-readable description")
    severity: RuleSeverity = Field(default=RuleSeverity.ERROR, description="Rule severity level")

class ValidationResultDetail(BaseModel):
    """Detailed result for a single validation rule"""
    rule_name: str = Field(..., description="Name of the validation rule")
    column_name: Optional[str] = Field(default=None, description="Column that was validated")
    success: bool = Field(..., description="Whether the validation passed")
    message: Optional[str] = Field(default=None, description="Human-readable validation result message")
    
    # Detailed information
    expected: Optional[Any] = Field(default=None, description="Expected value or condition")
    actual: Optional[Any] = Field(default=None, description="Actual value or result")
    details: Dict[str, Any] = Field(default_factory=dict, description="Additional validation details")
    
    # Statistics
    element_count: Optional[int] = Field(default=None, description="Number of elements validated")
    unexpected_count: Optional[int] = Field(default=None, description="Number of elements that failed validation")
    unexpected_percent: Optional[float] = Field(default=None, description="Percentage of elements that failed")

class ValidationSummary(BaseModel):
    """Summary statistics for validation results"""
    total_rules: int = Field(..., description="Total number of rules executed")
    successful_rules: int = Field(..., description="Number of rules that passed")
    failed_rules: int = Field(..., description="Number of rules that failed")
    success_rate: float = Field(default=0.0, description="Overall success rate (0.0 to 1.0)")
    
    # Data statistics
    total_rows: int = Field(default=0, description="Total number of data rows validated")
    total_columns: int = Field(default=0, description="Total number of columns in dataset")
    
    # Processing metadata
    execution_time_ms: int = Field(default=0, description="Total execution time in milliseconds")
    validation_engine: str = Field(default="great_expectations", description="Validation engine used")

# ============================================================================
# API REQUEST/RESPONSE MODELS
# ============================================================================

class APIDataEntry(BaseModel):
    """
    Incoming API data entry payload used for validation requests.
    Supports both the new enhanced format and legacy variations.
    """
    data_type: DataType = Field(..., description="Type of data being validated")
    domain_name: Optional[str] = Field(default=None, description="Domain name associated with the data")
    file_id: Optional[str] = Field(default=None, description="Unique file identifier")
    policy_id: Optional[str] = Field(default=None, description="Policy identifier")
    data: Union[Dict[str, Any], List[Dict[str, Any]]] = Field(
        ..., description="Data to validate (single record or list of records)"
    )
    validation_rules: Optional[List[ValidationRule]] = Field(
        default=None,
        description="Validation rules provided within the data entry payload"
    )

    @field_validator("data")
    @classmethod
    def validate_data(cls, value: Union[Dict[str, Any], List[Dict[str, Any]]]) -> Union[Dict[str, Any], List[Dict[str, Any]]]:
        """Ensure data is provided as a dict or a list of dicts."""
        if isinstance(value, dict):
            return value
        if isinstance(value, list):
            if all(isinstance(item, dict) for item in value):
                return value
            raise ValueError("Each item in data list must be an object")
        raise ValueError("data must be an object or a list of objects")


class ValidationRequest(BaseModel):
    """API validation request"""
    rules: List[ValidationRule] = Field(..., description="List of validation rules to apply")
    dataset: List[Dict[str, Any]] = Field(..., description="Dataset to validate")
    data_entry: Optional[APIDataEntry] = Field(default=None, description="Original data entry payload")

    @model_validator(mode="before")
    @classmethod
    def handle_enhanced_payload(cls, values: Any) -> Any:
        """
        Support enhanced payloads that provide a data_entry structure and/or
        embed validation_rules alongside the data.
        """
        if not isinstance(values, dict):
            return values

        values = values.copy()

        data_entry = values.get("data_entry")
        # Allow validation rules to appear at the top level under validation_rules key
        top_level_rules = values.get("validation_rules")

        if isinstance(data_entry, dict):
            # Extract validation rules from data_entry when present
            entry_rules = data_entry.get("validation_rules")
            if entry_rules is not None and "rules" not in values:
                values["rules"] = entry_rules
            elif top_level_rules is not None and "rules" not in values:
                values["rules"] = top_level_rules

            # Normalize dataset from data_entry.data
            raw_data = data_entry.get("data")
            normalized_dataset = cls._normalize_dataset(raw_data)
            if "dataset" not in values:
                values["dataset"] = normalized_dataset
        elif top_level_rules is not None and "rules" not in values:
            # Handle case where payload provides validation_rules without wrapping in data_entry
            values["rules"] = top_level_rules

        # If dataset is provided as a dict under "data", normalize it
        if "dataset" not in values and "data" in values:
            values["dataset"] = cls._normalize_dataset(values.get("data"))

        return values

    @staticmethod
    def _normalize_dataset(raw_data: Any) -> List[Dict[str, Any]]:
        """Convert raw data payloads into a list of dictionaries."""
        if raw_data is None:
            return []

        # Handle dict wrapping records in a key such as "records"
        if isinstance(raw_data, dict):
            if "records" in raw_data:
                records = raw_data["records"]
                if not isinstance(records, list):
                    raise ValueError("records must be provided as a list of objects")
                if not all(isinstance(item, dict) for item in records):
                    raise ValueError("Each record must be an object")
                return records
            return [raw_data]

        if isinstance(raw_data, list):
            if all(isinstance(item, dict) for item in raw_data):
                return raw_data
            raise ValueError("Each item in data list must be an object")

        raise ValueError("Dataset must be an object or list of objects")

class ValidationResponse(BaseModel):
    """API validation response"""
    results: List[ValidationResultDetail] = Field(..., description="Detailed validation results")
    summary: ValidationSummary = Field(..., description="Validation summary statistics")

# ============================================================================
# LEGACY SUPPORT MODELS
# ============================================================================

class LegacyValidationRule(BaseModel):
    """Legacy validation rule format"""
    rule_name: str
    column_name: Optional[str] = None
    value: Optional[Dict[str, Any]] = None
    expectation_type: Optional[str] = None  # Legacy field
    kwargs: Optional[Dict[str, Any]] = None  # Legacy field

class LegacyValidationResult(BaseModel):
    """Legacy validation result format"""
    rule: str
    column: Optional[str] = None
    success: bool
    message: str
    details: Dict[str, Any] = Field(default_factory=dict)

class LegacyValidationSummary(BaseModel):
    """Legacy validation summary format"""
    total_rules: int
    passed: int
    failed: int

class LegacyValidationResponse(BaseModel):
    """Legacy validation response format"""
    result: List[Dict[str, Any]]  # Legacy field name
    total_rules: int
    successful_rules: int
    failed_rules: int

# ============================================================================
# ENHANCED DATA MODELS
# ============================================================================

class DataEntry(BaseModel):
    """Enhanced data entry with metadata"""
    data_type: DataType = Field(..., description="Type of data being validated")
    data_key: str = Field(..., description="Unique identifier for this dataset")
    
    # Data Content
    columns: List[str] = Field(..., description="List of column names in the dataset")
    data: List[Dict[str, Any]] = Field(..., description="Actual data rows")
    
    # Metadata
    source: Optional[str] = Field(default=None, description="Data source")
    schema_version: Optional[str] = Field(default="1.0", description="Data schema version")
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc), description="Creation timestamp")

class ProcessingMetadata(BaseModel):
    """Processing metadata for validation results"""
    processed_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    processing_time_ms: int = Field(..., description="Processing time in milliseconds")
    worker_id: Optional[str] = Field(default=None, description="Worker that processed the request")
    batch_id: Optional[str] = Field(default=None, description="Batch identifier")

class EnhancedValidationRequest(BaseModel):
    """Enhanced validation request with metadata"""
    message_id: str = Field(..., description="Unique message identifier")
    correlation_id: Optional[str] = Field(default=None, description="Correlation ID")
    timestamp: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    source: Optional[str] = Field(default=None, description="Source system")
    
    # Core validation data
    data_entry: DataEntry = Field(..., description="Dataset to be validated")
    validation_rules: List[ValidationRule] = Field(..., description="Validation rules")
    
    # Processing options
    priority: int = Field(default=5, ge=1, le=10, description="Processing priority")
    max_retries: int = Field(default=3, ge=0, description="Maximum retry attempts")

class EnhancedValidationResponse(BaseModel):
    """Enhanced validation response with metadata"""
    message_id: str = Field(..., description="Original message identifier")
    correlation_id: Optional[str] = Field(default=None, description="Correlation ID")
    
    # Processing status
    status: MessageStatus = Field(..., description="Processing status")
    metadata: ProcessingMetadata = Field(..., description="Processing metadata")
    
    # Data information
    data_key: str = Field(..., description="Data key from request")
    data_type: DataType = Field(..., description="Data type from request")
    
    # Validation results
    validation_results: List[ValidationResultDetail] = Field(..., description="Detailed results")
    summary: ValidationSummary = Field(..., description="Summary statistics")
    
    # Error information
    error_message: Optional[str] = Field(default=None, description="Error message if failed")
    error_code: Optional[str] = Field(default=None, description="Error code")
