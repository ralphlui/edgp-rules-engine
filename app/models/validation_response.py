from typing import List, Dict, Any
from pydantic import BaseModel


class ValidationResult(BaseModel):
    rule: str
    column: str
    success: bool
    message: str
    details: Dict[str, Any]


class ValidationSummary(BaseModel):
    total_rules: int
    passed: int
    failed: int


class ValidationResponse(BaseModel):
    results: List[ValidationResult]
    summary: ValidationSummary
