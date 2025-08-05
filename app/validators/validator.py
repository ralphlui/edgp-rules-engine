from typing import List, Dict, Any
from app.models.validation import ValidationRequest, ValidationResponse, ValidationResultDetail, ValidationSummary
from app.validators.validator_registry import validate_rule


def data_validator(request: ValidationRequest) -> ValidationResponse:
    """
    Validate data against a list of rules using the appropriate validators.
    
    Args:
        request: ValidationRequest containing rules and dataset
        
    Returns:
        ValidationResponse containing validation results for all rules
    """
    rules = request.rules
    data = request.dataset
    
    validation_results = []
    successful_count = 0
    failed_count = 0
    
    # Validate each rule
    for rule in rules:
        try:
            result = validate_rule(data, rule)
            
            # Convert result to ValidationResultDetail model
            validation_result = ValidationResultDetail(
                rule_name=result.get("rule_name", rule.rule_name),
                column_name=result.get("column_name", rule.column_name),
                success=result.get("success", False),
                message=result.get("message") or result.get("error") or "No message provided",
                details=result.get("details", {})
            )
            
            validation_results.append(validation_result)
            
            if validation_result.success:
                successful_count += 1
            else:
                failed_count += 1
                
        except Exception as e:
            # Handle any unexpected errors during validation
            error_result = ValidationResultDetail(
                rule_name=rule.rule_name,
                column_name=rule.column_name,
                success=False,
                message=f"Failed to validate rule: {str(e)}",
                details={"error": str(e)}
            )
            validation_results.append(error_result)
            failed_count += 1
    
    # Create summary
    summary = ValidationSummary(
        total_rules=len(rules),
        successful_rules=successful_count,
        failed_rules=failed_count,
        success_rate=successful_count / len(rules) if len(rules) > 0 else 0.0,
        total_rows=len(data) if data else 0,
        total_columns=len(data[0].keys()) if data else 0,
        execution_time_ms=0  # We could add timing later
    )
    
    return ValidationResponse(results=validation_results, summary=summary)
