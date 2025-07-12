from typing import List, Dict, Any
from app.models.validation_request import ValidationRequest
from app.models.validation_response import ValidationResponse
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
    data = request.dataset  # Fixed: should be 'dataset' not 'data' based on the model
    
    validation_results = []
    
    # Validate each rule
    for rule in rules:
        try:
            result = validate_rule(data, rule)
            validation_results.append(result)
        except Exception as e:
            # Handle any unexpected errors during validation
            error_result = {
                "rule_name": rule.rule_name,
                "column_name": rule.column_name,
                "success": False,
                "error": f"Failed to validate rule: {str(e)}"
            }
            validation_results.append(error_result)
    
    return ValidationResponse(result=validation_results)
