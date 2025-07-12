from fastapi import APIRouter, HTTPException
from typing import List, Dict, Any
import logging
from app.models.rule import Rule
from app.rules.expectation_rules import get_all_expectation_rules
from app.validators.validator import data_validator
from app.models.validation_request import ValidationRequest
from app.models.validation_response import ValidationResponse

logger = logging.getLogger(__name__)

router = APIRouter()

@router.get("/api/rules", response_model=list[Rule])
def read_all_rules():
    return get_all_expectation_rules()

@router.get("/api/validation", response_model=list[dict])
def validate_data_get():
    return data_validator()

@router.post("/api/validate")
def validate_data(request: Dict[str, Any]) -> Dict[str, Any]:
    """
    Validate data using specified rules
    
    Args:
        request: Dictionary containing 'data' and 'rules' keys
        
    Returns:
        Validation results
    """
    try:
        data = request.get("data", [])
        rules = request.get("rules", [])
        
        if not data:
            raise HTTPException(status_code=400, detail="Data is required")
        if not rules:
            raise HTTPException(status_code=400, detail="Rules are required")
        
        # Process validation using existing validator
        results = []
        summary = {"total_rules": len(rules), "passed": 0, "failed": 0}
        
        for rule in rules:
            try:
                rule_name = rule.get("rule_name")
                column_name = rule.get("column_name")
                value = rule.get("value", {})
                
                # Import the appropriate validator based on rule name
                validator_module = __import__(f"app.validators.{rule_name}", fromlist=[rule_name])
                validator_class = getattr(validator_module, rule_name.replace("_", " ").title().replace(" ", ""))
                
                # Create validator instance and validate
                validator_instance = validator_class()
                result = validator_instance.validate(data, column_name, **value)
                
                results.append({
                    "rule": rule_name,
                    "column": column_name,
                    "success": result.get("success", False),
                    "message": result.get("message", ""),
                    "details": result.get("details", {})
                })
                
                if result.get("success", False):
                    summary["passed"] += 1
                else:
                    summary["failed"] += 1
                    
            except Exception as e:
                logger.error(f"Error validating rule {rule.get('rule_name', 'unknown')}: {e}")
                results.append({
                    "rule": rule.get("rule_name", "unknown"),
                    "column": rule.get("column_name"),
                    "success": False,
                    "message": f"Validation error: {str(e)}",
                    "details": {}
                })
                summary["failed"] += 1
        
        return {
            "results": results,
            "summary": summary
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Validation request failed: {e}")
        raise HTTPException(status_code=500, detail=f"Validation failed: {str(e)}")

