from fastapi import APIRouter
from app.models.rule import Rule
from app.rules.expectation_rules import get_all_expectation_rules
from app.validators.validator import data_validator
from app.models.validation_request import ValidationRequest
from app.models.validation_response import ValidationResponse

router = APIRouter()

@router.get("/api/rules", response_model=list[Rule])
def read_all_rules():
    return get_all_expectation_rules()

@router.get("/api/validation", response_model=list[dict])
def validate_data():
    return data_validator()

