from fastapi import APIRouter
from app.models.rule import Rule
from app.rules.expectation_rules import get_all_expectation_rules

router = APIRouter()

@router.get("/rules", response_model=list[Rule])
def read_all_rules():
    return get_all_expectation_rules()