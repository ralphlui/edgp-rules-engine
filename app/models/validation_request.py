from typing import List
from pydantic import BaseModel

from app.models.rule import Rule


class ValidationRequest(BaseModel):
    rules: List[Rule]
    dataset: List[dict]
