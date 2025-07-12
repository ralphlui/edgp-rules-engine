from typing import List
from pydantic import BaseModel

from app.models.rule import Rule


class ValidationResponse(BaseModel):
    result: List[dict]
