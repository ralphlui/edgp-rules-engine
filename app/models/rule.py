from pydantic import BaseModel
from typing import Any, List, Union, Set, Optional

class Rule(BaseModel):
    rule_name: str
    column_name: Optional[str] = None
    value: Optional[Union[List[Any], bool, Set[Any], str, int, float, dict]] = None