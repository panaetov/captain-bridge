import logging
from typing import List

import pydantic

from service.entities.common import generate_internal_id
from service.entities.role import Role

logger = logging.getLogger(__name__)


class Employee(pydantic.BaseModel):
    internal_id: str = pydantic.Field(default_factory=generate_internal_id)
    name: str
    roles: List[Role] = []
