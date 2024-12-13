import pydantic

from service.entities.common import generate_internal_id


class Role(pydantic.BaseModel):
    internal_id: str = pydantic.Field(default_factory=generate_internal_id)
    name: str
