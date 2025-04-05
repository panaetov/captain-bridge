import datetime

from typing import Any, Dict, List
from pydantic import BaseModel, Field
from service.entities.common import generate_internal_id


class IndexLog(BaseModel):
    created_at: datetime.datetime
    level: str
    message: str


class Redmine(BaseModel):
    internal_id: str = Field(default_factory=generate_internal_id)
    name: str
    url: str
    auth_method: str = "basic"
    login: str = ""
    password: str = ""
    token: str = ""
    indexed_at: datetime.datetime | None = None
    index_period: int = 300
    logs: List[IndexLog] = []
    status: str = "indexing"
    custom_fields: List[Dict] = []
    projects: List[str]


class Attribute(BaseModel):
    id: str
    name: str


class Journal(BaseModel):
    user: Attribute
    created_at: datetime.datetime
    notes: str = ''
    field: str | None = None
    value_from: Any = None
    value_to: Any = None


class Issue(BaseModel):
    internal_id: str = Field(default_factory=generate_internal_id)
    redmine: Redmine

    id: str
    created_at: datetime.datetime
    updated_at: datetime.datetime
    
    started_at: datetime.datetime | None
    closed_at: datetime.datetime | None

    subject: str
    description: str

    assigned_to: Attribute | None

    parent_id: str
    project: Attribute
    tracker: Attribute
    status: Attribute
    priority: Attribute
    author: Attribute

    due_date: datetime.datetime | None
    done_ratio: float = 0
    estimated_hours: float = 0
    spent_hours: float = 0
    total_spent_hours: float = 0
    total_estimated_hours: float = 0

    custom_fields_by_name: Dict = {}
    custom_fields_by_id: Dict = {}

    journals: List[Journal] = []
