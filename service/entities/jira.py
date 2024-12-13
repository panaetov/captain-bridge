import datetime
import logging
from typing import Dict, List, Optional

import pydantic

from service.entities.common import generate_internal_id
from service.entities.employee import Employee

logger = logging.getLogger(__name__)


class IndexLog(pydantic.BaseModel):
    created_at: datetime.datetime
    level: str
    message: str


class Jira(pydantic.BaseModel):
    internal_id: str = pydantic.Field(default_factory=generate_internal_id)
    name: str
    url: str
    auth_method: str = "basic"
    login: str = ""
    password: str = ""
    token: str = ""
    indexed_at: datetime.datetime | None = None
    index_period: int = 300
    logs: List[IndexLog] = []
    status: str = "provisioning"
    custom_fields: List[Dict] = []


class JiraIndexingSchedule(pydantic.BaseModel):
    internal_id: str = pydantic.Field(default_factory=generate_internal_id)
    jira: Jira
    cron: str

    status: str = "CREATED"
    started_at: datetime.datetime | None
    finished_at: datetime.datetime | None
    last_error: str = ""


class Project(pydantic.BaseModel):
    internal_id: str = pydantic.Field(default_factory=generate_internal_id)
    jira: Jira

    key: str
    name: str


class Sprint(pydantic.BaseModel):
    internal_id: str = pydantic.Field(default_factory=generate_internal_id)
    jira: Jira

    project: str
    sprint_id: int
    name: str
    start_date: datetime.datetime | None
    end_date: datetime.datetime | None
    complete_date: datetime.datetime | None
    activated_date: datetime.datetime | None
    goal: str = ""
    board_names: List[str]
    board_ids: List[int]
    issues: List[str]


class User(pydantic.BaseModel):
    internal_id: str = pydantic.Field(default_factory=generate_internal_id)
    jira: Jira | None = None
    employee: Employee | None = None

    key: str
    display_name: str
    name: str
    email: str
    active: bool


class Component(pydantic.BaseModel):
    id: str
    name: str


class IssueType(pydantic.BaseModel):
    name: str
    description: str
    subtask: bool


class IssueLink(pydantic.BaseModel):
    type: str
    issue: str


class Change(pydantic.BaseModel):
    issue: str
    author: User
    created: datetime.datetime
    value_from: Optional[str] = None
    value_from_string: Optional[str] = None
    value_to: Optional[str] = None
    value_to_string: Optional[str] = None
    field: str


class Status(pydantic.BaseModel):
    id: str
    name: str
    description: str


class Comment(pydantic.BaseModel):
    id: str
    created: datetime.datetime
    updated: datetime.datetime
    body: str


class Issue(pydantic.BaseModel):
    internal_id: str = pydantic.Field(default_factory=generate_internal_id)
    jira: Jira

    key: str
    created: datetime.datetime
    updated: datetime.datetime

    project: str

    status: Status

    issue_type: IssueType

    assignee: User | None
    creator: User | None
    reporter: User | None

    components: List[Component]
    links: List[IssueLink]

    subtasks: List[str]
    summary: str
    description: str

    changes: List[Change] = []
    storypoints: float = 0.0
    custom_fields: Dict = {}

    comments: List[Comment] = []
