import datetime

from typing import List
from pydantic import BaseModel, Field
from service.entities.common import generate_internal_id


class IndexLog(BaseModel):
    created_at: datetime.datetime
    level: str
    message: str


class Gitlab(BaseModel):
    internal_id: str = Field(default_factory=generate_internal_id)
    name: str
    url: str
    token: str
    logs: List[IndexLog] = []
    indexed_at: datetime.datetime | None = None
    status: str = "indexing"
    projects: List[str] = []
    index_period: int = 300


class CommitStats(BaseModel):
    changed_files: int
    added_lines: int
    deleted_lines: int


class Commit(BaseModel):
    gitlab: Gitlab | None = None

    internal_id: str = Field(default_factory=generate_internal_id)
    commit_id: str
    title: str
    created: datetime.datetime
    author: str
    project_id: int
    project_name: str
    stats: CommitStats
    is_merge: bool = False
    branches: List[str]
