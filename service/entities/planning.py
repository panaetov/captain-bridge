import datetime
import logging
from typing import Dict, List

import pydantic

from service.entities.common import generate_internal_id
from service.entities.metric import Metric

logger = logging.getLogger(__name__)


class DayOff(pydantic.BaseModel):
    date_from: datetime.date
    date_to: datetime.date
    employee: str


class Planning(pydantic.BaseModel):
    internal_id: str = pydantic.Field(default_factory=generate_internal_id)
    created_at: datetime.datetime = pydantic.Field(
        default_factory=datetime.datetime.utcnow
    )

    name: str
    velocity_metric_internal_id: str
    velocity_metric: Metric
    velocity_period: str

    issue_size_field: str
    default_issue_size: float = 0

    terminal_states: List[str]

    assigned_issues: Dict
    day_offs: List[DayOff] = []


class DonePercent(pydantic.BaseModel):
    issue_internal_id: str
    planning_internal_id: str
    changed_at: datetime.datetime = pydantic.Field(
        default_factory=datetime.datetime.utcnow
    )
    value: float
    overdue: int = 0
    started_at: datetime.datetime | None = None


class History(pydantic.BaseModel):
    planning_internal_id: str
    date: datetime.datetime
    employee: str
    issue_internal_id: str
