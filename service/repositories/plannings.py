import datetime
import logging

from service.entities.planning import DayOff, DonePercent, History, Planning
from service.repositories.common import Repository

logger = logging.getLogger(__name__)


class PlanningRepository(Repository):
    table = "planning_plannings"
    default_entity_factory = Planning

    def build_fetch_pipeline(self, match):
        pipeline = [
            {
                "$match": match,
            },
            {
                "$lookup": {
                    "from": "metric_metrics",
                    "localField": "velocity_metric_internal_id",
                    "foreignField": "internal_id",
                    "as": "velocity_metric",
                },
            },
            {
                "$unwind": {
                    "path": "$velocity_metric",
                    "preserveNullAndEmptyArrays": True,
                },
            },
        ]

        return pipeline

    def to_record_fields_from_dict(self, data, updated_fields=None):
        data = dict(data)

        m = data.pop("velocity_metric")
        data["velocity_metric_internal_id"] = m["internal_id"]

        for day_off in data["day_offs"]:
            day_off["date_from"] = datetime.datetime.combine(
                day_off["date_from"],
                datetime.time.min,
            )

            day_off["date_to"] = datetime.datetime.combine(
                day_off["date_to"],
                datetime.time.min,
            )

        return super().to_record_fields_from_dict(data, updated_fields)


class DonePercentRepository(Repository):
    table = "planning_done_percents"
    default_entity_factory = DonePercent

    def get_unique_key(self, entity):
        return {
            "issue_internal_id": entity.issue_internal_id,
            "planning_internal_id": entity.planning_internal_id,
        }

    async def filter_by_planning(self, planning_internal_id):
        records = await self.fetch(
            {
                "planning_internal_id": planning_internal_id,
            }
        )

        entities = [self.default_entity_factory(**r) for r in records]
        return entities

    async def get_for_issue(self, planning_internal_id, issue_internal_id):
        records = await self.fetch(
            {
                "planning_internal_id": planning_internal_id,
                "issue_internal_id": issue_internal_id,
            }
        )

        if records:
            return self.default_entity_factory(**records[0])


class HistoryRepository(Repository):
    table = "planning_history"
    default_entity_factory = History

    def get_unique_key(self, entity):
        return {
            "planning_internal_id": entity.planning_internal_id,
            "date": entity.date,
            "employee": entity.employee,
            "issue_internal_id": entity.issue_internal_id,
        }

    async def filter_by_dates(
        self, planning_internal_id, datetime_from, datetime_to
    ):
        records = await self.fetch(
            {
                "planning_internal_id": planning_internal_id,
                "date": {
                    "$gte": datetime_from,
                    "$lte": datetime_to,
                },
            }
        )

        return [self.default_entity_factory(**r) for r in records]


class DayOffRepository(Repository):
    table = "planning_day_offs"
    default_entity_factory = DayOff

    def get_unique_key(self, entity):
        return {}

    async def filter_by_date_from(
        self, planning_internal_id, datetime_from, datetime_to
    ):
        entities = await self.extract(
            {
                "planning_internal_id": planning_internal_id,
                "date_from": {
                    "$gte": datetime_from,
                    "$lte": datetime_to,
                },
            }
        )

        return entities
