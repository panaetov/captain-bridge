import logging
from typing import List

from service.entities.metric import Dashboard, Metric
from service.repositories.common import Repository

logger = logging.getLogger(__name__)


class MetricRepository(Repository):
    table = "metric_metrics"
    default_entity_factory = Metric

    def get_unique_key(self, metric: Metric):
        return {
            "internal_id": metric.internal_id,
        }

    async def filter_by_name(self, name: str):
        records = await self.fetch(
            {
                "name": {
                    "$regex": name,
                }
            }
        )
        return [Metric(**record) for record in records]

    async def filter_by_internal_id(self, internal_ids: List[str]):
        cursor = self.get_table().find({"internal_id": {"$in": internal_ids}})
        return [Metric(**record) async for record in cursor]


class DashboardRepository(Repository):
    table = "metric_dashboards"
    default_entity_factory = Dashboard

    def get_unique_key(self, dashboard: Dashboard):
        return {
            "internal_id": dashboard.internal_id,
        }

    async def filter_by_internal_id(self, internal_ids: List[str]):
        cursor = self.get_table().find({"internal_id": {"$in": internal_ids}})
        return [Dashboard(**record) async for record in cursor]
