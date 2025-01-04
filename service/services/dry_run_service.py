from typing import List

from service.entities.metric import Metric, Stage


class DryRunService:
    def __init__(self, db):
        self.db = db

    async def compute(
        self,
        stage_name,
        stages: List[Stage],
        datetime_from,
        datetime_to,
        scope=None,
    ):
        scope = scope or {}
        computed_stages = []
        for stage in stages:
            computed = stage.model_copy()
            computed.is_terminal = computed.name == stage_name
            computed_stages.append(computed)

        metric = Metric(
            name="dry-run-test",
            stages=computed_stages,
            variables=[],
        )
        result, logs = await metric.compute(
            datetime_from, datetime_to, scope, self.db
        )
        return {
            "result": result,
            "logs": logs,
        }
