import asyncio
import datetime
import logging

import aiohttp

logger = logging.getLogger(__name__)


class RunService:
    def __init__(self, db, metrics_repository, worker_url):
        self.db = db
        self.metrics_repository = metrics_repository
        self.worker_url = worker_url

    async def compute(
        self,
        internal_id,
        datetime_from,
        datetime_to,
        period_secs,
        metric_type,
        scope=None,
    ):
        scope = scope or {}

        metrics = await self.metrics_repository.filter_by_internal_id(
            [
                internal_id,
            ]
        )
        metric = metrics[0]

        if metric_type == "timeseries":
            in_background = datetime_from < datetime_to - datetime.timedelta(
                seconds=2 * period_secs
            )
            logger.info(
                f"Running metric {internal_id} in background={in_background}"
            )
            t = datetime_to

            result = []
            coros = []
            while t > datetime_from:
                if in_background:
                    coros.append(
                        self.compute_in_worker(
                            internal_id,
                            t - datetime.timedelta(seconds=period_secs),
                            t,
                            period_secs=period_secs,
                            metric_type=metric_type,
                            scope=scope,
                        )
                    )
                else:
                    points, _ = await metric.compute(
                        t - datetime.timedelta(seconds=period_secs),
                        t,
                        scope,
                        self.db,
                    )

                    logger.info(f"{t}, {points}")
                    for p in points:
                        p.setdefault("label", "default")

                    result.append((t, points))

                t -= datetime.timedelta(seconds=period_secs)

            if coros:
                results = await asyncio.gather(*coros)
                merged = []
                for result in results:
                    logger.info(f"Worker results: {result}")
                    if "error" in result:
                        raise RuntimeError(result["error"])

                    elif "data" in result:
                        merged.extend(result["data"])

                    else:
                        raise RuntimeError("Unexpected response from stage")

                return merged[::-1]

            else:
                return result[::-1]

        else:
            result, _ = await metric.compute(
                datetime_from,
                datetime_to,
                scope,
                self.db,
            )

            return result

    async def compute_in_worker(
        self,
        internal_id,
        datetime_from,
        datetime_to,
        period_secs,
        metric_type,
        scope,
    ):
        payload = {
            "internal_id": internal_id,
            "datetime_from": datetime_from.strftime("%Y-%m-%d %H:%M:%S"),
            "datetime_to": datetime_to.strftime("%Y-%m-%d %H:%M:%S"),
            "period_secs": period_secs,
            "metric_type": metric_type,
            "variables": scope,
        }
        async with aiohttp.ClientSession() as session:
            async with session.post(self.worker_url, json=payload) as resp:
                return await resp.json()
