import datetime
import logging

from pytimeparse import parse as parse_interval

from service.entities.planning import DonePercent, History

logger = logging.getLogger(__name__)


DAY = 3600 * 24


class PlanningActualizerService:
    def __init__(
        self,
        plannings_repository,
        done_percents_repository,
        issues_repository,
        run_service,
        history_repository,
    ):
        self.plannings_repository = plannings_repository
        self.done_percents_repository = done_percents_repository
        self.issues_repository = issues_repository
        self.run_service = run_service
        self.history_repository = history_repository

    def is_finished(self, planning, issue):
        return issue.status.name in planning.terminal_states

    async def actualize(self, planning_internal_id):
        plannings = await self.plannings_repository.filter_by_internal_id(
            [planning_internal_id],
        )
        planning = plannings[0]

        done_percents = await self.done_percents_repository.filter_by_planning(
            planning_internal_id,
        )
        id__progress = {d.issue_internal_id: d for d in done_percents}

        issues = await self.get_issues(planning)
        id__issue = {i.internal_id: i for i in issues}

        sizes = await self.get_sizes(planning, issues)
        velocities = await self.get_velocities(planning)

        now = datetime.datetime.utcnow()

        # Saturday or Sunday
        if now.weekday() > 4:
            return

        for employee, issues in planning.assigned_issues.items():
            employee_is_off = False

            for day_off in planning.day_offs:
                if (
                    day_off.employee == employee
                    and day_off.date_from <= now.date() <= day_off.date_to
                ):
                    employee_is_off = True
                    break

            if employee_is_off:
                logger.info(
                    f"Planning {planning.internal_id}: "
                    f"employee {employee} is off."
                )
                continue

            internal_id = None
            for maybe_id in [i["internal_id"] for i in issues]:
                issue = id__issue.get(maybe_id)
                if issue and not self.is_finished(planning, issue):
                    internal_id = maybe_id
                    break

            if not internal_id:
                continue

            await self.history_repository.save(
                History(
                    planning_internal_id=planning.internal_id,
                    issue_internal_id=internal_id,
                    date=datetime.datetime(
                        year=now.year, month=now.month, day=now.day
                    ),
                    employee=employee,
                )
            )

            progress = id__progress.get(internal_id)
            if not progress:
                progress = DonePercent(
                    issue_internal_id=internal_id,
                    planning_internal_id=planning_internal_id,
                    value=0,
                    overdue=0,
                    started_at=now,
                )
                logger.info(f"Creating new done percent: {progress}")
                await self.done_percents_repository.save(progress)
                continue

            if progress.value < 100:
                dt = now - progress.changed_at

                size = sizes[internal_id]
                velocity = velocities[employee]

                done_chunk = velocity * dt.seconds / DAY
                new_percent = (
                    100.0
                    * (size * (progress.value / 100.0) + done_chunk)
                    / size
                )
                if progress.value == 0 and new_percent > 0:
                    progress.started_at = now

                progress.value = new_percent
                progress.overdue = 0
                progress.changed_at = now
                logger.info(
                    f"Updating done percent for planning {planning_internal_id}: "
                    f"dt = {dt}, "
                    f"done_chunk = {done_chunk}, "
                    f"new_percent = {new_percent}."
                )
                await self.done_percents_repository.save(progress)

            else:
                dt = now.date() - progress.changed_at.date()
                if dt.days > 0:
                    logger.info(
                        f"Updating overdue for planning {planning_internal_id}: "
                        f"dt = {dt}, inc={dt.days}."
                    )
                    progress.changed_at = now
                    progress.overdue += dt.days
                    await self.done_percents_repository.save(progress)

    async def get_velocities(self, planning):
        datetime_to = datetime.datetime.utcnow()
        period_secs = parse_interval(planning.velocity_period)
        datetime_from = datetime_to - datetime.timedelta(seconds=period_secs)

        data = await self.run_service.compute(
            planning.velocity_metric_internal_id,
            datetime_from,
            datetime_to,
            period_secs,
            "table",
            {},
        )

        return {d["name"]: d["velocity"] for d in data}

    async def get_sizes(self, planning, issues):
        paths = planning.issue_size_field.split(".")

        sizes = {}

        for issue in issues:
            key = issue.key

            try:
                size = issue
                for path in paths:
                    try:
                        size = size[path]
                    except (KeyError, TypeError):
                        size = getattr(size, path)

                size = float(size)

            except (TypeError, KeyError, AttributeError):
                logger.warning(
                    f"Cannot get size for issue {key} in {paths}", exc_info=True
                )
                size = planning.default_issue_size

            logger.info(f"Size of issue {issue.key} = {size}")
            sizes[issue.internal_id] = size or planning.default_issue_size

        return sizes

    async def get_issues(self, planning):
        all_ids = set([])

        for employee, issues in planning.assigned_issues.items():
            all_ids.update(
                [i["internal_id"] for i in issues if "internal_id" in i]
            )

        issues = await self.issues_repository.filter_by_internal_id(
            list(all_ids)
        )
        return issues
