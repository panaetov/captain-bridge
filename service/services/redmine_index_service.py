import logging
import datetime
import dateutil

from service import utils
from service.entities.redmine import (
    Redmine,
    Issue,
    IndexLog,
)


logger = logging.getLogger(__name__)


class RedmineIndexService:
    def __init__(
        self,
        redmine_service_factory,
        redmine_repository,
        issue_repository,
    ):
        self.redmine_service_factory = redmine_service_factory
        self.issue_repository = issue_repository
        self.redmine_repository = redmine_repository

    def build_redmine_service(self, redmine: Redmine):
        redmine_service = self.redmine_service_factory(
            redmine.url,
            redmine.auth_method,
            redmine.login,
            redmine.password,
            redmine.token,
        )
        return redmine_service

    async def indexify(
        self, redmine_internal_id, full=False, watcher_callback=None
    ):
        worker = Worker(self, redmine_internal_id, watcher_callback, full=full)
        await worker.indexify()


class Worker:
    def __init__(
        self, launcher, redmine_internal_id, watcher_callback, full=False
    ):
        self.launcher = launcher
        self.redmine_internal_id = redmine_internal_id
        self.logger = utils.TeeLogger()
        if watcher_callback:
            self.logger.subscribe(watcher_callback)
        self.logger.subscribe(self.save_log)
        self.full = full

    async def save_log(self, message, level):
        await self.launcher.redmine_repository.add_log(
            self.redmine_internal_id,
            IndexLog(
                created_at=datetime.datetime.utcnow(),
                level=level,
                message=message,
            ),
        )

    async def indexify(self):
        try:
            await self._indexify()
        except Exception as e:
            self.logger.exception(f"Indexing failed, reason: {e}")

            await self.update_redmines(
                {
                    "status": "error",
                }
            )

        finally:
            await self.logger.wait_pending_callbacks()

    async def get_redmine(self):
        redmines = await self.launcher.redmine_repository.filter_by_internal_id(
            [self.redmine_internal_id]
        )

        return redmines[0]

    async def _indexify(self):
        self.logger.info("Start...")
        now = datetime.datetime.utcnow()
        await self.update_redmines(
            {
                "indexed_at": now,
                "status": "indexing",
            }
        )

        await self.__indexify()
        await self.update_redmines(
            {
                "indexed_at": now,
                "status": "indexed",
            }
        )
        self.logger.info("Done!")

    async def update_redmines(self, payload):
        await self.launcher.redmine_repository.update_many(
            payload, [self.redmine_internal_id] if self.redmine_internal_id else None
        )

    async def __indexify(self):
        redmine = await self.get_redmine()

        service = self.launcher.build_redmine_service(redmine)

        for project in redmine.projects:
            self.logger.info(f"Indexifying project {project}")

            project_ref = await service.get_project_by_name(project)
            await self.indexify_issues(redmine, service, project_ref)

        await self.launcher.issue_repository.refresh_presentation_view()

    def parse_custom_fields(self, field_refs, field_specs):
        result = []

        for f in field_refs:
            spec = field_specs.get(f['id'])
            field_format = getattr(spec, 'field_format', None)
            formatter = None
            if field_format == 'int':
                formatter = int

            elif field_format == 'bool':
                formatter = lambda x: x == '1'

            elif field_format == 'float':
                formatter = float

            elif field_format == 'date':
                formatter = dateutil.parser.parse

            try:
                f['value'] = formatter(f['value']) if f['value'] and formatter else f['value']
            except Exception:
                logger.error(f"Cannot format value {f['value']}")

            result.append(f)

        return result

    async def indexify_issues(self, redmine, service, project_ref):
        max_updated = None

        if not self.full:
            max_updated = await self.launcher.issue_repository.get_max_updated(
                redmine.internal_id,
                project_ref.name,
            )

        if not max_updated:
            max_updated = datetime.datetime.utcnow() - datetime.timedelta(
                days=90
            )

        self.logger.info(f"Fetching issues updated after {max_updated.isoformat()}.")
        issue_refs = await service.get_updated_issues(project_ref, max_updated)
        try:
            custom_field_specs = await service.get_issue_custom_fields()
        except service.ForbiddenError as e:
            self.logger.exception(str(e))
            custom_field_specs = {}

        for issue_ref in issue_refs:
            self.logger.info(f"Updating issue {issue_ref.id}, updated on {issue_ref.updated_on}.")
            raw_custom_fields = getattr(issue_ref, 'custom_fields', None)
            if raw_custom_fields:
                custom_fields = self.parse_custom_fields(
                    list(raw_custom_fields.values()),
                    custom_field_specs,
                )
            else:
                custom_fields = []

            assigned_to = getattr(issue_ref, 'assigned_to', None)
            parent = getattr(issue_ref, 'parent', None)

            issue = Issue(
                key=str(issue_ref.id),
                created=issue_ref.created_on,
                updated=issue_ref.updated_on,

                assigned_to=assigned_to.name if assigned_to else None,
                parent_key=str(parent.id) if parent else '',
                redmine=redmine,

                closed_at=issue_ref.closed_on,
                started_at=issue_ref.start_date,

                subject=issue_ref.subject,
                description=issue_ref.description,

                project=issue_ref.project.name,
                tracker=issue_ref.tracker.name,
                status=issue_ref.status.name,

                priority=issue_ref.priority.name,
                author=issue_ref.author.name,
                due_date=issue_ref.due_date,
                done_ratio=issue_ref.done_ratio or 0,
                estimated_hours=issue_ref.estimated_hours or 0,
                spent_hours=issue_ref.spent_hours,
                total_spent_hours=issue_ref.total_spent_hours or 0,
                total_estimated_hours=issue_ref.total_estimated_hours or 0,

                journals=self._parse_journals(issue_ref.journals),
                custom_fields_by_id={
                    str(f['id']): f['value']
                    for f in custom_fields
                },
                custom_fields_by_name={
                    f['name']: f['value']
                    for f in custom_fields
                },
            )

            await self.launcher.issue_repository.save(issue)

    def _parse_journals(self, journals_ref):
        result = []

        for jo in journals_ref:
            if jo.notes:
                result.append(dict(
                    created_at=jo.created_on,
                    user=jo.user.name,
                    notes=jo.notes,
                ))

            for detail in jo.details:
                result.append(dict(
                    created_at=jo.created_on,
                    user=jo.user.name,
                    field=detail['name'],
                    value_from=detail['old_value'],
                    value_to=detail['new_value'],
                ))

        return result
