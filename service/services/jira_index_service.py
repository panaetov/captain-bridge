import asyncio
import datetime
import logging
import time

import dateutil.parser

from service import utils
from service.entities.jira import (
    Comment,
    IndexLog,
    Issue,
    Jira,
    Sprint,
    Status,
    User,
)

logger = logging.getLogger(__name__)


class JiraIndexService:
    def __init__(
        self,
        jira_service_factory,
        projects_repository,
        sprints_repository,
        issues_repository,
        users_repository,
        jiras_repository,
    ):
        self.jira_service_factory = jira_service_factory

        self.jiras_repository = jiras_repository
        self.projects_repository = projects_repository
        self.issues_repository = issues_repository
        self.sprints_repository = sprints_repository
        self.users_repository = users_repository

    def build_jira_service(self, jira: Jira):
        jira_service = self.jira_service_factory(
            jira.url,
            jira.auth_method,
            jira.login,
            jira.password,
            jira.token,
        )
        return jira_service

    async def indexify(
        self, jira_internal_id, full=False, watcher_callback=None
    ):
        worker = Worker(self, jira_internal_id, watcher_callback, full=full)
        await worker.indexify()


class Worker:
    def __init__(
        self, launcher, jira_internal_id, watcher_callback, full=False
    ):
        self.launcher = launcher
        self.jira_internal_id = jira_internal_id
        self.logger = utils.TeeLogger()
        if watcher_callback:
            self.logger.subscribe(watcher_callback)
        self.logger.subscribe(self.save_log)
        self.full = full

    async def save_log(self, message, level):
        await self.launcher.jiras_repository.add_log(
            self.jira_internal_id,
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

            await self._update_jiras(
                {
                    "status": "error",
                }
            )

        finally:
            await self.logger.wait_pending_callbacks()

    async def _indexify(self):
        self.logger.info("Start...")
        now = datetime.datetime.utcnow()
        await self._update_jiras(
            {
                "indexed_at": now,
                "status": "indexing",
            }
        )

        if self.jira_internal_id:
            projects = await self.launcher.projects_repository.fetch_by_jira(
                self.jira_internal_id,
            )
        else:
            projects = await self.launcher.projects_repository.all()

        for project in projects:
            await asyncio.gather(
                self.indexify_project_sprints(project),
                self.indexify_project_issues(project),
            )

        self.logger.info("Refreshing sprints public view.")
        await self.launcher.sprints_repository.refresh_presentation_view()

        self.logger.info("Refreshing issues public view.")
        await self.launcher.issues_repository.refresh_presentation_view()

        await self._update_jiras(
            {
                "indexed_at": now,
                "status": "indexed",
            }
        )
        self.logger.info("Done!")

    async def _update_jiras(self, payload):
        await self.launcher.jiras_repository.update_many(
            payload, [self.jira_internal_id] if self.jira_internal_id else None
        )

    async def _build_custom_field_builders(self, jira, jira_service):
        if not jira.custom_fields:
            return {}

        fields = await jira_service.get_fields()
        name__id = {}
        for field in fields:
            field_id = field["id"]
            field_name = field["name"]
            name__id[field_name] = field_id

        builders = {}
        for custom_field in jira.custom_fields:
            # указывается либо имя, либо ID. Мы должны нормализовать к ID.
            field_id = name__id.get(
                custom_field["source"],
                custom_field["source"],
            )
            builders[field_id] = {
                "field_id": field_id,
                "target": custom_field["target"],
                "builder": self._get_field_builder(custom_field["type"]),
            }

        return builders

    def _get_field_builder(self, type_: str):
        if type_ == "text":
            return lambda v: str(v) if v else ""

        elif type_ == "number":
            return lambda v: float(v) if v else 0.0

        elif type_ == "datetime":
            return lambda v: (
                dateutil.parser.parse(v) if v else datetime.datetime(1900, 1, 1)
            )

        else:
            raise RuntimeError(f"Unknown field type {type_}")

    async def indexify_project_issues(self, project):
        self.logger.info(f"Indexify issues of project {project.key}...")

        jira_service = self.launcher.build_jira_service(project.jira)
        builders = await self._build_custom_field_builders(
            project.jira,
            jira_service,
        )

        max_updated = None

        if not self.full:
            max_updated = await self.launcher.issues_repository.get_max_updated(
                self.jira_internal_id,
                project.key,
            )

            tz = await jira_service.get_timezone()
            if max_updated:
                max_updated = utils.shift_timezone(
                    max_updated,
                    "UTC",
                    tz,
                )

        if not max_updated:
            max_updated = datetime.datetime.utcnow() - datetime.timedelta(
                days=90
            )

        self.logger.info(f"Finding issues updated after {max_updated}")
        if max_updated:
            max_updated_query = max_updated.strftime("%Y-%m-%d %H:%M")
            query = f"project={project.key} and updated >= '{max_updated_query}' order by created"

        else:
            query = f"project={project.key} order by created"

        offset = 0
        max_results = 50

        factory = _IssueFactory(
            project,
            self.launcher.users_repository,
            builders,
        )

        attemp = 0
        while True:
            try:
                result = await jira_service.search_issues(
                    query,
                    limit=max_results,
                    offset=offset,
                )
            except Exception as e:
                self.logger.exception(f"Indexing had a problem, reason: {e}")
                attemp += 1
                if attemp > 3:
                    raise

                await asyncio.sleep(2)
                continue

            attemp = 0
            if not result["issues"]:
                break

            for issue_ref in result["issues"]:
                comments = await jira_service.comments(issue_ref["key"])
                issue: Issue = await factory.build_from_ref(issue_ref, comments)
                self.logger.info(
                    f"Updating issue {issue.key}, updated at {issue.updated}, created_at {issue.created}"
                )
                await self.launcher.issues_repository.save(issue)

            offset += max_results

    def _sprint_is_too_old(self, sprint_ref):
        if not getattr(sprint_ref, "startDate", None):
            return True

        return dateutil.parser.parse(sprint_ref.startDate).replace(
            tzinfo=None
        ) < datetime.datetime.utcnow() - datetime.timedelta(days=60)

    async def indexify_project_sprints(self, project):
        self.logger.info(f"Indexify sprints of project {project.key}...")
        jira_service = self.launcher.build_jira_service(project.jira)
        sprints, boards = await jira_service.sprints(project.key)

        coros = []

        for sprint_id, sprint in sprints.items():
            if self._sprint_is_too_old(sprint):
                continue

            self.logger.info(
                f"Updating sprint {sprint.name}[{sprint.startDate} - {sprint.endDate}]."
            )
            coro = self._indexify_sprint(
                sprint,
                jira_service,
                project,
                boards,
            )
            coros.append(coro)

            if len(coros) == 20:
                await asyncio.gather(*coros, return_exceptions=False)
                coros = []

        if coros:
            await asyncio.gather(*coros, return_exceptions=False)

    async def _indexify_sprint(self, sprint, jira_service, project, boards):
        issue_keys = await self._get_sprint_issues(jira_service, sprint.id)

        await self.launcher.sprints_repository.save(
            Sprint(
                jira=project.jira,
                project=project.key,
                sprint_id=sprint.id,
                name=sprint.name,
                start_date=sprint.startDate,
                end_date=sprint.endDate,
                complete_date=getattr(sprint, "completeDate", None),
                activated_date=getattr(sprint, "activatedDate", None),
                goal=getattr(sprint, "goal", ""),
                board_names=[b["name"] for b in boards[sprint.id]],
                board_ids=[b["id"] for b in boards[sprint.id]],
                issues=issue_keys,
            )
        )

    async def _get_sprint_issues(self, jira_service, sprint_id):
        offset = 0
        max_results = 50

        query = f"sprint={sprint_id} order by created"
        issues = []
        while True:
            t = time.time()
            result = await jira_service.search_issues(
                query,
                limit=max_results,
                offset=offset,
            )
            if not result["issues"]:
                break

            for issue_ref in result["issues"]:
                issues.append(issue_ref["key"])

            logger.warning(f"DT = {time.time() - t}")
            offset += max_results

        return issues


class _IssueFactory:
    def __init__(self, project, users_repository, custom_field_builders):
        self.project = project
        self.users_repository = users_repository
        self.custom_field_builders = custom_field_builders

    def build_custom_fields(self, issue_ref):
        fields = {}

        for field_id, spec in self.custom_field_builders.items():
            builder = spec["builder"]
            target = spec["target"]
            value = builder(issue_ref["fields"].get(field_id))

            fields[target] = value

        return fields

    async def build_from_ref(self, issue_ref, comment_refs):
        changelog_field = self._map_changelog(
            issue_ref["key"],
            issue_ref["changelog"],
        )
        custom_fields = self.build_custom_fields(issue_ref)
        comments = [
            Comment(
                id=comment_ref.id,
                created=comment_ref.created,
                updated=comment_ref.updated,
                body=comment_ref.body,
            )
            for comment_ref in comment_refs
        ]
        return Issue(
            jira=self.project.jira,
            project=self.project.key,
            key=issue_ref["key"],
            created=issue_ref["fields"]["created"],
            updated=issue_ref["fields"]["updated"],
            summary=issue_ref["fields"]["summary"],
            description=issue_ref["fields"]["description"] or "",
            status=issue_ref["fields"]["status"],
            issue_type=issue_ref["fields"]["issuetype"],
            assignee=self._map_user(issue_ref["fields"]["assignee"]),
            creator=self._map_user(issue_ref["fields"]["creator"]),
            reporter=self._map_user(issue_ref["fields"]["reporter"]),
            components=issue_ref["fields"]["components"],
            subtasks=[i["key"] for i in issue_ref["fields"]["subtasks"]],
            links=[
                self._map_link(link)
                for link in issue_ref["fields"]["issuelinks"]
            ],
            changes=changelog_field,
            comments=comments,
            custom_fields=custom_fields,
        )

    def _map_user(self, user_ref):
        if user_ref:
            # Сохраняем с пустыми ссылками, которые будем
            # заполнять на чтении из базы.
            return User(
                internal_id="",
                employee=None,
                jira=self.project.jira,
                key=user_ref["key"],
                name=user_ref["name"],
                email=user_ref["emailAddress"],
                display_name=user_ref["displayName"],
                active=user_ref["active"],
            )

        return User(
            internal_id="",
            employee=None,
            jira=self.project.jira,
            key="",
            name="",
            email="",
            display_name="",
            active=False,
        )

    def _map_status_ref(self, status):
        return Status(**status)

    def _map_changelog(self, issue_key, changelog_ref):
        changes = []

        for history in changelog_ref["histories"]:
            for item in history["items"]:
                changes.append(
                    {
                        "author": self._map_user(history.get("author")),
                        "created": history["created"],
                        "value_from_string": item["fromString"],
                        "value_from": item["from"],
                        "value_to_string": item["toString"],
                        "value_to": item["to"],
                        "field": item["field"],
                        "issue": issue_key,
                    }
                )

        return changes

    def _map_link(self, link):
        if link.get("inwardIssue"):
            return {
                "issue": link["inwardIssue"]["key"],
                "type": link["type"]["inward"],
            }
        else:
            return {
                "issue": link["outwardIssue"]["key"],
                "type": link["type"]["outward"],
            }
