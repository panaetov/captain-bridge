import asyncio
import logging
import datetime

from service import utils
from service.entities.gitlab import (
    Gitlab,
    CommitStats,
    Commit,
    IndexLog,
)


logger = logging.getLogger(__name__)


class GitlabIndexService:
    def __init__(
        self,
        gitlab_service_factory,
        gitlab_repository,
        commit_repository,
    ):
        self.gitlab_service_factory = gitlab_service_factory
        self.commit_repository = commit_repository
        self.gitlab_repository = gitlab_repository

    def build_gitlab_service(self, gitlab: Gitlab):
        gitlab_service = self.gitlab_service_factory(
            gitlab.url,
            gitlab.token,
        )
        return gitlab_service

    async def indexify(
        self, gitlab_internal_id, full=False, watcher_callback=None
    ):
        worker = Worker(self, gitlab_internal_id, watcher_callback, full=full)
        await worker.indexify()


class Worker:
    def __init__(
        self, launcher, gitlab_internal_id, watcher_callback, full=False
    ):
        self.launcher = launcher
        self.gitlab_internal_id = gitlab_internal_id
        self.logger = utils.TeeLogger()
        if watcher_callback:
            self.logger.subscribe(watcher_callback)
        self.logger.subscribe(self.save_log)
        self.full = full

    async def save_log(self, message, level):
        await self.launcher.gitlab_repository.add_log(
            self.gitlab_internal_id,
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

            await self.update_gitlabs(
                {
                    "status": "error",
                }
            )

        finally:
            await self.logger.wait_pending_callbacks()

    async def get_gitlab(self):
        gitlabs = await self.launcher.gitlab_repository.filter_by_internal_id(
            [self.gitlab_internal_id]
        )

        return gitlabs[0]

    async def _update_gitlab(self, payload):
        await self.launcher.gitlab_repository.update_many(
            payload,
            [self.gitlab_internal_id] if self.gitlab_internal_id else None
        )

    async def _indexify(self):
        self.logger.info("Start...")
        now = datetime.datetime.utcnow()
        await self.update_gitlabs(
            {
                "indexed_at": now,
                "status": "indexing",
            }
        )

        await self.__indexify()
        await self.update_gitlabs(
            {
                "indexed_at": now,
                "status": "indexed",
            }
        )
        self.logger.info("Done!")

    async def update_gitlabs(self, payload):
        await self.launcher.gitlab_repository.update_many(
            payload, [self.gitlab_internal_id] if self.gitlab_internal_id else None
        )

    async def __indexify(self):
        gitlab = await self.get_gitlab()

        service = self.launcher.build_gitlab_service(gitlab)

        for project_ref in service.get_projects(gitlab.projects):
            project_name = project_ref.attributes['name']

            self.logger.info(f"Indexifying project {project_name}")
            await self.indexify_commits(gitlab, service, project_ref)

        await self.launcher.commit_repository.refresh_presentation_view()

    async def indexify_commits(self, gitlab, service, project_ref):
        chunk = []

        for commit_ref in service.get_commits(project_ref):
            chunk.append(commit_ref)

            if len(chunk) > 50:
                results = await asyncio.gather(*[
                    self.save_commit(gitlab, project_ref, commit_ref)
                    for commit_ref in chunk
                ])
                if any(not is_new for is_new in results):
                    break

                chunk = []

        if len(chunk) > 1:
            await asyncio.gather(*[
                self.save_commit(gitlab, project_ref, commit_ref)
                for commit_ref in chunk
            ])

    async def save_commit(self, gitlab, project_ref, commit_ref):
        commit = await asyncio.to_thread(
            self.build_commit,
            gitlab, project_ref, commit_ref,
        )
        self.logger.info(f"Inserting commit {commit.commit_id}, title '{commit.title}'.")
        return await self.launcher.commit_repository.save(commit)

    def build_commit(self, gitlab, project_ref, commit_ref):
        commit = Commit(
            commit_id=commit_ref.id,
            project_id=project_ref.id,
            gitlab=gitlab,
            project_name=project_ref.attributes['name'],
            title=commit_ref.title,
            created=commit_ref.created_at,
            author=commit_ref.author_name,
            is_merge=len(commit_ref.parent_ids) > 1,
            stats=self.commit_stat(commit_ref),
            branches=[r['name'] for r in commit_ref.refs('branch', get_all=True)],
        )
        return commit

    def commit_stat(self, commit_ref):
        diffs = commit_ref.diff(get_all=True)
        changed_files = len(diffs)

        total_new_lines = 0
        total_deleted_lines = 0
        for diff in diffs:
            new_lines, deleted_lines = self.diff_stat(diff['diff'])
            total_new_lines += new_lines
            total_deleted_lines += deleted_lines

        return CommitStats(
            changed_files=changed_files,
            added_lines=total_new_lines,
            deleted_lines=total_deleted_lines,
        )

    def diff_stat(self, diff):
        lines = diff.split('\n')

        new_lines = 0
        deleted_lines = 0
        for line in lines:
            if line.startswith('+'):
                new_lines += 1

            elif line.startswith('-'):
                deleted_lines += 1

        return new_lines, deleted_lines
