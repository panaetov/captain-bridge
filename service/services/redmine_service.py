import asyncio
import logging

import redminelib
from redminelib import Redmine


logger = logging.getLogger(__name__)


class RedmineService:
    class ForbiddenError(Exception):
        ...

    def __init__(self, url, auth_method, login, password, token):
        self.login = login
        self.password = password
        auth_kwargs = {}
        if auth_method == "basic":
            auth_kwargs["username"] = login
            auth_kwargs["password"] = password

        elif auth_method == "token":
            auth_kwargs["key"] = token

        self.redmine = Redmine(
            url,
            **auth_kwargs,
        )

    async def get_project_by_name(self, name):
        project = await asyncio.to_thread(
            self.redmine.project.get, name,
        )
        return project

    async def get_issues(self, project_ref):
        return project_ref.issues

    async def get_issue_custom_fields(self):
        try:
            fields = self.redmine.custom_field.all()
            return {
                f.id: f
                for f in fields
            }
        except redminelib.exceptions.ForbiddenError as e:
            logger.error(f"Cannot get custom fields, because {e}")
            raise self.ForbiddenError(
                'Cannot get custom fields. '
                'Please, enable REST API in settings of Redmine '
                'if you want custom fields to be parsed properly.'
            )
