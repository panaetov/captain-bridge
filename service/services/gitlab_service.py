import logging

import gitlab


logger = logging.getLogger(__name__)


class GitlabService:
    def __init__(self, url, private_token):
        self.url = url
        self.private_token = private_token
        self.gitlab = gitlab.Gitlab(
            url,
            private_token=private_token,
        )

    def get_projects(self, project_ids):
        result = []
        for project_id in project_ids:
            result.append(self.gitlab.projects.get(project_id))

        return result

    def get_commits(self, project_ref):
        return project_ref.commits.list(iterator=True)
