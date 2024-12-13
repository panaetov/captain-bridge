import asyncio
import logging

from jira import JIRA

logger = logging.getLogger(__name__)


class JiraService:
    def __init__(self, url, auth_method, login, password, token):
        self.login = login
        self.password = password
        auth_kwargs = {}
        if auth_method == "basic":
            auth_kwargs["basic_auth"] = (login, password)

        elif auth_method == "token":
            auth_kwargs["token_auth"] = token

        self.jira = JIRA(
            url,
            async_workers=30,
            **auth_kwargs,
        )

    async def get_timezone(self):
        data = await asyncio.to_thread(
            self.jira._get_json, f"user?username={self.login}"
        )
        return data["timeZone"]

    async def get_fields(self):
        return await asyncio.to_thread(self.jira.fields)

    async def comments(self, issue_key: str):
        return await asyncio.to_thread(self.jira.comments, issue=issue_key)

    async def boards(self, project, type: str | None = None):
        return await asyncio.to_thread(
            self.jira.boards,
            projectKeyOrID=project,
            type=type,
        )

    async def sprints(self, project):
        boards = await self.boards(project, type=None)
        board_ids = []
        for board in boards:
            board_ids.append(board.id)

        all_sprints = {}
        sprint__boards = {}
        for board in boards:
            offset = 0
            logger.info(f"Getting sprints for board {board}")
            while True:
                try:
                    sprints = await asyncio.to_thread(
                        self.jira.sprints,
                        board_id=board.id,
                        extended=True,
                        startAt=offset,
                    )
                except Exception as e:
                    logger.error(f"Cannot fetch sprints for board {board}: {e}")
                    sprints = []

                if not sprints:
                    break

                for sprint in sprints:
                    all_sprints[sprint.id] = sprint
                    sprint__boards.setdefault(sprint.id, []).append(
                        {
                            "id": board.id,
                            "name": board.name,
                        }
                    )

                offset += len(sprints)

        return all_sprints, sprint__boards

    async def search_issues(
        self, query, offset, limit=50, extra=("changelog", "names")
    ):
        return await asyncio.to_thread(
            self.jira.search_issues,
            query,
            json_result=True,
            startAt=offset,
            maxResults=limit,
            expand=",".join(extra),
        )

    async def search_users(self, username, offset=0, limit=50):
        return await asyncio.to_thread(
            self.jira.search_users,
            username,
            startAt=offset,
            maxResults=limit,
            includeInactive=True,
        )

    async def statuses(self):
        return await asyncio.to_thread(self.jira.statuses)
