import logging
from typing import List

from service.entities.jira import (
    Issue,
    Jira,
    JiraIndexingSchedule,
    Project,
    Sprint,
    User,
)
from service.repositories.common import Repository

logger = logging.getLogger(__name__)


class JiraRepository(Repository):
    table = "datasource_jira_jiras"
    logs_table = "datasource_jira_logs"
    default_entity_factory = Jira

    def to_record_fields(self, entity_or_dict, updated_fields=None):
        fields = super().to_record_fields(entity_or_dict, updated_fields)

        if not fields["password"]:
            fields.pop("password")

        if not fields["token"]:
            fields.pop("token")

        fields.pop("logs")
        fields.pop("indexed_at")

        return fields

    def get_logs_table(self):
        return getattr(self.db, self.logs_table)

    async def add_log(self, internal_id, log):
        await self.get_logs_table().insert_one(
            dict(
                jira_internal_id=internal_id,
                **log.model_dump(),
            )
        )

    def build_fetch_pipeline(self, match, limit=None):
        pipeline = [
            {
                "$match": match,
            },
            {
                "$lookup": {
                    "from": "datasource_jira_logs",
                    "let": {
                        "jira_internal_id": "$internal_id",
                    },
                    "pipeline": [
                        {
                            "$match": {
                                "$expr": {
                                    "$eq": [
                                        "$jira_internal_id",
                                        "$$jira_internal_id",
                                    ]
                                },
                            },
                        },
                        {
                            "$sort": {"created_at": -1},
                        },
                        {
                            "$limit": 100,
                        },
                    ],
                    "as": "logs",
                },
            },
        ]

        return pipeline


class JiraIndexingScheduleRepository(Repository):
    table = "datasource_jira_schedules"

    def get_unique_key(self, schedule: JiraIndexingSchedule):
        return {
            "jira_internal_id": schedule.jira.internal_id,
        }


class SprintRepository(Repository):
    table = "datasource_jira_sprints"
    public_table = "datasource_jira_sprints_public"

    def get_unique_key(self, sprint: Sprint):
        return {
            "jira_internal_id": sprint.jira.internal_id,
            "sprint_id": sprint.sprint_id,
        }

    def to_record_fields_from_dict(self, data, updated_fields=None):
        data = super().to_record_fields_from_dict(data, updated_fields)

        jira = data.pop("jira")
        data["jira_internal_id"] = jira["internal_id"]
        return data

    async def refresh_presentation_view(self):
        cursor = self.get_table().aggregate(
            [
                {
                    "$lookup": {
                        "from": "datasource_jira_jiras",
                        "localField": "jira_internal_id",
                        "foreignField": "internal_id",
                        "as": "jira",
                    },
                },
                {
                    "$unwind": {
                        "path": "$jira",
                        "preserveNullAndEmptyArrays": True,
                    },
                },
                {
                    "$merge": {
                        "into": self.public_table,
                        "whenMatched": "replace",
                    }
                },
            ]
        )
        await cursor.to_list(None)


class UserRepository(Repository):
    table = "datasource_jira_users"

    def get_unique_key(self, user: User):
        return {
            "jira_internal_id": user.jira.internal_id,
            "key": user.key,
        }

    async def fetch(self, match):
        cursor = self.get_table().aggregate(
            [
                {
                    "$match": match,
                },
                {
                    "$lookup": {
                        "from": "datasource_jira_jiras",
                        "localField": "jira_internal_id",
                        "foreignField": "internal_id",
                        "as": "jira",
                    },
                },
                {
                    "$unwind": {
                        "path": "$jira",
                        "preserveNullAndEmptyArrays": True,
                    },
                },
                {
                    "$lookup": {
                        "from": "datasource_jira_users",
                        "localField": "user_internal_id",
                        "foreignField": "internal_id",
                        "as": "employee",
                    },
                },
                {
                    "$unwind": {
                        "path": "$employee",
                        "preserveNullAndEmptyArrays": True,
                    },
                },
            ]
        )
        return await cursor.to_list(None)

    async def find_by_keys(self, jira_internal_id, keys):
        match = {
            "jira_internal_id": jira_internal_id,
            "key": {
                "$in": list(keys),
            },
        }
        return await self.fetch(match)


class IssueRepository(Repository):
    table = "datasource_jira_issues"
    public_table = "datasource_jira_issues_public"
    default_entity_factory = Issue

    def get_public_table(self):
        return getattr(self.db, self.public_table)

    async def fetch_public(self, match, limit=None):
        cursor = self.get_public_table().aggregate(
            self.build_fetch_pipeline(match, limit)
        )

        records = await cursor.to_list(None)
        return records

    async def filter_by_internal_id(self, internal_ids: List[str]):
        records = await self.fetch_public(
            {"internal_id": {"$in": internal_ids}}
        )
        return [
            self.get_default_entity_factory()(**record) for record in records
        ]

    def get_unique_key(self, issue: Issue):
        return {
            "jira_internal_id": issue.jira.internal_id,
            "key": issue.key,
        }

    async def filter_by_keys(self, keys: List[str]):
        records = await self.fetch_public(
            {
                "key": {
                    "$in": keys,
                }
            }
        )

        issues = []
        for record in records:
            issues.append(Issue(**self.to_entities_fields_from_record(record)))

        return issues

    async def filter_by_name(self, name: str, limit=50):
        records = await self.fetch_public(
            {
                "key": {
                    "$regex": name,
                }
            },
            limit,
        )

        issues = []
        for record in records:
            logger.info(record["changes"])
            issues.append(Issue(**self.to_entities_fields_from_record(record)))

        return issues

    async def get_max_updated(self, jira_internal_id, project_key):
        cursor = self.get_table().aggregate(
            [
                {
                    "$match": {
                        "project": project_key,
                        "jira_internal_id": jira_internal_id,
                    },
                },
                {
                    "$group": {
                        "_id": "$project",
                        "max_updated": {"$max": "$updated"},
                    }
                },
            ]
        )

        records = await cursor.to_list(None)
        return records[0]["max_updated"] if records else None

    def to_entities_fields_from_record(self, record: dict):
        fields = dict(record)

        fields["assignee"] = fields["assignee"] or None
        fields["reporter"] = fields["reporter"] or None
        fields["creator"] = fields["creator"] or None
        return fields

    def to_record_fields_from_dict(self, data, updated_fields=None):
        data = super().to_record_fields_from_dict(data, updated_fields)

        jira = data.pop("jira")
        data["jira_internal_id"] = jira["internal_id"]
        data["assignee"] = self._user_data_to_record_fields(
            data["assignee"],
        )
        data["reporter"] = self._user_data_to_record_fields(
            data["reporter"],
        )
        data["creator"] = self._user_data_to_record_fields(
            data["creator"],
        )

        for change in data["changes"]:
            if change["author"]:
                change["author"] = self._user_data_to_record_fields(
                    change["author"],
                )

        return data

    async def refresh_presentation_view(self):
        pipeline = [
            {
                "$match": {
                    # "assignee.name": "dsanany1",
                },
            },
            {
                "$lookup": {
                    "from": "datasource_jira_jiras",
                    "localField": "jira_internal_id",
                    "foreignField": "internal_id",
                    "as": "jira",
                },
            },
            {
                "$unwind": {
                    "path": "$jira",
                    "preserveNullAndEmptyArrays": True,
                },
            },
            {
                "$unset": ["jira.password"],
            },
            # {
            #     "$unwind": {
            #         "path": "$changes",
            #         "preserveNullAndEmptyArrays": True,
            #     }
            # },
            # {
            #     "$lookup": {
            #         "from": "datasource_jira_users",
            #         "localField": "changes.author.name",
            #         "foreignField": "name",
            #         "as": "change_authors",
            #     },
            # },
        ]
        # self._join_user(pipeline, 'assignee')
        # self._join_user(pipeline, 'creator')
        # self._join_user(pipeline, 'reporter')
        # self._join_user(pipeline, 'changes.author')

        pipeline.extend(
            [
                # {
                #     "$group": {
                #         "_id": "$_id",
                #         "document": {"$first": "$$ROOT"},
                #         "changes2": {"$push": "$changes"},
                #     }
                # },
                # {
                #     "$replaceWith": {
                #         "$mergeObjects": ["$$ROOT", "$document", {"changes": "$changes2"}],
                #     }
                # },
                # {
                #     "$project": {
                #         "changes2": 0,
                #     },
                # },
                {
                    "$merge": {
                        "into": self.public_table,
                        "whenMatched": "replace",
                    }
                }
            ]
        )

        cursor = self.get_table().aggregate(pipeline)
        return await cursor.to_list(None)

    def _join_user(self, pipeline, field):
        pipeline.extend(
            [
                {
                    "$lookup": {
                        "from": "datasource_jira_users",
                        "localField": f"{field}.name",
                        "foreignField": "name",
                        "as": f"{field}2",
                    },
                },
                {
                    "$unwind": {
                        "path": f"${field}2",
                        "preserveNullAndEmptyArrays": True,
                    },
                },
                {
                    "$set": {
                        f"{field}": {
                            "internal_id": f"${field}2.internal_id",
                            "jira_internal_id": f"${field}2.jira_internal_id",
                            "employee_internal_id": f"${field}2.employee_internal_id",
                            "key": f"${field}.key",
                            "name": f"${field}.name",
                            "display_name": f"${field}.display_name",
                            "email": f"${field}.email",
                            "active": f"${field}.active",
                        },
                    }
                },
                {
                    "$unset": f"{field}2",
                },
                {
                    "$lookup": {
                        "from": "core_employees",
                        "localField": f"{field}.employee_internal_id",
                        "foreignField": "internal_id",
                        "as": f"{field}.employee",
                    },
                },
                {
                    "$unwind": {
                        "path": f"${field}.employee",
                        "preserveNullAndEmptyArrays": True,
                    },
                },
            ]
        )

    def _user_data_to_record_fields(self, user_entity_data):
        if user_entity_data:
            return {
                "key": user_entity_data["key"],
                "name": user_entity_data["name"],
                "email": user_entity_data["email"],
                "display_name": user_entity_data["display_name"],
                "active": user_entity_data["active"],
            }


class ProjectRepository(Repository):
    table = "datasource_jira_projects"

    def to_record_fields(self, entity_or_dict, updated_fields=None):
        fields = super().to_record_fields(entity_or_dict, updated_fields)
        jira = fields.pop("jira")
        fields["jira_internal_id"] = jira["internal_id"]
        return fields

    def get_unique_key(self, project: Project):
        return {
            "jira_internal_id": project.jira.internal_id,
            "key": project.key,
        }

    async def delete_by_key(self, project_key: str):
        await self.get_table().delete_one({"key": project_key})

    async def fetch_by_jira(self, jira_internal_id):
        records = await self.fetch({"jira_internal_id": jira_internal_id})

        return [Project(**r) for r in records]

    async def fetch(self, match):
        cursor = self.get_table().aggregate(
            [
                {
                    "$match": match,
                },
                {
                    "$lookup": {
                        "from": "datasource_jira_jiras",
                        "localField": "jira_internal_id",
                        "foreignField": "internal_id",
                        "as": "jira",
                    },
                },
                {
                    "$unwind": {
                        "path": "$jira",
                        "preserveNullAndEmptyArrays": True,
                    },
                },
            ]
        )
        records = await cursor.to_list(None)
        return records

    async def all(self):
        records = await self.fetch({})

        return [Project(**r) for r in records]
