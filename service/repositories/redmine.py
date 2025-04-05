import logging

from service.entities.redmine import (
    Issue,
    Redmine,
)
from service.repositories.common import Repository


logger = logging.getLogger(__name__)


class RedmineRepository(Repository):
    table = "datasource_redmine_redmines"
    logs_table = "datasource_redmine_logs"
    default_entity_factory = Redmine

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
                redmine_internal_id=internal_id,
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
                    "from": "datasource_redmine_logs",
                    "let": {
                        "redmine_internal_id": "$internal_id",
                    },
                    "pipeline": [
                        {
                            "$match": {
                                "$expr": {
                                    "$eq": [
                                        "$redmine_internal_id",
                                        "$$redmine_internal_id",
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


class IssueRepository(Repository):
    table = "datasource_redmine_issues"
    public_table = "datasource_redmine_issues_public"
    default_entity_factory = Issue

    def get_public_table(self):
        return getattr(self.db, self.public_table)

    async def fetch_public(self, match, limit=None):
        cursor = self.get_public_table().aggregate(
            self.build_fetch_pipeline(match, limit)
        )

        records = await cursor.to_list(None)
        return records

    def get_unique_key(self, issue: Issue):
        return {
            "redmine_internal_id": issue.redmine.internal_id,
            "id": issue.id,
        }

    async def get_max_updated(self, project_key):
        cursor = self.get_table().aggregate(
            [
                {
                    "$match": {
                        "project": project_key,
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
        return fields

    def to_record_fields_from_dict(self, data, updated_fields=None):
        data = super().to_record_fields_from_dict(data, updated_fields)

        redmine = data.pop("redmine")
        data["redmine_internal_id"] = redmine["internal_id"]

        return data

    async def refresh_presentation_view(self):
        pipeline = [
            {
                "$lookup": {
                    "from": "datasource_redmine_redmines",
                    "localField": "redmine_internal_id",
                    "foreignField": "internal_id",
                    "as": "redmine",
                },
            },
            {
                "$unwind": {
                    "path": "$redmine",
                    "preserveNullAndEmptyArrays": True,
                },
            },
            {
                "$unset": ["redmine.password"],
            },
            {
                "$unset": ["redmine.token"],
            },
        ]

        pipeline.extend(
            [
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
