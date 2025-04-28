import logging

from service.entities.gitlab import (
    Gitlab, Commit,
)
from service.repositories.common import Repository


logger = logging.getLogger(__name__)


class GitlabRepository(Repository):
    table = "datasource_gitlab_gitlabs"
    logs_table = "datasource_gitlab_logs"
    default_entity_factory = Gitlab

    def to_record_fields(self, entity_or_dict, updated_fields=None):
        fields = super().to_record_fields(entity_or_dict, updated_fields)

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
                gitlab_internal_id=internal_id,
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
                    "from": "datasource_gitlab_logs",
                    "let": {
                        "gitlab_internal_id": "$internal_id",
                    },
                    "pipeline": [
                        {
                            "$match": {
                                "$expr": {
                                    "$eq": [
                                        "$gitlab_internal_id",
                                        "$$gitlab_internal_id",
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


class CommitRepository(Repository):
    table = "datasource_gitlab_commits"
    default_entity_factory = Commit
    public_table = "datasource_gitlab_commits_public"

    def to_record_fields_from_dict(self, data, updated_fields=None):
        data = super().to_record_fields_from_dict(data, updated_fields)

        gitlab = data.pop("gitlab")
        data["gitlab_internal_id"] = gitlab["internal_id"]

        return data

    def get_unique_key(self, commit: Commit):
        return {
            "gitlab_internal_id": commit.gitlab.internal_id,
            "commit_id": commit.commit_id,
        }

    def build_fetch_pipeline(self, match, limit=None):
        pipeline = [
            {
                "$match": match,
            },
            {
                "$lookup": {
                    "from": "datasource_gitlab_gitlabs",
                    "localField": "gitlab_internal_id",
                    "foreignField": "internal_id",
                    "as": "gitlab",
                },
            },
            {
                "$unwind": {
                    "path": "$gitlab",
                    "preserveNullAndEmptyArrays": True,
                },
            },
        ]

        return pipeline

    async def refresh_presentation_view(self):
        cursor = self.get_table().aggregate(
            [
                {
                    "$lookup": {
                        "from": "datasource_gitlab_commits",
                        "localField": "gitlab_internal_id",
                        "foreignField": "internal_id",
                        "as": "gitlab",
                    },
                },
                {
                    "$unwind": {
                        "path": "$gitlab",
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
