import pymongo


async def apply_all(db):
    await apply_1(db)


async def apply_1(db):
    collections = [
        "datasource_jira_jiras",
        "datasource_jira_issues",
        "datasource_jira_issues_public",
        "datasource_jira_logs",
        "datasource_jira_projects",
        "datasource_jira_sprints",
        "datasource_jira_sprints_public",
        "datasource_jira_users",
        "metric_dashboards",
        "metric_metrics",
        "planning_day_offs",
        "planning_done_percents",
        "planning_history",
        "planning_plannings",
        "planning_plannings",
    ]
    for col in collections:
        try:
            await db.create_collection(col, check_exists=True)
        except pymongo.errors.CollectionInvalid as e:
            if 'already exists' in str(e):
                continue
            else:
                raise e

    indexes = [
        # JIRA.JIRAS
        Index(
            "datasource_jira_jiras",
            ("internal_id", pymongo.ASCENDING),
            unique=True
        ),

        # JIRA.ISSUES
        Index(
            "datasource_jira_issues",
            ("internal_id", pymongo.ASCENDING),
            unique=True
        ),

        Index(
            "datasource_jira_issues",
            ("jira_internal_id", pymongo.ASCENDING),
        ),

        Index(
            "datasource_jira_issues",
            ("project", pymongo.ASCENDING),
        ),

        Index(
            "datasource_jira_issues",
            ("updated", pymongo.ASCENDING),
        ),

        Index(
            "datasource_jira_issues",
            ("created", pymongo.ASCENDING),
        ),

        # JIRA.ISSUES.PUBLIC
        Index(
            "datasource_jira_issues_public",
            ("internal_id", pymongo.ASCENDING),
            unique=True
        ),

        Index(
            "datasource_jira_issues_public",
            ("jira_internal_id", pymongo.ASCENDING),
        ),

        Index(
            "datasource_jira_issues_public",
            ("project", pymongo.ASCENDING),
        ),

        Index(
            "datasource_jira_issues_public",
            ("updated", pymongo.ASCENDING),
        ),

        Index(
            "datasource_jira_issues_public",
            ("created", pymongo.ASCENDING),
        ),

    ]

    for index in indexes:
        await getattr(db, index.collection).create_index(
            index.fields, unique=index.unique,
        )


class Index:
    def __init__(self, collection, *fields, unique=False):
        self.collection = collection
        self.fields = fields
        self.unique = unique
