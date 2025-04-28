import pymongo
import logging


logger = logging.getLogger(__name__)


async def apply_all(db):
    await apply_1(db)
    await apply_2(db)
    await apply_3(db)


async def create_collections(db, collections):
    for col in collections:
        try:
            logger.info(f"Creating collection {col}")
            await db.create_collection(col, check_exists=True)
        except pymongo.errors.CollectionInvalid as e:
            if "already exists" in str(e):
                continue
            else:
                raise e


async def create_indexes(db, indexes):
    for index in indexes:
        kwargs = {}
        if index.unique:
            kwargs["unique"] = True

        if index.ttl_secs:
            kwargs["expireAfterSeconds"] = index.ttl_secs

        logger.info(f"Creating index in {index.collection}: fields={index.fields}, options={kwargs}")
        await getattr(db, index.collection).create_index(
            index.fields,
            **kwargs,
        )


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

    await create_collections(db, collections)
    indexes = [
        # JIRA.JIRAS
        Index(
            "datasource_jira_jiras",
            ("internal_id", pymongo.ASCENDING),
            unique=True,
        ),
        # JIRA.ISSUES
        Index(
            "datasource_jira_issues",
            ("internal_id", pymongo.ASCENDING),
            unique=True,
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
        Index(
            "datasource_jira_issues",
            ("created", pymongo.ASCENDING),
        ),

        Index(
            "datasource_jira_issues",
            ("jira_internal_id", pymongo.ASCENDING),
            ("key", pymongo.ASCENDING),
            unique=True,
        ),

        # JIRA.ISSUES.PUBLIC
        Index(
            "datasource_jira_issues_public",
            ("internal_id", pymongo.ASCENDING),
            unique=True,
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
        Index(
            "datasource_jira_issues_public",
            ("jira_internal_id", pymongo.ASCENDING),
            ("key", pymongo.ASCENDING),
            unique=True,
        ),
        Index(
            "datasource_jira_issues_public",
            ("changes.created", pymongo.ASCENDING),
        ),
        Index(
            "datasource_jira_issues_public",
            ("comments.created", pymongo.ASCENDING),
        ),

        # JIRA.LOGS
        Index(
            "datasource_jira_logs",
            ("jira_internal_id", pymongo.ASCENDING),
        ),
        Index(
            "datasource_jira_logs",
            ("created_at", pymongo.ASCENDING),
            ttl_secs=600,
        ),
        # JIRA.PROJECTS
        Index(
            "datasource_jira_projects",
            ("jira_internal_id", pymongo.ASCENDING),
            ("key", pymongo.ASCENDING),
            unique=True,
        ),
        # JIRA.SPRINTS
        Index(
            "datasource_jira_sprints",
            ("jira_internal_id", pymongo.ASCENDING),
            ("sprint_id", pymongo.ASCENDING),
            unique=True,
        ),
        Index(
            "datasource_jira_sprints",
            ("internal_id", pymongo.ASCENDING),
            unique=True,
        ),
        Index(
            "datasource_jira_sprints",
            ("project", pymongo.ASCENDING),
        ),
        # JIRA.SPRINTS.PUBLIC
        Index(
            "datasource_jira_sprints_public",
            ("jira_internal_id", pymongo.ASCENDING),
            ("sprint_id", pymongo.ASCENDING),
            unique=True,
        ),
        Index(
            "datasource_jira_sprints_public",
            ("internal_id", pymongo.ASCENDING),
            unique=True,
        ),
        Index(
            "datasource_jira_sprints_public",
            ("project", pymongo.ASCENDING),
        ),
        # METRIC.DASHBOARDS
        Index(
            "metric_dashboards",
            ("internal_id", pymongo.ASCENDING),
            unique=True,
        ),
        # METRIC.METRICS
        Index(
            "metric_metrics",
            ("internal_id", pymongo.ASCENDING),
            unique=True,
        ),
        # PLANNING.PLANNINGS
        Index(
            "planning_plannings",
            ("internal_id", pymongo.ASCENDING),
            unique=True,
        ),
        # PLANNING.HISTORY
        Index(
            "planning_history",
            ("planning_internal_id", pymongo.ASCENDING),
            ("issue_internal_id", pymongo.ASCENDING),
            ("employee", pymongo.ASCENDING),
            ("date", pymongo.ASCENDING),
            unique=True,
        ),
        # PLANNING.DONE_PERCENTS
        Index(
            "planning_done_percents",
            ("planning_internal_id", pymongo.ASCENDING),
            ("issue_internal_id", pymongo.ASCENDING),
            unique=True,
        ),
        # PLANNING.DAY_OFFS
        Index(
            "planning_day_offs",
            ("internal_id", pymongo.ASCENDING),
            unique=True,
        ),
        Index(
            "planning_day_offs",
            ("planning_internal_id", pymongo.ASCENDING),
        ),
    ]
    await create_indexes(db, indexes)


async def apply_2(db):
    collections = [
        "datasource_gitlab_gitlabs",
        "datasource_gitlab_commits",
        "datasource_gitlab_commits_public",
        "datasource_gitlab_logs",
    ]
    await create_collections(db, collections)

    indexes = [
        Index(
            "datasource_gitlab_gitlabs",
            ("internal_id", pymongo.ASCENDING),
            unique=True,
        ),

        Index(
            "datasource_gitlab_commits",
            ("internal_id", pymongo.ASCENDING),
            unique=True,
        ),
        Index(
            "datasource_gitlab_commits",
            ("commit_id", pymongo.ASCENDING),
            ("gitlab_internal_id", pymongo.ASCENDING),
            unique=True,
        ),

        Index(
            "datasource_gitlab_commits_public",
            ("internal_id", pymongo.ASCENDING),
            unique=True,
        ),
        Index(
            "datasource_gitlab_commits_public",
            ("created", pymongo.ASCENDING),
        ),
        Index(
            "datasource_gitlab_commits_public",
            ("project_name", pymongo.ASCENDING),
        ),
        Index(
            "datasource_gitlab_commits_public",
            ("project_id", pymongo.ASCENDING),
        ),

        Index(
            "datasource_gitlab_commits_public",
            ("author", pymongo.ASCENDING),
        ),

        Index(
            "datasource_gitlab_logs",
            ("gitlab_internal_id", pymongo.ASCENDING),
        ),
        Index(
            "datasource_gitlab_logs",
            ("created_at", pymongo.ASCENDING),
            ttl_secs=600,
        ),
    ]
    await create_indexes(db, indexes)


async def apply_3(db):
    collections = [
        "datasource_redmine_redmines",
        "datasource_redmine_issues",
        "datasource_redmine_issues_public",
    ]
    await create_collections(db, collections)

    indexes = [
        Index(
            "datasource_redmine_redmines",
            ("internal_id", pymongo.ASCENDING),
            unique=True,
        ),

        Index(
            "datasource_redmine_issues",
            ("internal_id", pymongo.ASCENDING),
            unique=True,
        ),
        Index(
            "datasource_redmine_issues",
            ("created", pymongo.ASCENDING),
        ),
        Index(
            "datasource_redmine_issues",
            ("updated", pymongo.ASCENDING),
        ),
        Index(
            "datasource_redmine_issues",
            ("key", pymongo.ASCENDING),
            ("redmine_internal_id", pymongo.ASCENDING),
            unique=True,
        ),

        Index(
            "datasource_redmine_issues_public",
            ("internal_id", pymongo.ASCENDING),
            unique=True,
        ),
        Index(
            "datasource_redmine_issues_public",
            ("created", pymongo.ASCENDING),
        ),
        Index(
            "datasource_redmine_issues_public",
            ("updated", pymongo.ASCENDING),
        ),
        Index(
            "datasource_redmine_issues_public",
            ("project", pymongo.ASCENDING),
        ),
        Index(
            "datasource_redmine_issues_public",
            ("author", pymongo.ASCENDING),
        ),

        Index(
            "datasource_redmine_logs",
            ("redmine_internal_id", pymongo.ASCENDING),
        ),
        Index(
            "datasource_redmine_logs",
            ("created_at", pymongo.ASCENDING),
            ttl_secs=600,
        ),
    ]
    await create_indexes(db, indexes)


class Index:
    def __init__(self, collection, *fields, unique=False, ttl_secs=None):
        self.collection = collection
        self.fields = fields
        self.unique = unique
        self.ttl_secs = ttl_secs
