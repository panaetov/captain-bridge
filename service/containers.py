from dataclasses import dataclass
import urllib.parse
import logging

from service import db
from service.repositories import jira as jira_repositories
from service.repositories import metrics as metric_repositories
from service.repositories import plannings as planning_repositories
from service.repositories import gitlab as gitlab_repositories
from service.repositories import redmine as redmine_repositories
from service.services.dry_run_service import DryRunService
from service.services.jira_index_service import JiraIndexService
from service.services.gitlab_index_service import GitlabIndexService
from service.services.redmine_index_service import RedmineIndexService
from service.services.jira_service import JiraService
from service.services.redmine_service import RedmineService
from service.services.gitlab_service import GitlabService
from service.services.planning_actualizer_service import (
    PlanningActualizerService,
)
from service.services.run_service import RunService


logger = logging.getLogger(__name__)


@dataclass
class JiraRepositories:
    jiras: jira_repositories.JiraRepository
    sprints: jira_repositories.SprintRepository
    users: jira_repositories.UserRepository
    projects: jira_repositories.ProjectRepository
    issues: jira_repositories.IssueRepository


@dataclass
class GitlabRepositories:
    gitlabs: gitlab_repositories.GitlabRepository
    commits: gitlab_repositories.CommitRepository


@dataclass
class RedmineRepositories:
    redmines: redmine_repositories.RedmineRepository
    issues: redmine_repositories.IssueRepository


@dataclass
class MetricRepositories:
    dashboards: metric_repositories.DashboardRepository
    metrics: metric_repositories.MetricRepository


@dataclass
class PlanningRepositories:
    plannings: planning_repositories.PlanningRepository
    done_percents: planning_repositories.DonePercentRepository
    history: planning_repositories.HistoryRepository
    day_offs: planning_repositories.DayOffRepository


@dataclass
class Container:
    jira_repositories: JiraRepositories
    jira_index_service: JiraIndexService
    gitlab_index_service: GitlabIndexService
    redmine_index_service: RedmineIndexService
    metric_repositories: MetricRepositories
    gitlab_repositories: GitlabRepositories
    redmine_repositories: RedmineRepositories
    dry_run_service: DryRunService
    run_service: RunService
    planning_repositories: PlanningRepositories
    planning_actualizer: PlanningActualizerService


async def create(settings):
    gitlab_repositories_container = GitlabRepositories(
        gitlabs=gitlab_repositories.GitlabRepository(
            db=db.get_db(),
        ),
        commits=gitlab_repositories.CommitRepository(
            db=db.get_db(),
        ),
    )

    redmine_repositories_container = RedmineRepositories(
        redmines=redmine_repositories.RedmineRepository(
            db=db.get_db(),
        ),
        issues=redmine_repositories.IssueRepository(
            db=db.get_db(),
        ),
    )

    jira_repositories_container = JiraRepositories(
        jiras=jira_repositories.JiraRepository(
            db=db.get_db(),
        ),
        sprints=jira_repositories.SprintRepository(
            db=db.get_db(),
        ),
        projects=jira_repositories.ProjectRepository(
            db=db.get_db(),
        ),
        issues=jira_repositories.IssueRepository(
            db=db.get_db(),
        ),
        users=jira_repositories.UserRepository(
            db=db.get_db(),
        ),
    )

    jira_index_service = JiraIndexService(
        projects_repository=jira_repositories_container.projects,
        sprints_repository=jira_repositories_container.sprints,
        users_repository=jira_repositories_container.users,
        issues_repository=jira_repositories_container.issues,
        jira_service_factory=JiraService,
        jiras_repository=jira_repositories_container.jiras,
    )

    gitlab_index_service = GitlabIndexService(
        gitlab_service_factory=GitlabService,
        gitlab_repository=gitlab_repositories_container.gitlabs,
        commit_repository=gitlab_repositories_container.commits,
    )

    redmine_index_service = RedmineIndexService(
        redmine_service_factory=RedmineService,
        redmine_repository=redmine_repositories_container.redmines,
        issue_repository=redmine_repositories_container.issues,
    )

    metric_repositories_container = MetricRepositories(
        dashboards=metric_repositories.DashboardRepository(
            db=db.get_db(),
        ),
        metrics=metric_repositories.MetricRepository(
            db=db.get_db(),
        ),
    )

    planning_repositories_container = PlanningRepositories(
        plannings=planning_repositories.PlanningRepository(
            db=db.get_db(),
        ),
        done_percents=planning_repositories.DonePercentRepository(
            db=db.get_db(),
        ),
        history=planning_repositories.HistoryRepository(
            db=db.get_db(),
        ),
        day_offs=planning_repositories.DayOffRepository(
            db=db.get_db(),
        ),
    )

    logger.info(f"BASIC_URL = {settings.BASIC_URL}")
    worker_url = urllib.parse.urljoin(settings.BASIC_URL, '/metric/pipeline/run')
    logger.info(f"WORKER_URL = {worker_url}")
    run_service = RunService(
        db=db.get_db(),
        metrics_repository=metric_repositories_container.metrics,
        worker_url=worker_url,
    )

    planning_actualizer = PlanningActualizerService(
        plannings_repository=planning_repositories_container.plannings,
        done_percents_repository=planning_repositories_container.done_percents,
        issues_repository=jira_repositories_container.issues,
        run_service=run_service,
        history_repository=planning_repositories_container.history,
    )

    return Container(
        jira_repositories=jira_repositories_container,
        gitlab_repositories=gitlab_repositories_container,
        redmine_repositories=redmine_repositories_container,
        jira_index_service=jira_index_service,
        gitlab_index_service=gitlab_index_service,
        redmine_index_service=redmine_index_service,
        metric_repositories=metric_repositories_container,
        dry_run_service=DryRunService(
            db.get_db(),
        ),
        run_service=run_service,
        planning_repositories=planning_repositories_container,
        planning_actualizer=planning_actualizer,
    )
