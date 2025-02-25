import datetime
import json
import logging
from typing import Annotated, Dict, List

import pydantic
from fastapi import (
    APIRouter,
    Depends,
    HTTPException,
    Request,
    Response,
    WebSocket,
)
from fastapi.templating import Jinja2Templates
from pytimeparse import parse as parse_interval

from service import utils
from service.entities.jira import Jira, Project
from service.entities.metric import Dashboard, Metric, Stage
from service.entities.planning import DonePercent, Planning
from service.repositories import jira as jira_repositories
from service.repositories.metrics import DashboardRepository, MetricRepository
from service.repositories.plannings import (
    DayOffRepository,
    DonePercentRepository,
    HistoryRepository,
    PlanningRepository,
)
from service.services.dry_run_service import DryRunService
from service.services.jira_index_service import JiraIndexService
from service.services.run_service import RunService

logger = logging.getLogger(__name__)


router = APIRouter()


@router.get("/")
async def index_handler(
        request: Request,
):
    templates = Jinja2Templates(directory="front")
    context = {
        "backend_host": '',
        "ws_backend_host": '',
        "request": request,
    }
    return templates.TemplateResponse(
        "index.html",
        context=context,
    )


def _get_dashboard_repository(request: Request):
    return request.app.container.metric_repositories.dashboards


def _dump_dashboard(dashboard: Dashboard):
    return {
        "internal_id": dashboard.internal_id,
        "name": dashboard.name,
        "metrics": dashboard.metrics,
        "metric_types": dashboard.metric_types,
        "variables": dashboard.variables,
    }


@router.get("/metric/dashboards")
async def get_dashboards_handler(
    dashboard_repository: Annotated[
        DashboardRepository, Depends(_get_dashboard_repository)
    ],
    root: str = "",
):
    dashboards = await dashboard_repository.filter_by_catalog(root)

    grouped = set([])
    orphans = []

    for d in dashboards:
        name = d.name

        if root:
            name = name[len(f"{root}"):]

        path = name.split("/")
        if len(path) == 1 or not path[0] or not path[-1]:
            orphans.append(_dump_dashboard(d))

        else:
            if root:
                grouped.add(f"{root}{path[0]}/")

            else:
                grouped.add(path[0] + "/")

    if root:
        # root = cosmos/1/2/
        # root.strip(/) = cosmos/1/2
        # root.strip(/).split(/) = [cosmos, 1, 2]
        # super_root = cosmos/1/
        super_root = "/".join(root.strip("/").split("/")[:-1])
        if super_root:
            super_root += "/"

    else:
        super_root = None

    logger.info(f"Super root = {super_root}")
    return {
        "grouped": list(grouped),
        "orphans": orphans,
        "super_root": super_root,
    }


def _check_ro(request: Request):
    if request.app.settings.RO:
        raise HTTPException(status_code=403, detail="Read-only mode. Changes are not allowed.")


@router.post(
    "/metric/dashboards", dependencies=[Depends(_check_ro)],
)
async def save_dashboard_handler(
    dashboard: Dashboard,
    dashboard_repository: Annotated[
        DashboardRepository, Depends(_get_dashboard_repository)
    ],
):
    data = dashboard.model_dump()
    if not data["internal_id"]:
        data.pop("internal_id")

    dashboard = Dashboard(**data)
    await dashboard_repository.save(dashboard)
    return {
        "internal_id": dashboard.internal_id,
    }


def _get_metric_repository(request: Request):
    return request.app.container.metric_repositories.metrics


@router.get("/metric/dashboards/{internal_id}")
async def get_one_dashboard_handler(
    dashboard_repository: Annotated[
        DashboardRepository, Depends(_get_dashboard_repository)
    ],
    metric_repository: Annotated[
        MetricRepository, Depends(_get_metric_repository)
    ],
    internal_id: str,
):
    dashboards = await dashboard_repository.filter_by_internal_id(
        [
            internal_id,
        ]
    )

    if not dashboards:
        raise HTTPException(status_code=404)

    dashboard = dashboards[0]
    metrics = await metric_repository.filter_by_internal_id(dashboard.metrics)
    dashboard.metrics = [m.internal_id for m in metrics]
    return _dump_dashboard(dashboards[0])


@router.delete("/metric/dashboards/{internal_id}")
async def delete_dashboard_handler(
    dashboard_repository: Annotated[
        DashboardRepository, Depends(_get_dashboard_repository)
    ],
    internal_id: str,
):
    await dashboard_repository.delete_by_internal_id(internal_id)
    return {
        "internal_id": internal_id,
    }


def _dump_metric(metric: Metric):
    return {
        "internal_id": metric.internal_id,
        "name": metric.name,
        "pipeline": metric.stages,
        "variables": metric.variables,
    }


def _dump_metric_option(metric):
    return {
        "id": metric.internal_id,
        "text": metric.name,
    }


@router.get("/metric/options")
async def get_metric_options_handler(
    metric_repository: Annotated[
        MetricRepository, Depends(_get_metric_repository)
    ],
    term: str = "",
):
    metrics = await metric_repository.filter_by_name(term)

    result = []
    for d in sorted(metrics, key=lambda m: m.name):
        result.append(_dump_metric_option(d))

    return {"results": result}


@router.get("/metric/metrics")
async def get_metrics_handler(
    metric_repository: Annotated[
        MetricRepository, Depends(_get_metric_repository)
    ],
    root: str = "",
    mode: str = "tree",
):
    metrics = await metric_repository.filter_by_catalog(root)

    if mode == "flat":
        result = []
        for d in sorted(metrics, key=lambda m: m.name):
            result.append(_dump_metric(d))

        return result

    grouped = set([])
    orphans = []

    for d in metrics:
        name = d.name

        if root:
            name = name[len(f"{root}"):]

        path = name.split("/")
        if len(path) == 1 or not path[0] or not path[-1]:
            orphans.append(_dump_metric(d))

        else:
            if root:
                grouped.add(f"{root}{path[0]}/")

            else:
                grouped.add(path[0] + "/")

    if root:
        # root = cosmos/1/2/
        # root.strip(/) = cosmos/1/2
        # root.strip(/).split(/) = [cosmos, 1, 2]
        # super_root = cosmos/1/
        super_root = "/".join(root.strip("/").split("/")[:-1])
        if super_root:
            super_root += "/"
    else:
        super_root = None

    logger.info(f"SUPPER ROOT: {super_root}")
    return {
        "grouped": list(grouped),
        "orphans": orphans,
        "super_root": super_root,
    }


@router.delete("/metric/metrics/{internal_id}")
async def delete_metrics_handler(
    internal_id: str,
    metric_repository: Annotated[
        MetricRepository, Depends(_get_metric_repository)
    ],
):
    await metric_repository.delete_by_internal_id(internal_id)
    return {
        "internal_id": internal_id,
    }


@router.post("/metric/metrics", dependencies=[Depends(_check_ro)])
async def save_metrics_handler(
    metric: Metric,
    metric_repository: Annotated[
        MetricRepository, Depends(_get_metric_repository)
    ],
):
    data = metric.model_dump()
    if not data["internal_id"]:
        data.pop("internal_id")

    metric = Metric(**data)
    await metric_repository.save(metric)
    return {
        "internal_id": metric.internal_id,
    }


@router.get("/metric/metrics/{internal_id}")
async def get_one_metric_handler(
    internal_id,
    metric_repository: Annotated[
        MetricRepository, Depends(_get_metric_repository)
    ],
):
    metrics = await metric_repository.filter_by_internal_id([internal_id])
    if not metrics:
        raise HTTPException(status_code=404)

    return _dump_metric(metrics[0])


class DryRunRequest(pydantic.BaseModel):
    stages: List[Stage]
    stage_name: str
    datetime_from: datetime.datetime
    datetime_to: datetime.datetime
    variables: Dict = {}


def _get_dry_run_service(request: Request):
    return request.app.container.dry_run_service


@router.post("/metric/pipeline/dry-run")
async def dry_run_metric_handler(
    body: DryRunRequest,
    dry_run_service: Annotated[DryRunService, Depends(_get_dry_run_service)],
):
    try:
        result = await dry_run_service.compute(
            body.stage_name,
            body.stages,
            body.datetime_from,
            body.datetime_to,
            body.variables,
        )
    except Exception as e:
        if isinstance(e, KeyError):
            error = f"Variable is not set: {e}"
        else:
            error = str(e)

        logger.exception("Cannot run metric")
        result = {
            "error": error,
        }

    response = Response(
        content=json.dumps(result, default=str),
        media_type="application/json",
    )
    return response


class RunRequest(pydantic.BaseModel):
    internal_id: str
    datetime_from: datetime.datetime
    datetime_to: datetime.datetime
    period: str = ""
    metric_type: str = "table"
    period_secs: int = 0
    variables: Dict = {}


def _get_run_service(request: Request):
    return request.app.container.run_service


@router.post("/metric/pipeline/run")
async def run_metric_handler(
    body: RunRequest,
    run_service: Annotated[RunService, Depends(_get_run_service)],
):
    if body.period:
        period_secs = parse_interval(body.period)
    else:
        period_secs = body.period_secs

    assert period_secs

    try:
        data = await run_service.compute(
            body.internal_id,
            body.datetime_from,
            body.datetime_to,
            period_secs,
            body.metric_type,
            body.variables,
        )
    except Exception as e:
        logger.exception(f"Cannot run metric {body.internal_id}")
        payload = {"error": str(e)}

    else:
        payload = {
            "data": data,
        }

    response = Response(
        content=json.dumps(payload, default=str),
        media_type="application/json",
    )
    return response


def _get_jira_repository(request: Request):
    return request.app.container.jira_repositories.jiras


def _get_project_repository(request: Request):
    return request.app.container.jira_repositories.projects


PASSWORD_NOT_CHANGED = "*********"


def _dump_jira(jira: Jira, projects=None):
    projects = projects or []

    return {
        "internal_id": jira.internal_id,
        "name": jira.name,
        "url": jira.url,
        "login": jira.login,
        "password": "*********",
        "projects": [p.key for p in projects],
        "indexed_at": jira.indexed_at,
        "index_period": jira.index_period,
        "logs": jira.logs,
        "status": jira.status,
        "custom_fields": jira.custom_fields,
        "auth_method": jira.auth_method,
        "token": jira.token,
    }


@router.get("/sources/jiras")
async def get_jiras_handler(
    jira_repository: Annotated[
        jira_repositories.JiraRepository, Depends(_get_jira_repository)
    ],
):
    jiras = await jira_repository.all()

    return [_dump_jira(d) for d in jiras]


@router.get("/sources/jiras/{internal_id}")
async def get_one_jira_handler(
    jira_repository: Annotated[
        jira_repositories.JiraRepository, Depends(_get_jira_repository)
    ],
    project_repository: Annotated[
        jira_repositories.ProjectRepository, Depends(_get_project_repository)
    ],
    internal_id: str,
):
    jiras = await jira_repository.filter_by_internal_id(
        [
            internal_id,
        ]
    )

    if not jiras:
        raise HTTPException(status_code=404)

    jira = jiras[0]
    projects = await project_repository.fetch_by_jira(jira.internal_id)

    return _dump_jira(jiras[0], projects)


@router.delete("/sources/jiras/{internal_id}")
async def delete_jira_handler(
    jira_repository: Annotated[
        jira_repositories.JiraRepository, Depends(_get_jira_repository)
    ],
    internal_id: str,
):
    await jira_repository.delete_by_internal_id(internal_id)
    return {
        "internal_id": internal_id,
    }


class SaveJiraRequest(pydantic.BaseModel):
    internal_id: str | None = None
    name: str
    url: str
    login: str
    password: str
    projects: List[str]
    custom_fields: List[Dict]
    auth_method: str
    token: str


@router.post("/sources/jiras", dependencies=[Depends(_check_ro)])
async def save_jiras_handler(
    body: SaveJiraRequest,
    jira_repository: Annotated[
        jira_repositories.JiraRepository, Depends(_get_jira_repository)
    ],
    project_repository: Annotated[
        jira_repositories.ProjectRepository, Depends(_get_project_repository)
    ],
):
    data = dict(
        internal_id=body.internal_id,
        name=body.name,
        url=body.url,
        login=body.login,
        password=body.password if body.password != PASSWORD_NOT_CHANGED else "",
        custom_fields=body.custom_fields,
        auth_method=body.auth_method,
        token=body.token,
    )
    if not data["internal_id"]:
        data.pop("internal_id")

    jira = Jira(**data)

    await jira_repository.save(jira)

    for project_key in body.projects:
        project = Project(
            key=project_key,
            jira=jira,
            name=project_key,
        )
        await project_repository.save(project)

    projects = await project_repository.fetch_by_jira(jira.internal_id)

    for project in projects:
        if project.key not in body.projects:
            await project_repository.delete_by_key(project.key)

    return {
        "internal_id": jira.internal_id,
    }


def _get_jira_index_service(websocket: WebSocket):
    return websocket.app.container.jira_index_service


@router.websocket("/sources/jiras/indexify")
async def websocket_endpoint(
    websocket: WebSocket,
    index_service: Annotated[
        JiraIndexService, Depends(_get_jira_index_service)
    ],
):
    await websocket.accept()
    while True:
        message = await websocket.receive_text()
        jira_internal_id, mode = message.split("_")

        async def report_results(message, level):
            await websocket.send_text(
                utils.serialize_json(
                    {
                        "created_at": datetime.datetime.utcnow(),
                        "level": level,
                        "message": message,
                    }
                )
            )

        await index_service.indexify(
            jira_internal_id=jira_internal_id,
            watcher_callback=report_results,
            full=mode == "full",
        )
        await websocket.close()
        break


def _get_issue_repository(request: Request):
    return request.app.container.jira_repositories.issues


def _dump_issue_option(issue):
    return {
        "id": issue.internal_id,
        "text": f"{issue.key} <{issue.jira.name}> {issue.summary}",
        "issue": issue,
    }


@router.get("/sources/issues/options")
async def options_issues_handler(
    issue_repository: Annotated[
        jira_repositories.IssueRepository, Depends(_get_issue_repository)
    ],
    term: str = "",
):
    issues = await issue_repository.filter_by_name(term)

    return {"results": [_dump_issue_option(issue) for issue in issues]}


def get_month_name(num):
    return {
        1: "Jan",
        2: "Feb",
        3: "March",
        4: "April",
        5: "May",
        6: "June",
        7: "July",
        8: "August",
        9: "Sep",
        10: "Oct",
        11: "Nov",
        12: "Dec",
    }[num]


def _get_history_repository(request: Request):
    return request.app.container.planning_repositories.history


def _get_planning_repository(request: Request):
    return request.app.container.planning_repositories.plannings


def _get_done_percent_repository(request: Request):
    return request.app.container.planning_repositories.done_percents


class PlanningContract(pydantic.BaseModel):
    internal_id: str
    name: str
    velocity_metric_internal_id: str
    velocity_period: str
    issue_size_field: str
    default_issue_size: float
    terminal_states: List[str]
    assigned_issues: Dict
    day_offs: List[Dict] = []


DonePercentContract = DonePercent


@router.post("/planning/done_percents", dependencies=[Depends(_check_ro)])
async def save_done_percents_handler(
    done_percents: List[DonePercentContract],
    done_percent_repository: Annotated[
        DonePercentRepository, Depends(_get_done_percent_repository)
    ],
):
    await done_percent_repository.save_many(
        done_percents,
        updated_fields=["value", "changed_at", "overdue"],
    )
    return {}


@router.post("/planning/plannings", dependencies=[Depends(_check_ro)])
async def save_planning_handler(
    planning: PlanningContract,
    planning_repository: Annotated[
        PlanningRepository, Depends(_get_planning_repository)
    ],
    metric_repository: Annotated[
        MetricRepository, Depends(_get_metric_repository)
    ],
):
    data = planning.model_dump()
    if not data["internal_id"]:
        data.pop("internal_id")

    metric_id = data["velocity_metric_internal_id"]

    metrics = await metric_repository.filter_by_internal_id([metric_id])
    if not metrics:
        raise HTTPException(status_code=400)

    planning = Planning(velocity_metric=metrics[0], **data)
    await planning_repository.save(planning)

    return {
        "internal_id": planning.internal_id,
    }


def _dump_planning(p, issues, done_percents):
    id__percent = {p.issue_internal_id: p.value for p in done_percents}
    id__overdue = {p.issue_internal_id: p.overdue for p in done_percents}
    id__issue = {i.internal_id: i for i in issues}

    data = p.model_dump()

    for _, issues in data["assigned_issues"].items():
        for issue in issues:
            internal_id = issue["internal_id"]
            issue["issue"] = id__issue.get(internal_id, {})
            issue["done_percent"] = id__percent.get(internal_id, 0)
            issue["overdue"] = id__overdue.get(internal_id, 0)

    return data


@router.get("/planning/done_percent")
async def get_done_percent_handler(
    done_percent_repository: Annotated[
        DonePercentRepository, Depends(_get_done_percent_repository)
    ],
    planning_internal_id: str,
    issue_internal_id: str,
):
    p = await done_percent_repository.get_for_issue(
        planning_internal_id,
        issue_internal_id,
    )

    return {
        "done_percent": p.value if p else 0,
        "overdue": p.overdue if p else 0,
    }


@router.get("/planning/plannings")
async def get_plannings_handler(
    planning_repository: Annotated[
        PlanningRepository, Depends(_get_planning_repository)
    ],
):
    ps = await planning_repository.all()

    return [p.model_dump() for p in ps]


@router.delete("/planning/plannings/{internal_id}")
async def delete_planning_handler(
    internal_id: str,
    planning_repository: Annotated[
        PlanningRepository, Depends(_get_planning_repository)
    ],
):
    await planning_repository.delete_by_internal_id(internal_id)
    return {
        "internal_id": internal_id,
    }


@router.get("/planning/plannings/{internal_id}")
async def get_one_planning_handler(
    planning_repository: Annotated[
        PlanningRepository, Depends(_get_planning_repository)
    ],
    done_percent_repository: Annotated[
        DonePercentRepository, Depends(_get_done_percent_repository)
    ],
    issue_repository: Annotated[
        jira_repositories.IssueRepository, Depends(_get_issue_repository)
    ],
    internal_id: str,
):
    ps = await planning_repository.filter_by_internal_id(
        [
            internal_id,
        ]
    )

    if not ps:
        logger.info(f"No plannings with internal id = {internal_id}")
        raise HTTPException(status_code=404)

    p = ps[0]
    keys = set([])
    for emp, issues in p.assigned_issues.items():
        for issue in issues:
            keys.add(issue["internal_id"])

    keys = list(keys)

    issues = await issue_repository.filter_by_internal_id(list(keys))

    done_percents = await done_percent_repository.filter_by_planning(
        p.internal_id
    )
    return {
        "planning": _dump_planning(p, issues, done_percents),
        "employees": [],
    }


@router.get("/planning/history/{planning_internal_id}")
async def get_planning_history_handler(
    history_repository: Annotated[
        HistoryRepository, Depends(_get_history_repository)
    ],
    issue_repository: Annotated[
        jira_repositories.IssueRepository, Depends(_get_issue_repository)
    ],
    planning_internal_id: str,
    datetime_from: datetime.datetime,
    datetime_to: datetime.datetime,
):
    histories = await history_repository.filter_by_dates(
        planning_internal_id,
        datetime_from,
        datetime_to,
    )

    struct = {}
    for h in histories:
        struct.setdefault(h.employee, {})
        struct[h.employee].setdefault(h.date, []).append(h.issue_internal_id)

    ids = set([])
    for h in histories:
        ids.add(h.issue_internal_id)

    all_issues = await issue_repository.filter_by_internal_id(list(ids))
    id__issue = {i.internal_id: i for i in all_issues}

    result = {}
    d = datetime_from
    while d < datetime_to:
        for employee, day__ids in struct.items():
            ids = day__ids.get(d, [])

            issues = [id__issue[id] for id in ids if id in id__issue]

            result.setdefault(employee, [])
            result[employee].append(
                [
                    {
                        "key": i.key,
                        "internal_id": i.internal_id,
                        "summary": i.summary,
                    }
                    for i in issues
                ]
            )

        d += datetime.timedelta(days=1)

    return result


def _get_dayoff_repository(request: Request):
    return request.app.container.planning_repositories.day_offs


@router.get("/planning/dayoffs/{planning_internal_id}")
async def get_planning_dayoffs_handler(
    dayoff_repository: Annotated[
        DayOffRepository, Depends(_get_dayoff_repository)
    ],
    planning_internal_id: str,
    datetime_from: datetime.datetime,
    datetime_to: datetime.datetime,
):
    dayoffs = await dayoff_repository.filter_by_date_from(
        planning_internal_id,
        datetime_from,
        datetime_to,
    )

    flat = []
    employee__days = {}

    for dayoff in dayoffs:
        employee__days.setdefault(
            dayoff.employee,
            {
                "days": [],
                "slots": [],
            },
        )

        employee__days[dayoff.employee]["slots"].append(
            {
                "date_from": dayoff.date_from,
                "date_to": dayoff.date_to,
            }
        )
        flat.append(
            {
                "employee": dayoff.employee,
                "date_from": dayoff.date_from,
                "date_to": dayoff.date_to,
            }
        )

    flat.sort(key=lambda r: r["date_from"])

    for days in employee__days.values():
        days["slots"].sort(key=lambda r: r["date_from"])

    return {
        "flat": flat,
    }
    return employee__days


@router.get("/planning/calendar")
async def planing_calendar_handler(
    base_dt: datetime.date,
    days_qty: int,
    planning_repository: Annotated[
        PlanningRepository, Depends(_get_planning_repository)
    ],
    planning_internal_id: str = "",
):
    if planning_internal_id:
        ps = await planning_repository.filter_by_internal_id(
            [
                planning_internal_id,
            ]
        )

        if not ps:
            logger.info(
                f"No plannings with internal id = {planning_internal_id}"
            )
            raise HTTPException(status_code=404)

        day_offs = ps[0].day_offs
    else:
        day_offs = []

    months = []
    days = []

    if days_qty > 0:
        day_nums = list(range(0, days_qty + 1))
    else:
        day_nums = list(range(days_qty, 0))

    cur_month = None
    today = datetime.datetime.utcnow()
    for day_num in day_nums:
        dt = base_dt + datetime.timedelta(days=day_num)
        employees_off = []

        for day_off in day_offs:
            if day_off.date_from <= dt <= day_off.date_to:
                employees_off.append(day_off.employee)

        month = dt.month

        if cur_month != month:
            months.append(
                {
                    "id": month,
                    "name": get_month_name(month),
                    "length": 0,
                }
            )
            cur_month = month

        months[-1]["length"] += 1

        days.append(
            {
                "id": dt.day,
                "weekday": dt.weekday(),
                "is_today": dt == today.date(),
                "employees_off": employees_off,
                "date": dt,
            }
        )

    return {
        "months": months,
        "days": days,
    }
