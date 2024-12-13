import datetime
import logging
import re
import uuid
from typing import Dict, List, Literal, Union

import aiohttp
import pydantic
from pytimeparse import parse as parse_interval

from service.db import get_collection_class
from service.entities.common import generate_internal_id
from service.utils import serialize_json

logger = logging.getLogger(__name__)


class ActionQuery(pydantic.BaseModel):
    action_type: Literal["query"]
    pipeline: List


class ActionRPC(pydantic.BaseModel):
    action_type: Literal["rpc"]
    url: str
    secure: bool = False


class ActionPythonCode(pydantic.BaseModel):
    action_type: Literal["python"]
    code: str


class InputDatabase(pydantic.BaseModel):
    input_type: Literal["db"]
    alias: str

    collection_name: str


class InputStage(pydantic.BaseModel):
    input_type: Literal["stage"]
    alias: str

    stage_name: str


class Stage(pydantic.BaseModel):
    name: str
    is_terminal: bool

    inputs: List[Union[InputStage, InputDatabase]]
    action: Union[ActionQuery, ActionRPC, ActionPythonCode]


class Pipeline:
    def __init__(self, db, stages: List[Stage], scope: Dict):
        self.db = db
        self.stages = stages
        self.scope = scope
        self.logger = Logger()

    async def run(self):
        for stage in self.stages:
            if stage.is_terminal:
                return await self.run_stage(stage)

        raise RuntimeError("No terminal stages")

    async def run_stage(self, stage):
        self.logger(f"Running stage {stage.name}...")
        values = {}
        for input_ in stage.inputs:
            if input_.input_type == "db":
                value = self._get_collection(input_.collection_name)
                values[input_.alias] = value

            elif input_.input_type == "stage":
                for substage in self.stages:
                    if substage.name == input_.stage_name:
                        value = await self.run_stage(substage)
                        values[input_.alias] = value
            else:
                raise RuntimeError(f"Unknown input type {input_.input_type}")

        result = await self.apply_action(stage.action, values)
        self.logger(f"Stage {stage.name} finished.")
        return result

    async def apply_action(self, action, input_values):
        if action.action_type == "query":
            return await self._apply_action_query(action, input_values)

        elif action.action_type == "rpc":
            return await self._apply_action_rpc(action, input_values)

        elif action.action_type == "python":
            return await self._apply_action_python(action, input_values)

        else:
            raise RuntimeError(f"Unknown action type `{action.action_type}`")

    async def _apply_action_python(self, action, input_values):
        return eval_function(
            action.code,
            {
                "INPUTS": input_values,
                "SCOPE": self.scope,
            },
            self.logger,
        )

    def _build_action_query_pipeline(self, pipeline: str, scope: Dict | None):
        pipeline = self._replace_placeholders(pipeline, scope)
        return pipeline

    def _replace_placeholders(self, obj, scope):
        if isinstance(obj, list):
            new_obj = []
            for el in obj:
                new_obj.append(self._replace_placeholders(el, scope))

            return new_obj

        elif isinstance(obj, dict):
            new_obj = {}

            for k, v in obj.items():
                new_obj[k] = self._replace_placeholders(v, scope)

            return new_obj

        elif isinstance(obj, str):
            if "$$" in obj:
                return self.evaluate_parametrized_value(obj, scope)

            return obj

        else:
            return obj

    def evaluate_parametrized_value(self, tag, scope):
        params = re.findall("\$\$[a-zA-Z0-9_]+", tag)
        for param in params:
            value = scope[param[2:]]
            if isinstance(value, str):
                tag = tag.replace(param, value)

        if "$$" not in tag:
            return tag

        addend, subtrahend = 0, 0

        if "+" in tag:
            tag, addend = [t.strip() for t in tag.split("+")]
        elif "-" in tag:
            tag, subtrahend = [t.strip() for t in tag.split("-")]

        addend = self.evaluate_delta_tag(addend)
        subtrahend = self.evaluate_delta_tag(subtrahend)

        value = scope[tag[2:]]

        if addend:
            value += addend

        if subtrahend:
            value -= subtrahend
        return value

    def evaluate_delta_tag(self, tag):
        if not tag:
            return datetime.timedelta(seconds=0)

        return datetime.timedelta(seconds=parse_interval(tag))

    async def _apply_action_query(self, action, input_values):
        input_value = list(input_values.values())[0]

        mongo_pipeline = self._build_action_query_pipeline(
            action.pipeline,
            self.scope,
        )
        self.logger(f"Query: {mongo_pipeline}.")
        if self._is_collection(input_value):
            collection = input_value

            cursor = collection.aggregate(mongo_pipeline)
            return await cursor.to_list(None)

        elif self._is_documents_list(input_value):
            if not input_value:
                return []

            temp_name = f"temp_pipeline_{uuid.uuid4().hex}"
            collection = self._get_collection(temp_name)
            await collection.insert_many(input_value)

            cursor = collection.aggregate(mongo_pipeline)
            output = await cursor.to_list(None)
            self.db.drop_collection(collection)
            return output

    async def _apply_action_rpc(self, action, input_values):
        json_data = {
            "inputs": input_values,
            "variables": self.scope,
            "datetime_from": self.scope["datetime_from"],
            "datetime_to": self.scope["datetime_to"],
        }
        async with aiohttp.ClientSession(
            raise_for_status=False,
            json_serialize=serialize_json,
        ) as session:
            async with session.post(
                action.url,
                json=json_data,
                ssl=action.secure,
            ) as resp:
                if resp.status >= 400:
                    text = await resp.text()
                    logger.exception(
                        f"Cannot calculate metric {action.url}, reason={text}"
                    )
                    raise RuntimeError(
                        {
                            "status": resp.status,
                            "reason": text,
                        }
                    )

                else:
                    return await resp.json()

    def _get_collection(self, collection_name):
        return self.db[collection_name]

    def _is_collection(self, obj):
        return isinstance(obj, get_collection_class())

    def _is_documents_list(self, obj):
        return isinstance(obj, (list, tuple))


class Metric(pydantic.BaseModel):
    internal_id: str = pydantic.Field(default_factory=generate_internal_id)
    name: str

    stages: List[Stage]
    variables: Dict

    async def compute(self, datetime_from, datetime_to, scope, db):
        scope["datetime_from"] = datetime_from
        scope["datetime_to"] = datetime_to

        pipeline = Pipeline(
            stages=self.stages,
            db=db,
            scope=scope,
        )
        result = await pipeline.run()
        return result, pipeline.logger.messages


class Dashboard(pydantic.BaseModel):
    internal_id: str = pydantic.Field(default_factory=generate_internal_id)

    name: str
    metrics: List[str] = []
    variables: List = []

    metric_types: Dict[str, str] = {}


class Logger:
    def __init__(self):
        self.messages = []

    def __call__(self, *messages):
        self.messages.append(
            {
                "created_at": datetime.datetime.utcnow(),
                "message": "\n".join(map(str, messages)),
            }
        )


def eval_function(code, kwargs=None, logger=Logger()):
    kwargs = kwargs or {}

    retval = None
    context = {
        "print": logger,
    }
    # добавляем отступ в 4 пробела в начало каждой строки
    code = re.sub(r"(?m)^", "    ", code)
    code = "def function(" + ",".join(kwargs.keys()) + "):\n" + code

    try:
        exec(code, context)
        retval = context["function"](*(kwargs.values()))
    except Exception as e:
        import traceback

        tb = "<p>".join(traceback.format_exception(e)[-2:])
        raise RuntimeError(f"<p>{tb}")

    return retval
