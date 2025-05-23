import asyncio
import datetime
import json
import logging

import bson
import pydantic
import pytz


logger = logging.getLogger(__name__)


class SexyEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime.datetime):
            return obj.strftime("%Y-%m-%d %H:%M:%S")

        elif isinstance(obj, pydantic.BaseModel):
            return obj.model_dump(by_alias=True)

        elif isinstance(obj, bson.ObjectId):
            return str(obj)

        else:
            return super().default(obj)


def serialize_json(obj):
    return json.dumps(obj, cls=SexyEncoder)


def shift_timezone(
    naive_datetime: datetime.datetime, from_tz_name: str, to_tz_name: str
):
    from_tz = pytz.timezone(from_tz_name)
    to_tz = pytz.timezone(to_tz_name)

    return (
        from_tz.localize(naive_datetime)
        .astimezone(tz=to_tz)
        .replace(tzinfo=None)
    )


class TeeLogger:
    def __init__(self):
        self.callbacks = []
        self.pending_callbacks = []

    def subscribe(self, callback):
        self.callbacks.append(callback)

    def info(self, message, public=True):
        logger.info(message)
        if public:
            self.propagate_subscribers(message, "info")

    def exception(self, message, public=True):
        logger.exception(message)
        if public:
            self.propagate_subscribers(message, "error")

    def propagate_subscribers(self, message, level):
        for cb in self.callbacks:
            task = asyncio.create_task(cb(message, level))
            self.pending_callbacks.append(task)

    async def wait_pending_callbacks(self):
        await asyncio.gather(*self.pending_callbacks)
