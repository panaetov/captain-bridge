import datetime
import json

import bson
import pydantic
import pytz


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
