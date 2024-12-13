import asyncio
import logging
from typing import List

import pydantic
import pymongo

logger = logging.getLogger(__name__)


class Repository:
    table: str = ""
    default_entity_factory = None

    def __init__(self, db):
        self.db = db

    def get_table_name(self):
        if not self.table:
            raise NotImplementedError

        return self.table

    def get_table(self):
        return getattr(self.db, self.get_table_name())

    def get_default_entity_factory(self):
        if not self.default_entity_factory:
            raise NotImplementedError

        return self.default_entity_factory

    async def save_many(self, entities, updated_fields=None):
        await asyncio.gather(
            *[self.save(entity, updated_fields) for entity in entities]
        )

    async def save(self, entity, updated_fields=None):
        data = self.to_record_fields(entity)

        logger.info(f"Inserting data to db: {data}")
        try:
            await self.get_table().insert_one(data)
        except pymongo.errors.DuplicateKeyError:
            await self.update_one(entity, updated_fields)
        else:
            logger.info(f"Created entity: {entity}")

    async def delete_by_internal_id(self, internal_id):
        await self.get_table().delete_one({"internal_id": internal_id})

    async def update_many(self, data, internal_ids=None):
        if internal_ids is None:
            match = {}
        else:
            match = {"internal_id": {"$in": internal_ids}}

        await self.get_table().update_many(
            match,
            {"$set": data},
        )
        logger.info(f"Updated entities, match={match}")

    async def update_one(self, entity, updated_fields):
        unique_key = self.get_unique_key(entity)
        if not unique_key:
            raise

        data = self.to_record_fields(entity, updated_fields)
        data.pop("_id", None)
        data.pop("internal_id", None)

        await self.get_table().update_one(
            unique_key,
            {"$set": data},
        )
        logger.info(f"Updated entity: {entity}")

    def to_record_fields(self, entity_or_dict, updated_fields=None):
        if isinstance(entity_or_dict, pydantic.BaseModel):
            data = entity_or_dict.model_dump()

        else:
            data = entity_or_dict

        return self.to_record_fields_from_dict(data, updated_fields)

    def to_record_fields_from_dict(self, data, updated_fields):
        if updated_fields:
            return {k: v for k, v in data.items() if k in updated_fields}

        return data

    def get_unique_key(self, entity):
        return {
            "internal_id": entity.internal_id,
        }

    async def all(self):
        records = await self.fetch({})

        return [self.get_default_entity_factory()(**r) for r in records]

    async def filter_by_internal_id(self, internal_ids: List[str]):
        records = await self.fetch({"internal_id": {"$in": internal_ids}})
        return [
            self.get_default_entity_factory()(**record) for record in records
        ]

    async def fetch(self, match):
        cursor = self.get_table().aggregate(self.build_fetch_pipeline(match))

        records = await cursor.to_list(None)
        return records

    async def extract(self, match):
        records = await self.fetch(match)

        return [self.default_entity_factory(**r) for r in records]

    def build_fetch_pipeline(self, match, limit=None):
        pipeline = [
            {
                "$match": match,
            },
        ]

        if limit is not None:
            pipeline.append(
                {
                    "$limit": limit,
                }
            )

        return pipeline

    async def filter_by_catalog(self, catalog: str = ""):
        match = {}
        if catalog:
            match = {"name": {"$regex": f"^{catalog}"}}

        records = await self.fetch(match)
        logger.info(f"Records = {records}")
        return [
            self.get_default_entity_factory()(**record) for record in records
        ]
