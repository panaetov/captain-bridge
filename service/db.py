import aiohttp
import motor.motor_asyncio

from service import migrations


_DB = None


def get_db():
    if _DB is None:
        raise RuntimeError("DB is not initialized")

    return _DB


def get_collection_class():
    return motor.motor_asyncio.AsyncIOMotorCollection


def get_database_class():
    return motor.motor_asyncio.AsyncIOMotorDatabase


async def init(db_dsn, db_name, ca_file=None, ca_file_url=None):
    global _DB

    if ca_file_url:
        async with aiohttp.ClientSession() as session:
            async with session.get(ca_file_url) as resp:
                ca_file_content = await resp.text()
                ca_file = "./.cbcert.crt"
                with open(ca_file, "w") as f:
                    f.write(ca_file_content)

    options = {}
    if ca_file:
        options["tls"] = True
        options["tlsCAFile"] = ca_file

    client = motor.motor_asyncio.AsyncIOMotorClient(db_dsn, **options)
    _DB = getattr(client, db_name)
    await migrations.apply_all(_DB)


async def cleanup(): ...
