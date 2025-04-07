import asyncio
import logging
import logging.config

from service import containers, db, settings

logger = logging.getLogger(__name__)


async def main(settings):
    logging.config.dictConfig(settings.LOGGING)
    logger.info("Indexing redmines: started.")
    await db.init(
        settings.DB_DSN,
        settings.DB_NAME,
        settings.DB_CAFILE,
        settings.DB_CAFILE_URL,
    )

    container = await containers.create(settings)

    while True:
        try:
            await iterate(container)
        except Exception:
            logger.exception("Cannot indexify redmine")
        finally:
            await asyncio.sleep(10)


PENDING_TASKS = {}


async def iterate(container):
    redmines = await container.redmine_repositories.redmines.all()

    for redmine in redmines:
        pending_task = PENDING_TASKS.get(redmine.internal_id, None)
        if not pending_task or pending_task.done():
            PENDING_TASKS.pop(redmine.internal_id, None)

            task = asyncio.create_task(
                container.redmine_index_service.indexify(redmine.internal_id)
            )
            PENDING_TASKS[redmine.internal_id] = task


if __name__ == "__main__":
    asyncio.run(main(settings))
