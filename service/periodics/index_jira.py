import asyncio
import logging
import logging.config

from service import containers, db, settings

logger = logging.getLogger(__name__)


async def main(settings):
    logging.config.dictConfig(settings.LOGGING)
    logger.info("Indexing jiras: started.")
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
            logger.exception("Cannot indexify jira")
        finally:
            await asyncio.sleep(10)


PENDING_TASKS = {}


async def iterate(container):
    jiras = await container.jira_repositories.jiras.all()

    for jira in jiras:
        pending_task = PENDING_TASKS.get(jira.internal_id, None)
        if not pending_task or pending_task.done():
            PENDING_TASKS.pop(jira.internal_id, None)

            task = asyncio.create_task(
                container.jira_index_service.indexify(jira.internal_id)
            )
            PENDING_TASKS[jira.internal_id] = task


if __name__ == "__main__":
    asyncio.run(main(settings))
