import asyncio
import logging
import logging.config

from service import containers, db, settings

logger = logging.getLogger(__name__)


async def main(settings):
    logging.config.dictConfig(settings.LOGGING)
    logger.info("Actualizing planning info: started.")
    await db.init(settings.DB_DSN, settings.DB_NAME, settings.DB_CAFILE)
    container = await containers.create(settings)

    while True:
        try:
            await iterate(container)
        except Exception:
            logger.exception("Cannot actualize plannings")
        finally:
            await asyncio.sleep(60)


PENDING_TASKS = {}


async def iterate(container):
    plannings = await container.planning_repositories.plannings.all()

    logger.info(f"Found {len(plannings)} plannings.")
    for planning in plannings:
        pending_task = PENDING_TASKS.get(planning.internal_id, None)
        if not pending_task or pending_task.done():
            PENDING_TASKS.pop(planning.internal_id, None)

            task = asyncio.create_task(worker(container, planning))
            PENDING_TASKS[planning.internal_id] = task


async def worker(container, planning):
    try:
        await container.planning_actualizer.actualize(planning.internal_id)
    except Exception:
        logger.exception(f"Cannot actualize planning {planning.internal_id}")


if __name__ == "__main__":
    asyncio.run(main(settings))
