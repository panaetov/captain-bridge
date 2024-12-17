import asyncio
import logging
import logging.config

from service import db
from service import settings
from service import migrations


logger = logging.getLogger(__name__)


async def main(settings):
    logging.config.dictConfig(settings.LOGGING)
    logger.info("Migration started.")
    await db.init(
        settings.DB_DSN,
        settings.DB_NAME,
        settings.DB_CAFILE,
        settings.DB_CAFILE_URL,
    )

    await migrations.apply_all(db.get_db())


if __name__ == "__main__":
    asyncio.run(main(settings))
