import logging
import logging.config

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from service import containers, db, http_plugins, settings
from service.presentation.handlers import router

logging.config.dictConfig(settings.LOGGING)
app = FastAPI()

from fastapi.staticfiles import StaticFiles
app.mount("/front", StaticFiles(directory="front"), name="front")

origins = [
    "*",
]


app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.on_event("startup")
async def startup():
    await db.init(
        settings.DB_DSN,
        settings.DB_NAME,
        settings.DB_CAFILE,
        settings.DB_CAFILE_URL,
    )

    container = await containers.create(settings)
    app.container = container
    app.settings = settings
    http_plugins.init()


@app.on_event("shutdown")
async def shutdown():
    await db.cleanup()


app.include_router(router)
