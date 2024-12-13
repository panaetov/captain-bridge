import os

DB_DSN = os.environ.get(
    "SERVICE_DB_DSN", "mongodb://panaetov:FuckChaseyLain@158.160.124.40:27017"
)
DB_NAME = os.environ.get("SERVICE_DB_NAME", "test-1")
DB_CAFILE = os.environ.get("SERVICE_DB_CAFILE", "")
DB_CAFILE_URL = os.environ.get("SERVICE_DB_CAFILE_URL", "")
WORKER_URL = os.environ.get(
    "SERVICE_WORKER_URL", "http://localhost:9000/metric/pipeline/run"
)

LOGGING = {
    "version": 1,
    "disable_existing_loggers": False,
    "formatters": {
        "default": {
            "format": (
                "[%(asctime)s] %(levelname)s [%(name)s:%(lineno)s] %(message)s"
            ),
            "datefmt": "%d/%b/%Y %H:%M:%S",
        },
    },
    "handlers": {
        "console": {
            "level": os.getenv("SERVICE_LOGGERS_HANDLERS_LEVEL", "INFO"),
            "class": "logging.StreamHandler",
            "formatter": "default",
        },
    },
    "loggers": {
        "": {
            "handlers": ["console"],
            "level": os.getenv("SERVICE_LOGGERS_HANDLERS_LEVEL", "INFO"),
        },
    },
}
