import os

DB_DSN = os.environ.get(
    "SERVICE_DB_DSN", ""
)
if not DB_DSN:
    raise RuntimeError("SERVICE_DB_DSN environment variable is empty.")

DB_NAME = os.environ.get("SERVICE_DB_NAME", "test-1")
DB_CAFILE = os.environ.get("SERVICE_DB_CAFILE", "")
DB_CAFILE_URL = os.environ.get("SERVICE_DB_CAFILE_URL", "")

BASIC_URL = os.environ.get("SERVICE_BASIC_URL", "")
WS_BASIC_URL = os.environ.get("SERVICE_WS_BASIC_URL", "/")

if not BASIC_URL:
    raise RuntimeError("SERVICE_BASIC_URL environment variable is empty.")

if not WS_BASIC_URL:
    raise RuntimeError("SERVICE_WS_BASIC_URL environment variable is empty.")

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
            "level": os.getenv("SERVICE_LOG_LEVEL", "INFO"),
            "class": "logging.StreamHandler",
            "formatter": "default",
        },
    },
    "loggers": {
        "": {
            "handlers": ["console"],
            "level": os.getenv("SERVICE_LOG_LEVEL", "INFO"),
        },
    },
}
