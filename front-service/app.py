import os

from fastapi import FastAPI, Request
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

BACKEND_HOST = os.environ.get("SERVICE_BACKEND_HOST", "http://localhost:9000")
WS_BACKEND_HOST = os.environ.get(
    "SERVICE_WS_BACKEND_HOST", "ws://localhost:9000"
)


app = FastAPI()


app.mount("/front", StaticFiles(directory="front"), name="front")
templates = Jinja2Templates(directory="front")


@app.get("/")
async def index_handler(request: Request):
    context = {
        "backend_host": BACKEND_HOST,
        "ws_backend_host": WS_BACKEND_HOST,
    }
    return templates.TemplateResponse(
        request=request,
        name="index.html",
        context=context,
    )
