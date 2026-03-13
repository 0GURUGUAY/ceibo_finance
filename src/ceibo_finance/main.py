from pathlib import Path

from fastapi import FastAPI
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

from ceibo_finance.api.router import api_router
from ceibo_finance.core.config import settings
from ceibo_finance.core.logging import setup_logging

setup_logging()

app = FastAPI(title=settings.app_name)
app.include_router(api_router)

STATIC_DIR = Path(__file__).parent / 'static'
app.mount('/static', StaticFiles(directory=STATIC_DIR), name='static')


@app.get('/')
def index():
    return FileResponse(STATIC_DIR / 'index.html')
