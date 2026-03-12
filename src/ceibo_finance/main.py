from fastapi import FastAPI

from ceibo_finance.api.router import api_router
from ceibo_finance.core.config import settings
from ceibo_finance.core.logging import setup_logging

setup_logging()

app = FastAPI(title=settings.app_name)
app.include_router(api_router)
