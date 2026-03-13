from fastapi import APIRouter

from ceibo_finance.api.routes.analysis import router as analysis_router
from ceibo_finance.api.routes.accounts import router as accounts_router
from ceibo_finance.api.routes.activities import router as activities_router
from ceibo_finance.api.routes.health import router as health_router
from ceibo_finance.api.routes.marketdata import router as marketdata_router
from ceibo_finance.api.routes.orders import router as orders_router
from ceibo_finance.api.routes.positions import router as positions_router
from ceibo_finance.api.routes.strategy import router as strategy_router

api_router = APIRouter()
api_router.include_router(health_router)
api_router.include_router(accounts_router, prefix='/api/v1')
api_router.include_router(positions_router, prefix='/api/v1')
api_router.include_router(orders_router, prefix='/api/v1')
api_router.include_router(marketdata_router, prefix='/api/v1')
api_router.include_router(analysis_router, prefix='/api/v1')
api_router.include_router(strategy_router, prefix='/api/v1')
api_router.include_router(activities_router, prefix='/api/v1')
