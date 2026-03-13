from fastapi import APIRouter

from ceibo_finance.services.alpaca_client import alpaca_service

router = APIRouter(prefix='/marketdata', tags=['marketdata'])


@router.get('/assets/search')
def search_assets(q: str = '', limit: int = 10):
    safe_limit = min(max(1, limit), 20)
    return alpaca_service.search_assets(query=q, limit=safe_limit)


@router.get('/latest')
def latest_quote(symbol: str):
    return alpaca_service.latest_quote(symbol)


@router.get('/history')
def history(symbol: str, days: int = 5):
    return {
        'symbol': symbol.upper(),
        'days': days,
        'series': alpaca_service.history_daily_closes(symbol=symbol, days=days),
    }
