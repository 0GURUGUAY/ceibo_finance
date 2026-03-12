from fastapi import APIRouter

from ceibo_finance.services.alpaca_client import alpaca_service

router = APIRouter(prefix='/marketdata', tags=['marketdata'])


@router.get('/latest')
def latest_quote(symbol: str):
    result = alpaca_service.latest_quote(symbol)
    return result.model_dump() if hasattr(result, 'model_dump') else result
