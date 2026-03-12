from fastapi import APIRouter

from ceibo_finance.services.alpaca_client import alpaca_service

router = APIRouter(prefix='/positions', tags=['positions'])


@router.get('')
def get_positions():
    positions = alpaca_service.list_positions()
    return [p.model_dump() if hasattr(p, 'model_dump') else p for p in positions]
