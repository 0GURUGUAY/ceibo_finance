from fastapi import APIRouter
from pydantic import BaseModel, Field

from ceibo_finance.services.alpaca_client import alpaca_service

router = APIRouter(prefix='/orders', tags=['orders'])


class OrderIn(BaseModel):
    symbol: str = Field(min_length=1)
    qty: float = Field(gt=0)
    side: str = Field(pattern='^(buy|sell)$')


@router.get('')
def list_orders(status: str = 'open', limit: int = 50):
    orders = alpaca_service.list_orders(status=status, limit=limit)
    return [o.model_dump() if hasattr(o, 'model_dump') else o for o in orders]


@router.post('')
def create_order(payload: OrderIn):
    order = alpaca_service.place_market_order(payload.symbol, payload.qty, payload.side)
    return order.model_dump() if hasattr(order, 'model_dump') else order
