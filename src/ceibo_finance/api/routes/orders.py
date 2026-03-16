from datetime import datetime, timezone
import uuid

from fastapi import APIRouter
from pydantic import BaseModel, Field

from ceibo_finance.services.alpaca_client import alpaca_service
from ceibo_finance.services.strategy_history import strategy_history_service

router = APIRouter(prefix='/orders', tags=['orders'])


class OrderIn(BaseModel):
    symbol: str = Field(min_length=1)
    qty: float = Field(gt=0)
    side: str = Field(pattern='^(buy|sell)$')


def _resolve_reference_price(order_payload: dict, symbol: str, side: str) -> float:
    for field_name in ('filled_avg_price', 'limit_price', 'stop_price'):
        raw_value = order_payload.get(field_name)
        try:
            value = float(raw_value)
            if value > 0:
                return value
        except (TypeError, ValueError):
            continue

    try:
        quote_payload = alpaca_service.latest_quote(symbol).get(symbol, {})
    except Exception:
        quote_payload = {}

    preferred_fields = ('ask_price', 'bid_price') if side == 'buy' else ('bid_price', 'ask_price')
    for field_name in (*preferred_fields, 'price'):
        raw_value = quote_payload.get(field_name)
        try:
            value = float(raw_value)
            if value > 0:
                return value
        except (TypeError, ValueError):
            continue

    return 0.0


def _record_manual_order(order_payload: dict, request_payload: OrderIn) -> None:
    normalized_symbol = request_payload.symbol.strip().upper()
    normalized_side = request_payload.side.strip().lower()
    order_id = str(order_payload.get('id') or '').strip() or f'manual-{uuid.uuid4()}'
    recorded_at = (
        order_payload.get('submitted_at')
        or order_payload.get('created_at')
        or datetime.now(timezone.utc).isoformat()
    )

    raw_qty = order_payload.get('qty', request_payload.qty)
    try:
        qty = float(raw_qty)
    except (TypeError, ValueError):
        qty = float(request_payload.qty)

    strategy_history_service.record_trade(
        {
            'event_id': order_id,
            'recorded_at': str(recorded_at),
            'strategy_name': 'manual_order',
            'source_event': 'manual_order_accepted',
            'side': normalized_side,
            'symbol': normalized_symbol,
            'qty': qty,
            'price': _resolve_reference_price(order_payload, normalized_symbol, normalized_side),
            'reason': 'manual_order_buy' if normalized_side == 'buy' else 'manual_order_sell',
            'order_id': order_id,
            'simulation_mode': False,
            'pnl_usd': None,
            'pnl_pct': None,
            'metadata': {
                'status': order_payload.get('status'),
                'alpaca_order_class': order_payload.get('order_class'),
                'time_in_force': order_payload.get('time_in_force'),
                'validation_recorded': True,
            },
        }
    )


@router.get('')
def list_orders(status: str = 'open', limit: int = 50):
    orders = alpaca_service.list_orders(status=status, limit=limit)
    return [o.model_dump() if hasattr(o, 'model_dump') else o for o in orders]


@router.post('')
def create_order(payload: OrderIn):
    order = alpaca_service.place_market_order(payload.symbol, payload.qty, payload.side)
    order_payload = order.model_dump() if hasattr(order, 'model_dump') else order
    if isinstance(order_payload, dict):
        _record_manual_order(order_payload, payload)
    return order_payload
