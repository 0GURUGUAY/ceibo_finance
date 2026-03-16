from fastapi import APIRouter

from ceibo_finance.services.alpaca_client import alpaca_service

router = APIRouter(prefix='/positions', tags=['positions'])


def _to_float(value, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


@router.get('')
def get_positions():
    positions = alpaca_service.list_positions()
    return [p.model_dump() if hasattr(p, 'model_dump') else p for p in positions]


@router.post('/reset')
def reset_positions():
    result = alpaca_service.liquidate_all_positions()
    return result


@router.get('/best-last-hour')
def get_best_position_last_hour():
    raw_positions = alpaca_service.list_positions()
    positions = [p.model_dump() if hasattr(p, 'model_dump') else p for p in raw_positions]
    symbols = [str((item or {}).get('symbol', '')).strip().upper() for item in positions if (item or {}).get('symbol')]
    metadata_by_symbol = alpaca_service.get_assets_metadata(symbols)

    best_payload = None
    for position in positions:
        symbol = str((position or {}).get('symbol', '')).strip().upper()
        if not symbol:
            continue

        bars = alpaca_service.history_intraday_closes(symbol=symbol, minutes=60, timeframe_minutes=5)
        closes = [row for row in bars if row.get('close') is not None]
        if len(closes) < 2:
            continue

        start_price = _to_float(closes[0].get('close'))
        current_price = _to_float((position or {}).get('current_price'))
        if start_price <= 0 or current_price <= 0:
            continue

        change_pct = ((current_price - start_price) / start_price) * 100
        candidate = {
            'symbol': symbol,
            'company_name': metadata_by_symbol.get(symbol, {}).get('name') or symbol,
            'exchange': metadata_by_symbol.get(symbol, {}).get('exchange') or '',
            'change_pct_1h': round(change_pct, 2),
            'start_price_1h': round(start_price, 4),
            'current_price': round(current_price, 4),
            'market_value': round(_to_float((position or {}).get('market_value')), 2),
            'unrealized_pl': round(_to_float((position or {}).get('unrealized_pl')), 2),
        }
        if best_payload is None or candidate['change_pct_1h'] > best_payload['change_pct_1h']:
            best_payload = candidate

    return best_payload or {
        'symbol': None,
        'company_name': None,
        'exchange': None,
        'change_pct_1h': None,
        'start_price_1h': None,
        'current_price': None,
        'market_value': None,
        'unrealized_pl': None,
    }
