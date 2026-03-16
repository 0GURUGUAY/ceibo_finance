from pathlib import Path

from fastapi.testclient import TestClient

from ceibo_finance.main import app
from ceibo_finance.api.routes import orders as orders_route
from ceibo_finance.api.routes import strategy as strategy_route
from ceibo_finance.services.strategy_history import StrategyHistoryService
from ceibo_finance.services.trend_following import TrendFollowingConfig, TrendFollowingService


def test_strategy_history_service_persists_trade_event(tmp_path: Path) -> None:
    history_service = StrategyHistoryService(db_path=str(tmp_path / 'strategy_history.db'))
    service = TrendFollowingService()
    service.state.config = TrendFollowingConfig(symbols=['AAPL'], simulation_mode=True)

    from ceibo_finance.services import trend_following as trend_following_module
    original_trend_history_service = trend_following_module.strategy_history_service
    trend_following_module.strategy_history_service = history_service
    try:
        service._append_event(
            'entry',
            {
                'symbol': 'AAPL',
                'price': 201.25,
                'qty': 3,
                'order_id': 'SIM-1',
            },
        )
    finally:
        trend_following_module.strategy_history_service = original_trend_history_service

    rows = history_service.list_trades(limit=10)
    assert len(rows) == 1
    assert rows[0]['symbol'] == 'AAPL'
    assert rows[0]['side'] == 'buy'
    assert rows[0]['reason'] == 'trend_entry'
    assert rows[0]['order_id'] == 'SIM-1'
    assert rows[0]['simulation_mode'] is True


def test_strategy_history_endpoint_returns_persisted_records(tmp_path: Path) -> None:
    client = TestClient(app)
    history_service = StrategyHistoryService(db_path=str(tmp_path / 'strategy_history.db'))
    history_service.record_trade(
        {
            'event_id': 'evt-1',
            'recorded_at': '2026-03-16T10:00:00+00:00',
            'strategy_name': 'trend_following',
            'source_event': 'exit',
            'side': 'sell',
            'symbol': 'NVDA',
            'qty': 1.5,
            'price': 900.0,
            'reason': 'take_profit',
            'order_id': 'order-1',
            'simulation_mode': False,
            'pnl_usd': 45.0,
            'pnl_pct': 5.0,
            'metadata': {'source': 'test'},
        }
    )

    original_history_service = strategy_route.strategy_history_service
    strategy_route.strategy_history_service = history_service
    try:
        response = client.get('/api/v1/strategy/trend-following/history?limit=20&symbol=NVDA&side=sell')
    finally:
        strategy_route.strategy_history_service = original_history_service

    assert response.status_code == 200
    payload = response.json()
    assert payload['count'] == 1
    assert payload['records'][0]['symbol'] == 'NVDA'
    assert payload['records'][0]['side'] == 'sell'
    assert payload['records'][0]['pnl_usd'] == 45.0


def test_manual_order_records_history_on_alpaca_accept(tmp_path: Path, monkeypatch) -> None:
    client = TestClient(app)
    history_service = StrategyHistoryService(db_path=str(tmp_path / 'strategy_history.db'))

    order_payload = {
        'id': 'manual-order-1',
        'symbol': 'AAPL',
        'side': 'buy',
        'qty': '2',
        'status': 'accepted',
        'submitted_at': '2026-03-16T14:00:00+00:00',
        'time_in_force': 'day',
        'order_class': 'simple',
    }

    original_history_service = orders_route.strategy_history_service
    monkeypatch.setattr(orders_route.alpaca_service, 'place_market_order', lambda symbol, qty, side: order_payload)
    monkeypatch.setattr(orders_route.alpaca_service, 'latest_quote', lambda symbol: {symbol: {'ask_price': 205.5, 'bid_price': 205.2}})
    orders_route.strategy_history_service = history_service
    try:
        response = client.post('/api/v1/orders', json={'symbol': 'AAPL', 'qty': 2, 'side': 'buy'})
    finally:
        orders_route.strategy_history_service = original_history_service

    assert response.status_code == 200
    rows = history_service.list_trades(limit=10)
    assert len(rows) == 1
    assert rows[0]['strategy_name'] == 'manual_order'
    assert rows[0]['source_event'] == 'manual_order_accepted'
    assert rows[0]['symbol'] == 'AAPL'
    assert rows[0]['side'] == 'buy'
    assert rows[0]['qty'] == 2.0
    assert rows[0]['price'] == 205.5
    assert rows[0]['order_id'] == 'manual-order-1'
    assert rows[0]['reason'] == 'manual_order_buy'