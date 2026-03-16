from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

from ceibo_finance.services.alpaca_client import AlpacaService


def test_history_daily_closes_keeps_most_recent_bars() -> None:
    start = datetime(2026, 3, 10, tzinfo=timezone.utc)
    bars = [
        SimpleNamespace(timestamp=start + timedelta(days=index), close=40.0 + index)
        for index in range(5)
    ]
    captured_request = {}

    def get_stock_bars(request):
        captured_request['limit'] = getattr(request, 'limit', None)
        return SimpleNamespace(data={'INTC': bars})

    service = AlpacaService.__new__(AlpacaService)
    service.data = SimpleNamespace(get_stock_bars=get_stock_bars)

    result = service.history_daily_closes('INTC', days=2)

    assert captured_request['limit'] is None
    assert [item['close'] for item in result] == [43.0, 44.0]
    assert [item['timestamp'] for item in result] == [
        (start + timedelta(days=3)).isoformat(),
        (start + timedelta(days=4)).isoformat(),
    ]