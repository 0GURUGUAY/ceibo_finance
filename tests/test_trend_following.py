import asyncio
from unittest.mock import AsyncMock

from ceibo_finance.services.trend_following import PositionState, TrendFollowingConfig, TrendFollowingService


def test_profitable_full_exit_tracks_savings_without_rebuy() -> None:
    service = TrendFollowingService()
    service.state.config = TrendFollowingConfig(symbols=['AAPL'])
    service.state.positions['AAPL'] = PositionState(qty=2.0, entry_price=100.0, entry_time='2026-03-14T00:00:00+00:00')
    service._resolve_live_exit_qty = AsyncMock(return_value=2.0)
    service._place_order = AsyncMock()
    service._rebuy_after_positive_exit = AsyncMock()

    asyncio.run(
        service._exit_position(
            symbol='AAPL',
            position=service.state.positions['AAPL'],
            price=110.0,
            reason='take_profit',
            allow_rebuy=False,
        )
    )

    assert service.state.realized_margin_usd == 20.0
    assert service.state.savings_usd == 20.0
    assert 'AAPL' not in service.state.positions
    service._rebuy_after_positive_exit.assert_not_awaited()