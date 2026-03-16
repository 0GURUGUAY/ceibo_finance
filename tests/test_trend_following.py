import asyncio
from collections import deque
from unittest.mock import AsyncMock

from ceibo_finance.services import trend_following as trend_following_module
from ceibo_finance.services.trend_following import PositionState, TrendFollowingConfig, TrendFollowingService


def test_profitable_full_exit_tracks_savings_without_rebuy() -> None:
    loop = asyncio.new_event_loop()
    previous_loop = None
    try:
        try:
            previous_loop = asyncio.get_event_loop()
        except RuntimeError:
            previous_loop = None
        asyncio.set_event_loop(loop)

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
    finally:
        asyncio.set_event_loop(previous_loop)
        loop.close()


def test_start_adopts_current_broker_positions(monkeypatch) -> None:
    class DummyPosition:
        symbol = 'NVDA'
        qty = '3'
        avg_entry_price = '183.10'
        created_at = '2026-03-16T09:00:00+00:00'

    loop = asyncio.new_event_loop()
    previous_loop = None
    try:
        try:
            previous_loop = asyncio.get_event_loop()
        except RuntimeError:
            previous_loop = None
        asyncio.set_event_loop(loop)

        monkeypatch.setattr(trend_following_module.alpaca_service, 'list_positions', lambda: [DummyPosition()])

        service = TrendFollowingService()
        status = asyncio.run(service.start(TrendFollowingConfig(symbols=[], use_current_positions=True)))

        assert status['symbols'] == ['NVDA']
        assert 'NVDA' in status['positions']
        assert status['positions']['NVDA']['qty'] == 3.0
        assert status['positions']['NVDA']['entry_price'] == 183.1

        asyncio.run(service.stop())
    finally:
        asyncio.set_event_loop(previous_loop)
        loop.close()


def test_stop_loss_on_adopted_position_without_history_warmup() -> None:
    loop = asyncio.new_event_loop()
    previous_loop = None
    try:
        try:
            previous_loop = asyncio.get_event_loop()
        except RuntimeError:
            previous_loop = None
        asyncio.set_event_loop(loop)

        service = TrendFollowingService()
        service.state.config = TrendFollowingConfig(symbols=['INTC'], stop_loss_pct=0.4, take_profit_pct=0.5)
        service.state.positions['INTC'] = PositionState(qty=10.0, entry_price=48.61, entry_time='2026-03-16T13:56:00+00:00')
        service.state.price_history['INTC'] = deque([48.50, 48.40], maxlen=25)
        service._resolve_negative_sell_confirmations = AsyncMock(return_value=(3, -1.2))
        service._exit_position = AsyncMock()

        asyncio.run(service._evaluate_symbol('INTC', 47.965))

        service._exit_position.assert_not_awaited()
        assert service.state.sell_signal_hits['INTC']['reason'] == 'stop_loss'
        assert service.state.sell_signal_hits['INTC']['hits'] == 1
        assert service.state.sell_signal_hits['INTC']['required_hits'] == 3

        asyncio.run(service._evaluate_symbol('INTC', 47.965))
        asyncio.run(service._evaluate_symbol('INTC', 47.965))

        service._exit_position.assert_awaited_with(
            'INTC',
            service.state.positions['INTC'],
            47.965,
            'stop_loss',
        )
    finally:
        asyncio.set_event_loop(previous_loop)
        loop.close()


def test_rebuy_deferred_until_no_open_sell_order() -> None:
    loop = asyncio.new_event_loop()
    previous_loop = None
    try:
        try:
            previous_loop = asyncio.get_event_loop()
        except RuntimeError:
            previous_loop = None
        asyncio.set_event_loop(loop)

        service = TrendFollowingService()
        service.state.config = TrendFollowingConfig(symbols=['TTE'])
        service.state.latest_price['TTE'] = 91.05
        service._place_order = AsyncMock()

        service._has_open_order_conflict = AsyncMock(side_effect=[True, False, False])

        asyncio.run(service._rebuy_after_positive_exit(symbol='TTE', entry_cost_usd=10000.0, price=91.05))

        assert 'TTE' in service.state.pending_rebuys
        service._place_order.assert_not_awaited()

        asyncio.run(service._process_pending_rebuys())

        service._place_order.assert_awaited_with(symbol='TTE', qty=109.829764, side='buy', reason='positive_exit_rebuy')
        assert 'TTE' not in service.state.pending_rebuys
        assert 'TTE' in service.state.positions
    finally:
        asyncio.set_event_loop(previous_loop)
        loop.close()


def test_loss_reentry_executes_after_delay_when_trend_positive() -> None:
    loop = asyncio.new_event_loop()
    previous_loop = None
    try:
        try:
            previous_loop = asyncio.get_event_loop()
        except RuntimeError:
            previous_loop = None
        asyncio.set_event_loop(loop)

        service = TrendFollowingService()
        service.state.config = TrendFollowingConfig(symbols=['INTC'], reentry_after_loss_enabled=True, reentry_delay_minutes=30)
        service.state.pending_loss_reentries['INTC'] = {
            'symbol': 'INTC',
            'qty': 10.0,
            'sell_price': 48.0,
            'reason': 'stop_loss',
            'scheduled_at': '2026-03-16T10:00:00+00:00',
            'due_at_epoch': 0.0,
            'attempts': 0,
        }
        service.state.latest_price['INTC'] = 48.4
        service._has_open_order_conflict = AsyncMock(return_value=False)
        service._place_order = AsyncMock()

        asyncio.run(service._process_pending_loss_reentries())

        service._place_order.assert_awaited_with(symbol='INTC', qty=10.0, side='buy', reason='loss_reentry_after_delay')
        assert 'INTC' in service.state.positions
        assert 'INTC' not in service.state.pending_loss_reentries
    finally:
        asyncio.set_event_loop(previous_loop)
        loop.close()


def test_loss_reentry_skips_when_trend_negative() -> None:
    loop = asyncio.new_event_loop()
    previous_loop = None
    try:
        try:
            previous_loop = asyncio.get_event_loop()
        except RuntimeError:
            previous_loop = None
        asyncio.set_event_loop(loop)

        service = TrendFollowingService()
        service.state.config = TrendFollowingConfig(symbols=['INTC'], reentry_after_loss_enabled=True, reentry_delay_minutes=30)
        service.state.pending_loss_reentries['INTC'] = {
            'symbol': 'INTC',
            'qty': 10.0,
            'sell_price': 48.0,
            'reason': 'stop_loss',
            'scheduled_at': '2026-03-16T10:00:00+00:00',
            'due_at_epoch': 0.0,
            'attempts': 0,
        }
        service.state.latest_price['INTC'] = 47.5
        service._place_order = AsyncMock()

        asyncio.run(service._process_pending_loss_reentries())

        service._place_order.assert_not_awaited()
        assert 'INTC' not in service.state.pending_loss_reentries
        assert 'INTC' not in service.state.positions
    finally:
        asyncio.set_event_loop(previous_loop)
        loop.close()