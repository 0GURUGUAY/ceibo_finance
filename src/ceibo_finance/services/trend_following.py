import asyncio
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Optional
from uuid import UUID

from fastapi import WebSocket

from ceibo_finance.services.alpaca_client import alpaca_service
from ceibo_finance.services.strategy_history import strategy_history_service


@dataclass
class PositionState:
    qty: float
    entry_price: float
    entry_time: str


@dataclass
class TrendFollowingConfig:
    symbols: list[str]
    use_current_positions: bool = True
    simulation_mode: bool = False
    capital_usd: float = 10000.0
    short_window: int = 5
    long_window: int = 20
    poll_seconds: float = 30.0
    universe_refresh_seconds: float = 60.0
    take_profit_pct: float = 0.5
    stop_loss_pct: float = 0.4
    stop_loss_confirmations: int = 2
    reentry_after_loss_enabled: bool = True
    reentry_delay_minutes: int = 30


@dataclass
class TrendFollowingState:
    running: bool = False
    started_at: Optional[str] = None
    config: Optional[TrendFollowingConfig] = None
    positions: dict[str, PositionState] = field(default_factory=dict)
    price_history: dict[str, deque] = field(default_factory=dict)
    latest_price: dict[str, float] = field(default_factory=dict)
    sell_signal_hits: dict[str, dict[str, Any]] = field(default_factory=dict)
    positive_trend_partial_exit_stage: dict[str, int] = field(default_factory=dict)
    pending_rebuys: dict[str, dict[str, Any]] = field(default_factory=dict)
    pending_loss_reentries: dict[str, dict[str, Any]] = field(default_factory=dict)
    events: list[dict[str, Any]] = field(default_factory=list)
    realized_margin_usd: float = 0.0
    savings_usd: float = 0.0
    paused_symbols: set[str] = field(default_factory=set)


class TrendFollowingService:
    def __init__(self) -> None:
        self.state = TrendFollowingState()
        self._task: Optional[asyncio.Task] = None
        self._lock = asyncio.Lock()
        self._clients: set[WebSocket] = set()
        self._daily_trend_cache: dict[str, dict[str, Any]] = {}

    async def start(self, config: TrendFollowingConfig) -> dict:
        async with self._lock:
            if self.state.running:
                return self.status()

            sanitized = await self._sanitize_config(config)
            live_positions = []
            if sanitized.use_current_positions:
                try:
                    live_positions = await asyncio.to_thread(alpaca_service.list_positions)
                except Exception:
                    live_positions = []

            self.state = TrendFollowingState(
                running=True,
                started_at=datetime.now(timezone.utc).isoformat(),
                config=sanitized,
                positions=self._build_position_state_from_live_positions(live_positions),
                price_history={symbol: deque(maxlen=sanitized.long_window + 5) for symbol in sanitized.symbols},
                latest_price={},
                sell_signal_hits={},
                positive_trend_partial_exit_stage={},
                pending_rebuys={},
                pending_loss_reentries={},
                events=[],
                realized_margin_usd=0.0,
                savings_usd=0.0,
            )
            self._daily_trend_cache = {}
            self._append_event('strategy_started', {'config': self._config_to_dict(sanitized)})
            self._task = asyncio.create_task(self._run_loop())
            return self.status()

    async def stop(self) -> dict:
        async with self._lock:
            if not self.state.running:
                return self.status()
            self.state.running = False
            self._append_event('strategy_stopping', {})
            if self._task:
                self._task.cancel()
                self._task = None
            return self.status()

    def pause_symbol(self, symbol: str) -> dict:
        normalized = str(symbol or '').strip().upper()
        if not normalized:
            return self.status()
        self.state.paused_symbols.add(normalized)
        self.state.sell_signal_hits.pop(normalized, None)
        self._append_event('symbol_paused', {'symbol': normalized})
        return self.status()

    def resume_symbol(self, symbol: str) -> dict:
        normalized = str(symbol or '').strip().upper()
        self.state.paused_symbols.discard(normalized)
        self._append_event('symbol_resumed', {'symbol': normalized})
        return self.status()

    def status(self) -> dict:
        config = self.state.config
        invested = self._invested_notional_usd()
        capital = float(config.capital_usd) if config else 0.0
        available = max(0.0, capital - invested)
        monitoring = self._build_monitoring_summary(self.state.events)

        return {
            'running': self.state.running,
            'started_at': self.state.started_at,
            'config': self._config_to_dict(config) if config else None,
            'symbols': config.symbols if config else [],
            'paused_symbols': sorted(self.state.paused_symbols),
            'capital_usd': round(capital, 2),
            'invested_usd': round(invested, 2),
            'available_investable_usd': round(available, 2),
            'realized_margin_usd': round(self.state.realized_margin_usd, 2),
            'savings_usd': round(self.state.savings_usd, 2),
            'simulation_mode': bool(config.simulation_mode) if config else False,
            'monitoring': monitoring,
            'positions': {
                symbol: {
                    'qty': position.qty,
                    'entry_price': position.entry_price,
                    'entry_time': position.entry_time,
                    'unrealized_pct': self._compute_unrealized_pct(symbol, position),
                    'notional_usd': round(position.qty * position.entry_price, 2),
                }
                for symbol, position in self.state.positions.items()
            },
            'pending_rebuys': {
                symbol: self._json_safe(payload)
                for symbol, payload in self.state.pending_rebuys.items()
            },
            'pending_loss_reentries': {
                symbol: self._json_safe(payload)
                for symbol, payload in self.state.pending_loss_reentries.items()
            },
            'sell_signal_hits': {
                symbol: {
                    'reason': str((state or {}).get('reason') or ''),
                    'hits': int((state or {}).get('hits') or 0),
                    'required_hits': int((state or {}).get('required_hits') or 0),
                    'daily_trend_pct': (None if (state or {}).get('daily_trend_pct') is None else round(float((state or {}).get('daily_trend_pct')), 2)),
                }
                for symbol, state in self.state.sell_signal_hits.items()
            },
            'latest_price': self.state.latest_price,
            'events': [self._json_safe(event) for event in self.state.events[-80:]],
        }

    async def subscribe(self, websocket: WebSocket) -> None:
        await websocket.accept()
        self._clients.add(websocket)
        await websocket.send_json({'type': 'snapshot', 'payload': self.status()})

    async def unsubscribe(self, websocket: WebSocket) -> None:
        if websocket in self._clients:
            self._clients.remove(websocket)

    async def _run_loop(self) -> None:
        try:
            last_universe_refresh = 0.0
            market_seen_open = False
            while self.state.running and self.state.config:
                loop_started = asyncio.get_running_loop().time()
                config = self.state.config
                market_clock = await asyncio.to_thread(alpaca_service.get_market_clock)
                market_is_open = market_clock.get('is_open')
                if market_is_open is True:
                    market_seen_open = True
                elif market_seen_open and market_is_open is False:
                    self._append_event(
                        'strategy_stopped_market_closed',
                        {
                            'reason': 'market_closed',
                            'next_open': str(market_clock.get('next_open') or ''),
                            'next_close': str(market_clock.get('next_close') or ''),
                        },
                    )
                    self.state.running = False
                    await self._broadcast({'type': 'stopped', 'payload': self.status()})
                    return
                now_monotonic = asyncio.get_running_loop().time()
                if config.use_current_positions and (now_monotonic - last_universe_refresh >= config.universe_refresh_seconds):
                    await self._refresh_universe_from_positions()
                    last_universe_refresh = now_monotonic

                monitored_symbols = self._monitored_symbols()
                if not monitored_symbols:
                    await self._broadcast({'type': 'tick', 'payload': self.status()})
                    await asyncio.sleep(max(2.0, config.poll_seconds))
                    continue

                tasks = [self._fetch_symbol_price(symbol) for symbol in monitored_symbols]
                results = await asyncio.gather(*tasks, return_exceptions=True)

                for symbol, result in zip(monitored_symbols, results):
                    if isinstance(result, BaseException):
                        self._append_event('price_error', {'symbol': symbol, 'error': str(result)})
                        continue
                    if result is None:
                        continue

                    price = float(result)
                    self.state.latest_price[symbol] = price
                    self.state.price_history[symbol].append(price)
                    try:
                        await self._evaluate_symbol(symbol, price)
                    except Exception as exc:
                        self._append_event(
                            'evaluation_error',
                            {
                                'symbol': symbol,
                                'price': round(price, 4),
                                'error': str(exc),
                            },
                        )
                        continue

                    await self._process_pending_rebuys()
                    await self._process_pending_loss_reentries()

                loop_duration_ms = (asyncio.get_running_loop().time() - loop_started) * 1000
                self._append_event(
                    'loop_cycle',
                    {
                        'duration_ms': round(loop_duration_ms, 2),
                        'symbols_polled': len(monitored_symbols),
                    },
                )

                await self._broadcast({'type': 'tick', 'payload': self.status()})
                await asyncio.sleep(max(2.0, config.poll_seconds))
        except asyncio.CancelledError:
            self._append_event('strategy_stopped', {'reason': 'cancelled'})
            await self._broadcast({'type': 'stopped', 'payload': self.status()})
            raise
        except Exception as exc:
            self._append_event('strategy_error', {'error': str(exc)})
            self.state.running = False
            await self._broadcast({'type': 'error', 'payload': self.status()})

    async def _evaluate_symbol(self, symbol: str, price: float) -> None:
        config = self.state.config
        if not config:
            return

        history = self.state.price_history.get(symbol) or deque()
        position = self.state.positions.get(symbol)
        if position is None:
            if symbol in self.state.pending_rebuys:
                return
            if len(history) < config.long_window:
                return

            short_slice = list(history)[-config.short_window:]
            long_slice = list(history)[-config.long_window:]
            short_ma = sum(short_slice) / len(short_slice)
            long_ma = sum(long_slice) / len(long_slice)
            self.state.sell_signal_hits.pop(symbol, None)
            self.state.positive_trend_partial_exit_stage.pop(symbol, None)
            if short_ma > long_ma:
                qty = self._compute_entry_qty(symbol, price)
                if qty <= 0:
                    self._append_event(
                        'entry_skipped_budget',
                        {
                            'symbol': symbol,
                            'price': round(price, 4),
                            'available_usd': round(max(0.0, config.capital_usd - self._invested_notional_usd()), 2),
                        },
                    )
                    return
                order_id = await self._place_order(symbol=symbol, qty=qty, side='buy', reason='trend_entry')
                self.state.positions[symbol] = PositionState(
                    qty=qty,
                    entry_price=price,
                    entry_time=datetime.now(timezone.utc).isoformat(),
                )
                self._append_event(
                    'entry',
                    {
                        'symbol': symbol,
                        'price': round(price, 4),
                        'qty': round(qty, 6),
                        'order_id': order_id,
                        'short_ma': round(short_ma, 4),
                        'long_ma': round(long_ma, 4),
                    },
                )
            return

        take_profit = position.entry_price * (1 + (config.take_profit_pct / 100.0))
        stop_loss = position.entry_price * (1 - (config.stop_loss_pct / 100.0))

        if price >= take_profit:
            self.state.sell_signal_hits.pop(symbol, None)
            self.state.positive_trend_partial_exit_stage.pop(symbol, None)
            await self._exit_position(symbol, position, price, 'take_profit')
            return

        negative_reason: Optional[str] = None
        if price <= stop_loss:
            negative_reason = 'stop_loss'
        elif len(history) >= config.long_window:
            short_slice = list(history)[-config.short_window:]
            long_slice = list(history)[-config.long_window:]
            short_ma = sum(short_slice) / len(short_slice)
            long_ma = sum(long_slice) / len(long_slice)
            if short_ma < long_ma:
                negative_reason = 'trend_reversal'

        if negative_reason == 'stop_loss' and len(history) < config.long_window:
            # Stop loss must protect adopted/live positions immediately, even before MA warmup.
            pass
        elif negative_reason is None and len(history) < config.long_window:
            return

        if negative_reason:
            required_hits, daily_trend_pct = await self._resolve_negative_sell_confirmations(symbol=symbol, current_price=price)
            previous_state = self.state.sell_signal_hits.get(symbol) or {}
            previous_reason = str(previous_state.get('reason') or '')
            hits = (int(previous_state.get('hits') or 0) + 1) if previous_reason == negative_reason else 1
            self.state.sell_signal_hits[symbol] = {
                'reason': negative_reason,
                'hits': hits,
                'required_hits': required_hits,
                'daily_trend_pct': daily_trend_pct,
            }
            self._append_event(
                'negative_sell_candidate',
                {
                    'symbol': symbol,
                    'reason': negative_reason,
                    'price': round(price, 4),
                    'stop_loss_price': round(stop_loss, 4),
                    'hits': hits,
                    'required_hits': required_hits,
                    'daily_trend_pct': None if daily_trend_pct is None else round(daily_trend_pct, 2),
                },
            )
            if hits >= required_hits:
                self.state.sell_signal_hits.pop(symbol, None)
                if daily_trend_pct is not None and daily_trend_pct > 0:
                    stage = int(self.state.positive_trend_partial_exit_stage.get(symbol, 0) or 0)
                    if stage <= 0:
                        await self._exit_position(
                            symbol,
                            position,
                            price,
                            f'{negative_reason}_partial_1',
                            sell_fraction=0.5,
                            allow_rebuy=False,
                        )
                        if symbol in self.state.positions:
                            self.state.positive_trend_partial_exit_stage[symbol] = 1
                        else:
                            self.state.positive_trend_partial_exit_stage.pop(symbol, None)
                    else:
                        await self._exit_position(
                            symbol,
                            position,
                            price,
                            f'{negative_reason}_partial_2',
                        )
                        self.state.positive_trend_partial_exit_stage.pop(symbol, None)
                else:
                    self.state.positive_trend_partial_exit_stage.pop(symbol, None)
                    await self._exit_position(symbol, position, price, negative_reason)
            return

        self.state.sell_signal_hits.pop(symbol, None)

        if len(history) >= config.long_window:
            short_slice = list(history)[-config.short_window:]
            long_slice = list(history)[-config.long_window:]
            short_ma = sum(short_slice) / len(short_slice)
            long_ma = sum(long_slice) / len(long_slice)
        else:
            short_ma = None
            long_ma = None

        if symbol in self.state.positive_trend_partial_exit_stage and short_ma is not None and long_ma is not None and short_ma >= long_ma and price > stop_loss:
            self.state.positive_trend_partial_exit_stage.pop(symbol, None)

    async def _exit_position(
        self,
        symbol: str,
        position: PositionState,
        price: float,
        reason: str,
        sell_fraction: Optional[float] = None,
        allow_rebuy: bool = True,
    ) -> None:
        live_qty = await self._resolve_live_exit_qty(symbol=symbol, fallback_qty=position.qty)
        if live_qty <= 0:
            self._append_event(
                'exit_skipped_no_qty',
                {
                    'symbol': symbol,
                    'reason': reason,
                },
            )
            if symbol in self.state.positions:
                del self.state.positions[symbol]
            return

        if sell_fraction is not None:
            fraction = max(0.0, min(float(sell_fraction), 1.0))
            exit_qty = round(live_qty * fraction, 6)
            if exit_qty <= 0 and live_qty > 0:
                exit_qty = min(live_qty, round(live_qty, 6))
        else:
            exit_qty = live_qty

        exit_qty = min(live_qty, max(0.0, round(exit_qty, 6)))
        if exit_qty <= 0:
            return

        order_id = await self._place_order(symbol=symbol, qty=exit_qty, side='sell', reason=reason)
        pnl_usd = (price - position.entry_price) * exit_qty
        pnl_pct = ((price - position.entry_price) / position.entry_price) * 100 if position.entry_price else 0.0
        entry_cost_usd = round(position.entry_price * exit_qty, 4)
        self.state.realized_margin_usd += pnl_usd
        estimated_remaining_qty = max(0.0, round(live_qty - exit_qty, 6))
        is_partial = estimated_remaining_qty > 0

        self._append_event(
            'exit',
            {
                'symbol': symbol,
                'reason': reason,
                'entry_price': round(position.entry_price, 4),
                'exit_price': round(price, 4),
                'qty': round(exit_qty, 6),
                'order_id': order_id,
                'estimated_remaining_qty': estimated_remaining_qty,
                'is_partial': is_partial,
                'pnl_pct': round(pnl_pct, 2),
                'pnl_usd': round(pnl_usd, 2),
                'entry_cost_usd': round(entry_cost_usd, 4),
                'realized_margin_usd': round(self.state.realized_margin_usd, 2),
                'savings_usd': round(self.state.savings_usd + (pnl_usd if pnl_usd > 0 and not is_partial else 0.0), 2),
            },
        )
        self.state.sell_signal_hits.pop(symbol, None)
        if is_partial:
            self.state.positions[symbol] = PositionState(
                qty=estimated_remaining_qty,
                entry_price=position.entry_price,
                entry_time=position.entry_time,
            )
        elif symbol in self.state.positions:
            del self.state.positions[symbol]

        if not is_partial and pnl_usd > 0:
            self.state.savings_usd += pnl_usd

        if allow_rebuy and not is_partial and pnl_usd > 0:
            await self._rebuy_after_positive_exit(symbol=symbol, entry_cost_usd=entry_cost_usd, price=price)

        if allow_rebuy and not is_partial and pnl_usd < 0:
            await self._schedule_reentry_after_loss(
                symbol=symbol,
                exited_qty=exit_qty,
                sell_price=price,
                reason=reason,
            )

    async def _resolve_live_exit_qty(self, symbol: str, fallback_qty: float) -> float:
        try:
            positions = await asyncio.to_thread(alpaca_service.list_positions)
        except Exception:
            return max(0.0, round(float(fallback_qty or 0.0), 6))

        normalized = str(symbol or '').strip().upper()
        for position in positions or []:
            current_symbol = getattr(position, 'symbol', None)
            if current_symbol is None and isinstance(position, dict):
                current_symbol = position.get('symbol')
            if str(current_symbol or '').strip().upper() != normalized:
                continue

            qty_value = getattr(position, 'qty', None)
            if qty_value is None and isinstance(position, dict):
                qty_value = position.get('qty')
            qty_parsed = self._to_positive_float(qty_value)
            if qty_parsed is not None:
                return round(qty_parsed, 6)

        return max(0.0, round(float(fallback_qty or 0.0), 6))

    async def _resolve_negative_sell_confirmations(self, symbol: str, current_price: float) -> tuple[int, Optional[float]]:
        now_mono = asyncio.get_running_loop().time()
        cache_entry = self._daily_trend_cache.get(symbol)
        if isinstance(cache_entry, dict):
            cached_at = float(cache_entry.get('cached_at') or 0.0)
            if now_mono - cached_at <= 60.0:
                required_hits = int(cache_entry.get('required_hits') or 3)
                daily_trend_pct = cache_entry.get('daily_trend_pct')
                return max(1, required_hits), daily_trend_pct if isinstance(daily_trend_pct, (int, float)) else None

        daily_trend_pct: Optional[float] = None
        required_hits = 3
        try:
            history_rows = await asyncio.to_thread(alpaca_service.history_daily_closes, symbol, 2)
            closes = [self._to_positive_float(item.get('close')) for item in (history_rows or []) if isinstance(item, dict)]
            closes = [value for value in closes if value is not None]
            previous_close = closes[-2] if len(closes) >= 2 else None

            if previous_close and previous_close > 0 and current_price > 0:
                daily_trend_pct = ((current_price - previous_close) / previous_close) * 100.0
                required_hits = 10 if daily_trend_pct > 0 else 3
        except Exception:
            required_hits = 3
            daily_trend_pct = None

        self._daily_trend_cache[symbol] = {
            'cached_at': now_mono,
            'required_hits': required_hits,
            'daily_trend_pct': daily_trend_pct,
        }
        return required_hits, daily_trend_pct

    async def _rebuy_after_positive_exit(self, symbol: str, entry_cost_usd: float, price: float) -> None:
        config = self.state.config
        if not config or price <= 0:
            return

        if await self._has_open_order_conflict(symbol=symbol, desired_side='buy'):
            self.state.pending_rebuys[symbol] = {
                'symbol': symbol,
                'entry_cost_usd': round(entry_cost_usd, 4),
                'reference_price': round(price, 4),
                'created_at': datetime.now(timezone.utc).isoformat(),
                'attempts': int((self.state.pending_rebuys.get(symbol) or {}).get('attempts') or 0),
            }
            self._append_event(
                'rebuy_after_positive_exit_deferred',
                {
                    'symbol': symbol,
                    'entry_cost_usd': round(entry_cost_usd, 4),
                    'reference_price': round(price, 4),
                    'reason': 'open_opposite_order',
                },
            )
            return

        # Reinvest only the original entry cost — profit stays in savings
        qty = round(entry_cost_usd / price, 6)
        if qty <= 0:
            return

        try:
            order_id = await self._place_order(symbol=symbol, qty=qty, side='buy', reason='positive_exit_rebuy')
            self.state.positions[symbol] = PositionState(
                qty=qty,
                entry_price=price,
                entry_time=datetime.now(timezone.utc).isoformat(),
            )
            if symbol not in config.symbols:
                config.symbols.append(symbol)
            self._append_event(
                'rebuy_after_positive_exit',
                {
                    'symbol': symbol,
                    'qty': round(qty, 6),
                    'order_id': order_id,
                    'entry_price': round(price, 4),
                    'entry_cost_usd': round(entry_cost_usd, 4),
                    'savings_usd': round(self.state.savings_usd, 2),
                },
            )
        except Exception as exc:
            self._append_event(
                'rebuy_after_positive_exit_error',
                {
                    'symbol': symbol,
                    'qty': round(qty, 6),
                    'error': str(exc),
                },
            )

    async def _process_pending_rebuys(self) -> None:
        if not self.state.pending_rebuys:
            return

        for symbol, payload in list(self.state.pending_rebuys.items()):
            if symbol in self.state.positions:
                self.state.pending_rebuys.pop(symbol, None)
                continue

            if await self._has_open_order_conflict(symbol=symbol, desired_side='buy'):
                continue

            current_price = self.state.latest_price.get(symbol)
            fallback_price = self._to_positive_float(payload.get('reference_price'))
            price = self._to_positive_float(current_price) or fallback_price
            entry_cost_usd = self._to_positive_float(payload.get('entry_cost_usd'))
            if price is None or entry_cost_usd is None:
                self.state.pending_rebuys.pop(symbol, None)
                self._append_event(
                    'rebuy_after_positive_exit_error',
                    {
                        'symbol': symbol,
                        'error': 'pending_rebuy_missing_price_or_entry_cost',
                    },
                )
                continue

            self.state.pending_rebuys[symbol]['attempts'] = int(payload.get('attempts') or 0) + 1
            await self._rebuy_after_positive_exit(symbol=symbol, entry_cost_usd=entry_cost_usd, price=price)
            if symbol in self.state.positions:
                self.state.pending_rebuys.pop(symbol, None)

    async def _schedule_reentry_after_loss(self, symbol: str, exited_qty: float, sell_price: float, reason: str) -> None:
        config = self.state.config
        qty = self._to_positive_float(exited_qty)
        price = self._to_positive_float(sell_price)
        if not config or not config.reentry_after_loss_enabled or qty is None or price is None:
            return

        now = datetime.now(timezone.utc)
        due_at = now.timestamp() + (max(0, int(config.reentry_delay_minutes or 30)) * 60)
        self.state.pending_loss_reentries[symbol] = {
            'symbol': symbol,
            'qty': round(qty, 6),
            'sell_price': round(price, 4),
            'reason': str(reason or ''),
            'scheduled_at': now.isoformat(),
            'due_at_epoch': due_at,
            'attempts': 0,
            'current_price': None,
            'trend_since_exit_pct': None,
        }
        self._append_event(
            'loss_reentry_scheduled',
            {
                'symbol': symbol,
                'qty': round(qty, 6),
                'sell_price': round(price, 4),
                'reason': str(reason or ''),
                'due_in_minutes': int(config.reentry_delay_minutes or 30),
            },
        )

    async def _process_pending_loss_reentries(self) -> None:
        if not self.state.pending_loss_reentries:
            return

        now_epoch = datetime.now(timezone.utc).timestamp()
        for symbol, payload in list(self.state.pending_loss_reentries.items()):
            due_at_epoch = float(payload.get('due_at_epoch') or 0.0)

            sell_price_for_display = self._to_positive_float(payload.get('sell_price'))
            current_price_for_display = self._to_positive_float(self.state.latest_price.get(symbol))
            if current_price_for_display is not None:
                self.state.pending_loss_reentries[symbol]['current_price'] = round(current_price_for_display, 4)
                if sell_price_for_display and sell_price_for_display > 0:
                    trend_pct = ((current_price_for_display - sell_price_for_display) / sell_price_for_display) * 100.0
                    self.state.pending_loss_reentries[symbol]['trend_since_exit_pct'] = round(trend_pct, 2)

            if now_epoch < due_at_epoch:
                continue

            if symbol in self.state.positions:
                self.state.pending_loss_reentries.pop(symbol, None)
                continue

            qty = self._to_positive_float(payload.get('qty'))
            sell_price = self._to_positive_float(payload.get('sell_price'))
            if qty is None or sell_price is None:
                self.state.pending_loss_reentries.pop(symbol, None)
                self._append_event(
                    'loss_reentry_error',
                    {
                        'symbol': symbol,
                        'error': 'pending_loss_reentry_missing_qty_or_sell_price',
                    },
                )
                continue

            current_price = self.state.latest_price.get(symbol)
            if self._to_positive_float(current_price) is None:
                fetched_price = await self._fetch_symbol_price(symbol)
                if fetched_price is not None:
                    self.state.latest_price[symbol] = float(fetched_price)
                    current_price = float(fetched_price)

            current_price_value = self._to_positive_float(current_price)
            if current_price_value is None:
                continue

            trend_since_exit_pct = ((current_price_value - sell_price) / sell_price) * 100.0
            if trend_since_exit_pct <= 0:
                self.state.pending_loss_reentries.pop(symbol, None)
                self._append_event(
                    'loss_reentry_skipped_negative_trend',
                    {
                        'symbol': symbol,
                        'qty': round(qty, 6),
                        'sell_price': round(sell_price, 4),
                        'current_price': round(current_price_value, 4),
                        'trend_since_exit_pct': round(trend_since_exit_pct, 2),
                    },
                )
                continue

            if await self._has_open_order_conflict(symbol=symbol, desired_side='buy'):
                self.state.pending_loss_reentries[symbol]['attempts'] = int(payload.get('attempts') or 0) + 1
                self.state.pending_loss_reentries[symbol]['due_at_epoch'] = now_epoch + 30.0
                self._append_event(
                    'loss_reentry_deferred',
                    {
                        'symbol': symbol,
                        'reason': 'open_opposite_order',
                    },
                )
                continue

            try:
                order_id = await self._place_order(symbol=symbol, qty=qty, side='buy', reason='loss_reentry_after_delay')
                self.state.positions[symbol] = PositionState(
                    qty=round(qty, 6),
                    entry_price=round(current_price_value, 6),
                    entry_time=datetime.now(timezone.utc).isoformat(),
                )
                config = self.state.config
                if config and symbol not in config.symbols:
                    config.symbols.append(symbol)
                self._append_event(
                    'loss_reentry_executed',
                    {
                        'symbol': symbol,
                        'qty': round(qty, 6),
                        'order_id': order_id,
                        'sell_price': round(sell_price, 4),
                        'buy_price': round(current_price_value, 4),
                        'trend_since_exit_pct': round(trend_since_exit_pct, 2),
                    },
                )
                self.state.pending_loss_reentries.pop(symbol, None)
            except Exception as exc:
                self.state.pending_loss_reentries[symbol]['attempts'] = int(payload.get('attempts') or 0) + 1
                self.state.pending_loss_reentries[symbol]['due_at_epoch'] = now_epoch + 30.0
                self._append_event(
                    'loss_reentry_error',
                    {
                        'symbol': symbol,
                        'qty': round(qty, 6),
                        'error': str(exc),
                    },
                )
                continue

    async def _has_open_order_conflict(self, symbol: str, desired_side: str) -> bool:
        desired = str(desired_side or '').strip().lower()
        normalized = str(symbol or '').strip().upper()
        if not desired or not normalized:
            return False

        try:
            open_orders = await asyncio.to_thread(alpaca_service.list_orders, 'open', 200)
        except Exception:
            return False

        for order in open_orders or []:
            order_symbol = getattr(order, 'symbol', None)
            if order_symbol is None and isinstance(order, dict):
                order_symbol = order.get('symbol')
            if str(order_symbol or '').strip().upper() != normalized:
                continue

            order_side = getattr(order, 'side', None)
            if order_side is None and isinstance(order, dict):
                order_side = order.get('side')
            normalized_side = str(order_side or '').strip().lower()
            if normalized_side and normalized_side != desired:
                return True
        return False

    async def _place_order(self, symbol: str, qty: float, side: str, reason: str) -> Optional[str]:
        config = self.state.config
        if not config:
            return None

        if config.simulation_mode:
            order_id = f'SIM-{int(datetime.now(timezone.utc).timestamp() * 1000)}'
            self._append_event(
                'order_simulated',
                {
                    'symbol': symbol,
                    'side': side,
                    'qty': round(qty, 6),
                    'reason': reason,
                    'order_id': order_id,
                },
            )
            return order_id

        try:
            result = await asyncio.to_thread(alpaca_service.place_market_order, symbol, qty, side)
            order_id = getattr(result, 'id', None) or (result.get('id') if isinstance(result, dict) else None)
            self._append_event(
                'order_sent',
                {
                    'symbol': symbol,
                    'side': side,
                    'qty': round(qty, 6),
                    'reason': reason,
                    'order_id': order_id,
                },
            )
            return None if order_id is None else str(order_id)
        except Exception as exc:
            self._append_event(
                'order_error',
                {
                    'symbol': symbol,
                    'side': side,
                    'qty': round(qty, 6),
                    'reason': reason,
                    'error': str(exc),
                },
            )
            raise

    async def _fetch_symbol_price(self, symbol: str) -> Optional[float]:
        quote_data = await asyncio.to_thread(alpaca_service.latest_quote, symbol)
        payload = quote_data.get(symbol, {}) if isinstance(quote_data, dict) else {}
        trade = self._to_positive_float(payload.get('trade_price'))
        bid = self._to_positive_float(payload.get('bid_price'))
        ask = self._to_positive_float(payload.get('ask_price'))

        if trade is not None:
            return trade
        if bid is not None and ask is not None:
            return (bid + ask) / 2
        if bid is not None:
            return bid
        if ask is not None:
            return ask
        return None

    async def _sanitize_config(self, config: TrendFollowingConfig) -> TrendFollowingConfig:
        symbols = [str(symbol or '').strip().upper() for symbol in (config.symbols or [])]
        symbols = [symbol for symbol in symbols if symbol]

        if config.use_current_positions:
            try:
                positions = await asyncio.to_thread(alpaca_service.list_positions)
            except Exception:
                positions = []
            current_symbols = []
            for position in positions or []:
                symbol = getattr(position, 'symbol', None)
                if symbol is None and isinstance(position, dict):
                    symbol = position.get('symbol')
                normalized = str(symbol or '').strip().upper()
                if normalized and normalized not in current_symbols:
                    current_symbols.append(normalized)
            if current_symbols:
                symbols = current_symbols

        if not symbols:
            raise ValueError('Aucun symbole disponible: ajoute des symboles ou ouvre des lignes.')

        unique_symbols = []
        for symbol in symbols:
            if symbol not in unique_symbols:
                unique_symbols.append(symbol)

        short_window = max(2, min(int(config.short_window or 5), 30))
        long_window = max(short_window + 1, min(int(config.long_window or 20), 120))
        poll_seconds = max(2.0, min(float(config.poll_seconds or 30.0), 60.0))
        universe_refresh_seconds = max(10.0, min(float(config.universe_refresh_seconds or 60.0), 900.0))
        take_profit_pct = max(0.1, min(float(config.take_profit_pct or 0.5), 50.0))
        stop_loss_pct = max(0.1, min(float(config.stop_loss_pct or 0.4), 20.0))
        stop_loss_confirmations = max(1, min(int(config.stop_loss_confirmations or 2), 5))
        reentry_after_loss_enabled = bool(config.reentry_after_loss_enabled)
        reentry_delay_minutes = max(0, min(int(config.reentry_delay_minutes or 30), 240))
        capital_usd = max(100.0, min(float(config.capital_usd or 10000.0), 1_000_000.0))

        return TrendFollowingConfig(
            symbols=unique_symbols,
            use_current_positions=bool(config.use_current_positions),
            simulation_mode=bool(config.simulation_mode),
            capital_usd=capital_usd,
            short_window=short_window,
            long_window=long_window,
            poll_seconds=poll_seconds,
            universe_refresh_seconds=universe_refresh_seconds,
            take_profit_pct=take_profit_pct,
            stop_loss_pct=stop_loss_pct,
            stop_loss_confirmations=stop_loss_confirmations,
            reentry_after_loss_enabled=reentry_after_loss_enabled,
            reentry_delay_minutes=reentry_delay_minutes,
        )

    async def _refresh_universe_from_positions(self) -> None:
        config = self.state.config
        if not config or not config.use_current_positions:
            return
        try:
            positions = await asyncio.to_thread(alpaca_service.list_positions)
        except Exception as exc:
            self._append_event('universe_refresh_error', {'error': str(exc)})
            return

        refreshed_symbols = []
        for position in positions or []:
            symbol = getattr(position, 'symbol', None)
            if symbol is None and isinstance(position, dict):
                symbol = position.get('symbol')
            normalized = str(symbol or '').strip().upper()
            if normalized and normalized not in refreshed_symbols:
                refreshed_symbols.append(normalized)

        previous = list(config.symbols)
        config.symbols = refreshed_symbols
        self.state.positions = self._build_position_state_from_live_positions(positions)

        for symbol in refreshed_symbols:
            if symbol not in self.state.price_history:
                self.state.price_history[symbol] = deque(maxlen=config.long_window + 5)

        if previous != refreshed_symbols:
            self._append_event(
                'universe_refreshed',
                {
                    'before': previous,
                    'after': refreshed_symbols,
                },
            )

    def _monitored_symbols(self) -> list[str]:
        config = self.state.config
        configured = list(config.symbols) if config else []
        open_positions = list(self.state.positions.keys())
        merged = []
        for symbol in configured + open_positions:
            if symbol and symbol not in merged:
                merged.append(symbol)
        return [s for s in merged if s not in self.state.paused_symbols]

    def _compute_entry_qty(self, symbol: str, price: float) -> float:
        config = self.state.config
        if not config or price <= 0:
            return 0.0

        invested = self._invested_notional_usd()
        available = max(0.0, config.capital_usd - invested)
        if available <= 0:
            return 0.0

        active_symbols = max(1, len(config.symbols))
        ticket_budget = min(config.capital_usd / active_symbols, available)
        qty = ticket_budget / price
        return max(0.0, round(qty, 6))

    def _invested_notional_usd(self) -> float:
        total = 0.0
        for position in self.state.positions.values():
            total += float(position.qty) * float(position.entry_price)
        return total

    def _build_position_state_from_live_positions(self, positions: Optional[list[Any]]) -> dict[str, PositionState]:
        adopted: dict[str, PositionState] = {}
        for raw_position in positions or []:
            symbol = getattr(raw_position, 'symbol', None)
            if symbol is None and isinstance(raw_position, dict):
                symbol = raw_position.get('symbol')
            normalized = str(symbol or '').strip().upper()
            if not normalized:
                continue

            qty_value = getattr(raw_position, 'qty', None)
            if qty_value is None and isinstance(raw_position, dict):
                qty_value = raw_position.get('qty')
            qty = self._to_positive_float(qty_value)
            if qty is None:
                continue

            entry_price_value = getattr(raw_position, 'avg_entry_price', None)
            if entry_price_value is None and isinstance(raw_position, dict):
                entry_price_value = raw_position.get('avg_entry_price')
            entry_price = self._to_positive_float(entry_price_value)
            if entry_price is None:
                continue

            entry_time_value = getattr(raw_position, 'created_at', None)
            if entry_time_value is None and isinstance(raw_position, dict):
                entry_time_value = raw_position.get('created_at')
            if isinstance(entry_time_value, datetime):
                entry_time = entry_time_value.isoformat()
            else:
                entry_time = str(entry_time_value or datetime.now(timezone.utc).isoformat())

            adopted[normalized] = PositionState(
                qty=round(qty, 6),
                entry_price=float(entry_price),
                entry_time=entry_time,
            )

        return adopted

    def _append_event(self, event_type: str, payload: dict[str, Any]) -> None:
        import uuid
        event = {
            'event_id': str(uuid.uuid4()),
            'timestamp': datetime.now(timezone.utc).isoformat(),
            'event': event_type,
            'payload': self._json_safe(payload),
        }
        self._store_event(event)
        self._persist_trade_history(event)

    def _store_event(self, event: dict[str, Any]) -> None:
        self.state.events.append(event)
        if len(self.state.events) > 300:
            self.state.events = self.state.events[-300:]

    def _persist_trade_history(self, event: dict[str, Any]) -> None:
        trade_record = self._build_trade_history_record(event)
        if not trade_record:
            return
        try:
            strategy_history_service.record_trade(trade_record)
        except Exception:
            return

    def _build_trade_history_record(self, event: dict[str, Any]) -> Optional[dict[str, Any]]:
        event_type = str(event.get('event') or '')
        payload_raw = event.get('payload')
        payload = payload_raw if isinstance(payload_raw, dict) else {}
        symbol = str(payload.get('symbol') or '').strip().upper()
        qty = self._to_positive_float(payload.get('qty'))

        side: Optional[str] = None
        price: Optional[float] = None
        reason: str = ''

        if event_type == 'entry':
            side = 'buy'
            price = self._to_positive_float(payload.get('price'))
            reason = 'trend_entry'
        elif event_type == 'rebuy_after_positive_exit':
            side = 'buy'
            price = self._to_positive_float(payload.get('entry_price'))
            reason = 'positive_exit_rebuy'
        elif event_type == 'loss_reentry_executed':
            side = 'buy'
            price = self._to_positive_float(payload.get('buy_price'))
            reason = 'loss_reentry_after_delay'
        elif event_type == 'exit':
            side = 'sell'
            price = self._to_positive_float(payload.get('exit_price'))
            reason = str(payload.get('reason') or 'exit')

        if not symbol or not side or qty is None or price is None:
            return None

        config = self.state.config
        return {
            'event_id': str(event.get('event_id') or ''),
            'recorded_at': str(event.get('timestamp') or ''),
            'strategy_name': 'trend_following',
            'source_event': event_type,
            'side': side,
            'symbol': symbol,
            'qty': round(qty, 6),
            'price': round(price, 6),
            'reason': reason,
            'order_id': payload.get('order_id'),
            'simulation_mode': bool(config.simulation_mode) if config else False,
            'pnl_usd': payload.get('pnl_usd'),
            'pnl_pct': payload.get('pnl_pct'),
            'metadata': payload,
        }

    @classmethod
    def _json_safe(cls, value: Any) -> Any:
        if value is None or isinstance(value, (str, int, float, bool)):
            return value
        if isinstance(value, UUID):
            return str(value)
        if isinstance(value, datetime):
            return value.isoformat()
        if isinstance(value, dict):
            return {str(key): cls._json_safe(val) for key, val in value.items()}
        if isinstance(value, (list, tuple, set)):
            return [cls._json_safe(item) for item in value]
        return str(value)

    def _build_monitoring_summary(self, events: list[dict[str, Any]]) -> dict[str, Any]:
        summary = {
            'events_total': len(events),
            'ticks_count': 0,
            'last_tick_at': None,
            'orders_sent': 0,
            'order_errors': 0,
            'evaluation_errors': 0,
            'rebuy_success': 0,
            'rebuy_errors': 0,
            'entries': 0,
            'exits': 0,
            'exits_take_profit': 0,
            'exits_stop_loss': 0,
            'exits_trend_reversal': 0,
            'universe_refreshes': 0,
            'price_errors': 0,
            'avg_cycle_ms': None,
            'last_cycle_ms': None,
            'max_cycle_ms': None,
            'pnl_by_symbol': [],
        }

        cycle_values = []
        pnl_by_symbol = {}
        for event in events:
            event_type = event.get('event')
            payload_raw = event.get('payload')
            payload: dict[str, Any] = payload_raw if isinstance(payload_raw, dict) else {}

            if event_type == 'loop_cycle':
                summary['ticks_count'] += 1
                summary['last_tick_at'] = event.get('timestamp')
                duration = self._to_positive_float(payload.get('duration_ms'))
                if duration is not None:
                    cycle_values.append(duration)
            elif event_type in {'order_sent', 'order_simulated'}:
                summary['orders_sent'] += 1
            elif event_type == 'order_error':
                summary['order_errors'] += 1
            elif event_type == 'evaluation_error':
                summary['evaluation_errors'] += 1
            elif event_type == 'rebuy_after_positive_exit':
                summary['rebuy_success'] += 1
            elif event_type == 'rebuy_after_positive_exit_error':
                summary['rebuy_errors'] += 1
            elif event_type == 'entry':
                summary['entries'] += 1
            elif event_type == 'exit':
                summary['exits'] += 1
                reason = str(payload.get('reason', ''))
                if reason.startswith('take_profit'):
                    summary['exits_take_profit'] += 1
                elif reason.startswith('stop_loss'):
                    summary['exits_stop_loss'] += 1
                elif reason.startswith('trend_reversal'):
                    summary['exits_trend_reversal'] += 1

                symbol = str(payload.get('symbol', '')).upper()
                if symbol:
                    pnl_value = float(payload.get('pnl_usd') or 0.0)
                    if symbol not in pnl_by_symbol:
                        pnl_by_symbol[symbol] = {'symbol': symbol, 'realized_margin_usd': 0.0, 'exits': 0}
                    pnl_by_symbol[symbol]['realized_margin_usd'] += pnl_value
                    pnl_by_symbol[symbol]['exits'] += 1
            elif event_type == 'universe_refreshed':
                summary['universe_refreshes'] += 1
            elif event_type == 'price_error':
                summary['price_errors'] += 1

        if cycle_values:
            summary['avg_cycle_ms'] = round(sum(cycle_values) / len(cycle_values), 2)
            summary['last_cycle_ms'] = round(cycle_values[-1], 2)
            summary['max_cycle_ms'] = round(max(cycle_values), 2)

        summary['pnl_by_symbol'] = sorted(
            [
                {
                    'symbol': item['symbol'],
                    'realized_margin_usd': round(item['realized_margin_usd'], 2),
                    'exits': item['exits'],
                }
                for item in pnl_by_symbol.values()
            ],
            key=lambda row: row['realized_margin_usd'],
            reverse=True,
        )
        return summary

    async def _broadcast(self, message: dict[str, Any]) -> None:
        if not self._clients:
            return
        dead_clients = []
        for client in list(self._clients):
            try:
                await client.send_json(message)
            except Exception:
                dead_clients.append(client)
        for client in dead_clients:
            await self.unsubscribe(client)

    @staticmethod
    def _to_positive_float(value: Any) -> Optional[float]:
        try:
            parsed = float(value)
            return parsed if parsed > 0 else None
        except (TypeError, ValueError):
            return None

    def _compute_unrealized_pct(self, symbol: str, position: PositionState) -> Optional[float]:
        current = self.state.latest_price.get(symbol)
        if current is None or not position.entry_price:
            return None
        return round(((current - position.entry_price) / position.entry_price) * 100, 2)

    @staticmethod
    def _config_to_dict(config: Optional[TrendFollowingConfig]) -> Optional[dict]:
        if not config:
            return None
        return {
            'symbols': config.symbols,
            'use_current_positions': config.use_current_positions,
            'simulation_mode': config.simulation_mode,
            'capital_usd': config.capital_usd,
            'short_window': config.short_window,
            'long_window': config.long_window,
            'poll_seconds': config.poll_seconds,
            'universe_refresh_seconds': config.universe_refresh_seconds,
            'take_profit_pct': config.take_profit_pct,
            'stop_loss_pct': config.stop_loss_pct,
            'stop_loss_confirmations': config.stop_loss_confirmations,
            'reentry_after_loss_enabled': config.reentry_after_loss_enabled,
            'reentry_delay_minutes': config.reentry_delay_minutes,
        }


trend_following_service = TrendFollowingService()
