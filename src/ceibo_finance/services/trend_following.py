import asyncio
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Optional
from uuid import UUID

from fastapi import WebSocket

from ceibo_finance.services.alpaca_client import alpaca_service


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
    events: list[dict[str, Any]] = field(default_factory=list)
    realized_margin_usd: float = 0.0


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
            self.state = TrendFollowingState(
                running=True,
                started_at=datetime.now(timezone.utc).isoformat(),
                config=sanitized,
                positions={},
                price_history={symbol: deque(maxlen=sanitized.long_window + 5) for symbol in sanitized.symbols},
                latest_price={},
                sell_signal_hits={},
                positive_trend_partial_exit_stage={},
                events=[],
                realized_margin_usd=0.0,
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
            'capital_usd': round(capital, 2),
            'invested_usd': round(invested, 2),
            'available_investable_usd': round(available, 2),
            'realized_margin_usd': round(self.state.realized_margin_usd, 2),
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
            while self.state.running and self.state.config:
                loop_started = asyncio.get_running_loop().time()
                config = self.state.config
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
        if len(history) < config.long_window:
            return

        short_slice = list(history)[-config.short_window:]
        long_slice = list(history)[-config.long_window:]
        short_ma = sum(short_slice) / len(short_slice)
        long_ma = sum(long_slice) / len(long_slice)

        position = self.state.positions.get(symbol)
        if position is None:
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
                await self._place_order(symbol=symbol, qty=qty, side='buy', reason='trend_entry')
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
        elif short_ma < long_ma:
            negative_reason = 'trend_reversal'

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

        if symbol in self.state.positive_trend_partial_exit_stage and short_ma >= long_ma and price > stop_loss:
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

        await self._place_order(symbol=symbol, qty=exit_qty, side='sell', reason=reason)
        pnl_usd = (price - position.entry_price) * exit_qty
        pnl_pct = ((price - position.entry_price) / position.entry_price) * 100 if position.entry_price else 0.0
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
                'estimated_remaining_qty': estimated_remaining_qty,
                'is_partial': is_partial,
                'pnl_pct': round(pnl_pct, 2),
                'pnl_usd': round(pnl_usd, 2),
                'realized_margin_usd': round(self.state.realized_margin_usd, 2),
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

        if allow_rebuy and not is_partial and pnl_usd > 0:
            await self._rebuy_after_positive_exit(symbol=symbol, qty=exit_qty, price=price)

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

    async def _rebuy_after_positive_exit(self, symbol: str, qty: float, price: float) -> None:
        config = self.state.config
        if not config:
            return

        try:
            await self._place_order(symbol=symbol, qty=qty, side='buy', reason='positive_exit_rebuy')
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
                    'entry_price': round(price, 4),
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

    async def _place_order(self, symbol: str, qty: float, side: str, reason: str) -> None:
        config = self.state.config
        if not config:
            return

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
            return

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
        return merged

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

    def _append_event(self, event_type: str, payload: dict[str, Any]) -> None:
        event = {
            'timestamp': datetime.now(timezone.utc).isoformat(),
            'event': event_type,
            'payload': self._json_safe(payload),
        }
        self.state.events.append(event)
        if len(self.state.events) > 300:
            self.state.events = self.state.events[-300:]

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
        }


trend_following_service = TrendFollowingService()
