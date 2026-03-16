from datetime import datetime, timedelta, timezone
import httpx

from alpaca.data.historical.stock import StockHistoricalDataClient
from alpaca.data.requests import StockBarsRequest, StockLatestQuoteRequest, StockLatestTradeRequest
from alpaca.data.timeframe import TimeFrame, TimeFrameUnit
from alpaca.trading.client import TradingClient
from alpaca.trading.enums import OrderSide, QueryOrderStatus, TimeInForce
from alpaca.trading.requests import GetOrdersRequest, MarketOrderRequest

from ceibo_finance.core.config import settings


class AlpacaService:
    def __init__(self) -> None:
        self.trading = TradingClient(
            api_key=settings.alpaca_api_key,
            secret_key=settings.alpaca_api_secret,
            paper=settings.alpaca_paper,
        )
        self.data = StockHistoricalDataClient(
            api_key=settings.alpaca_api_key,
            secret_key=settings.alpaca_api_secret,
        )

    def get_account(self):
        return self.trading.get_account()

    def list_positions(self):
        return self.trading.get_all_positions()

    def list_orders(self, status: str = 'open', limit: int = 50):
        parsed_status = QueryOrderStatus.OPEN if status.lower() == 'open' else QueryOrderStatus.ALL
        request = GetOrdersRequest(status=parsed_status, limit=limit)
        return self.trading.get_orders(filter=request)

    def list_account_activities(self, limit: int = 100) -> list[dict]:
        safe_limit = min(max(1, int(limit or 100)), 500)
        base_url = (
            'https://paper-api.alpaca.markets'
            if settings.alpaca_paper
            else 'https://api.alpaca.markets'
        )
        try:
            resp = httpx.get(
                f'{base_url}/v2/account/activities',
                params={
                    'direction': 'desc',
                    'page_size': safe_limit,
                },
                headers={
                    'APCA-API-KEY-ID': settings.alpaca_api_key,
                    'APCA-API-SECRET-KEY': settings.alpaca_api_secret,
                },
                timeout=10.0,
            )
            resp.raise_for_status()
            payload = resp.json()
            return payload if isinstance(payload, list) else []
        except Exception:
            return []

    def place_market_order(self, symbol: str, qty: float, side: str):
        order = MarketOrderRequest(
            symbol=symbol.upper(),
            qty=qty,
            side=OrderSide.BUY if side.lower() == 'buy' else OrderSide.SELL,
            time_in_force=TimeInForce.DAY,
        )
        return self.trading.submit_order(order)

    def latest_quote(self, symbol: str):
        normalized_symbol = symbol.upper()
        primary_feed = settings.alpaca_data_feed

        primary_entry = self._fetch_latest_quote_trade(symbol=normalized_symbol, feed=primary_feed)
        selected_entry = primary_entry
        selected_feed = primary_feed
        fallback_error = None

        if not self._is_quote_complete(primary_entry) and primary_feed != 'sip':
            try:
                sip_entry = self._fetch_latest_quote_trade(symbol=normalized_symbol, feed='sip')
                if self._is_quote_better(primary_entry, sip_entry):
                    selected_entry = sip_entry
                    selected_feed = 'sip'
            except Exception as exc:
                fallback_error = str(exc)

        merged_entry = {
            **selected_entry,
            'data_feed_requested': primary_feed,
            'data_feed_used': selected_feed,
        }
        metadata = self.get_assets_metadata([normalized_symbol]).get(normalized_symbol, {})
        if metadata:
            merged_entry['company_name'] = metadata.get('name')
            merged_entry['exchange'] = metadata.get('exchange')
        if fallback_error:
            merged_entry['feed_fallback_error'] = fallback_error

        return {normalized_symbol: merged_entry}

    def history_intraday_closes(self, symbol: str, minutes: int = 60, timeframe_minutes: int = 5):
        safe_minutes = max(15, min(int(minutes or 60), 240))
        safe_timeframe = max(1, min(int(timeframe_minutes or 5), 30))
        end = datetime.now(timezone.utc)
        start = end - timedelta(minutes=safe_minutes + safe_timeframe * 2)

        request = StockBarsRequest(
            symbol_or_symbols=symbol.upper(),
            timeframe=TimeFrame(safe_timeframe, TimeFrameUnit.Minute),
            start=start,
            end=end,
            feed=settings.alpaca_data_feed,
        )
        result = self.data.get_stock_bars(request)
        bars = result.data.get(symbol.upper(), []) if hasattr(result, 'data') else []

        return [
            {
                'timestamp': bar.timestamp.isoformat() if getattr(bar, 'timestamp', None) else None,
                'close': float(bar.close) if getattr(bar, 'close', None) is not None else None,
            }
            for bar in bars
        ]

    def _fetch_latest_quote_trade(self, symbol: str, feed: str) -> dict:
        req = StockLatestQuoteRequest(
            symbol_or_symbols=symbol,
            feed=feed,
        )
        quote_result = self.data.get_stock_latest_quote(req)

        trade_req = StockLatestTradeRequest(
            symbol_or_symbols=symbol,
            feed=feed,
        )
        trade_result = self.data.get_stock_latest_trade(trade_req)

        quote_payload = quote_result.model_dump() if hasattr(quote_result, 'model_dump') else quote_result
        trade_payload = trade_result.model_dump() if hasattr(trade_result, 'model_dump') else trade_result

        quote_entry = quote_payload.get(symbol, {}) if isinstance(quote_payload, dict) else {}
        trade_entry = trade_payload.get(symbol, {}) if isinstance(trade_payload, dict) else {}

        if hasattr(quote_entry, 'model_dump'):
            quote_entry = quote_entry.model_dump()
        if hasattr(trade_entry, 'model_dump'):
            trade_entry = trade_entry.model_dump()

        return {
            **(quote_entry if isinstance(quote_entry, dict) else {}),
            'trade_price': (trade_entry or {}).get('price') if isinstance(trade_entry, dict) else None,
            'trade_size': (trade_entry or {}).get('size') if isinstance(trade_entry, dict) else None,
            'trade_timestamp': (trade_entry or {}).get('timestamp') if isinstance(trade_entry, dict) else None,
            'data_feed': feed,
        }

    @staticmethod
    def _is_quote_complete(entry: dict) -> bool:
        bid = float(entry.get('bid_price') or 0)
        ask = float(entry.get('ask_price') or 0)
        return bid > 0 and ask > 0

    @staticmethod
    def _is_quote_better(primary_entry: dict, candidate_entry: dict) -> bool:
        primary_complete = AlpacaService._is_quote_complete(primary_entry)
        candidate_complete = AlpacaService._is_quote_complete(candidate_entry)
        if candidate_complete and not primary_complete:
            return True

        primary_trade = float(primary_entry.get('trade_price') or 0)
        candidate_trade = float(candidate_entry.get('trade_price') or 0)
        return candidate_trade > 0 and primary_trade <= 0

    def history_daily_closes(self, symbol: str, days: int = 5):
        safe_days = max(1, min(days, 30))
        end = datetime.now(timezone.utc)
        start = end - timedelta(days=safe_days * 4)

        request = StockBarsRequest(
            symbol_or_symbols=symbol.upper(),
            timeframe=TimeFrame.Day,
            start=start,
            end=end,
            feed=settings.alpaca_data_feed,
        )
        result = self.data.get_stock_bars(request)
        bars = result.data.get(symbol.upper(), []) if hasattr(result, 'data') else []
        if bars:
            bars = sorted(bars, key=lambda bar: getattr(bar, 'timestamp', datetime.min.replace(tzinfo=timezone.utc)))
            bars = bars[-safe_days:]

        return [
            {
                'timestamp': bar.timestamp.isoformat() if getattr(bar, 'timestamp', None) else None,
                'close': float(bar.close) if getattr(bar, 'close', None) is not None else None,
            }
            for bar in bars
        ]


    def search_assets(self, query: str, limit: int = 10) -> list:
        q = (query or '').strip()
        if not q:
            return []

        safe_limit = min(max(1, int(limit or 10)), 20)
        q_upper = q.upper()
        q_lower = q.lower()
        base_url = (
            'https://paper-api.alpaca.markets'
            if settings.alpaca_paper
            else 'https://api.alpaca.markets'
        )

        try:
            resp = httpx.get(
                f'{base_url}/v2/assets',
                params={
                    'status': 'active',
                    'asset_class': 'us_equity',
                },
                headers={
                    'APCA-API-KEY-ID': settings.alpaca_api_key,
                    'APCA-API-SECRET-KEY': settings.alpaca_api_secret,
                },
                timeout=8.0,
            )
            resp.raise_for_status()
            assets = resp.json()
            if not isinstance(assets, list):
                return []

            ranked = []
            for asset in assets:
                if not isinstance(asset, dict):
                    continue
                if not asset.get('tradable', False):
                    continue
                symbol = str(asset.get('symbol', '')).strip().upper()
                name = str(asset.get('name', '')).strip()
                exchange = str(asset.get('exchange', '')).strip()
                if not symbol:
                    continue

                score = self._asset_match_score(
                    symbol=symbol,
                    name=name,
                    exchange=exchange,
                    query_upper=q_upper,
                    query_lower=q_lower,
                )
                if score <= 0:
                    continue

                ranked.append((
                    score,
                    {
                        'symbol': symbol,
                        'name': name,
                        'exchange': exchange,
                    },
                ))

            ranked.sort(key=lambda item: (-item[0], item[1]['symbol']))
            return [item[1] for item in ranked[:safe_limit]]
        except Exception:
            return []

    def get_assets_metadata(self, symbols: list[str]) -> dict[str, dict]:
        normalized_symbols = {str(symbol or '').strip().upper() for symbol in (symbols or []) if str(symbol or '').strip()}
        if not normalized_symbols:
            return {}

        base_url = (
            'https://paper-api.alpaca.markets'
            if settings.alpaca_paper
            else 'https://api.alpaca.markets'
        )

        try:
            resp = httpx.get(
                f'{base_url}/v2/assets',
                params={
                    'status': 'active',
                    'asset_class': 'us_equity',
                },
                headers={
                    'APCA-API-KEY-ID': settings.alpaca_api_key,
                    'APCA-API-SECRET-KEY': settings.alpaca_api_secret,
                },
                timeout=8.0,
            )
            resp.raise_for_status()
            assets = resp.json()
            if not isinstance(assets, list):
                return {}

            metadata = {}
            for asset in assets:
                if not isinstance(asset, dict):
                    continue
                symbol = str(asset.get('symbol', '')).strip().upper()
                if symbol not in normalized_symbols:
                    continue
                metadata[symbol] = {
                    'symbol': symbol,
                    'name': str(asset.get('name', '')).strip(),
                    'exchange': str(asset.get('exchange', '')).strip(),
                    'tradable': bool(asset.get('tradable', False)),
                }
            return metadata
        except Exception:
            return {}

    @staticmethod
    def _asset_match_score(symbol: str, name: str, exchange: str, query_upper: str, query_lower: str) -> int:
        symbol_upper = symbol.upper()
        name_lower = name.lower()

        score = 0
        if symbol_upper == query_upper:
            score += 1000
        elif symbol_upper.startswith(query_upper):
            score += 850
        elif query_upper in symbol_upper:
            score += 650

        if name_lower.startswith(query_lower):
            score += 500
        elif f' {query_lower}' in name_lower:
            score += 420
        elif query_lower in name_lower:
            score += 260

        if AlpacaService._is_subsequence(query_upper, symbol_upper):
            score += 140
        elif AlpacaService._is_subsequence(query_lower, name_lower):
            score += 100

        if 2 <= len(query_upper) <= 6 and 1 <= len(symbol_upper) <= 6:
            distance = AlpacaService._levenshtein_distance(query_upper, symbol_upper)
            if distance == 0:
                score += 200
            elif distance == 1:
                score += 420
            elif distance == 2:
                score += 180

        if exchange in {'NASDAQ', 'NYSE', 'ARCA'}:
            score += 15

        return score

    @staticmethod
    def _is_subsequence(needle: str, haystack: str) -> bool:
        if not needle:
            return True
        cursor = 0
        for char in haystack:
            if cursor < len(needle) and char == needle[cursor]:
                cursor += 1
                if cursor == len(needle):
                    return True
        return False

    @staticmethod
    def _levenshtein_distance(left: str, right: str) -> int:
        if left == right:
            return 0
        if not left:
            return len(right)
        if not right:
            return len(left)

        previous_row = list(range(len(right) + 1))
        for i, left_char in enumerate(left, start=1):
            current_row = [i]
            for j, right_char in enumerate(right, start=1):
                insertions = previous_row[j] + 1
                deletions = current_row[j - 1] + 1
                substitutions = previous_row[j - 1] + (left_char != right_char)
                current_row.append(min(insertions, deletions, substitutions))
            previous_row = current_row
        return previous_row[-1]


alpaca_service = AlpacaService()
