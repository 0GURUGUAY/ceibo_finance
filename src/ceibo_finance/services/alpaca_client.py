from alpaca.data.historical.stock import StockHistoricalDataClient
from alpaca.data.requests import StockLatestQuoteRequest
from alpaca.trading.client import TradingClient
from alpaca.trading.enums import OrderSide, OrderStatus, TimeInForce
from alpaca.trading.requests import MarketOrderRequest

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
        parsed_status = OrderStatus.OPEN if status.lower() == 'open' else OrderStatus.ALL
        return self.trading.get_orders(status=parsed_status, limit=limit)

    def place_market_order(self, symbol: str, qty: float, side: str):
        order = MarketOrderRequest(
            symbol=symbol.upper(),
            qty=qty,
            side=OrderSide.BUY if side.lower() == 'buy' else OrderSide.SELL,
            time_in_force=TimeInForce.DAY,
        )
        return self.trading.submit_order(order)

    def latest_quote(self, symbol: str):
        req = StockLatestQuoteRequest(
            symbol_or_symbols=symbol.upper(),
            feed=settings.alpaca_data_feed,
        )
        return self.data.get_stock_latest_quote(req)


alpaca_service = AlpacaService()
