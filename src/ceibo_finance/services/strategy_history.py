import json
import sqlite3
from pathlib import Path
from typing import Any, Optional

from ceibo_finance.core.config import settings


class StrategyHistoryService:
    def __init__(self, db_path: Optional[str] = None) -> None:
        configured_path = db_path or settings.strategy_history_db_path
        self._db_path = Path(configured_path)
        self._ensure_db()

    def _ensure_db(self) -> None:
        self._db_path.parent.mkdir(parents=True, exist_ok=True)
        with sqlite3.connect(self._db_path) as conn:
            conn.execute(
                '''
                CREATE TABLE IF NOT EXISTS strategy_trade_history (
                    event_id TEXT PRIMARY KEY,
                    recorded_at TEXT NOT NULL,
                    strategy_name TEXT NOT NULL,
                    source_event TEXT NOT NULL,
                    side TEXT NOT NULL,
                    symbol TEXT NOT NULL,
                    qty REAL NOT NULL,
                    price REAL NOT NULL,
                    reason TEXT NOT NULL,
                    order_id TEXT,
                    simulation_mode INTEGER NOT NULL DEFAULT 0,
                    pnl_usd REAL,
                    pnl_pct REAL,
                    metadata_json TEXT NOT NULL
                )
                '''
            )
            conn.execute(
                'CREATE INDEX IF NOT EXISTS idx_strategy_trade_history_time ON strategy_trade_history(recorded_at DESC)'
            )
            conn.execute(
                'CREATE INDEX IF NOT EXISTS idx_strategy_trade_history_symbol_time ON strategy_trade_history(symbol, recorded_at DESC)'
            )

    def record_trade(self, trade: dict[str, Any]) -> None:
        with sqlite3.connect(self._db_path) as conn:
            conn.execute(
                '''
                INSERT OR IGNORE INTO strategy_trade_history(
                    event_id,
                    recorded_at,
                    strategy_name,
                    source_event,
                    side,
                    symbol,
                    qty,
                    price,
                    reason,
                    order_id,
                    simulation_mode,
                    pnl_usd,
                    pnl_pct,
                    metadata_json
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''',
                (
                    str(trade.get('event_id') or ''),
                    str(trade.get('recorded_at') or ''),
                    str(trade.get('strategy_name') or ''),
                    str(trade.get('source_event') or ''),
                    str(trade.get('side') or ''),
                    str(trade.get('symbol') or ''),
                    float(trade.get('qty') or 0.0),
                    float(trade.get('price') or 0.0),
                    str(trade.get('reason') or ''),
                    (None if trade.get('order_id') in (None, '') else str(trade.get('order_id'))),
                    1 if bool(trade.get('simulation_mode')) else 0,
                    (None if trade.get('pnl_usd') is None else float(trade.get('pnl_usd'))),
                    (None if trade.get('pnl_pct') is None else float(trade.get('pnl_pct'))),
                    json.dumps(trade.get('metadata') or {}, ensure_ascii=False),
                ),
            )

    def list_trades(self, limit: int = 200, symbol: Optional[str] = None, side: Optional[str] = None) -> list[dict[str, Any]]:
        safe_limit = max(1, min(int(limit or 200), 1000))
        conditions: list[str] = []
        params: list[Any] = []

        normalized_symbol = str(symbol or '').strip().upper()
        if normalized_symbol:
            conditions.append('symbol = ?')
            params.append(normalized_symbol)

        normalized_side = str(side or '').strip().lower()
        if normalized_side in {'buy', 'sell'}:
            conditions.append('side = ?')
            params.append(normalized_side)

        where_clause = f"WHERE {' AND '.join(conditions)}" if conditions else ''

        query = f'''
            SELECT event_id, recorded_at, strategy_name, source_event, side, symbol, qty, price, reason,
                   order_id, simulation_mode, pnl_usd, pnl_pct, metadata_json
            FROM strategy_trade_history
            {where_clause}
            ORDER BY recorded_at DESC, event_id DESC
            LIMIT ?
        '''
        params.append(safe_limit)

        with sqlite3.connect(self._db_path) as conn:
            conn.row_factory = sqlite3.Row
            rows = conn.execute(query, params).fetchall()

        return [self._row_to_dict(row) for row in rows]

    def clear_trades(self) -> dict[str, Any]:
        with sqlite3.connect(self._db_path) as conn:
            before_count = conn.execute('SELECT COUNT(*) FROM strategy_trade_history').fetchone()[0]
            conn.execute('DELETE FROM strategy_trade_history')
            after_count = conn.execute('SELECT COUNT(*) FROM strategy_trade_history').fetchone()[0]

        return {
            'ok': True,
            'deleted_count': int(before_count or 0),
            'remaining_count': int(after_count or 0),
        }

    @staticmethod
    def _row_to_dict(row: sqlite3.Row) -> dict[str, Any]:
        metadata_raw = row['metadata_json'] if 'metadata_json' in row.keys() else '{}'
        try:
            metadata = json.loads(metadata_raw or '{}')
        except Exception:
            metadata = {}

        return {
            'event_id': row['event_id'],
            'recorded_at': row['recorded_at'],
            'strategy_name': row['strategy_name'],
            'source_event': row['source_event'],
            'side': row['side'],
            'symbol': row['symbol'],
            'qty': float(row['qty']),
            'price': float(row['price']),
            'reason': row['reason'],
            'order_id': row['order_id'],
            'simulation_mode': bool(row['simulation_mode']),
            'pnl_usd': None if row['pnl_usd'] is None else float(row['pnl_usd']),
            'pnl_pct': None if row['pnl_pct'] is None else float(row['pnl_pct']),
            'metadata': metadata,
        }


strategy_history_service = StrategyHistoryService()