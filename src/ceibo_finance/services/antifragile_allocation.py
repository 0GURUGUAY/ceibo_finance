from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import numpy as np
import pandas as pd
import yfinance as yf


DEFAULT_SECTOR_ETFS = ['XLB', 'XLC', 'XLE', 'XLF', 'XLI', 'XLK', 'XLP', 'XLRE', 'XLU', 'XLV', 'XLY']
DEFAULT_DEFENSIVE_ETFS = ['TLT', 'IEF', 'SHY', 'TIP', 'GLD', 'UUP', 'BIL']
DEFAULT_CASH_SYMBOL = 'BIL'


@dataclass
class RankingWeights:
    momentum: float = 1.0
    volatility: float = 1.0
    correlation: float = 1.0
    trend: float = 1.0


def compute_momentum(close_prices: pd.DataFrame | pd.Series, months: int = 4, trading_days_per_month: int = 21) -> pd.DataFrame | pd.Series:
    periods = int(months * trading_days_per_month)
    return close_prices.pct_change(periods=periods)


def compute_volatility(close_prices: pd.DataFrame | pd.Series, span: int = 20, annualization_factor: int = 252) -> pd.DataFrame | pd.Series:
    returns = close_prices.pct_change()
    ewma_var = returns.pow(2).ewm(span=span, adjust=False).mean()
    return np.sqrt(ewma_var) * np.sqrt(float(annualization_factor))


def compute_correlation(returns: pd.DataFrame, window: int = 84) -> pd.Series:
    trailing_returns = returns.tail(window)
    if trailing_returns.empty or trailing_returns.shape[1] <= 1:
        return pd.Series(dtype=float)
    corr = trailing_returns.corr()
    avg_corr: dict[str, float] = {}
    for symbol in corr.columns:
        peers = corr.loc[symbol].drop(index=symbol, errors='ignore')
        avg_corr[symbol] = float(peers.mean()) if not peers.empty else np.nan
    return pd.Series(avg_corr, dtype=float)


def compute_atr(high: pd.Series, low: pd.Series, close: pd.Series, period: int = 42) -> pd.Series:
    prev_close = close.shift(1)
    tr = pd.concat(
        [
            (high - low).abs(),
            (high - prev_close).abs(),
            (low - prev_close).abs(),
        ],
        axis=1,
    ).max(axis=1)
    return tr.rolling(window=period, min_periods=period).mean()


def rank_assets(metrics: pd.DataFrame, weights: RankingWeights | None = None) -> pd.DataFrame:
    if metrics.empty:
        return metrics.copy()

    weights = weights or RankingWeights()
    working = metrics.replace([np.inf, -np.inf], np.nan).dropna(subset=['momentum', 'volatility', 'correlation', 'trend'])
    if working.empty:
        return working

    momentum_rank = working['momentum'].rank(pct=True, ascending=True)
    volatility_rank = working['volatility'].rank(pct=True, ascending=False)
    correlation_rank = working['correlation'].rank(pct=True, ascending=False)

    n_assets = max(1, int(len(working.index)))
    working = working.assign(
        momentum_rank=momentum_rank,
        volatility_rank=volatility_rank,
        correlation_rank=correlation_rank,
    )

    working['trank'] = (
        (weights.momentum * working['momentum_rank'])
        + (weights.volatility * working['volatility_rank'])
        + (weights.correlation * working['correlation_rank'])
        - (weights.trend * working['trend'])
        + (working['momentum'] / float(n_assets))
    )

    return working.sort_values('trank', ascending=False)


def _download_ohlc(symbols: list[str], start: str | None = None, end: str | None = None) -> dict[str, pd.DataFrame]:
    if not symbols:
        raise ValueError('At least one symbol is required.')

    data = yf.download(
        tickers=symbols,
        start=start,
        end=end,
        interval='1d',
        auto_adjust=True,
        progress=False,
        group_by='column',
    )
    if data.empty:
        raise ValueError('No market data returned from yfinance.')

    if isinstance(data.columns, pd.MultiIndex):
        close = data['Close'].copy()
        high = data['High'].copy()
        low = data['Low'].copy()
    else:
        close = data[['Close']].rename(columns={'Close': symbols[0]})
        high = data[['High']].rename(columns={'High': symbols[0]})
        low = data[['Low']].rename(columns={'Low': symbols[0]})

    return {'close': close.dropna(how='all'), 'high': high.dropna(how='all'), 'low': low.dropna(how='all')}


def _compute_trend_signal(close_slice: pd.DataFrame, high_slice: pd.DataFrame, low_slice: pd.DataFrame) -> pd.Series:
    trend_values: dict[str, float] = {}
    for symbol in close_slice.columns:
        close_series = close_slice[symbol].dropna()
        high_series = high_slice[symbol].dropna()
        low_series = low_slice[symbol].dropna()

        if len(close_series) < 105 or len(high_series) < 42 or len(low_series) < 105:
            trend_values[symbol] = np.nan
            continue

        atr = compute_atr(high_series, low_series, close_series, period=42).iloc[-1]
        highest_close = close_series.tail(63).max()
        highest_low = low_series.tail(105).max()

        upper_band = float(atr + highest_close)
        lower_band = float(atr + highest_low)
        last_close = float(close_series.iloc[-1])

        trend_values[symbol] = 1.0 if (last_close > upper_band or last_close < lower_band) else 0.0

    return pd.Series(trend_values, dtype=float)


def _latest_metrics(close_slice: pd.DataFrame, high_slice: pd.DataFrame, low_slice: pd.DataFrame) -> pd.DataFrame:
    momentum = compute_momentum(close_slice).iloc[-1]
    volatility = compute_volatility(close_slice).iloc[-1]
    returns = close_slice.pct_change().dropna(how='all')
    correlation = compute_correlation(returns, window=84)
    trend = _compute_trend_signal(close_slice, high_slice, low_slice)

    return pd.DataFrame(
        {
            'momentum': momentum,
            'volatility': volatility,
            'correlation': correlation,
            'trend': trend,
        }
    )


def sector_rotation_model(
    close_slice: pd.DataFrame,
    high_slice: pd.DataFrame,
    low_slice: pd.DataFrame,
    sector_symbols: list[str],
    weights: RankingWeights | None = None,
    selection_size: int = 5,
) -> dict[str, Any]:
    metrics = _latest_metrics(close_slice[sector_symbols], high_slice[sector_symbols], low_slice[sector_symbols])
    ranked = rank_assets(metrics, weights=weights)
    selected = ranked.head(selection_size)

    selected_symbols = list(selected.index)
    if not selected_symbols:
        return {
            'selected': [],
            'scores': ranked,
            'sector_allocations': {},
            'transfer_to_hedge': 1.0,
            'all_negative': True,
        }

    per_asset_weight = 1.0 / float(selection_size)
    all_negative = bool((selected['momentum'] <= 0).all())
    transfer_to_hedge = 1.0 if all_negative else 0.0
    allocations: dict[str, float] = {}

    if not all_negative:
        for symbol, row in selected.iterrows():
            if float(row['momentum']) > 0:
                allocations[symbol] = allocations.get(symbol, 0.0) + per_asset_weight
            else:
                transfer_to_hedge += per_asset_weight

    return {
        'selected': selected_symbols,
        'scores': ranked,
        'sector_allocations': allocations,
        'transfer_to_hedge': float(transfer_to_hedge),
        'all_negative': all_negative,
    }


def black_swan_model(
    close_slice: pd.DataFrame,
    high_slice: pd.DataFrame,
    low_slice: pd.DataFrame,
    defensive_symbols: list[str],
    cash_symbol: str,
    model_budget: float,
    weights: RankingWeights | None = None,
    selection_size: int = 3,
) -> dict[str, Any]:
    if model_budget <= 0:
        return {'selected': [], 'scores': pd.DataFrame(), 'allocations': {}}

    metrics = _latest_metrics(close_slice[defensive_symbols], high_slice[defensive_symbols], low_slice[defensive_symbols])
    ranked = rank_assets(metrics, weights=weights)
    selected = ranked.head(selection_size)

    if selected.empty:
        return {
            'selected': [],
            'scores': ranked,
            'allocations': {cash_symbol: float(model_budget)},
        }

    per_asset_weight = float(model_budget) / float(selection_size)
    allocations: dict[str, float] = {}

    if bool((selected['momentum'] <= 0).all()):
        allocations[cash_symbol] = float(model_budget)
    else:
        for symbol, row in selected.iterrows():
            if float(row['momentum']) > 0:
                allocations[symbol] = allocations.get(symbol, 0.0) + per_asset_weight
            else:
                allocations[cash_symbol] = allocations.get(cash_symbol, 0.0) + per_asset_weight

    return {
        'selected': list(selected.index),
        'scores': ranked,
        'allocations': allocations,
    }


def _merge_allocations(*chunks: dict[str, float]) -> dict[str, float]:
    merged: dict[str, float] = {}
    for chunk in chunks:
        for symbol, weight in chunk.items():
            merged[symbol] = merged.get(symbol, 0.0) + float(weight)
    return {symbol: weight for symbol, weight in merged.items() if weight > 0}


def _normalize(weights: dict[str, float]) -> dict[str, float]:
    total = float(sum(weights.values()))
    if total <= 0:
        return {}
    return {symbol: float(weight / total) for symbol, weight in weights.items()}


def _calculate_drawdown(equity_curve: pd.Series) -> pd.Series:
    running_max = equity_curve.cummax()
    return (equity_curve / running_max) - 1.0


def antifragile_portfolio(
    start: str,
    end: str,
    sector_symbols: list[str] | None = None,
    defensive_symbols: list[str] | None = None,
    cash_symbol: str = DEFAULT_CASH_SYMBOL,
    weights: RankingWeights | None = None,
    data: dict[str, pd.DataFrame] | None = None,
) -> dict[str, Any]:
    sector_symbols = sector_symbols or DEFAULT_SECTOR_ETFS
    defensive_symbols = defensive_symbols or DEFAULT_DEFENSIVE_ETFS
    symbols = sorted(set(sector_symbols + defensive_symbols + [cash_symbol]))

    market_data = data or _download_ohlc(symbols=symbols, start=start, end=end)
    close = market_data['close'].copy().sort_index()
    high = market_data['high'].copy().sort_index()
    low = market_data['low'].copy().sort_index()

    required_lookback = 210
    if len(close.index) < required_lookback:
        raise ValueError('Insufficient history to compute indicators. Provide at least ~210 daily rows.')

    month_ends = close.resample('ME').last().index
    rebalance_dates = [date for date in month_ends if date in close.index and close.loc[:date].shape[0] >= required_lookback]
    if len(rebalance_dates) < 2:
        raise ValueError('Insufficient monthly points for backtest. Expand date range.')

    allocation_rows: list[dict[str, Any]] = []
    period_returns: list[dict[str, Any]] = []
    equity_values: list[dict[str, Any]] = []

    equity = 1.0
    equity_values.append({'date': rebalance_dates[0], 'equity': equity})

    for idx, rebalance_date in enumerate(rebalance_dates[:-1]):
        next_date = rebalance_dates[idx + 1]
        close_slice = close.loc[:rebalance_date]
        high_slice = high.loc[:rebalance_date]
        low_slice = low.loc[:rebalance_date]

        sector_result = sector_rotation_model(
            close_slice=close_slice,
            high_slice=high_slice,
            low_slice=low_slice,
            sector_symbols=sector_symbols,
            weights=weights,
            selection_size=5,
        )

        hedge_budget = 1.0 if sector_result['all_negative'] else float(sector_result['transfer_to_hedge'])
        black_swan_result = black_swan_model(
            close_slice=close_slice,
            high_slice=high_slice,
            low_slice=low_slice,
            defensive_symbols=defensive_symbols,
            cash_symbol=cash_symbol,
            model_budget=hedge_budget,
            weights=weights,
            selection_size=3,
        )

        if sector_result['all_negative']:
            combined_allocation = black_swan_result['allocations']
        else:
            combined_allocation = _merge_allocations(
                sector_result['sector_allocations'],
                black_swan_result['allocations'],
            )

        normalized_allocation = _normalize(combined_allocation)
        allocation_record = {'date': rebalance_date}
        allocation_record.update(normalized_allocation)
        allocation_rows.append(allocation_record)

        period_return = 0.0
        for symbol, weight in normalized_allocation.items():
            start_price = close.at[rebalance_date, symbol]
            end_price = close.at[next_date, symbol]
            asset_return = float(end_price / start_price) - 1.0
            period_return += float(weight) * asset_return

        equity *= 1.0 + period_return
        period_returns.append({'date': next_date, 'monthly_return': period_return})
        equity_values.append({'date': next_date, 'equity': equity})

    allocations_df = pd.DataFrame(allocation_rows).set_index('date').fillna(0.0)
    backtest_df = pd.DataFrame(period_returns).set_index('date')
    equity_curve = pd.DataFrame(equity_values).set_index('date')
    equity_curve['drawdown'] = _calculate_drawdown(equity_curve['equity'])

    monthly_returns = backtest_df['monthly_return'] if not backtest_df.empty else pd.Series(dtype=float)
    sharpe_ratio = 0.0
    if not monthly_returns.empty and float(monthly_returns.std(ddof=0)) > 0:
        sharpe_ratio = float((monthly_returns.mean() / monthly_returns.std(ddof=0)) * np.sqrt(12.0))

    return {
        'monthly_allocation': allocations_df,
        'historical_backtest': backtest_df,
        'equity_curve': equity_curve[['equity']],
        'drawdown': equity_curve[['drawdown']],
        'sharpe_ratio': sharpe_ratio,
    }