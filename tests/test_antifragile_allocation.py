import numpy as np
import pandas as pd

from ceibo_finance.services.antifragile_allocation import (
    RankingWeights,
    antifragile_portfolio,
    black_swan_model,
    compute_atr,
    compute_correlation,
    compute_momentum,
    compute_volatility,
    rank_assets,
    sector_rotation_model,
)


def _build_synthetic_ohlc(symbols: list[str], periods: int = 340) -> dict[str, pd.DataFrame]:
    idx = pd.bdate_range('2024-01-01', periods=periods)

    close_dict: dict[str, pd.Series] = {}
    high_dict: dict[str, pd.Series] = {}
    low_dict: dict[str, pd.Series] = {}

    for i, symbol in enumerate(symbols):
        drift = 0.0005 + (i * 0.00012)
        wave = np.sin(np.linspace(0, 18, periods) + i) * 0.0015
        returns = drift + wave
        close = 100.0 * np.cumprod(1.0 + returns)
        close_series = pd.Series(close, index=idx)
        high_series = close_series * (1.0 + 0.008)
        low_series = close_series * (1.0 - 0.008)

        close_dict[symbol] = close_series
        high_dict[symbol] = high_series
        low_dict[symbol] = low_series

    return {
        'close': pd.DataFrame(close_dict, index=idx),
        'high': pd.DataFrame(high_dict, index=idx),
        'low': pd.DataFrame(low_dict, index=idx),
    }


def test_indicator_functions_shapes() -> None:
    symbols = ['AAA', 'BBB', 'CCC']
    data = _build_synthetic_ohlc(symbols, periods=260)
    close = data['close']

    momentum = compute_momentum(close)
    volatility = compute_volatility(close)
    correlation = compute_correlation(close.pct_change().dropna(), window=84)
    atr = compute_atr(data['high']['AAA'], data['low']['AAA'], close['AAA'])

    assert momentum.shape == close.shape
    assert volatility.shape == close.shape
    assert set(correlation.index) == set(symbols)
    assert atr.index.equals(close.index)


def test_rank_assets_returns_trank() -> None:
    metrics = pd.DataFrame(
        {
            'momentum': [0.10, 0.05, -0.03],
            'volatility': [0.11, 0.09, 0.30],
            'correlation': [0.30, 0.50, 0.80],
            'trend': [0.0, 1.0, 1.0],
        },
        index=['A', 'B', 'C'],
    )

    ranked = rank_assets(metrics, weights=RankingWeights(momentum=1.0, volatility=1.0, correlation=1.0, trend=1.0))

    assert 'trank' in ranked.columns
    assert ranked.index[0] in {'A', 'B'}


def test_sector_and_black_swan_models_allocation_budget() -> None:
    sectors = ['XLB', 'XLC', 'XLE', 'XLF', 'XLI', 'XLK', 'XLP', 'XLRE', 'XLU', 'XLV', 'XLY']
    defensive = ['TLT', 'IEF', 'SHY', 'TIP', 'GLD', 'UUP', 'BIL']
    symbols = sorted(set(sectors + defensive))
    data = _build_synthetic_ohlc(symbols, periods=320)

    close_slice = data['close'].iloc[:300]
    high_slice = data['high'].iloc[:300]
    low_slice = data['low'].iloc[:300]

    sector_result = sector_rotation_model(close_slice, high_slice, low_slice, sector_symbols=sectors)
    hedge_budget = 1.0 if sector_result['all_negative'] else float(sector_result['transfer_to_hedge'])

    hedge_result = black_swan_model(
        close_slice,
        high_slice,
        low_slice,
        defensive_symbols=defensive,
        cash_symbol='BIL',
        model_budget=hedge_budget,
    )

    total_weight = sum(sector_result['sector_allocations'].values()) + sum(hedge_result['allocations'].values())
    assert abs(total_weight - 1.0) < 1e-8


def test_antifragile_portfolio_outputs() -> None:
    sectors = ['XLB', 'XLC', 'XLE', 'XLF', 'XLI', 'XLK', 'XLP', 'XLRE', 'XLU', 'XLV', 'XLY']
    defensive = ['TLT', 'IEF', 'SHY', 'TIP', 'GLD', 'UUP', 'BIL']
    symbols = sorted(set(sectors + defensive))
    data = _build_synthetic_ohlc(symbols, periods=360)

    result = antifragile_portfolio(
        start='2024-01-01',
        end='2025-12-31',
        sector_symbols=sectors,
        defensive_symbols=defensive,
        cash_symbol='BIL',
        data=data,
    )

    monthly_allocation = result['monthly_allocation']
    historical_backtest = result['historical_backtest']
    equity_curve = result['equity_curve']
    drawdown = result['drawdown']

    assert not monthly_allocation.empty
    assert not historical_backtest.empty
    assert not equity_curve.empty
    assert not drawdown.empty
    assert isinstance(result['sharpe_ratio'], float)

    row_sums = monthly_allocation.sum(axis=1)
    assert np.allclose(row_sums.values, np.ones_like(row_sums.values), atol=1e-8)
