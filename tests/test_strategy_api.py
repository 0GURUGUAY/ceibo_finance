import pandas as pd
import pytest
from fastapi.testclient import TestClient

from ceibo_finance.main import app
from ceibo_finance.api.routes import strategy as strategy_route


def test_antifragile_backtest_endpoint(monkeypatch) -> None:
    client = TestClient(app)

    def fake_antifragile_portfolio(**kwargs):
        assert kwargs['start'] == '2024-01-01'
        assert kwargs['end'] == '2025-12-31'
        return {
            'monthly_allocation': pd.DataFrame(
                [{'XLK': 0.4, 'TLT': 0.6}],
                index=pd.to_datetime(['2025-01-31']),
            ),
            'historical_backtest': pd.DataFrame(
                [{'monthly_return': 0.02}],
                index=pd.to_datetime(['2025-02-28']),
            ),
            'equity_curve': pd.DataFrame(
                [{'equity': 1.02}],
                index=pd.to_datetime(['2025-02-28']),
            ),
            'drawdown': pd.DataFrame(
                [{'drawdown': -0.01}],
                index=pd.to_datetime(['2025-02-28']),
            ),
            'sharpe_ratio': 1.75,
        }

    monkeypatch.setattr(strategy_route, 'antifragile_portfolio', fake_antifragile_portfolio)

    response = client.post(
        '/api/v1/strategy/antifragile/backtest',
        json={
            'start': '2024-01-01',
            'end': '2025-12-31',
            'cash_symbol': 'BIL',
            'weights': {
                'momentum': 1.0,
                'volatility': 1.0,
                'correlation': 1.0,
                'trend': 1.0,
            },
        },
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload['sharpe_ratio'] == 1.75
    assert payload['monthly_allocation'][0]['date'] == '2025-01-31'
    assert payload['monthly_allocation'][0]['XLK'] == 0.4
    assert payload['historical_backtest'][0]['monthly_return'] == 0.02
    assert payload['equity_curve'][0]['equity'] == 1.02
    assert payload['drawdown'][0]['drawdown'] == -0.01


def test_viewer_invite_returns_token_and_url(monkeypatch) -> None:
    client = TestClient(app)
    monkeypatch.setattr(strategy_route.settings, 'viewer_token_secret', 'test-secret')
    monkeypatch.setattr(strategy_route.settings, 'viewer_admin_key', 'admin-key')

    response = client.post(
        '/api/v1/strategy/trend-following/viewer/invite',
        json={'expires_minutes': 30},
        headers={'x-viewer-admin-key': 'admin-key'},
    )

    assert response.status_code == 200
    payload = response.json()
    assert isinstance(payload.get('token'), str)
    assert payload['token']
    assert '/spectator?t=' in payload.get('spectator_url', '')
    assert payload.get('expires_minutes') == 30


def test_viewer_ws_rejects_invalid_token(monkeypatch) -> None:
    client = TestClient(app)
    monkeypatch.setattr(strategy_route.settings, 'viewer_token_secret', 'test-secret')

    with pytest.raises(Exception):
        with client.websocket_connect('/api/v1/strategy/ws/trend-following/viewer?token=invalid-token'):
            pass


def test_viewer_ws_accepts_valid_token(monkeypatch) -> None:
    client = TestClient(app)
    monkeypatch.setattr(strategy_route.settings, 'viewer_token_secret', 'test-secret')
    valid_token = strategy_route._build_viewer_token(5)

    with client.websocket_connect(f'/api/v1/strategy/ws/trend-following/viewer?token={valid_token}') as ws:
        message = ws.receive_json()
        assert message.get('type') == 'snapshot'
