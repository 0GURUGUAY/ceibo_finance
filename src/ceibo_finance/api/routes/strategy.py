from __future__ import annotations

from datetime import datetime, timezone
import base64
import hashlib
import hmac
import json
import secrets
from typing import Any, Optional

from fastapi import APIRouter, Header, HTTPException, Request, WebSocket, WebSocketDisconnect
import pandas as pd
from pydantic import BaseModel, Field

from ceibo_finance.core.config import settings
from ceibo_finance.services.antifragile_allocation import RankingWeights, antifragile_portfolio
from ceibo_finance.services.trend_following import (
    TrendFollowingConfig,
    trend_following_service,
)

router = APIRouter(prefix='/strategy', tags=['strategy'])


class TrendFollowingStartRequest(BaseModel):
    symbols: list[str] = Field(default_factory=list)
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


class AntifragileWeightsRequest(BaseModel):
    momentum: float = 1.0
    volatility: float = 1.0
    correlation: float = 1.0
    trend: float = 1.0


class AntifragileBacktestRequest(BaseModel):
    start: str
    end: str
    sector_symbols: list[str] = Field(default_factory=list)
    defensive_symbols: list[str] = Field(default_factory=list)
    cash_symbol: str = 'BIL'
    weights: AntifragileWeightsRequest = Field(default_factory=AntifragileWeightsRequest)


class TrendFollowingViewerInviteRequest(BaseModel):
    expires_minutes: int = Field(default_factory=lambda: settings.viewer_token_default_ttl_minutes)


def _viewer_secret() -> bytes:
    secret = str(settings.viewer_token_secret or '').strip()
    if not secret:
        raise HTTPException(status_code=503, detail='viewer_token_secret is not configured')
    return secret.encode('utf-8')


def _base64url_encode(raw: bytes) -> str:
    return base64.urlsafe_b64encode(raw).decode('utf-8').rstrip('=')


def _base64url_decode(data: str) -> bytes:
    padding = '=' * ((4 - len(data) % 4) % 4)
    return base64.urlsafe_b64decode(data + padding)


def _build_viewer_token(expires_minutes: int) -> str:
    safe_minutes = max(1, min(int(expires_minutes or settings.viewer_token_default_ttl_minutes), 7 * 24 * 60))
    expires_at = int(datetime.now(timezone.utc).timestamp()) + safe_minutes * 60
    payload = {
        'scope': 'trend_following_viewer',
        'exp': expires_at,
        'nonce': secrets.token_hex(8),
    }
    payload_bytes = json.dumps(payload, separators=(',', ':'), ensure_ascii=False).encode('utf-8')
    payload_encoded = _base64url_encode(payload_bytes)
    signature = hmac.new(_viewer_secret(), payload_encoded.encode('utf-8'), hashlib.sha256).hexdigest()
    return f'{payload_encoded}.{signature}'


def _verify_viewer_token(token: str) -> bool:
    try:
        payload_encoded, provided_sig = str(token or '').split('.', 1)
    except ValueError:
        return False

    if not payload_encoded or not provided_sig:
        return False

    expected_sig = hmac.new(_viewer_secret(), payload_encoded.encode('utf-8'), hashlib.sha256).hexdigest()
    if not hmac.compare_digest(provided_sig, expected_sig):
        return False

    try:
        payload = json.loads(_base64url_decode(payload_encoded).decode('utf-8'))
    except Exception:
        return False

    if payload.get('scope') != 'trend_following_viewer':
        return False

    exp = int(payload.get('exp') or 0)
    now = int(datetime.now(timezone.utc).timestamp())
    return exp > now


def _serialize_frame(frame: pd.DataFrame) -> list[dict[str, Any]]:
    if frame.empty:
        return []

    serialized = frame.reset_index().copy()
    date_column = serialized.columns[0]
    serialized[date_column] = pd.to_datetime(serialized[date_column]).dt.strftime('%Y-%m-%d')
    return serialized.rename(columns={date_column: 'date'}).to_dict(orient='records')


@router.post('/trend-following/start')
async def trend_following_start(payload: TrendFollowingStartRequest):
    try:
        config = TrendFollowingConfig(
            symbols=payload.symbols,
            use_current_positions=payload.use_current_positions,
            simulation_mode=payload.simulation_mode,
            capital_usd=payload.capital_usd,
            short_window=payload.short_window,
            long_window=payload.long_window,
            poll_seconds=payload.poll_seconds,
            universe_refresh_seconds=payload.universe_refresh_seconds,
            take_profit_pct=payload.take_profit_pct,
            stop_loss_pct=payload.stop_loss_pct,
            stop_loss_confirmations=payload.stop_loss_confirmations,
            reentry_after_loss_enabled=payload.reentry_after_loss_enabled,
            reentry_delay_minutes=payload.reentry_delay_minutes,
        )
        return await trend_following_service.start(config)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f'Strategy start failed: {exc}') from exc


@router.post('/trend-following/stop')
async def trend_following_stop():
    try:
        return await trend_following_service.stop()
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f'Strategy stop failed: {exc}') from exc


@router.get('/trend-following/status')
async def trend_following_status():
    return trend_following_service.status()


@router.post('/antifragile/backtest')
async def antifragile_backtest(payload: AntifragileBacktestRequest):
    try:
        weights = RankingWeights(
            momentum=payload.weights.momentum,
            volatility=payload.weights.volatility,
            correlation=payload.weights.correlation,
            trend=payload.weights.trend,
        )
        result = antifragile_portfolio(
            start=payload.start,
            end=payload.end,
            sector_symbols=payload.sector_symbols or None,
            defensive_symbols=payload.defensive_symbols or None,
            cash_symbol=payload.cash_symbol,
            weights=weights,
        )
        return {
            'monthly_allocation': _serialize_frame(result['monthly_allocation']),
            'historical_backtest': _serialize_frame(result['historical_backtest']),
            'equity_curve': _serialize_frame(result['equity_curve']),
            'drawdown': _serialize_frame(result['drawdown']),
            'sharpe_ratio': float(result['sharpe_ratio']),
        }
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f'Antifragile backtest failed: {exc}') from exc


@router.websocket('/ws/trend-following')
async def trend_following_ws(websocket: WebSocket):
    await trend_following_service.subscribe(websocket)
    try:
        while True:
            message = await websocket.receive_text()
            if str(message or '').strip().lower() == 'ping':
                await websocket.send_json(
                    {
                        'type': 'pong',
                        'payload': {'timestamp': datetime.now(timezone.utc).isoformat()},
                    }
                )
    except WebSocketDisconnect:
        await trend_following_service.unsubscribe(websocket)
    except Exception:
        await trend_following_service.unsubscribe(websocket)


@router.post('/trend-following/viewer/invite')
async def trend_following_viewer_invite(
    payload: TrendFollowingViewerInviteRequest,
    request: Request,
    x_viewer_admin_key: Optional[str] = Header(default=None),
):
    configured_admin_key = str(settings.viewer_admin_key or '').strip()
    if configured_admin_key and x_viewer_admin_key != configured_admin_key:
        raise HTTPException(status_code=403, detail='Forbidden')

    token = _build_viewer_token(payload.expires_minutes)
    base_url = str(request.base_url).rstrip('/')
    return {
        'token': token,
        'spectator_url': f'{base_url}/spectator?t={token}',
        'expires_minutes': max(1, min(int(payload.expires_minutes or settings.viewer_token_default_ttl_minutes), 7 * 24 * 60)),
    }


@router.websocket('/ws/trend-following/viewer')
async def trend_following_viewer_ws(websocket: WebSocket):
    token = websocket.query_params.get('token')
    if not _verify_viewer_token(str(token or '')):
        await websocket.close(code=1008)
        return

    await trend_following_service.subscribe(websocket)
    try:
        while True:
            message = await websocket.receive_text()
            if str(message or '').strip().lower() == 'ping':
                await websocket.send_json(
                    {
                        'type': 'pong',
                        'payload': {'timestamp': datetime.now(timezone.utc).isoformat()},
                    }
                )
    except WebSocketDisconnect:
        await trend_following_service.unsubscribe(websocket)
    except Exception:
        await trend_following_service.unsubscribe(websocket)
