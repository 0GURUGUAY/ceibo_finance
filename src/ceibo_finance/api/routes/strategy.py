from datetime import datetime, timezone

from fastapi import APIRouter, HTTPException, WebSocket, WebSocketDisconnect
from pydantic import BaseModel, Field

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
