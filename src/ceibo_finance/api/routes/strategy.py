from __future__ import annotations

from datetime import datetime, timezone
import base64
import csv
import hashlib
import hmac
import io
import json
import secrets
from typing import Any, Optional

from fastapi import APIRouter, Header, HTTPException, Request, Response, WebSocket, WebSocketDisconnect
import pandas as pd
from pydantic import BaseModel, Field

from ceibo_finance.core.config import settings
from ceibo_finance.services.antifragile_allocation import RankingWeights, antifragile_portfolio
from ceibo_finance.services.strategy_history import strategy_history_service
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


def _pdf_safe_text(value: str) -> str:
    text = str(value or '').replace('\\', '\\\\').replace('(', '\\(').replace(')', '\\)')
    return text.encode('latin-1', errors='replace').decode('latin-1')


def _build_pdf_from_lines(lines: list[str]) -> bytes:
    page_width = 595
    page_height = 842
    left = 40
    top = 800
    line_height = 14
    lines_per_page = 52
    chunks = [lines[i:i + lines_per_page] for i in range(0, len(lines), lines_per_page)] or [[]]

    objects: list[str] = []

    def add_obj(content: str) -> int:
        objects.append(content)
        return len(objects)

    font_id = add_obj('<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica >>')
    pages_id = add_obj('')
    page_ids: list[int] = []

    for chunk in chunks:
        commands = ['BT', '/F1 11 Tf', f'{line_height} TL', f'{left} {top} Td']
        for line in chunk:
            commands.append(f'({_pdf_safe_text(line)}) Tj')
            commands.append('T*')
        commands.append('ET')
        stream = '\n'.join(commands)
        stream_bytes = stream.encode('latin-1', errors='replace')
        content_id = add_obj(f'<< /Length {len(stream_bytes)} >>\nstream\n{stream}\nendstream')
        page_id = add_obj(
            f'<< /Type /Page /Parent {pages_id} 0 R '
            f'/MediaBox [0 0 {page_width} {page_height}] '
            f'/Resources << /Font << /F1 {font_id} 0 R >> >> '
            f'/Contents {content_id} 0 R >>'
        )
        page_ids.append(page_id)

    kids = ' '.join(f'{pid} 0 R' for pid in page_ids)
    objects[pages_id - 1] = f'<< /Type /Pages /Kids [{kids}] /Count {len(page_ids)} >>'
    catalog_id = add_obj(f'<< /Type /Catalog /Pages {pages_id} 0 R >>')

    pdf_parts: list[bytes] = [b'%PDF-1.4\n%\xe2\xe3\xcf\xd3\n']
    offsets = [0]

    for index, obj in enumerate(objects, start=1):
        offsets.append(sum(len(part) for part in pdf_parts))
        pdf_parts.append(f'{index} 0 obj\n{obj}\nendobj\n'.encode('latin-1', errors='replace'))

    xref_start = sum(len(part) for part in pdf_parts)
    xref_lines = [f'xref\n0 {len(objects) + 1}\n', '0000000000 65535 f \n']
    for offset in offsets[1:]:
        xref_lines.append(f'{offset:010d} 00000 n \n')

    trailer = (
        f'trailer\n<< /Size {len(objects) + 1} /Root {catalog_id} 0 R >>\n'
        f'startxref\n{xref_start}\n%%EOF\n'
    )

    pdf_parts.append(''.join(xref_lines).encode('latin-1'))
    pdf_parts.append(trailer.encode('latin-1'))
    return b''.join(pdf_parts)


def _format_strategy_history_report(records: list[dict[str, Any]], symbol: Optional[str], side: Optional[str]) -> list[str]:
    lines = [
        'HISTORIQUE SYSTEME - TREND FOLLOWING',
        '',
        f'Date export: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}',
        f'Filtre symbol: {symbol or "-"}',
        f'Filtre side: {side or "-"}',
        f'Nombre de lignes: {len(records)}',
        '',
    ]

    if not records:
        lines.append('Aucun historique disponible.')
        return lines

    for index, row in enumerate(records, start=1):
        lines.append(
            f'{index}. {row.get("recorded_at") or "-"} | {str(row.get("side") or "-").upper()} '
            f'| {row.get("symbol") or "-"} | qty={row.get("qty") or "-"} | price={row.get("price") or "-"}'
        )
        lines.append(
            f'   reason={row.get("reason") or "-"} | source_event={row.get("source_event") or "-"} '
            f'| order_id={row.get("order_id") or "-"}'
        )
        lines.append(
            f'   pnl_usd={row.get("pnl_usd") if row.get("pnl_usd") is not None else "-"} '
            f'| pnl_pct={row.get("pnl_pct") if row.get("pnl_pct") is not None else "-"} '
            f'| simulation_mode={bool(row.get("simulation_mode"))}'
        )
        lines.append('')

    return lines


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


@router.post('/trend-following/pause/{symbol}')
async def trend_following_pause_symbol(symbol: str):
    normalized = str(symbol or '').strip().upper()
    if not normalized:
        raise HTTPException(status_code=400, detail='Symbol is required')
    return trend_following_service.pause_symbol(normalized)


@router.post('/trend-following/resume/{symbol}')
async def trend_following_resume_symbol(symbol: str):
    normalized = str(symbol or '').strip().upper()
    if not normalized:
        raise HTTPException(status_code=400, detail='Symbol is required')
    return trend_following_service.resume_symbol(normalized)


@router.get('/trend-following/status')
async def trend_following_status():
    return trend_following_service.status()


@router.get('/trend-following/history')
async def trend_following_history(limit: int = 200, symbol: Optional[str] = None, side: Optional[str] = None):
    records = strategy_history_service.list_trades(limit=limit, symbol=symbol, side=side)
    return {
        'records': records,
        'count': len(records),
        'limit': max(1, min(int(limit or 200), 1000)),
        'symbol': str(symbol or '').strip().upper() or None,
        'side': str(side or '').strip().lower() or None,
    }


@router.post('/trend-following/history/clear')
async def trend_following_history_clear():
    return strategy_history_service.clear_trades()


@router.get('/trend-following/history/export')
async def trend_following_history_export(
    format: str = 'csv',
    limit: int = 500,
    symbol: Optional[str] = None,
    side: Optional[str] = None,
):
    normalized_symbol = str(symbol or '').strip().upper() or None
    normalized_side = str(side or '').strip().lower() or None
    records = strategy_history_service.list_trades(limit=limit, symbol=normalized_symbol, side=normalized_side)

    export_format = str(format or 'csv').strip().lower()
    base_name = 'trend_following_history'

    if export_format == 'csv':
        stream = io.StringIO()
        writer = csv.writer(stream)
        writer.writerow([
            'recorded_at',
            'strategy_name',
            'source_event',
            'side',
            'symbol',
            'qty',
            'price',
            'reason',
            'order_id',
            'simulation_mode',
            'pnl_usd',
            'pnl_pct',
            'event_id',
            'metadata_json',
        ])
        for row in records:
            writer.writerow([
                row.get('recorded_at'),
                row.get('strategy_name'),
                row.get('source_event'),
                row.get('side'),
                row.get('symbol'),
                row.get('qty'),
                row.get('price'),
                row.get('reason'),
                row.get('order_id'),
                bool(row.get('simulation_mode')),
                row.get('pnl_usd'),
                row.get('pnl_pct'),
                row.get('event_id'),
                json.dumps(row.get('metadata') or {}, ensure_ascii=False),
            ])
        content = stream.getvalue().encode('utf-8')
        return Response(
            content=content,
            media_type='text/csv; charset=utf-8',
            headers={'Content-Disposition': f'attachment; filename="{base_name}.csv"'},
        )

    if export_format == 'pdf':
        lines = _format_strategy_history_report(records=records, symbol=normalized_symbol, side=normalized_side)
        pdf_bytes = _build_pdf_from_lines(lines)
        return Response(
            content=pdf_bytes,
            media_type='application/pdf',
            headers={'Content-Disposition': f'attachment; filename="{base_name}.pdf"'},
        )

    raise HTTPException(status_code=400, detail='format must be csv or pdf')


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
