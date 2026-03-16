from datetime import datetime
from typing import Any, Optional

from fastapi import APIRouter, HTTPException, Response
from pydantic import BaseModel, Field

from ceibo_finance.services.llm_analysis import llm_analysis_service
from ceibo_finance.services.quote_intel import quote_intel_service

router = APIRouter(prefix='/analysis', tags=['analysis'])


class OpportunitiesPdfPayload(BaseModel):
    result: dict[str, Any] = Field(default_factory=dict)
    days: int = 7
    limit: int = 10


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


def _format_opportunities_report(payload: dict, days: int, limit: int) -> list[str]:
    criteria = payload.get('criteria') or []
    picks = payload.get('top_picks') or []
    rankings = payload.get('llm_rankings') or []
    providers_ok = [item.get('provider') for item in rankings if item.get('status') == 'ok' and item.get('provider')]

    lines: list[str] = [
        f'RESULTAT RECHERCHE IA - TOP {limit} OPPORTUNITES ({days} JOURS)',
        '',
        f'Date export: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}',
        f'Univers scanne: {payload.get("candidates_considered", "-")}',
        f'Preselection: {len(payload.get("preselected_candidates") or [])}',
        f'Providers LLM OK: {", ".join(providers_ok) if providers_ok else "aucun"}',
        '',
    ]

    if criteria:
        lines.append('CRITERES UTILISES')
        for item in criteria:
            lines.append(f'- {item}')
        lines.append('')

    if not picks:
        lines.append('Aucune opportunite retournee.')
        return lines

    lines.append('TOP OPPORTUNITES')
    for index, item in enumerate(picks, start=1):
        lines.append(f'{index}. {item.get("symbol") or "-"} - {item.get("company_name") or "-"}')
        lines.append(f'   Score final: {item.get("final_score", "-")} | Marche: {item.get("market", "-")}')
        lines.append(
            f'   Votes LLM: {item.get("provider_votes", 0)} | '
            f'Providers: {", ".join(item.get("providers") or []) or "-"} | '
            f'Score marche: {item.get("market_score", "-")}'
        )
        lines.append(f'   Activite: {item.get("activity") or "Activite non renseignee."}')

        rationales = item.get('rationales') or []
        evidence = item.get('evidence') or []
        if rationales:
            lines.append('   Rationales IA:')
            for rationale in rationales:
                lines.append(f'    - {rationale}')
        if evidence:
            lines.append('   Elements quantitatifs:')
            for element in evidence:
                lines.append(f'    - {element}')
        lines.append('')

    return lines


def _truncate(value: Any, max_len: int = 96) -> str:
    text = str(value or '').strip()
    if len(text) <= max_len:
        return text
    return f'{text[: max_len - 1]}…'


def _format_quotes_only_report(payload: dict[str, Any], days: int, limit: int) -> list[str]:
    picks = payload.get('top_picks') or []
    lines: list[str] = [
        f'TOP {limit} QUOTES - IA ({days} JOURS)',
        f'Export: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}',
        '',
    ]

    if not picks:
        lines.append('Aucune quote disponible.')
        return lines

    for index, item in enumerate(picks, start=1):
        symbol = _truncate(item.get('symbol') or '-', 16)
        company_name = _truncate(item.get('company_name') or '-', 48)
        market = _truncate(item.get('market') or '-', 16)
        score = _truncate(item.get('final_score') or '-', 16)
        activity = _truncate(item.get('activity') or '-', 110)

        lines.append('────────────────────────────────────────────────────────')
        lines.append(f'#{index:02d}  {symbol}  |  {company_name}')
        lines.append(f'Score: {score}   Marché: {market}')
        lines.append(f'Quote: {activity}')
        lines.append('')

    return lines


@router.get('/providers')
def analysis_providers_status():
    return {'providers': llm_analysis_service.provider_status()}


@router.get('/position')
async def analyze_position(symbol: str):
    try:
        return await llm_analysis_service.analyze_symbol(symbol)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f'Analysis failed: {exc}') from exc


@router.get('/opportunities')
async def analyze_opportunities(days: int = 7, limit: int = 10):
    try:
        return await llm_analysis_service.analyze_weekly_opportunities(days=days, limit=limit)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f'Opportunities analysis failed: {exc}') from exc


@router.get('/opportunities/pdf')
async def analyze_opportunities_pdf(days: int = 7, limit: int = 10):
    try:
        result = await llm_analysis_service.analyze_weekly_opportunities(days=days, limit=limit)
        lines = _format_quotes_only_report(result, days=days, limit=limit)
        pdf_bytes = _build_pdf_from_lines(lines)
        filename = f'opportunites_{days}j_{limit}.pdf'
        return Response(
            content=pdf_bytes,
            media_type='application/pdf',
            headers={'Content-Disposition': f'attachment; filename="{filename}"'},
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f'Opportunities PDF export failed: {exc}') from exc


@router.post('/opportunities/pdf')
async def opportunities_pdf_from_existing_result(payload: OpportunitiesPdfPayload):
    try:
        lines = _format_quotes_only_report(payload.result or {}, days=payload.days, limit=payload.limit)
        pdf_bytes = _build_pdf_from_lines(lines)
        filename = f'opportunites_{payload.days}j_{payload.limit}.pdf'
        return Response(
            content=pdf_bytes,
            media_type='application/pdf',
            headers={'Content-Disposition': f'attachment; filename="{filename}"'},
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f'Opportunities PDF export failed: {exc}') from exc


@router.get('/v2/quote')
async def analyze_quote_v2(symbol: str, force_refresh: bool = False):
    try:
        return await quote_intel_service.collect_symbol(symbol=symbol, force_refresh=force_refresh)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f'Quote intelligence failed: {exc}') from exc


@router.post('/v2/collect-us')
async def collect_us_market_v2(limit: int = 30, force_refresh: bool = False):
    try:
        return await quote_intel_service.collect_us_market(limit=limit, force_refresh=force_refresh)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f'US market collection failed: {exc}') from exc


@router.get('/v2/opportunities')
def opportunities_today_v2(limit: int = 10, max_age_minutes: int = 1440):
    try:
        return quote_intel_service.opportunities_today(limit=limit, max_age_minutes=max_age_minutes)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f'V2 opportunities failed: {exc}') from exc


@router.post('/v2/auto/start')
async def start_v2_auto_collection(limit: int = 30, force_refresh: bool = False, refresh_minutes: Optional[int] = None):
    try:
        return await quote_intel_service.start_auto_collection(
            limit=limit,
            force_refresh=force_refresh,
            refresh_minutes=refresh_minutes,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f'V2 auto start failed: {exc}') from exc


@router.post('/v2/auto/stop')
async def stop_v2_auto_collection():
    try:
        return await quote_intel_service.stop_auto_collection()
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f'V2 auto stop failed: {exc}') from exc


@router.get('/v2/auto/status')
def status_v2_auto_collection():
    try:
        return quote_intel_service.auto_collection_status()
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f'V2 auto status failed: {exc}') from exc


@router.get('/v2/universe')
def get_universe():
    try:
        return quote_intel_service.universe_status()
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f'Universe status failed: {exc}') from exc


@router.post('/v2/universe/discover')
async def discover_universe(count: int = 40, context: str = ''):
    try:
        return await quote_intel_service.discover_universe_with_llm(count=count, context=context)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f'Universe discovery failed: {exc}') from exc


@router.post('/v2/universe/discover-batch')
async def discover_universe_batch(count_per_batch: int = 60, batches: int = 3, context: str = ''):
    try:
        return await quote_intel_service.discover_universe_with_llm_batches(
            count_per_batch=count_per_batch,
            batches=batches,
            context=context,
        )
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f'Batch universe discovery failed: {exc}') from exc
