from fastapi import APIRouter, HTTPException

from ceibo_finance.services.llm_analysis import llm_analysis_service

router = APIRouter(prefix='/analysis', tags=['analysis'])


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
