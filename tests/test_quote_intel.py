import asyncio

from ceibo_finance.core.config import settings
from ceibo_finance.services.quote_intel import QuoteIntelService


async def _fake_collect(service: QuoteIntelService, symbol: str) -> dict:
    return await service.collect_symbol(symbol=symbol, force_refresh=True)


def test_collect_symbol_persists_and_uses_cache(tmp_path) -> None:
    service = QuoteIntelService(db_path=str(tmp_path / 'quote_intel.db'), refresh_minutes=15)

    async def fake_fetch_sources(symbol: str):
        return {
            'yahoo_finance': {'status': 'ok', 'data': {'price': 100.0, 'change_pct': 1.2, 'market_cap': 3_000_000_000_000}},
            'google_finance': {'status': 'ok', 'data': {'price_text': '$100.0'}},
            'boursorama': {'status': 'error', 'error': 'not available'},
            'quandl': {'status': 'error', 'error': 'not configured'},
            'newsapi': {'status': 'error', 'error': 'not configured'},
        }

    async def fake_summary(symbol: str, sources: dict):
        return {
            'summary_text': f'{symbol} summary',
            'key_points': ['point 1'],
            'sentiment': 'haussier',
            'risk': 'modéré',
            'sources_ok': 2,
            'llm_provider': 'test',
            'llm_status': 'ok',
        }

    service._fetch_sources = fake_fetch_sources
    service._build_summary_with_llm = fake_summary

    first = asyncio.run(_fake_collect(service, 'AAPL'))
    assert first['cache_hit'] is False
    assert first['record']['symbol'] == 'AAPL'
    assert first['record']['summary']['summary_text'] == 'AAPL summary'

    second = asyncio.run(service.collect_symbol('AAPL', force_refresh=False))
    assert second['cache_hit'] is True
    assert second['record']['symbol'] == 'AAPL'


def test_opportunities_sorted_by_score(tmp_path) -> None:
    service = QuoteIntelService(db_path=str(tmp_path / 'quote_intel.db'), refresh_minutes=15)

    service._insert_record(
        {
            'symbol': 'AAA',
            'collected_at': '2026-03-14T10:00:00+00:00',
            'score': 72.5,
            'sources': {'yahoo_finance': {'status': 'ok'}},
            'summary': {'summary_text': 'AAA', 'key_points': [], 'sources_ok': 1, 'llm_provider': 'local'},
        }
    )
    service._insert_record(
        {
            'symbol': 'BBB',
            'collected_at': '2026-03-14T10:01:00+00:00',
            'score': 81.0,
            'sources': {'yahoo_finance': {'status': 'ok'}},
            'summary': {'summary_text': 'BBB', 'key_points': [], 'sources_ok': 1, 'llm_provider': 'local'},
        }
    )

    result = service.opportunities_today(limit=2, max_age_minutes=60 * 24 * 365)
    top = result['top_picks']

    assert len(top) == 2
    assert top[0]['score'] >= top[1]['score']
    assert all('score_breakdown' in item for item in top)


def test_extract_price_from_text_handles_decimal_comma() -> None:
    assert QuoteIntelService._extract_price_from_text('119,38 USD') == 119.38


def test_build_summary_uses_sanitized_facts(tmp_path) -> None:
    service = QuoteIntelService(db_path=str(tmp_path / 'quote_intel.db'), refresh_minutes=15)
    captured: dict[str, str] = {}

    async def fake_query(prompt: str):
        captured['prompt'] = prompt
        return {
            'provider': 'test',
            'status': 'ok',
            'content': '{"summary_text":"ok","key_points":[],"sentiment":"neutre","risk":"modéré"}',
        }

    service._query_best_provider = fake_query

    sources = {
        'yahoo_finance': {'status': 'ok', 'data': {'symbol': 'AAPL', 'price': 210.0, 'change_pct': 1.2}},
        'google_finance': {
            'status': 'ok',
            'data': {'exchange_hint': 'NASDAQ', 'price_text': '$210.0', 'change_text': '-119,38 USD (-0,11%)'},
        },
        'boursorama': {'status': 'ok', 'data': {'title': 'AAPL', 'snippet': 'noise 119,38 USD'}},
        'quandl': {'status': 'ok', 'data': {'dataset': 'FRED/DGS10', 'value': 4.1, 'delta': 0.02}},
        'newsapi': {'status': 'ok', 'data': {'article_count': 7, 'sentiment_hint': 'haussier', 'headlines': ['AAPL beats estimates']}},
    }

    summary = asyncio.run(service._build_summary_with_llm('AAPL', sources))

    assert summary['summary_text'] == 'ok'
    assert 'DATA:' in captured['prompt']
    assert 'change_text' not in captured['prompt']
    assert 'snippet' not in captured['prompt']


def test_query_best_provider_falls_back_to_next_provider(tmp_path, monkeypatch) -> None:
    service = QuoteIntelService(db_path=str(tmp_path / 'quote_intel.db'), refresh_minutes=15)

    monkeypatch.setattr(settings, 'openai_api_key', 'openai-key')
    monkeypatch.setattr(settings, 'anthropic_api_key', 'anthropic-key')
    monkeypatch.setattr(settings, 'openrouter_api_key', '')
    monkeypatch.setattr(settings, 'gemini_api_key', '')

    async def failing_openai(prompt: str) -> str:
        raise RuntimeError('openai temporary failure')

    async def successful_anthropic(prompt: str) -> str:
        return '{"summary_text":"ok"}'

    service._query_openai = failing_openai
    service._query_anthropic = successful_anthropic

    result = asyncio.run(service._query_best_provider('test prompt'))

    assert result['status'] == 'ok'
    assert result['provider'] == 'anthropic'
    assert result['content'] == '{"summary_text":"ok"}'


def test_query_best_provider_retries_before_success(tmp_path, monkeypatch) -> None:
    service = QuoteIntelService(db_path=str(tmp_path / 'quote_intel.db'), refresh_minutes=15)

    monkeypatch.setattr(settings, 'openai_api_key', 'openai-key')
    monkeypatch.setattr(settings, 'anthropic_api_key', '')
    monkeypatch.setattr(settings, 'openrouter_api_key', '')
    monkeypatch.setattr(settings, 'gemini_api_key', '')

    attempts = {'count': 0}

    async def flaky_openai(prompt: str) -> str:
        attempts['count'] += 1
        if attempts['count'] == 1:
            raise RuntimeError('transient timeout')
        return '{"summary_text":"recovered"}'

    service._query_openai = flaky_openai

    result = asyncio.run(service._query_best_provider('test prompt'))

    assert result['status'] == 'ok'
    assert result['provider'] == 'openai'
    assert result['attempts'] == 2
    assert attempts['count'] == 2


def test_discover_universe_with_llm_batches_merges_and_dedupes(tmp_path) -> None:
    service = QuoteIntelService(db_path=str(tmp_path / 'quote_intel.db'), refresh_minutes=15)
    service.us_universe = lambda: []

    responses = iter(
        [
            {
                'status': 'ok',
                'provider': 'test',
                'content': '{"symbols":["AAA","BBB","CCC","DDD","EEE"],"rationale":"batch1"}',
            },
            {
                'status': 'ok',
                'provider': 'test',
                'content': '{"symbols":["CCC","FFF","GGG","HHH","III"],"rationale":"batch2"}',
            },
        ]
    )

    async def fake_query(prompt: str):
        return next(responses)

    service._query_best_provider = fake_query

    result = asyncio.run(service.discover_universe_with_llm_batches(count_per_batch=10, batches=2, context='test'))

    assert result['discovered'] is True
    assert result['count'] == 9
    assert 'AAA' in result['symbols']
    assert 'III' in result['symbols']
    assert len(result['symbols']) == len(set(result['symbols']))
    assert len(result['batches']) == 2


def test_discover_universe_once_fallback_extracts_symbols_from_text(tmp_path) -> None:
    service = QuoteIntelService(db_path=str(tmp_path / 'quote_intel.db'), refresh_minutes=15)

    async def fake_query(prompt: str):
        return {
            'status': 'ok',
            'provider': 'test',
            'content': 'symbols: AAPL, MSFT, NVDA, AMZN, META, QQQ, SPY',
        }

    service._query_best_provider = fake_query

    result = asyncio.run(service._discover_universe_once(count=20, context=''))

    assert result['status'] == 'ok'
    assert 'AAPL' in result['symbols']
    assert 'QQQ' in result['symbols']
