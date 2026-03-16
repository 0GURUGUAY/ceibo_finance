import asyncio
import json
import re
import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Optional

import httpx
import pandas as pd

from ceibo_finance.core.config import settings


class QuoteIntelService:
    def __init__(self, db_path: Optional[str] = None, refresh_minutes: Optional[int] = None) -> None:
        configured_path = db_path or settings.quote_intel_db_path
        self._db_path = Path(configured_path)
        self._refresh_minutes = max(5, int(refresh_minutes or settings.quote_intel_refresh_minutes or 15))
        self._llm_timeout_seconds = 20.0
        self._llm_max_attempts = 2
        self._auto_task: Optional[asyncio.Task] = None
        self._auto_started_at: Optional[str] = None
        self._auto_last_run_at: Optional[str] = None
        self._auto_next_run_at: Optional[str] = None
        self._auto_last_result: dict[str, Any] = {}
        self._auto_last_error: str = ''
        self._auto_limit: int = 30
        self._auto_force_refresh: bool = False
        self._ensure_db()

    async def start_auto_collection(
        self,
        limit: int = 30,
        force_refresh: bool = False,
        refresh_minutes: Optional[int] = None,
    ) -> dict[str, Any]:
        safe_limit = min(max(1, int(limit or 30)), 200)
        self._auto_limit = safe_limit
        self._auto_force_refresh = bool(force_refresh)
        if refresh_minutes is not None:
            self._refresh_minutes = max(5, min(int(refresh_minutes), 24 * 60))

        if self._auto_task and not self._auto_task.done():
            return {
                'started': False,
                'message': 'Auto-collection already running.',
                'status': self.auto_collection_status(),
            }

        self._auto_started_at = datetime.now(timezone.utc).isoformat()
        self._auto_last_error = ''
        self._auto_task = asyncio.create_task(self._auto_collection_loop(), name='quote-intel-auto-collector')

        return {
            'started': True,
            'message': 'Auto-collection started.',
            'status': self.auto_collection_status(),
        }

    async def stop_auto_collection(self) -> dict[str, Any]:
        if not self._auto_task or self._auto_task.done():
            self._auto_task = None
            self._auto_next_run_at = None
            return {
                'stopped': False,
                'message': 'Auto-collection is not running.',
                'status': self.auto_collection_status(),
            }

        self._auto_task.cancel()
        try:
            await self._auto_task
        except asyncio.CancelledError:
            pass
        finally:
            self._auto_task = None
            self._auto_next_run_at = None

        return {
            'stopped': True,
            'message': 'Auto-collection stopped.',
            'status': self.auto_collection_status(),
        }

    def auto_collection_status(self) -> dict[str, Any]:
        running = bool(self._auto_task and not self._auto_task.done())
        return {
            'running': running,
            'refresh_minutes': self._refresh_minutes,
            'started_at': self._auto_started_at,
            'next_run_at': self._auto_next_run_at,
            'last_run_at': self._auto_last_run_at,
            'limit': self._auto_limit,
            'force_refresh': self._auto_force_refresh,
            'last_error': self._auto_last_error,
            'last_result': self._auto_last_result,
        }

    async def collect_symbol(self, symbol: str, force_refresh: bool = False) -> dict[str, Any]:
        normalized_symbol = (symbol or '').strip().upper()
        if not normalized_symbol:
            raise ValueError('Symbol is required')

        cached = self._get_latest_record(normalized_symbol)
        if cached and not force_refresh and self._is_fresh(cached.get('collected_at')):
            return {
                'symbol': normalized_symbol,
                'refresh_minutes': self._refresh_minutes,
                'cache_hit': True,
                'record': cached,
            }

        sources = await self._fetch_sources(normalized_symbol)
        summary = await self._build_summary_with_llm(normalized_symbol, sources)
        score, score_breakdown = self._compute_interest_score(sources=sources, summary=summary)
        summary['score_breakdown'] = score_breakdown

        record = {
            'symbol': normalized_symbol,
            'collected_at': datetime.now(timezone.utc).isoformat(),
            'score': round(score, 2),
            'score_breakdown': score_breakdown,
            'sources': sources,
            'summary': summary,
        }
        self._insert_record(record)

        return {
            'symbol': normalized_symbol,
            'refresh_minutes': self._refresh_minutes,
            'cache_hit': False,
            'record': record,
        }

    async def collect_us_market(self, limit: int = 30, force_refresh: bool = False) -> dict[str, Any]:
        safe_limit = min(max(1, int(limit or 30)), 200)
        symbols = self.us_universe()[:safe_limit]

        semaphore = asyncio.Semaphore(6)

        async def _collect_one(item: str) -> dict[str, Any]:
            async with semaphore:
                try:
                    result = await self.collect_symbol(item, force_refresh=force_refresh)
                    record = result.get('record') or {}
                    return {
                        'symbol': item,
                        'status': 'ok',
                        'cache_hit': bool(result.get('cache_hit')),
                        'score': record.get('score'),
                    }
                except Exception as exc:
                    return {'symbol': item, 'status': 'error', 'error': str(exc)}

        results = await asyncio.gather(*(_collect_one(symbol) for symbol in symbols))

        ok_items = [item for item in results if item.get('status') == 'ok']
        errors = [item for item in results if item.get('status') == 'error']
        ok_items.sort(key=lambda row: float(row.get('score') or 0.0), reverse=True)

        return {
            'generated_at': datetime.now(timezone.utc).isoformat(),
            'refresh_minutes': self._refresh_minutes,
            'symbols_requested': len(symbols),
            'symbols_ok': len(ok_items),
            'symbols_error': len(errors),
            'top_scores': ok_items[:10],
            'errors': errors[:20],
        }

    def opportunities_today(self, limit: int = 10, max_age_minutes: int = 24 * 60) -> dict[str, Any]:
        safe_limit = min(max(1, int(limit or 10)), 50)
        safe_max_age = min(max(self._refresh_minutes, int(max_age_minutes or 24 * 60)), 7 * 24 * 60)

        rows = self._latest_records(max_age_minutes=safe_max_age)
        enriched_rows = []
        for row in rows:
            effective = self._effective_row_score(row)
            enriched = dict(row)
            enriched['effective_score'] = effective['score']
            enriched['effective_score_breakdown'] = effective['score_breakdown']
            enriched_rows.append(enriched)

        enriched_rows.sort(key=lambda item: float(item.get('effective_score') or 0.0), reverse=True)

        picks = []
        for row in enriched_rows[:safe_limit]:
            summary = row.get('summary') or {}
            picks.append(
                {
                    'symbol': row.get('symbol'),
                    'score': row.get('effective_score', row.get('score')),
                    'collected_at': row.get('collected_at'),
                    'summary_text': summary.get('summary_text', ''),
                    'key_points': summary.get('key_points', []),
                    'sources_ok': summary.get('sources_ok', 0),
                    'llm_provider': summary.get('llm_provider', 'local'),
                    'score_breakdown': row.get('effective_score_breakdown', row.get('score_breakdown', {})),
                }
            )

        return {
            'generated_at': datetime.now(timezone.utc).isoformat(),
            'refresh_minutes': self._refresh_minutes,
            'max_age_minutes': safe_max_age,
            'universe_covered': len(enriched_rows),
            'top_picks': picks,
        }

    def _effective_row_score(self, row: dict[str, Any]) -> dict[str, Any]:
        summary = row.get('summary') or {}
        stored_breakdown = row.get('score_breakdown') or summary.get('score_breakdown') or {}

        if isinstance(stored_breakdown, dict) and stored_breakdown.get('final') is not None:
            final = self._to_float(stored_breakdown.get('final'))
            if final is not None:
                return {'score': round(final, 2), 'score_breakdown': stored_breakdown}

        recalculated_score, recalculated_breakdown = self._compute_interest_score(
            sources=row.get('sources') or {},
            summary=summary,
        )
        return {
            'score': round(float(recalculated_score), 2),
            'score_breakdown': recalculated_breakdown,
        }

    _DEFAULT_UNIVERSE: list[str] = [
        'AAPL', 'MSFT', 'NVDA', 'AMZN', 'GOOGL', 'META', 'TSLA', 'AMD', 'NFLX', 'AVGO',
        'ADBE', 'CRM', 'ORCL', 'INTC', 'QCOM', 'MU', 'SHOP', 'UBER', 'PLTR', 'SNOW',
        'JPM', 'BAC', 'GS', 'V', 'MA', 'UNH', 'LLY', 'JNJ', 'XOM', 'CVX',
        'SPY', 'QQQ', 'IWM', 'SMH', 'XLF', 'XLK', 'XLE', 'ARKK', 'SOXX', 'TLT',
    ]

    def us_universe(self) -> list[str]:
        universe_path = self._db_path.parent / 'us_universe.json'
        try:
            if universe_path.exists():
                data = self._safe_json_load(universe_path.read_text(encoding='utf-8'))
                symbols = data.get('symbols') if isinstance(data, dict) else None
                if isinstance(symbols, list) and len(symbols) >= 5:
                    return [str(s).strip().upper() for s in symbols if str(s).strip()]
        except Exception:
            pass
        return list(self._DEFAULT_UNIVERSE)

    def universe_status(self) -> dict[str, Any]:
        universe_path = self._db_path.parent / 'us_universe.json'
        try:
            if universe_path.exists():
                data = self._safe_json_load(universe_path.read_text(encoding='utf-8'))
                if isinstance(data, dict) and isinstance(data.get('symbols'), list):
                    return {
                        'source': 'llm_discovered',
                        'discovered_at': data.get('discovered_at'),
                        'llm_provider': data.get('llm_provider'),
                        'rationale': data.get('rationale', ''),
                        'symbols': data['symbols'],
                        'count': len(data['symbols']),
                    }
        except Exception:
            pass
        return {
            'source': 'default_hardcoded',
            'discovered_at': None,
            'llm_provider': None,
            'rationale': 'Liste par défaut (40 symboles US représentatifs).',
            'symbols': list(self._DEFAULT_UNIVERSE),
            'count': len(self._DEFAULT_UNIVERSE),
        }

    async def discover_universe_with_llm(self, count: int = 40, context: str = '') -> dict[str, Any]:
        safe_count = min(max(10, int(count or 40)), 100)
        llm_result = await self._discover_universe_once(count=safe_count, context=context)
        if llm_result.get('status') != 'ok':
            return {
                'discovered': False,
                'error': llm_result.get('error') or 'LLM unavailable',
                'raw_content': llm_result.get('raw_content') or '',
                'current': self.universe_status(),
            }

        cleaned = llm_result.get('symbols') or []
        rationale = llm_result.get('rationale') or ''
        provider = llm_result.get('provider') or 'unknown'
        discovered_at = self._write_universe_file(
            symbols=cleaned,
            llm_provider=provider,
            rationale=rationale,
        )

        return {
            'discovered': True,
            'symbols': cleaned,
            'count': len(cleaned),
            'rationale': rationale,
            'llm_provider': provider,
            'discovered_at': discovered_at,
        }

    async def discover_universe_with_llm_batches(
        self,
        count_per_batch: int = 60,
        batches: int = 3,
        context: str = '',
    ) -> dict[str, Any]:
        safe_count = min(max(10, int(count_per_batch or 60)), 100)
        safe_batches = min(max(1, int(batches or 3)), 5)

        merged_symbols: list[str] = []
        seen: set[str] = set()
        for symbol in self.us_universe():
            normalized = re.sub(r'[^A-Z0-9.]', '', str(symbol).strip().upper())[:10]
            if normalized and normalized not in seen:
                seen.add(normalized)
                merged_symbols.append(normalized)

        batch_results: list[dict[str, Any]] = []
        rationale_chunks: list[str] = []

        for batch_index in range(safe_batches):
            exclusion_seed = ', '.join(merged_symbols[-80:]) if merged_symbols else ''
            batch_context_parts = [
                f'Batch {batch_index + 1}/{safe_batches}.',
                'Favorise des symboles différents de ceux déjà proposés.',
            ]
            if exclusion_seed:
                batch_context_parts.append(f'À éviter si possible: {exclusion_seed}.')
            if (context or '').strip():
                batch_context_parts.append(f'Contrainte utilisateur: {(context or "").strip()}')
            batch_context = ' '.join(batch_context_parts)

            discovered = await self._discover_universe_once(count=safe_count, context=batch_context)
            if discovered.get('status') != 'ok':
                batch_results.append(
                    {
                        'batch': batch_index + 1,
                        'status': 'error',
                        'error': discovered.get('error') or 'unknown error',
                    }
                )
                continue

            symbols = discovered.get('symbols') or []
            added = 0
            for symbol in symbols:
                if symbol not in seen:
                    seen.add(symbol)
                    merged_symbols.append(symbol)
                    added += 1

            rationale_piece = str(discovered.get('rationale') or '').strip()
            if rationale_piece:
                rationale_chunks.append(rationale_piece)

            batch_results.append(
                {
                    'batch': batch_index + 1,
                    'status': 'ok',
                    'provider': discovered.get('provider', 'unknown'),
                    'returned': len(symbols),
                    'added': added,
                }
            )

        if len(merged_symbols) < 5:
            return {
                'discovered': False,
                'error': 'No valid universe produced after batch discovery.',
                'batches': batch_results,
                'current': self.universe_status(),
            }

        final_rationale = ' | '.join(chunk[:220] for chunk in rationale_chunks[:3])[:700]
        discovered_at = self._write_universe_file(
            symbols=merged_symbols,
            llm_provider='multi-batch',
            rationale=final_rationale,
        )

        return {
            'discovered': True,
            'symbols': merged_symbols,
            'count': len(merged_symbols),
            'llm_provider': 'multi-batch',
            'discovered_at': discovered_at,
            'batches': batch_results,
            'rationale': final_rationale,
        }

    async def _discover_universe_once(self, count: int, context: str = '') -> dict[str, Any]:
        safe_count = min(max(10, int(count or 40)), 100)
        context_block = f'\nContexte supplémentaire: {context.strip()}' if (context or '').strip() else ''

        prompt = (
            f'Tu es le moteur Ceibo. Génère une liste de {safe_count} symboles d\'actions ou ETFs US '
            'cotés sur NASDAQ ou NYSE, diversifiés et intéressants à surveiller. '
            'Réponds en JSON strict sans markdown : '
            '{"symbols":["AAPL",...],"rationale":"..."}. '
            'Critères: liquidité élevée, diversification sectorielle (tech, finance, santé, énergie, '
            'consommation, ETFs sectoriels), momentum ou actualité récente. '
            'Uniquement des symboles réels et valides sur US markets.'
            + context_block
        )

        llm_result = await self._query_best_provider(prompt)
        if llm_result.get('status') != 'ok':
            return {
                'status': 'error',
                'error': f'LLM unavailable: {llm_result.get("error", llm_result.get("status"))}',
            }

        parsed = self._extract_json_object(str(llm_result.get('content') or ''))
        raw_symbols = parsed.get('symbols') if isinstance(parsed, dict) else None
        if not isinstance(raw_symbols, list) or len(raw_symbols) < 5:
            fallback_symbols = self._extract_symbols_from_text(str(llm_result.get('content') or ''))
            if len(fallback_symbols) >= 5:
                return {
                    'status': 'ok',
                    'symbols': fallback_symbols,
                    'rationale': 'Extracted from non-structured LLM output.',
                    'provider': llm_result.get('provider', 'unknown'),
                }
            return {
                'status': 'error',
                'error': 'LLM did not return a valid symbols list.',
                'raw_content': str(llm_result.get('content') or '')[:400],
            }

        cleaned = []
        seen: set[str] = set()
        for item in raw_symbols:
            sym = re.sub(r'[^A-Z0-9.]', '', str(item).strip().upper())[:10]
            if sym and sym not in seen:
                seen.add(sym)
                cleaned.append(sym)

        rationale = str(parsed.get('rationale') or '').strip()[:500]
        return {
            'status': 'ok',
            'symbols': cleaned,
            'rationale': rationale,
            'provider': llm_result.get('provider', 'unknown'),
        }

    def _write_universe_file(self, symbols: list[str], llm_provider: str, rationale: str) -> str:
        discovered_at = datetime.now(timezone.utc).isoformat()
        universe_data = {
            'symbols': symbols,
            'discovered_at': discovered_at,
            'llm_provider': llm_provider,
            'rationale': str(rationale or '').strip()[:700],
            'count': len(symbols),
        }

        universe_path = self._db_path.parent / 'us_universe.json'
        universe_path.parent.mkdir(parents=True, exist_ok=True)
        universe_path.write_text(json.dumps(universe_data, ensure_ascii=False, indent=2), encoding='utf-8')
        return discovered_at

    async def _auto_collection_loop(self) -> None:
        try:
            while True:
                started = datetime.now(timezone.utc)
                self._auto_last_run_at = started.isoformat()
                self._auto_next_run_at = None

                try:
                    result = await self.collect_us_market(limit=self._auto_limit, force_refresh=self._auto_force_refresh)
                    self._auto_last_result = {
                        'generated_at': result.get('generated_at'),
                        'symbols_requested': result.get('symbols_requested'),
                        'symbols_ok': result.get('symbols_ok'),
                        'symbols_error': result.get('symbols_error'),
                    }
                    self._auto_last_error = ''
                except Exception as exc:
                    self._auto_last_error = str(exc)

                elapsed = (datetime.now(timezone.utc) - started).total_seconds()
                sleep_seconds = max(1.0, (self._refresh_minutes * 60.0) - elapsed)
                self._auto_next_run_at = (datetime.now(timezone.utc) + timedelta(seconds=sleep_seconds)).isoformat()
                await asyncio.sleep(sleep_seconds)
        except asyncio.CancelledError:
            raise

    async def _fetch_sources(self, symbol: str) -> dict[str, Any]:
        async with httpx.AsyncClient(timeout=10.0, follow_redirects=True) as client:
            yahoo_task = self._fetch_yahoo(client, symbol)
            google_task = self._fetch_google_finance(client, symbol)
            boursorama_task = self._fetch_boursorama(client, symbol)
            quandl_task = self._fetch_quandl_market(client, symbol)
            newsapi_task = self._fetch_newsapi_market(client, symbol)
            yahoo, google, boursorama, quandl, newsapi = await asyncio.gather(
                yahoo_task, google_task, boursorama_task, quandl_task, newsapi_task
            )

        yahoo_data = yahoo.get('data') if isinstance(yahoo, dict) else None
        if isinstance(yahoo_data, dict) and (
            yahoo_data.get('market_cap') is None or yahoo_data.get('week52_high') is None
        ):
            supplement = await self._fetch_supplement_market_data(symbol)
            for field in ('market_cap', 'week52_high', 'week52_low'):
                if yahoo_data.get(field) is None and supplement.get(field) is not None:
                    yahoo_data[field] = supplement[field]
            if supplement.get('supplement_source'):
                yahoo_data['supplement_source'] = supplement['supplement_source']

        return {
            'yahoo_finance': yahoo,
            'google_finance': google,
            'boursorama': boursorama,
            'quandl': quandl,
            'newsapi': newsapi,
        }

    async def _fetch_quandl_market(self, client: httpx.AsyncClient, symbol: str) -> dict[str, Any]:
        api_key = (settings.quandl_api_key or '').strip()

        # Preferred: symbol-level EOD. Fallback: macro market proxy (US 10Y yield).
        symbol_url = f'https://data.nasdaq.com/api/v3/datasets/EOD/{symbol}/data.json'
        params = {'rows': 2, 'order': 'desc'}
        if api_key:
            params['api_key'] = api_key
        try:
            response = await client.get(symbol_url, params=params)
            if response.status_code == 403 and api_key:
                response = await client.get(symbol_url, params={'rows': 2, 'order': 'desc'})
            response.raise_for_status()
            payload = response.json()
            dataset_data = (payload.get('dataset_data') or {}) if isinstance(payload, dict) else {}
            data_rows = dataset_data.get('data') or []
            columns = dataset_data.get('column_names') or []

            if isinstance(data_rows, list) and len(data_rows) >= 1 and isinstance(columns, list) and columns:
                row_map = dict(zip(columns, data_rows[0]))
                prev_map = dict(zip(columns, data_rows[1])) if len(data_rows) > 1 else {}
                close = self._to_float(row_map.get('Close') or row_map.get('close'))
                prev_close = self._to_float(prev_map.get('Close') or prev_map.get('close'))
                change_pct = None
                if close is not None and prev_close not in (None, 0):
                    change_pct = ((close - prev_close) / prev_close) * 100.0

                return {
                    'status': 'ok',
                    'source_url': symbol_url,
                    'data': {
                        'symbol': symbol,
                        'dataset': 'EOD',
                        'last_date': row_map.get('Date') or row_map.get('date'),
                        'close': close,
                        'change_pct': change_pct,
                        'volume': self._to_float(row_map.get('Volume') or row_map.get('volume')),
                    },
                }
        except Exception:
            pass

        macro_url = 'https://data.nasdaq.com/api/v3/datasets/FRED/DGS10/data.json'
        try:
            macro_params = {'rows': 2, 'order': 'desc'}
            if api_key:
                macro_params['api_key'] = api_key
            response = await client.get(macro_url, params=macro_params)
            if response.status_code == 403 and api_key:
                response = await client.get(macro_url, params={'rows': 2, 'order': 'desc'})
            response.raise_for_status()
            payload = response.json()
            dataset_data = (payload.get('dataset_data') or {}) if isinstance(payload, dict) else {}
            data_rows = dataset_data.get('data') or []
            if isinstance(data_rows, list) and len(data_rows) >= 1:
                latest = data_rows[0]
                prev = data_rows[1] if len(data_rows) > 1 else None
                latest_value = self._to_float(latest[1] if len(latest) > 1 else None)
                prev_value = self._to_float(prev[1] if isinstance(prev, list) and len(prev) > 1 else None)
                delta = (latest_value - prev_value) if (latest_value is not None and prev_value is not None) else None
                return {
                    'status': 'ok',
                    'source_url': macro_url,
                    'data': {
                        'symbol': symbol,
                        'dataset': 'FRED/DGS10',
                        'last_date': latest[0] if isinstance(latest, list) and latest else None,
                        'value': latest_value,
                        'delta': delta,
                    },
                }
        except Exception as exc:
            return {'status': 'error', 'source_url': macro_url, 'error': str(exc)}

        return {'status': 'error', 'source_url': macro_url, 'error': 'Quandl dataset unavailable'}

    async def _fetch_newsapi_market(self, client: httpx.AsyncClient, symbol: str) -> dict[str, Any]:
        api_key = (settings.newsapi_api_key or '').strip()
        if not api_key:
            return {'status': 'error', 'source_url': '', 'error': 'NEWSAPI_API_KEY not configured'}

        if api_key.startswith('pub_'):
            url = 'https://newsdata.io/api/1/news'
            query = f'{symbol} stock market earnings analyst outlook'
            try:
                response = await client.get(
                    url,
                    params={
                        'apikey': api_key,
                        'q': query,
                        'language': 'en',
                        'size': 8,
                    },
                )
                response.raise_for_status()
                payload = response.json()
                results = payload.get('results') if isinstance(payload, dict) else []
                if not isinstance(results, list):
                    results = []

                headlines = [str((item or {}).get('title') or '').strip() for item in results][:5]
                headlines = [item for item in headlines if item]
                negative_words = ('downgrade', 'lawsuit', 'miss', 'cuts', 'fall', 'risk', 'warning')
                positive_words = ('upgrade', 'beats', 'growth', 'record', 'strong', 'raises', 'outperform')

                text_blob = ' '.join(headlines).lower()
                neg = sum(1 for w in negative_words if w in text_blob)
                pos = sum(1 for w in positive_words if w in text_blob)
                sentiment_hint = 'neutre'
                if pos > neg:
                    sentiment_hint = 'haussier'
                elif neg > pos:
                    sentiment_hint = 'baissier'

                return {
                    'status': 'ok',
                    'source_url': url,
                    'data': {
                        'symbol': symbol,
                        'article_count': int(payload.get('totalResults') or len(results)) if isinstance(payload, dict) else len(results),
                        'headlines': headlines,
                        'sentiment_hint': sentiment_hint,
                    },
                }
            except Exception as exc:
                return {'status': 'error', 'source_url': url, 'error': str(exc)}

        url = 'https://newsapi.org/v2/everything'
        query = f'({symbol} OR "{symbol} stock") AND (earnings OR guidance OR analyst OR outlook OR market)'
        try:
            response = await client.get(
                url,
                params={
                    'q': query,
                    'language': 'en',
                    'sortBy': 'publishedAt',
                    'pageSize': 8,
                    'apiKey': api_key,
                },
            )
            response.raise_for_status()
            payload = response.json()
            articles = payload.get('articles') if isinstance(payload, dict) else []
            if not isinstance(articles, list):
                articles = []

            headlines = [str((item or {}).get('title') or '').strip() for item in articles][:5]
            headlines = [item for item in headlines if item]
            negative_words = ('downgrade', 'lawsuit', 'miss', 'cuts', 'fall', 'risk', 'warning')
            positive_words = ('upgrade', 'beats', 'growth', 'record', 'strong', 'raises', 'outperform')

            text_blob = ' '.join(headlines).lower()
            neg = sum(1 for w in negative_words if w in text_blob)
            pos = sum(1 for w in positive_words if w in text_blob)
            sentiment_hint = 'neutre'
            if pos > neg:
                sentiment_hint = 'haussier'
            elif neg > pos:
                sentiment_hint = 'baissier'

            return {
                'status': 'ok',
                'source_url': url,
                'data': {
                    'symbol': symbol,
                    'article_count': int(payload.get('totalResults') or len(articles)) if isinstance(payload, dict) else len(articles),
                    'headlines': headlines,
                    'sentiment_hint': sentiment_hint,
                },
            }
        except Exception as exc:
            return {'status': 'error', 'source_url': url, 'error': str(exc)}

    async def _fetch_supplement_market_data(self, symbol: str) -> dict[str, Any]:
        if (settings.fmp_api_key or '').strip():
            try:
                data = await self._fetch_fmp(symbol)
                if data.get('market_cap') is not None or data.get('week52_high') is not None:
                    return data
            except Exception:
                pass

        if (settings.alpha_vantage_api_key or '').strip():
            try:
                data = await self._fetch_alpha_vantage_overview(symbol)
                if data.get('market_cap') is not None or data.get('week52_high') is not None:
                    return data
            except Exception:
                pass

        try:
            return await asyncio.to_thread(self._yfinance_sync, symbol)
        except Exception:
            return {}

    async def _fetch_fmp(self, symbol: str) -> dict[str, Any]:
        api_key = (settings.fmp_api_key or '').strip()
        if not api_key:
            return {}

        quote_url = 'https://financialmodelingprep.com/stable/quote'
        profile_url = 'https://financialmodelingprep.com/stable/profile'
        async with httpx.AsyncClient(timeout=10.0) as client:
            quote_resp = await client.get(quote_url, params={'symbol': symbol, 'apikey': api_key})
            quote_resp.raise_for_status()
            quote_data = quote_resp.json()

            profile_resp = await client.get(profile_url, params={'symbol': symbol, 'apikey': api_key})
            profile_resp.raise_for_status()
            profile_data = profile_resp.json()

        quote_df = pd.DataFrame(quote_data if isinstance(quote_data, list) else [])
        profile_df = pd.DataFrame(profile_data if isinstance(profile_data, list) else [])
        quote_first = quote_df.iloc[0].to_dict() if not quote_df.empty else {}
        profile_first = profile_df.iloc[0].to_dict() if not profile_df.empty else {}
        combined = {
            'marketCap': profile_first.get('marketCap'),
            'yearHigh': quote_first.get('yearHigh'),
            'yearLow': quote_first.get('yearLow'),
        }
        return self._normalize_market_payload(
            payload=combined,
            source='fmp',
            mapping={
                'market_cap': 'marketCap',
                'week52_high': 'yearHigh',
                'week52_low': 'yearLow',
            },
        )

    async def _fetch_alpha_vantage_overview(self, symbol: str) -> dict[str, Any]:
        api_key = (settings.alpha_vantage_api_key or '').strip()
        if not api_key:
            return {}
        url = 'https://www.alphavantage.co/query'
        async with httpx.AsyncClient(timeout=10.0) as client:
            response = await client.get(url, params={
                'function': 'OVERVIEW',
                'symbol': symbol,
                'apikey': api_key,
            })
            response.raise_for_status()
            data = response.json()
        if not data or 'Symbol' not in data:
            return {}
        return self._normalize_market_payload(
            payload=data,
            source='alpha_vantage',
            mapping={
                'market_cap': 'MarketCapitalization',
                'week52_high': '52WeekHigh',
                'week52_low': '52WeekLow',
            },
        )

    def _yfinance_sync(self, symbol: str) -> dict[str, Any]:
        import yfinance as yf  # noqa: PLC0415
        info = yf.Ticker(symbol).info or {}
        return self._normalize_market_payload(
            payload=info,
            source='yfinance',
            mapping={
                'market_cap': 'marketCap',
                'week52_high': 'fiftyTwoWeekHigh',
                'week52_low': 'fiftyTwoWeekLow',
            },
        )

    def _normalize_market_payload(self, payload: dict[str, Any], source: str, mapping: dict[str, str]) -> dict[str, Any]:
        frame = pd.DataFrame([payload if isinstance(payload, dict) else {}])
        if frame.empty:
            return {}

        normalized: dict[str, Any] = {'supplement_source': source}
        for target_key, source_key in mapping.items():
            raw_value = frame.at[0, source_key] if source_key in frame.columns else None
            numeric = pd.to_numeric(pd.Series([raw_value]), errors='coerce').iloc[0]
            normalized[target_key] = None if pd.isna(numeric) else float(numeric)

        return normalized

    def _normalize_numeric_fields(self, payload: dict[str, Any], mapping: dict[str, str]) -> dict[str, Optional[float]]:
        frame = pd.DataFrame([payload if isinstance(payload, dict) else {}])
        if frame.empty:
            return {key: None for key in mapping}

        normalized: dict[str, Optional[float]] = {}
        for target_key, source_key in mapping.items():
            raw_value = frame.at[0, source_key] if source_key in frame.columns else None
            numeric = pd.to_numeric(pd.Series([raw_value]), errors='coerce').iloc[0]
            normalized[target_key] = None if pd.isna(numeric) else float(numeric)

        return normalized

    async def _fetch_yahoo(self, client: httpx.AsyncClient, symbol: str) -> dict[str, Any]:
        url = f'https://query1.finance.yahoo.com/v7/finance/quote?symbols={symbol}'
        try:
            response = await client.get(url, headers={'User-Agent': 'Mozilla/5.0'})
            if response.status_code == 401:
                return await self._fetch_yahoo_chart_fallback(client, symbol)
            response.raise_for_status()
            payload = response.json()
            result = (((payload or {}).get('quoteResponse') or {}).get('result') or [])
            first = result[0] if result else {}
            if not isinstance(first, dict) or not first:
                return await self._fetch_yahoo_chart_fallback(client, symbol)

            numeric = self._normalize_numeric_fields(
                payload=first,
                mapping={
                    'price': 'regularMarketPrice',
                    'change_pct': 'regularMarketChangePercent',
                    'change': 'regularMarketChange',
                    'day_high': 'regularMarketDayHigh',
                    'day_low': 'regularMarketDayLow',
                    'volume': 'regularMarketVolume',
                    'market_cap': 'marketCap',
                    'week52_high': 'fiftyTwoWeekHigh',
                    'week52_low': 'fiftyTwoWeekLow',
                },
            )

            return {
                'status': 'ok',
                'source_url': url,
                'data': {
                    'symbol': first.get('symbol'),
                    'short_name': first.get('shortName') or first.get('longName'),
                    'exchange': first.get('fullExchangeName') or first.get('exchange'),
                    'currency': first.get('currency'),
                    **numeric,
                },
            }
        except Exception as exc:
            fallback = await self._fetch_yahoo_chart_fallback(client, symbol)
            if fallback.get('status') == 'ok':
                return fallback
            return {'status': 'error', 'source_url': url, 'error': str(exc)}

    async def _fetch_yahoo_chart_fallback(self, client: httpx.AsyncClient, symbol: str) -> dict[str, Any]:
        fallback_url = f'https://query1.finance.yahoo.com/v8/finance/chart/{symbol}?interval=1d&range=1mo'
        try:
            response = await client.get(fallback_url, headers={'User-Agent': 'Mozilla/5.0'})
            response.raise_for_status()
            payload = response.json()

            chart = (payload or {}).get('chart') or {}
            result = (chart.get('result') or [None])[0] or {}
            meta = result.get('meta') or {}
            indicators = (result.get('indicators') or {}).get('quote') or [{}]
            quote_data = indicators[0] if indicators else {}

            closes = [item for item in (quote_data.get('close') or []) if item is not None]
            price = self._to_float(meta.get('regularMarketPrice'))
            previous_close = self._to_float(meta.get('previousClose'))
            if price is None and closes:
                price = self._to_float(closes[-1])
            if previous_close is None and len(closes) >= 2:
                previous_close = self._to_float(closes[-2])

            change = None
            change_pct = None
            if price is not None and previous_close not in (None, 0):
                change = price - previous_close
                change_pct = (change / previous_close) * 100.0

            numeric = self._normalize_numeric_fields(
                payload={
                    'price': price,
                    'change_pct': change_pct,
                    'change': change,
                    'day_high': meta.get('regularMarketDayHigh'),
                    'day_low': meta.get('regularMarketDayLow'),
                    'volume': meta.get('regularMarketVolume'),
                    'market_cap': meta.get('marketCap'),
                    'week52_high': meta.get('fiftyTwoWeekHigh'),
                    'week52_low': meta.get('fiftyTwoWeekLow'),
                },
                mapping={
                    'price': 'price',
                    'change_pct': 'change_pct',
                    'change': 'change',
                    'day_high': 'day_high',
                    'day_low': 'day_low',
                    'volume': 'volume',
                    'market_cap': 'market_cap',
                    'week52_high': 'week52_high',
                    'week52_low': 'week52_low',
                },
            )
            if numeric.get('week52_high') is None and closes:
                numeric['week52_high'] = float(max(closes))
            if numeric.get('week52_low') is None and closes:
                numeric['week52_low'] = float(min(closes))

            return {
                'status': 'ok',
                'source_url': fallback_url,
                'data': {
                    'symbol': symbol,
                    'short_name': meta.get('shortName') or meta.get('symbol') or symbol,
                    'exchange': meta.get('exchangeName') or meta.get('exchangeTimezoneName'),
                    'currency': meta.get('currency'),
                    **numeric,
                },
            }
        except Exception as exc:
            return {'status': 'error', 'source_url': fallback_url, 'error': str(exc)}

    async def _fetch_google_finance(self, client: httpx.AsyncClient, symbol: str) -> dict[str, Any]:
        exchanges = ('NASDAQ', 'NYSE')
        for exchange in exchanges:
            url = f'https://www.google.com/finance/quote/{symbol}:{exchange}'
            try:
                response = await client.get(url, headers={'User-Agent': 'Mozilla/5.0'})
                response.raise_for_status()
                html = response.text

                title = self._extract_html_text(r'<title>(.*?)</title>', html)
                price_text = self._extract_html_text(r'class="YMlKec fxKbKc">([^<]+)<', html)
                change_text = self._extract_html_text(r'class="P2Luy[^"]*">([^<]+)<', html)

                if change_text and '%' not in change_text:
                    change_text = ''

                numeric = self._normalize_numeric_fields(
                    payload={
                        'price_text': self._extract_price_from_text(price_text),
                        'change_pct_text': self._extract_change_pct_from_text(change_text),
                    },
                    mapping={
                        'price': 'price_text',
                        'change_pct': 'change_pct_text',
                    },
                )

                if price_text or change_text:
                    return {
                        'status': 'ok',
                        'source_url': url,
                        'data': {
                            'exchange_hint': exchange,
                            'title': title,
                            'price_text': price_text,
                            'change_text': change_text,
                            **numeric,
                        },
                    }
            except Exception:
                continue

        return {
            'status': 'error',
            'source_url': f'https://www.google.com/finance/quote/{symbol}:NASDAQ',
            'error': 'Google Finance parsing failed',
        }

    async def _fetch_boursorama(self, client: httpx.AsyncClient, symbol: str) -> dict[str, Any]:
        url = f'https://www.boursorama.com/recherche/?query={symbol}'
        try:
            response = await client.get(url, headers={'User-Agent': 'Mozilla/5.0'})
            response.raise_for_status()
            html = response.text

            title = self._extract_html_text(r'<title>(.*?)</title>', html)
            first_course_link = self._extract_html_text(r'href="(/cours/[^"]+)"', html)
            normalized_snippet = self._clean_text(html)[:280]

            if title or first_course_link:
                return {
                    'status': 'ok',
                    'source_url': url,
                    'data': {
                        'title': title,
                        'first_course_link': f'https://www.boursorama.com{first_course_link}' if first_course_link else None,
                        'snippet': normalized_snippet,
                    },
                }

            return {'status': 'error', 'source_url': url, 'error': 'No Boursorama content matched'}
        except Exception as exc:
            return {'status': 'error', 'source_url': url, 'error': str(exc)}

    async def _build_summary_with_llm(self, symbol: str, sources: dict[str, Any]) -> dict[str, Any]:
        sources_ok = 0
        for _, source_payload in sources.items():
            if (source_payload or {}).get('status') == 'ok':
                sources_ok += 1

        facts = self._build_llm_facts(symbol=symbol, sources=sources)

        prompt = (
            'Tu es le moteur LLM privé Ceibo. '\
            'Tu dois résumer des données de marché multi-sources pour un ticker US. '\
            'Réponds en JSON strict sans markdown avec ce schéma: '\
            '{"summary_text":"...","key_points":["..."],"sentiment":"haussier|neutre|baissier","risk":"faible|modéré|élevé"}. '\
            'N\'invente aucun chiffre: cite uniquement des valeurs présentes dans DATA. '\
            'Si une valeur source est marquée indisponible/incohérente, ignore-la.\n\n'
            f'Symbol: {symbol}\n'
            f'DATA: {json.dumps(facts, ensure_ascii=False)}'
        )

        llm_result = await self._query_best_provider(prompt)
        summary_text = ''
        key_points: list[str] = []
        sentiment = 'neutre'
        risk = 'modéré'

        if llm_result.get('status') == 'ok':
            parsed = self._extract_json_object(str(llm_result.get('content') or ''))
            if isinstance(parsed, dict):
                summary_text = str(parsed.get('summary_text') or '').strip()
                maybe_points = parsed.get('key_points')
                if isinstance(maybe_points, list):
                    key_points = [str(item).strip() for item in maybe_points if str(item).strip()][:6]
                sentiment = str(parsed.get('sentiment') or sentiment).strip().lower()
                risk = str(parsed.get('risk') or risk).strip().lower()

        if not summary_text:
            yahoo_data = (((sources or {}).get('yahoo_finance') or {}).get('data') or {})
            price = yahoo_data.get('price')
            change_pct = yahoo_data.get('change_pct')
            sources_total = max(1, len(sources or {}))
            summary_text = (
                f'{symbol}: synthèse locale (sans sortie LLM structurée). '
                f'Prix Yahoo={self._fmt(price)}, variation={self._fmt(change_pct)}%.'
            )
            key_points = [
                f'Sources valides: {sources_ok}/{sources_total}',
                'Certaines APIs externes (Google/Boursorama/Quandl/NewsAPI) peuvent être partiellement indisponibles.',
            ]

        return {
            'summary_text': summary_text,
            'key_points': key_points,
            'sentiment': sentiment,
            'risk': risk,
            'sources_ok': sources_ok,
            'llm_provider': llm_result.get('provider', 'local'),
            'llm_status': llm_result.get('status', 'fallback'),
        }

    def _build_llm_facts(self, symbol: str, sources: dict[str, Any]) -> dict[str, Any]:
        yahoo = ((sources or {}).get('yahoo_finance') or {})
        google = ((sources or {}).get('google_finance') or {})
        boursorama = ((sources or {}).get('boursorama') or {})
        quandl = ((sources or {}).get('quandl') or {})
        newsapi = ((sources or {}).get('newsapi') or {})

        yahoo_data = yahoo.get('data') if isinstance(yahoo, dict) else {}
        yahoo_price = self._to_float((yahoo_data or {}).get('price')) if isinstance(yahoo_data, dict) else None

        yahoo_facts = {
            'status': yahoo.get('status'),
            'symbol': (yahoo_data or {}).get('symbol') if isinstance(yahoo_data, dict) else None,
            'price': self._to_float((yahoo_data or {}).get('price')) if isinstance(yahoo_data, dict) else None,
            'change_pct': self._to_float((yahoo_data or {}).get('change_pct')) if isinstance(yahoo_data, dict) else None,
            'change': self._to_float((yahoo_data or {}).get('change')) if isinstance(yahoo_data, dict) else None,
            'day_high': self._to_float((yahoo_data or {}).get('day_high')) if isinstance(yahoo_data, dict) else None,
            'day_low': self._to_float((yahoo_data or {}).get('day_low')) if isinstance(yahoo_data, dict) else None,
            'volume': self._to_float((yahoo_data or {}).get('volume')) if isinstance(yahoo_data, dict) else None,
            'market_cap': self._to_float((yahoo_data or {}).get('market_cap')) if isinstance(yahoo_data, dict) else None,
            'week52_high': self._to_float((yahoo_data or {}).get('week52_high')) if isinstance(yahoo_data, dict) else None,
            'week52_low': self._to_float((yahoo_data or {}).get('week52_low')) if isinstance(yahoo_data, dict) else None,
            'supplement_source': (yahoo_data or {}).get('supplement_source') if isinstance(yahoo_data, dict) else None,
        }

        google_data = google.get('data') if isinstance(google, dict) else {}
        google_price = self._extract_price_from_text(str((google_data or {}).get('price_text') or ''))
        google_change_pct = self._extract_change_pct_from_text(str((google_data or {}).get('change_text') or ''))

        google_facts: dict[str, Any] = {
            'status': google.get('status'),
            'exchange_hint': (google_data or {}).get('exchange_hint') if isinstance(google_data, dict) else None,
        }

        if google_price is not None:
            if yahoo_price not in (None, 0):
                spread_pct = abs((google_price - yahoo_price) / yahoo_price) * 100.0
                if spread_pct <= 5.0:
                    google_facts['price'] = round(google_price, 6)
                else:
                    google_facts['price'] = None
                    google_facts['price_status'] = 'ignored_cross_source_divergence'
            else:
                google_facts['price'] = round(google_price, 6)

        if google_change_pct is not None and abs(google_change_pct) <= 25.0:
            google_facts['change_pct'] = round(google_change_pct, 6)

        boursorama_data = boursorama.get('data') if isinstance(boursorama, dict) else {}
        boursorama_facts = {
            'status': boursorama.get('status'),
            'title': (boursorama_data or {}).get('title') if isinstance(boursorama_data, dict) else None,
            'first_course_link': (boursorama_data or {}).get('first_course_link') if isinstance(boursorama_data, dict) else None,
        }

        quandl_data = quandl.get('data') if isinstance(quandl, dict) else {}
        quandl_facts = {
            'status': quandl.get('status'),
            'dataset': (quandl_data or {}).get('dataset') if isinstance(quandl_data, dict) else None,
            'last_date': (quandl_data or {}).get('last_date') if isinstance(quandl_data, dict) else None,
            'close': self._to_float((quandl_data or {}).get('close')) if isinstance(quandl_data, dict) else None,
            'change_pct': self._to_float((quandl_data or {}).get('change_pct')) if isinstance(quandl_data, dict) else None,
            'value': self._to_float((quandl_data or {}).get('value')) if isinstance(quandl_data, dict) else None,
            'delta': self._to_float((quandl_data or {}).get('delta')) if isinstance(quandl_data, dict) else None,
        }

        newsapi_data = newsapi.get('data') if isinstance(newsapi, dict) else {}
        newsapi_facts = {
            'status': newsapi.get('status'),
            'article_count': int((newsapi_data or {}).get('article_count') or 0) if isinstance(newsapi_data, dict) else 0,
            'sentiment_hint': (newsapi_data or {}).get('sentiment_hint') if isinstance(newsapi_data, dict) else None,
            'headlines': ((newsapi_data or {}).get('headlines') or [])[:5] if isinstance(newsapi_data, dict) else [],
        }

        return {
            'symbol': symbol,
            'sources_ok': sum(1 for payload in (yahoo, google, boursorama, quandl, newsapi) if (payload or {}).get('status') == 'ok'),
            'yahoo_finance': yahoo_facts,
            'google_finance': google_facts,
            'boursorama': boursorama_facts,
            'quandl': quandl_facts,
            'newsapi': newsapi_facts,
        }

    async def _query_best_provider(self, prompt: str) -> dict[str, Any]:
        providers = [
            ('openai', (settings.openai_api_key or '').strip(), self._query_openai),
            ('anthropic', (settings.anthropic_api_key or '').strip(), self._query_anthropic),
            ('openrouter', (settings.openrouter_api_key or '').strip(), self._query_openrouter),
            ('gemini', (settings.gemini_api_key or '').strip(), self._query_gemini),
        ]

        provider_errors: list[dict[str, Any]] = []
        for provider_name, key, handler in providers:
            if not key:
                continue
            try:
                content, attempts = await self._call_provider_with_retries(provider_name=provider_name, handler=handler, prompt=prompt)
                return {
                    'provider': provider_name,
                    'status': 'ok',
                    'content': content,
                    'attempts': attempts,
                }
            except Exception as exc:
                provider_errors.append({'provider': provider_name, 'error': str(exc)})

        return {
            'provider': 'local',
            'status': 'fallback',
            'content': '',
            'errors': provider_errors,
        }

    async def _call_provider_with_retries(self, provider_name: str, handler, prompt: str) -> tuple[str, int]:
        last_error = 'unknown error'
        attempts = max(1, int(self._llm_max_attempts or 1))

        for attempt in range(1, attempts + 1):
            try:
                content = await asyncio.wait_for(handler(prompt), timeout=self._llm_timeout_seconds)
                text = str(content or '').strip()
                if not text:
                    raise ValueError('Empty LLM response')
                return text, attempt
            except Exception as exc:
                last_error = str(exc)

        raise RuntimeError(f'{provider_name} failed after {attempts} attempt(s): {last_error}')

    async def _query_openai(self, prompt: str) -> str:
        url = 'https://api.openai.com/v1/responses'
        payload = {
            'model': settings.openai_model,
            'input': [
                {'role': 'system', 'content': 'Réponds uniquement en JSON strict sans markdown.'},
                {'role': 'user', 'content': prompt},
            ],
            'temperature': 0.1,
            'max_output_tokens': 420,
        }
        headers = {'Authorization': f"Bearer {(settings.openai_api_key or '').strip()}"}

        async with httpx.AsyncClient(timeout=self._llm_timeout_seconds) as client:
            response = await client.post(url, json=payload, headers=headers)
            response.raise_for_status()
            data = response.json()

        content = data.get('output_text') or ''
        if content:
            return content

        output_items = data.get('output') or []
        for item in output_items:
            for content_item in item.get('content', []):
                if content_item.get('type') == 'output_text':
                    return str(content_item.get('text') or '')
        return ''

    async def _query_anthropic(self, prompt: str) -> str:
        url = 'https://api.anthropic.com/v1/messages'
        payload = {
            'model': settings.anthropic_model,
            'max_tokens': 420,
            'temperature': 0.1,
            'system': 'Réponds uniquement en JSON strict sans markdown.',
            'messages': [{'role': 'user', 'content': prompt}],
        }
        headers = {
            'x-api-key': (settings.anthropic_api_key or '').strip(),
            'anthropic-version': '2023-06-01',
        }

        async with httpx.AsyncClient(timeout=self._llm_timeout_seconds) as client:
            response = await client.post(url, json=payload, headers=headers)
            response.raise_for_status()
            data = response.json()

        for item in data.get('content', []):
            if item.get('type') == 'text':
                return str(item.get('text') or '')
        return ''

    async def _query_openrouter(self, prompt: str) -> str:
        url = 'https://openrouter.ai/api/v1/chat/completions'
        payload = {
            'model': settings.openrouter_model,
            'messages': [
                {'role': 'system', 'content': 'Réponds uniquement en JSON strict sans markdown.'},
                {'role': 'user', 'content': prompt},
            ],
            'temperature': 0.1,
            'max_tokens': 420,
        }
        headers = {'Authorization': f"Bearer {(settings.openrouter_api_key or '').strip()}"}

        async with httpx.AsyncClient(timeout=self._llm_timeout_seconds) as client:
            response = await client.post(url, json=payload, headers=headers)
            response.raise_for_status()
            data = response.json()

        choices = data.get('choices') or []
        if choices:
            return str((choices[0].get('message') or {}).get('content') or '')
        return ''

    async def _query_gemini(self, prompt: str) -> str:
        model_name = settings.gemini_model
        api_key = (settings.gemini_api_key or '').strip()
        url = f'https://generativelanguage.googleapis.com/v1beta/models/{model_name}:generateContent'
        payload = {
            'systemInstruction': {'parts': [{'text': 'Réponds uniquement en JSON strict sans markdown.'}]},
            'contents': [{'role': 'user', 'parts': [{'text': prompt}]}],
            'generationConfig': {'temperature': 0.1, 'maxOutputTokens': 420},
        }

        async with httpx.AsyncClient(timeout=self._llm_timeout_seconds) as client:
            response = await client.post(url, json=payload, params={'key': api_key})
            response.raise_for_status()
            data = response.json()

        candidates = data.get('candidates') or []
        if not candidates:
            return ''
        parts = (candidates[0].get('content') or {}).get('parts') or []
        return ''.join(str(part.get('text') or '') for part in parts if isinstance(part, dict))

    def _compute_interest_score(self, sources: dict[str, Any], summary: Optional[dict[str, Any]] = None) -> tuple[float, dict[str, Any]]:
        base_score = 50.0
        score = base_score
        yahoo = ((sources or {}).get('yahoo_finance') or {})
        google = ((sources or {}).get('google_finance') or {})
        boursorama = ((sources or {}).get('boursorama') or {})

        breakdown: dict[str, Any] = {
            'base': round(base_score, 2),
            'components': {},
            'notes': [],
        }

        ok_sources = sum(1 for src in (yahoo, google, boursorama) if src.get('status') == 'ok')
        source_score = ok_sources * 6.0
        score += source_score
        breakdown['components']['sources_available'] = round(source_score, 2)

        yahoo_data = yahoo.get('data') if isinstance(yahoo, dict) else {}
        if isinstance(yahoo_data, dict):
            pct = self._to_float(yahoo_data.get('change_pct'))
            if pct is not None:
                momentum_score = self._clamp(pct, -5.0, 5.0) * 3.0
                score += momentum_score
                breakdown['components']['yahoo_momentum_pct'] = round(momentum_score, 2)
            else:
                breakdown['notes'].append('Yahoo change_pct unavailable')

            price = self._to_float(yahoo_data.get('price'))
            day_high = self._to_float(yahoo_data.get('day_high'))
            day_low = self._to_float(yahoo_data.get('day_low'))
            if price and day_high is not None and day_low is not None and price > 0:
                intraday_range_pct = ((day_high - day_low) / price) * 100.0
                volatility_penalty = -self._clamp(intraday_range_pct - 2.0, 0.0, 5.0) * 1.2
                score += volatility_penalty
                breakdown['components']['yahoo_intraday_risk'] = round(volatility_penalty, 2)

            volume = self._to_float(yahoo_data.get('volume'))
            if volume is not None and volume > 0:
                liquidity_score = self._clamp((volume / 10_000_000.0) * 3.0, 0.0, 4.0)
                score += liquidity_score
                breakdown['components']['yahoo_liquidity'] = round(liquidity_score, 2)
            else:
                breakdown['notes'].append('Yahoo volume unavailable')

            market_cap = self._to_float(yahoo_data.get('market_cap'))
            if market_cap is not None:
                if market_cap >= 2_000_000_000_000:
                    market_cap_score = 4.0
                elif market_cap >= 200_000_000_000:
                    market_cap_score = 2.0
                elif market_cap >= 20_000_000_000:
                    market_cap_score = 1.0
                else:
                    market_cap_score = 0.0
                score += market_cap_score
                breakdown['components']['yahoo_market_cap'] = round(market_cap_score, 2)

            price_now = self._to_float(yahoo_data.get('price'))
            week52_high = self._to_float(yahoo_data.get('week52_high'))
            week52_low = self._to_float(yahoo_data.get('week52_low'))
            if price_now and week52_high and week52_low and week52_high > week52_low:
                position = (price_now - week52_low) / (week52_high - week52_low)
                week52_score = round(self._clamp(position, 0.0, 1.0) * 4.0, 2)
                score += week52_score
                breakdown['components']['yahoo_week52_position'] = week52_score
                breakdown['notes'].append(f'Position 52sem: {position * 100:.0f}% ({price_now:.2f} / [{week52_low:.2f}–{week52_high:.2f}])')
            else:
                breakdown['notes'].append('Yahoo week52 range unavailable')

        google_data = google.get('data') if isinstance(google, dict) else {}
        if isinstance(google_data, dict) and (google_data.get('price_text') or google_data.get('change_text')):
            google_signal_score = 3.0
            score += google_signal_score
            breakdown['components']['google_signal'] = round(google_signal_score, 2)

            yahoo_price = self._to_float((yahoo_data or {}).get('price')) if isinstance(yahoo_data, dict) else None
            google_price = self._extract_price_from_text(str(google_data.get('price_text') or ''))
            if yahoo_price and google_price:
                spread_pct = abs((google_price - yahoo_price) / yahoo_price) * 100.0
                if spread_pct > 2.0:
                    coherence_penalty = -self._clamp(spread_pct - 2.0, 0.0, 8.0)
                    score += coherence_penalty
                    breakdown['components']['cross_source_coherence'] = round(coherence_penalty, 2)
                    breakdown['notes'].append(f'Cross-source divergence {spread_pct:.2f}% (Google vs Yahoo)')
                else:
                    coherence_bonus = 1.0
                    score += coherence_bonus
                    breakdown['components']['cross_source_coherence'] = round(coherence_bonus, 2)
            else:
                breakdown['notes'].append('Cross-source coherence unavailable')
        else:
            breakdown['notes'].append('Google signal unavailable')

        boursorama_data = boursorama.get('data') if isinstance(boursorama, dict) else {}
        if isinstance(boursorama_data, dict) and boursorama_data.get('title'):
            boursorama_score = 1.5
            score += boursorama_score
            breakdown['components']['boursorama_presence'] = round(boursorama_score, 2)
        else:
            breakdown['notes'].append('Boursorama signal unavailable')

        sentiment = str((summary or {}).get('sentiment') or '').strip().lower()
        if sentiment == 'haussier':
            sentiment_score = 2.0
            score += sentiment_score
            breakdown['components']['llm_sentiment'] = round(sentiment_score, 2)
        elif sentiment == 'baissier':
            sentiment_score = -2.0
            score += sentiment_score
            breakdown['components']['llm_sentiment'] = round(sentiment_score, 2)

        risk = str((summary or {}).get('risk') or '').strip().lower()
        if risk == 'élevé':
            risk_score = -2.0
            score += risk_score
            breakdown['components']['llm_risk'] = round(risk_score, 2)
        elif risk == 'faible':
            risk_score = 1.0
            score += risk_score
            breakdown['components']['llm_risk'] = round(risk_score, 2)

        final_score = self._clamp(score, 0.0, 100.0)
        breakdown['final'] = round(final_score, 2)
        return final_score, breakdown

    def _ensure_db(self) -> None:
        self._db_path.parent.mkdir(parents=True, exist_ok=True)
        with sqlite3.connect(self._db_path) as conn:
            conn.execute(
                '''
                CREATE TABLE IF NOT EXISTS quote_intel_records (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    symbol TEXT NOT NULL,
                    collected_at TEXT NOT NULL,
                    score REAL NOT NULL,
                    sources_json TEXT NOT NULL,
                    summary_json TEXT NOT NULL
                )
                '''
            )
            conn.execute(
                'CREATE INDEX IF NOT EXISTS idx_quote_intel_symbol_time ON quote_intel_records(symbol, collected_at DESC)'
            )

    def _insert_record(self, record: dict[str, Any]) -> None:
        with sqlite3.connect(self._db_path) as conn:
            conn.execute(
                '''
                INSERT INTO quote_intel_records(symbol, collected_at, score, sources_json, summary_json)
                VALUES (?, ?, ?, ?, ?)
                ''',
                (
                    str(record.get('symbol') or ''),
                    str(record.get('collected_at') or ''),
                    float(record.get('score') or 0.0),
                    json.dumps(record.get('sources') or {}, ensure_ascii=False),
                    json.dumps(record.get('summary') or {}, ensure_ascii=False),
                ),
            )

    def _get_latest_record(self, symbol: str) -> Optional[dict[str, Any]]:
        with sqlite3.connect(self._db_path) as conn:
            conn.row_factory = sqlite3.Row
            row = conn.execute(
                '''
                SELECT symbol, collected_at, score, sources_json, summary_json
                FROM quote_intel_records
                WHERE symbol = ?
                ORDER BY collected_at DESC
                LIMIT 1
                ''',
                (symbol,),
            ).fetchone()

        if not row:
            return None

        return {
            'symbol': row['symbol'],
            'collected_at': row['collected_at'],
            'score': float(row['score']),
            'sources': self._safe_json_load(row['sources_json']),
            'summary': self._safe_json_load(row['summary_json']),
            'score_breakdown': (self._safe_json_load(row['summary_json']) or {}).get('score_breakdown', {}),
        }

    def _latest_records(self, max_age_minutes: int) -> list[dict[str, Any]]:
        cutoff = datetime.now(timezone.utc) - timedelta(minutes=max_age_minutes)
        cutoff_iso = cutoff.isoformat()

        query = '''
            SELECT r.symbol, r.collected_at, r.score, r.sources_json, r.summary_json
            FROM quote_intel_records r
            INNER JOIN (
                SELECT symbol, MAX(collected_at) AS max_collected_at
                FROM quote_intel_records
                GROUP BY symbol
            ) latest
            ON latest.symbol = r.symbol AND latest.max_collected_at = r.collected_at
            WHERE r.collected_at >= ?
        '''

        rows_out: list[dict[str, Any]] = []
        with sqlite3.connect(self._db_path) as conn:
            conn.row_factory = sqlite3.Row
            rows = conn.execute(query, (cutoff_iso,)).fetchall()

        for row in rows:
            rows_out.append(
                {
                    'symbol': row['symbol'],
                    'collected_at': row['collected_at'],
                    'score': float(row['score']),
                    'sources': self._safe_json_load(row['sources_json']),
                    'summary': self._safe_json_load(row['summary_json']),
                    'score_breakdown': (self._safe_json_load(row['summary_json']) or {}).get('score_breakdown', {}),
                }
            )
        return rows_out

    def _is_fresh(self, iso_value: Any) -> bool:
        try:
            dt = datetime.fromisoformat(str(iso_value))
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return datetime.now(timezone.utc) - dt <= timedelta(minutes=self._refresh_minutes)
        except Exception:
            return False

    @staticmethod
    def _extract_json_object(raw: str) -> dict[str, Any]:
        if not raw:
            return {}
        start = raw.find('{')
        end = raw.rfind('}')
        if start < 0 or end <= start:
            return {}
        try:
            parsed = json.loads(raw[start:end + 1])
            return parsed if isinstance(parsed, dict) else {}
        except Exception:
            return {}

    @staticmethod
    def _extract_symbols_from_text(raw: str) -> list[str]:
        if not raw:
            return []

        candidates = re.findall(r'\b[A-Z]{1,5}(?:\.[A-Z])?\b', str(raw).upper())
        stopwords = {
            'JSON', 'STRICT', 'NASDAQ', 'NYSE', 'ETFS', 'ETF', 'US', 'USA', 'AND', 'THE', 'WITH', 'FOR',
            'LIST', 'SYMBOLS', 'RATIONALE', 'MARKET', 'STOCK', 'STOCKS', 'VALID', 'ONLY', 'DATA',
        }

        output: list[str] = []
        seen: set[str] = set()
        for symbol in candidates:
            normalized = symbol.strip().upper()
            if not normalized or normalized in stopwords:
                continue
            if normalized not in seen:
                seen.add(normalized)
                output.append(normalized)
            if len(output) >= 150:
                break
        return output

    @staticmethod
    def _extract_html_text(pattern: str, html: str) -> str:
        if not html:
            return ''
        match = re.search(pattern, html, flags=re.IGNORECASE | re.DOTALL)
        if not match:
            return ''
        return QuoteIntelService._clean_text(match.group(1))

    @staticmethod
    def _clean_text(value: Any) -> str:
        text = str(value or '')
        text = re.sub(r'<[^>]+>', ' ', text)
        text = re.sub(r'\s+', ' ', text)
        return text.strip()

    @staticmethod
    def _safe_json_load(raw: Any) -> Any:
        try:
            return json.loads(str(raw or '{}'))
        except Exception:
            return {}

    @staticmethod
    def _clamp(value: float, min_value: float, max_value: float) -> float:
        return max(min_value, min(max_value, float(value)))

    @staticmethod
    def _to_float(value: Any) -> Optional[float]:
        if value is None:
            return None
        try:
            return float(value)
        except Exception:
            return None

    @staticmethod
    def _fmt(value: Any) -> str:
        parsed = QuoteIntelService._to_float(value)
        return f'{parsed:.4f}' if parsed is not None else '-'

    @staticmethod
    def _extract_price_from_text(value: str) -> Optional[float]:
        cleaned = str(value or '').strip()
        if not cleaned:
            return None
        cleaned = cleaned.replace('US$', '').replace('$', '').replace('€', '').replace('\u202f', ' ')
        match = re.search(r'(-?\d[\d\s.,]*)', cleaned)
        if not match:
            return None
        try:
            token = match.group(1).strip().replace(' ', '')
            if ',' in token and '.' in token:
                if token.rfind(',') > token.rfind('.'):
                    token = token.replace('.', '').replace(',', '.')
                else:
                    token = token.replace(',', '')
            elif ',' in token:
                parts = token.split(',')
                if len(parts) == 2 and len(parts[1]) <= 2:
                    token = parts[0].replace('.', '') + '.' + parts[1]
                else:
                    token = token.replace(',', '')
            return float(token)
        except Exception:
            return None

    @staticmethod
    def _extract_change_pct_from_text(value: str) -> Optional[float]:
        cleaned = str(value or '').strip()
        if not cleaned:
            return None
        match = re.search(r'(-?\d+(?:[\.,]\d+)?)\s*%', cleaned)
        if not match:
            return None
        token = match.group(1).replace(',', '.')
        try:
            return float(token)
        except Exception:
            return None


quote_intel_service = QuoteIntelService()
