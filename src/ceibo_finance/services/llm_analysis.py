import asyncio
import json
from datetime import datetime, timezone
from statistics import mean, pstdev

import httpx

from ceibo_finance.core.config import settings
from ceibo_finance.services.alpaca_client import alpaca_service


class MultiLlmAnalysisService:
    async def analyze_symbol(self, symbol: str) -> dict:
        normalized_symbol = (symbol or '').strip().upper()
        if not normalized_symbol:
            raise ValueError('Symbol is required')

        snapshot = self._build_snapshot(normalized_symbol)
        prompt = self._build_prompt(snapshot)

        providers = self.provider_status()
        provider_tasks = self._build_provider_tasks(prompt, providers)
        analyses = []

        if provider_tasks:
            results = await asyncio.gather(*(task for _, task in provider_tasks), return_exceptions=True)
            for (provider_name, _), result in zip(provider_tasks, results):
                if isinstance(result, Exception):
                    analyses.append(
                        {
                            'provider': provider_name,
                            'status': 'error',
                            'analysis': '',
                            'error': self._sanitize_error(provider_name, result),
                        }
                    )
                    continue
                analyses.append(result)
        else:
            analyses = [
                {
                    'provider': 'none',
                    'status': 'skipped',
                    'analysis': '',
                    'error': 'No LLM provider configured (set OPENAI_API_KEY, ANTHROPIC_API_KEY, OPENROUTER_API_KEY, or GEMINI_API_KEY).',
                }
            ]

        summary = self._build_summary(snapshot, analyses)
        trade_signal = self._build_trade_signal(snapshot, analyses)

        return {
            'symbol': normalized_symbol,
            'generated_at': datetime.now(timezone.utc).isoformat(),
            'snapshot': snapshot,
            'providers': providers,
            'analyses': analyses,
            'summary': summary,
            'trade_signal': trade_signal,
        }

    async def analyze_weekly_opportunities(self, days: int = 7, limit: int = 10) -> dict:
        safe_days = min(max(5, int(days or 7)), 10)
        safe_limit = min(max(3, int(limit or 10)), 20)

        universe = self._opportunity_universe()
        asset_metadata = alpaca_service.get_assets_metadata(universe)
        candidates = []
        for symbol in universe:
            try:
                snapshot = self._build_snapshot(symbol, history_days=30)
            except Exception:
                continue

            latest_price = snapshot.get('latest_price')
            if latest_price is None:
                continue

            candidate = self._compute_market_opportunity(snapshot, asset_metadata.get(symbol) or {})
            candidates.append(candidate)

        if not candidates:
            return {
                'generated_at': datetime.now(timezone.utc).isoformat(),
                'window_days': safe_days,
                'requested_limit': safe_limit,
                'criteria': self._opportunity_criteria_text(),
                'providers': self.provider_status(),
                'llm_rankings': [],
                'top_picks': [],
                'warning': 'Aucune donnée exploitable sur l’univers de titres.',
            }

        candidates.sort(key=lambda item: item['market_score'], reverse=True)
        preselected = candidates[:20]

        providers = self.provider_status()
        prompt = self._build_opportunity_prompt(preselected, safe_days, safe_limit)
        provider_tasks = self._build_opportunity_provider_tasks(prompt, providers)
        rankings = []

        if provider_tasks:
            results = await asyncio.gather(*(task for _, task in provider_tasks), return_exceptions=True)
            for (provider_name, _), result in zip(provider_tasks, results):
                if isinstance(result, Exception):
                    rankings.append(
                        {
                            'provider': provider_name,
                            'status': 'error',
                            'analysis': '',
                            'error': self._sanitize_error(provider_name, result),
                            'parsed': {'selection_criteria': [], 'top_symbols': []},
                        }
                    )
                    continue

                parsed = self._parse_opportunity_payload(result.get('analysis', ''))
                result['parsed'] = parsed
                rankings.append(result)

        merged = self._merge_opportunity_rankings(preselected, rankings, safe_limit)

        return {
            'generated_at': datetime.now(timezone.utc).isoformat(),
            'window_days': safe_days,
            'requested_limit': safe_limit,
            'criteria': self._opportunity_criteria_text(),
            'providers': providers,
            'candidates_considered': len(candidates),
            'preselected_candidates': preselected,
            'llm_rankings': rankings,
            'top_picks': merged,
        }

    def provider_status(self) -> list[dict]:
        return [
            {
                'provider': 'openai',
                'configured': bool((settings.openai_api_key or '').strip()),
                'model': settings.openai_model,
            },
            {
                'provider': 'anthropic',
                'configured': bool((settings.anthropic_api_key or '').strip()),
                'model': settings.anthropic_model,
            },
            {
                'provider': 'openrouter',
                'configured': bool((settings.openrouter_api_key or '').strip()),
                'model': settings.openrouter_model,
            },
            {
                'provider': 'gemini',
                'configured': bool((settings.gemini_api_key or '').strip()),
                'model': settings.gemini_model,
            },
        ]

    def _build_snapshot(self, symbol: str, history_days: int = 30) -> dict:
        history_rows = alpaca_service.history_daily_closes(symbol=symbol, days=history_days)
        closes = [float(item['close']) for item in history_rows if item.get('close') is not None]

        latest_quote_raw = alpaca_service.latest_quote(symbol)
        quote_obj = latest_quote_raw.get(symbol, {}) if isinstance(latest_quote_raw, dict) else {}
        model_dump = getattr(quote_obj, 'model_dump', None)
        latest_quote: dict[str, object]
        if callable(model_dump):
            dumped = model_dump()
            latest_quote = dumped if isinstance(dumped, dict) else {}
        elif isinstance(quote_obj, dict):
            latest_quote = quote_obj
        else:
            latest_quote = {}

        bid = self._to_positive_float(latest_quote.get('bid_price'))
        ask = self._to_positive_float(latest_quote.get('ask_price'))
        trade_price = self._to_positive_float(latest_quote.get('trade_price'))

        if trade_price is not None:
            latest_price = trade_price
        elif bid is not None and ask is not None:
            latest_price = (bid + ask) / 2
        elif bid is not None:
            latest_price = bid
        elif ask is not None:
            latest_price = ask
        else:
            latest_price = closes[-1] if closes else None

        returns = []
        for index in range(1, len(closes)):
            previous = closes[index - 1]
            current = closes[index]
            if previous:
                returns.append((current - previous) / previous)

        last_close = closes[-1] if closes else None
        prev_close = closes[-2] if len(closes) >= 2 else None
        close_5d_ago = closes[-6] if len(closes) >= 6 else None
        close_1m_ago = closes[-22] if len(closes) >= 22 else (closes[0] if len(closes) >= 2 else None)

        change_1d_pct = self._pct_change(last_close, prev_close)
        change_5d_pct = self._pct_change(last_close, close_5d_ago)
        change_1m_pct = self._pct_change(last_close, close_1m_ago)

        return {
            'symbol': symbol,
            'latest_price': latest_price,
            'latest_trade_price': trade_price,
            'latest_bid': bid,
            'latest_ask': ask,
            'last_close': last_close,
            'daily_close_average_5d': mean(closes[-5:]) if len(closes) >= 5 else (mean(closes) if closes else None),
            'change_1d_pct': change_1d_pct,
            'change_5d_pct': change_5d_pct,
            'change_1m_pct': change_1m_pct,
            'volatility_daily_pct': (pstdev(returns) * 100) if len(returns) >= 2 else None,
            'history_closes': closes,
        }

    def _build_prompt(self, snapshot: dict) -> str:
        closes_preview = ', '.join(f'{value:.2f}' for value in snapshot['history_closes'][-10:])

        return (
            'Tu es un analyste actions. Fais une analyse fine, concise et actionnable en français. '\
            'Format attendu: 4 puces maximum: tendance, momentum, risque, niveau de vigilance. '\
            'Pas de conseil d\'investissement catégorique.\n\n'
            f"Symbole: {snapshot['symbol']}\n"
            f"Prix actuel estimé: {self._fmt(snapshot['latest_price'])}\n"
            f"Bid: {self._fmt(snapshot['latest_bid'])} | Ask: {self._fmt(snapshot['latest_ask'])}\n"
            f"Dernière clôture: {self._fmt(snapshot['last_close'])}\n"
            f"Moyenne clôture 5j: {self._fmt(snapshot['daily_close_average_5d'])}\n"
            f"Perf 1j (%): {self._fmt(snapshot['change_1d_pct'])}\n"
            f"Perf 5j (%): {self._fmt(snapshot['change_5d_pct'])}\n"
            f"Volatilité journalière (%): {self._fmt(snapshot['volatility_daily_pct'])}\n"
            f"Clôtures récentes (jusqu\'à 10): {closes_preview}"
        )

    def _build_provider_tasks(self, prompt: str, providers: list[dict]):
        provider_map = {item['provider']: item for item in providers}
        tasks = []
        if provider_map.get('openai', {}).get('configured'):
            tasks.append(('openai', self._query_openai(prompt)))
        if provider_map.get('anthropic', {}).get('configured'):
            tasks.append(('anthropic', self._query_anthropic(prompt)))
        if provider_map.get('openrouter', {}).get('configured'):
            tasks.append(('openrouter', self._query_openrouter(prompt)))
        if provider_map.get('gemini', {}).get('configured'):
            tasks.append(('gemini', self._query_gemini(prompt)))
        return tasks

    def _build_opportunity_provider_tasks(self, prompt: str, providers: list[dict]):
        provider_map = {item['provider']: item for item in providers}
        tasks = []
        if provider_map.get('openai', {}).get('configured'):
            tasks.append(('openai', self._query_openai_opportunities(prompt)))
        if provider_map.get('anthropic', {}).get('configured'):
            tasks.append(('anthropic', self._query_anthropic_opportunities(prompt)))
        if provider_map.get('openrouter', {}).get('configured'):
            tasks.append(('openrouter', self._query_openrouter_opportunities(prompt)))
        if provider_map.get('gemini', {}).get('configured'):
            tasks.append(('gemini', self._query_gemini_opportunities(prompt)))
        return tasks

    async def _query_openai(self, prompt: str) -> dict:
        url = 'https://api.openai.com/v1/responses'
        payload = {
            'model': settings.openai_model,
            'input': [
                {'role': 'system', 'content': 'Réponds en français avec 4 puces max.'},
                {'role': 'user', 'content': prompt},
            ],
            'temperature': 0.2,
            'max_output_tokens': 260,
        }
        headers = {'Authorization': f"Bearer {(settings.openai_api_key or '').strip()}"}

        async with httpx.AsyncClient(timeout=20.0) as client:
            response = await client.post(url, json=payload, headers=headers)
            response.raise_for_status()
            data = response.json()

        content = data.get('output_text') or ''
        if not content:
            output_items = data.get('output') or []
            for item in output_items:
                for content_item in item.get('content', []):
                    if content_item.get('type') == 'output_text':
                        content = content_item.get('text', '')
                        break
                if content:
                    break

        return {
            'provider': 'openai',
            'model': settings.openai_model,
            'status': 'ok',
            'analysis': content.strip(),
        }

    async def _query_anthropic(self, prompt: str) -> dict:
        url = 'https://api.anthropic.com/v1/messages'
        payload = {
            'model': settings.anthropic_model,
            'max_tokens': 260,
            'temperature': 0.2,
            'system': 'Réponds en français avec 4 puces max.',
            'messages': [{'role': 'user', 'content': prompt}],
        }
        headers = {
            'x-api-key': (settings.anthropic_api_key or '').strip(),
            'anthropic-version': '2023-06-01',
        }

        async with httpx.AsyncClient(timeout=20.0) as client:
            response = await client.post(url, json=payload, headers=headers)
            response.raise_for_status()
            data = response.json()

        content = ''
        for item in data.get('content', []):
            if item.get('type') == 'text':
                content = item.get('text', '')
                break

        return {
            'provider': 'anthropic',
            'model': settings.anthropic_model,
            'status': 'ok',
            'analysis': content.strip(),
        }

    async def _query_openrouter(self, prompt: str) -> dict:
        url = 'https://openrouter.ai/api/v1/chat/completions'
        payload = {
            'model': settings.openrouter_model,
            'messages': [
                {'role': 'system', 'content': 'Réponds en français avec 4 puces max.'},
                {'role': 'user', 'content': prompt},
            ],
            'temperature': 0.2,
            'max_tokens': 260,
        }
        headers = {'Authorization': f"Bearer {(settings.openrouter_api_key or '').strip()}"}

        async with httpx.AsyncClient(timeout=20.0) as client:
            response = await client.post(url, json=payload, headers=headers)
            response.raise_for_status()
            data = response.json()

        content = ''
        choices = data.get('choices') or []
        if choices:
            content = (choices[0].get('message') or {}).get('content', '')

        return {
            'provider': 'openrouter',
            'model': settings.openrouter_model,
            'status': 'ok',
            'analysis': content.strip(),
        }

    async def _query_gemini(self, prompt: str) -> dict:
        model_name = settings.gemini_model
        api_key = (settings.gemini_api_key or '').strip()
        url = f'https://generativelanguage.googleapis.com/v1beta/models/{model_name}:generateContent'
        payload = {
            'systemInstruction': {
                'parts': [{'text': 'Réponds en français avec 4 puces max.'}],
            },
            'contents': [
                {
                    'role': 'user',
                    'parts': [{'text': prompt}],
                }
            ],
            'generationConfig': {
                'temperature': 0.2,
                'maxOutputTokens': 260,
            },
        }

        async with httpx.AsyncClient(timeout=20.0) as client:
            response = await client.post(url, json=payload, params={'key': api_key})
            response.raise_for_status()
            data = response.json()

        content = ''
        candidates = data.get('candidates') or []
        if candidates:
            parts = (candidates[0].get('content') or {}).get('parts') or []
            text_parts = [part.get('text', '') for part in parts if isinstance(part, dict)]
            content = ''.join(text_parts)

        return {
            'provider': 'gemini',
            'model': model_name,
            'status': 'ok',
            'analysis': content.strip(),
        }

    async def _query_openai_opportunities(self, prompt: str) -> dict:
        url = 'https://api.openai.com/v1/responses'
        payload = {
            'model': settings.openai_model,
            'input': [
                {'role': 'system', 'content': 'Réponds uniquement en JSON valide sans markdown.'},
                {'role': 'user', 'content': prompt},
            ],
            'temperature': 0.2,
            'max_output_tokens': 900,
        }
        headers = {'Authorization': f"Bearer {(settings.openai_api_key or '').strip()}"}

        async with httpx.AsyncClient(timeout=30.0) as client:
            response = await client.post(url, json=payload, headers=headers)
            response.raise_for_status()
            data = response.json()

        content = data.get('output_text') or ''
        if not content:
            output_items = data.get('output') or []
            for item in output_items:
                for content_item in item.get('content', []):
                    if content_item.get('type') == 'output_text':
                        content = content_item.get('text', '')
                        break
                if content:
                    break

        return {
            'provider': 'openai',
            'model': settings.openai_model,
            'status': 'ok',
            'analysis': content.strip(),
        }

    async def _query_anthropic_opportunities(self, prompt: str) -> dict:
        url = 'https://api.anthropic.com/v1/messages'
        payload = {
            'model': settings.anthropic_model,
            'max_tokens': 900,
            'temperature': 0.2,
            'system': 'Réponds uniquement en JSON valide sans markdown.',
            'messages': [{'role': 'user', 'content': prompt}],
        }
        headers = {
            'x-api-key': (settings.anthropic_api_key or '').strip(),
            'anthropic-version': '2023-06-01',
        }

        async with httpx.AsyncClient(timeout=30.0) as client:
            response = await client.post(url, json=payload, headers=headers)
            response.raise_for_status()
            data = response.json()

        content = ''
        for item in data.get('content', []):
            if item.get('type') == 'text':
                content = item.get('text', '')
                break

        return {
            'provider': 'anthropic',
            'model': settings.anthropic_model,
            'status': 'ok',
            'analysis': content.strip(),
        }

    async def _query_openrouter_opportunities(self, prompt: str) -> dict:
        url = 'https://openrouter.ai/api/v1/chat/completions'
        payload = {
            'model': settings.openrouter_model,
            'messages': [
                {'role': 'system', 'content': 'Réponds uniquement en JSON valide sans markdown.'},
                {'role': 'user', 'content': prompt},
            ],
            'temperature': 0.2,
            'max_tokens': 900,
        }
        headers = {'Authorization': f"Bearer {(settings.openrouter_api_key or '').strip()}"}

        async with httpx.AsyncClient(timeout=30.0) as client:
            response = await client.post(url, json=payload, headers=headers)
            response.raise_for_status()
            data = response.json()

        content = ''
        choices = data.get('choices') or []
        if choices:
            content = (choices[0].get('message') or {}).get('content', '')

        return {
            'provider': 'openrouter',
            'model': settings.openrouter_model,
            'status': 'ok',
            'analysis': content.strip(),
        }

    async def _query_gemini_opportunities(self, prompt: str) -> dict:
        model_name = settings.gemini_model
        api_key = (settings.gemini_api_key or '').strip()
        url = f'https://generativelanguage.googleapis.com/v1beta/models/{model_name}:generateContent'
        payload = {
            'systemInstruction': {
                'parts': [{'text': 'Réponds uniquement en JSON valide sans markdown.'}],
            },
            'contents': [
                {
                    'role': 'user',
                    'parts': [{'text': prompt}],
                }
            ],
            'generationConfig': {
                'temperature': 0.2,
                'maxOutputTokens': 900,
            },
        }

        async with httpx.AsyncClient(timeout=30.0) as client:
            response = await client.post(url, json=payload, params={'key': api_key})
            response.raise_for_status()
            data = response.json()

        content = ''
        candidates = data.get('candidates') or []
        if candidates:
            parts = (candidates[0].get('content') or {}).get('parts') or []
            text_parts = [part.get('text', '') for part in parts if isinstance(part, dict)]
            content = ''.join(text_parts)

        return {
            'provider': 'gemini',
            'model': model_name,
            'status': 'ok',
            'analysis': content.strip(),
        }

    def _opportunity_universe(self) -> list[str]:
        return [
            'AAPL', 'MSFT', 'NVDA', 'AMZN', 'GOOGL', 'META', 'TSLA', 'AMD', 'NFLX', 'AVGO',
            'ADBE', 'CRM', 'ORCL', 'INTC', 'QCOM', 'MU', 'SHOP', 'UBER', 'PLTR', 'SNOW',
            'JPM', 'BAC', 'GS', 'V', 'MA', 'UNH', 'LLY', 'JNJ', 'XOM', 'CVX',
            'SPY', 'QQQ', 'IWM', 'SMH', 'XLF', 'XLK', 'XLE', 'ARKK', 'SOXX', 'TLT',
        ]

    def _compute_market_opportunity(self, snapshot: dict, asset_meta: dict) -> dict:
        change_1d = snapshot.get('change_1d_pct') or 0.0
        change_5d = snapshot.get('change_5d_pct') or 0.0
        change_1m = snapshot.get('change_1m_pct') or 0.0
        volatility = snapshot.get('volatility_daily_pct')
        latest_price = snapshot.get('latest_price')
        avg_5d = snapshot.get('daily_close_average_5d')

        momentum_vs_avg5 = 0.0
        if latest_price is not None and avg_5d:
            momentum_vs_avg5 = ((latest_price - avg_5d) / avg_5d) * 100

        score = 50.0
        score += self._clamp(change_5d, -8, 8) * 2.5
        score += self._clamp(change_1d, -5, 5) * 1.5
        score += self._clamp(change_1m, -15, 15) * 0.8
        score += self._clamp(momentum_vs_avg5, -8, 8) * 2.0

        if volatility is None:
            score -= 6
        else:
            score += max(0.0, 12.0 - (abs(volatility - 2.2) * 4.0)) - 6.0

        score = self._clamp(score, 0, 100)

        return {
            'symbol': snapshot.get('symbol'),
            'company_name': asset_meta.get('name') or snapshot.get('symbol'),
            'market': asset_meta.get('exchange') or self._market_label(snapshot.get('symbol')),
            'activity': self._company_activity(snapshot.get('symbol')),
            'market_score': round(float(score), 2),
            'change_1d_pct': round(float(change_1d), 2),
            'change_5d_pct': round(float(change_5d), 2),
            'change_1m_pct': round(float(change_1m), 2),
            'volatility_daily_pct': round(float(volatility), 2) if volatility is not None else None,
            'momentum_vs_avg5_pct': round(float(momentum_vs_avg5), 2),
            'latest_price': snapshot.get('latest_price'),
        }

    def _build_opportunity_prompt(self, candidates: list[dict], days: int, limit: int) -> str:
        rows = [
            'Format table: SYMBOL | market_score | perf_1j_pct | perf_5j_pct | vol_j_pct | momentum_vs_avg5_pct | perf_1m_pct'
        ]
        for item in candidates:
            rows.append(
                f"{item['symbol']} | {item['market_score']:.2f} | {item['change_1d_pct']:.2f} | "
                f"{item['change_5d_pct']:.2f} | {self._fmt(item['volatility_daily_pct'])} | {item['momentum_vs_avg5_pct']:.2f} | {item['change_1m_pct']:.2f}"
            )

        table = '\n'.join(rows)
        return (
            'Tu es un comité d’allocation quant + discrétionnaire. '
            f'À partir des métriques ci-dessous (fenêtre {days} jours), propose les {limit} tickets les plus intéressants. '
            'Réponds STRICTEMENT en JSON valide, sans markdown, avec ce schéma:\n'
            '{"selection_criteria":["..."],"top_symbols":[{"symbol":"AAPL","rationale":"...","risk":"...","confidence":"faible|moyenne|élevée"}]}.\n'
            'Règles: '
            '1) N’invente pas de symbole hors table. '
            '2) Donne exactement top_symbols de taille demandée si possible. '
            '3) Le texte doit être en français.\n\n'
            f'{table}'
        )

    def _parse_opportunity_payload(self, text: str) -> dict:
        raw = (text or '').strip()
        if not raw:
            return {'selection_criteria': [], 'top_symbols': []}

        start = raw.find('{')
        end = raw.rfind('}')
        if start < 0 or end <= start:
            return {'selection_criteria': [], 'top_symbols': []}

        json_blob = raw[start:end + 1]
        try:
            data = json.loads(json_blob)
        except Exception:
            return {'selection_criteria': [], 'top_symbols': []}

        criteria = data.get('selection_criteria')
        if not isinstance(criteria, list):
            criteria = []
        criteria = [str(item).strip() for item in criteria if str(item).strip()][:8]

        symbols = data.get('top_symbols')
        parsed_symbols = []
        if isinstance(symbols, list):
            for item in symbols:
                if not isinstance(item, dict):
                    continue
                symbol = str(item.get('symbol', '')).strip().upper()
                if not symbol:
                    continue
                parsed_symbols.append(
                    {
                        'symbol': symbol,
                        'rationale': str(item.get('rationale', '')).strip(),
                        'risk': str(item.get('risk', '')).strip(),
                        'confidence': str(item.get('confidence', '')).strip(),
                    }
                )

        return {'selection_criteria': criteria, 'top_symbols': parsed_symbols}

    def _merge_opportunity_rankings(self, preselected: list[dict], rankings: list[dict], limit: int) -> list[dict]:
        by_symbol = {item['symbol']: item for item in preselected}
        aggregate = {
            symbol: {
                'symbol': symbol,
                'market_score': base['market_score'],
                'llm_points': 0,
                'provider_votes': 0,
                'providers': [],
                'rationales': [],
                'company_name': base.get('company_name') or symbol,
                'market': base.get('market') or '-',
                'activity': base.get('activity') or 'Activité non renseignée.',
                'evidence': [
                    f"Perf 5j: {base['change_5d_pct']}%",
                    f"Perf 1j: {base['change_1d_pct']}%",
                    f"Perf 1 mois: {base['change_1m_pct']}%",
                    f"Momentum vs moyenne 5j: {base['momentum_vs_avg5_pct']}%",
                    f"Volatilité journalière: {self._fmt(base['volatility_daily_pct'])}%",
                ],
            }
            for symbol, base in by_symbol.items()
        }

        for ranking in rankings:
            if ranking.get('status') != 'ok':
                continue
            provider = ranking.get('provider', 'unknown')
            parsed = ranking.get('parsed', {})
            picks = parsed.get('top_symbols', []) if isinstance(parsed, dict) else []
            for index, pick in enumerate(picks):
                symbol = str(pick.get('symbol', '')).upper()
                if symbol not in aggregate:
                    continue

                points = max(1, limit - index)
                aggregate[symbol]['llm_points'] += points
                aggregate[symbol]['provider_votes'] += 1
                if provider not in aggregate[symbol]['providers']:
                    aggregate[symbol]['providers'].append(provider)

                rationale = str(pick.get('rationale', '')).strip()
                if rationale and rationale not in aggregate[symbol]['rationales']:
                    aggregate[symbol]['rationales'].append(rationale)

        merged = []
        for symbol, item in aggregate.items():
            final_score = (item['market_score'] * 0.6) + (item['llm_points'] * 1.8)
            merged.append(
                {
                    'symbol': symbol,
                    'company_name': item['company_name'],
                    'market': item['market'],
                    'activity': item['activity'],
                    'final_score': round(final_score, 2),
                    'market_score': item['market_score'],
                    'provider_votes': item['provider_votes'],
                    'providers': item['providers'],
                    'rationales': item['rationales'][:3],
                    'evidence': item['evidence'],
                }
            )

        merged.sort(key=lambda entry: (entry['final_score'], entry['provider_votes'], entry['market_score']), reverse=True)
        return merged[:limit]

    def _opportunity_criteria_text(self) -> list[str]:
        return [
            'Performance 5 jours (momentum hebdomadaire).',
            'Performance 1 jour (accélération/récence).',
            'Performance 1 mois pour réduire le bruit des seuls derniers jours.',
            'Écart du prix à la moyenne de clôture 5 jours.',
            'Volatilité journalière (équilibre opportunité/risque).',
            'Consensus multi-LLM (votes et rang dans les shortlists).',
            'Transparence: chaque ticket expose les éléments chiffrés utilisés.',
        ]

    @staticmethod
    def _market_label(symbol: str) -> str:
        market_map = {
            'SPY': 'NYSE Arca', 'QQQ': 'NASDAQ', 'IWM': 'NYSE Arca', 'SMH': 'NASDAQ', 'XLF': 'NYSE Arca',
            'XLK': 'NYSE Arca', 'XLE': 'NYSE Arca', 'ARKK': 'NYSE Arca', 'SOXX': 'NASDAQ', 'TLT': 'NASDAQ',
        }
        return market_map.get(symbol or '', 'NASDAQ/NYSE')

    @staticmethod
    def _company_activity(symbol: str) -> str:
        descriptions = {
            'AAPL': 'Conception et vente d’iPhone, Mac, services numériques et wearables.',
            'MSFT': 'Logiciels, cloud Azure, outils professionnels et IA.',
            'NVDA': 'Semi-conducteurs GPU pour IA, data centers et gaming.',
            'AMZN': 'E-commerce, cloud AWS et logistique.',
            'GOOGL': 'Recherche en ligne, publicité digitale, cloud et YouTube.',
            'META': 'Réseaux sociaux, publicité digitale et plateformes immersives.',
            'TSLA': 'Véhicules électriques, batteries et énergie.',
            'AMD': 'Processeurs et puces pour PC, serveurs et IA.',
            'NFLX': 'Streaming vidéo et production de contenus.',
            'AVGO': 'Semi-conducteurs et logiciels d’infrastructure.',
            'ADBE': 'Logiciels créatifs et marketing digital.',
            'CRM': 'Logiciels CRM et plateforme cloud pour entreprises.',
            'ORCL': 'Bases de données, cloud et logiciels d’entreprise.',
            'INTC': 'Semi-conducteurs et infrastructures informatiques.',
            'QCOM': 'Puces et licences technologiques mobile/télécom.',
            'MU': 'Mémoire DRAM/NAND et stockage.',
            'SHOP': 'Plateforme e-commerce pour marchands.',
            'UBER': 'Mobilité, livraison et logistique urbaine.',
            'PLTR': 'Logiciels d’analyse de données et IA.',
            'SNOW': 'Plateforme data cloud et analytique.',
            'JPM': 'Banque universelle et marchés de capitaux.',
            'BAC': 'Banque de détail, entreprises et marchés.',
            'GS': 'Banque d’investissement et gestion d’actifs.',
            'V': 'Réseau mondial de paiements électroniques.',
            'MA': 'Réseau mondial de paiements et services financiers.',
            'UNH': 'Assurance santé et services médicaux.',
            'LLY': 'Pharmacie et biotechnologies.',
            'JNJ': 'Santé, pharma et technologies médicales.',
            'XOM': 'Pétrole, gaz et énergie.',
            'CVX': 'Énergie intégrée: exploration, production et raffinage.',
            'SPY': 'ETF large cap répliquant le S&P 500.',
            'QQQ': 'ETF orienté Nasdaq 100, fortement technologique.',
            'IWM': 'ETF exposé aux small caps américaines.',
            'SMH': 'ETF thématique sur les semi-conducteurs.',
            'XLF': 'ETF secteur financier américain.',
            'XLK': 'ETF secteur technologique américain.',
            'XLE': 'ETF secteur énergie américain.',
            'ARKK': 'ETF innovation disruptive à gestion active.',
            'SOXX': 'ETF secteur semi-conducteurs.',
            'TLT': 'ETF obligations long terme du Trésor US.',
        }
        return descriptions.get(symbol or '', 'Activité non renseignée.')

    @staticmethod
    def _clamp(value: float, min_value: float, max_value: float) -> float:
        return max(min_value, min(max_value, float(value)))

    def _build_summary(self, snapshot: dict, analyses: list[dict]) -> str:
        available = [item for item in analyses if item.get('status') == 'ok' and item.get('analysis')]
        local_summary = self._build_local_summary(snapshot)

        if not available:
            return f"{local_summary} | Aucun retour LLM disponible (vérifier les clés API)."

        provider_names = ', '.join(item['provider'] for item in available)
        first_insights = []
        for item in available[:2]:
            first_line = self._first_meaningful_line(item['analysis'])
            if first_line:
                first_insights.append(f"{item['provider']}: {first_line}")

        insights_part = ' | '.join(first_insights) if first_insights else 'Retours LLM reçus.'
        return f"{local_summary} | Convergence {provider_names}. {insights_part}"

    @staticmethod
    def _sanitize_error(provider_name: str, error: Exception) -> str:
        if isinstance(error, httpx.HTTPStatusError):
            status_code = error.response.status_code if error.response is not None else 'n/a'
            if status_code == 401:
                return 'Unauthorized (clé API invalide ou manquante).'
            if status_code == 403:
                return 'Forbidden provider error (droit d’accès ou clé invalide).'
            if status_code == 404:
                if provider_name == 'anthropic':
                    return 'Model/API introuvable chez Anthropic (vérifiez ANTHROPIC_MODEL et l’accès au modèle).'
                if provider_name == 'gemini':
                    return 'Model/API introuvable chez Gemini (vérifiez GEMINI_MODEL et l’activation API Google AI).'
                return 'Resource provider introuvable (modèle ou endpoint).'
            if status_code == 429:
                return 'Rate limit atteint chez le provider.'
            return f'HTTP {status_code} provider error.'
        if isinstance(error, httpx.TimeoutException):
            return 'Timeout lors de l’appel provider.'
        return 'Erreur provider.'

    def _build_local_summary(self, snapshot: dict) -> str:
        change_5d = snapshot.get('change_5d_pct')
        vol = snapshot.get('volatility_daily_pct')

        if change_5d is None:
            trend = 'tendance indéterminée'
        elif change_5d > 1:
            trend = 'tendance court terme haussière'
        elif change_5d < -1:
            trend = 'tendance court terme baissière'
        else:
            trend = 'tendance court terme neutre'

        if vol is None:
            risk = 'risque non estimable'
        elif vol >= 3:
            risk = 'volatilité élevée'
        elif vol >= 1.5:
            risk = 'volatilité modérée'
        else:
            risk = 'volatilité contenue'

        return f"{trend}, {risk}."

    def _build_trade_signal(self, snapshot: dict, analyses: list[dict]) -> dict:
        score = 0.0
        reasons = []
        consensus = self._build_llm_consensus(analyses)

        change_1d = snapshot.get('change_1d_pct')
        change_5d = snapshot.get('change_5d_pct')
        latest_price = snapshot.get('latest_price')
        average_5d = snapshot.get('daily_close_average_5d')
        volatility = snapshot.get('volatility_daily_pct')

        if change_5d is not None:
            if change_5d >= 3:
                score += 2
                reasons.append('hausse nette sur 5 jours')
            elif change_5d <= -3:
                score -= 2
                reasons.append('baisse nette sur 5 jours')

        if change_1d is not None:
            if change_1d >= 1:
                score += 1
                reasons.append('momentum positif sur 1 jour')
            elif change_1d <= -1:
                score -= 1
                reasons.append('momentum négatif sur 1 jour')

        if latest_price is not None and average_5d is not None:
            if latest_price > average_5d:
                score += 1
                reasons.append('prix au-dessus de la moyenne 5 jours')
            elif latest_price < average_5d:
                score -= 1
                reasons.append('prix sous la moyenne 5 jours')

        if volatility is not None:
            if volatility >= 3:
                score -= 1
                reasons.append('volatilité élevée')
            elif volatility <= 1.2:
                score += 0.5
                reasons.append('volatilité contenue')

        provider_ok_count = len([item for item in analyses if item.get('status') == 'ok'])
        if provider_ok_count >= 2:
            score += 0.5
            reasons.append('validation par plusieurs providers')

        if consensus['dominant'] == 'buy':
            score += 1
            reasons.append('consensus LLM orienté achat')
        elif consensus['dominant'] == 'sell':
            score -= 1
            reasons.append('consensus LLM orienté vente')
        elif consensus['dominant'] == 'hold':
            reasons.append('consensus LLM prudent/neutre')

        if score >= 3:
            label = 'Achat'
            tone = 'buy'
        elif score >= 1:
            label = 'Achat prudent'
            tone = 'buy-watch'
        elif score <= -3:
            label = 'Vente'
            tone = 'sell'
        elif score <= -1:
            label = 'Allègement'
            tone = 'sell-watch'
        else:
            label = 'Attente'
            tone = 'hold'

        confidence = 'faible'
        if abs(score) >= 3:
            confidence = 'élevée'
        elif abs(score) >= 1.5:
            confidence = 'moyenne'

        return {
            'label': label,
            'tone': tone,
            'score': round(score, 2),
            'confidence': confidence,
            'rationale': reasons[:4],
            'consensus': consensus,
            'disclaimer': 'Indicateur quantitatif simplifié, pas un conseil d’investissement.',
        }

    def _build_llm_consensus(self, analyses: list[dict]) -> dict:
        counts = {'buy': 0, 'hold': 0, 'sell': 0}

        for item in analyses:
            if item.get('status') != 'ok':
                continue
            stance = self._infer_provider_stance(item.get('analysis', ''))
            counts[stance] += 1

        dominant = 'none'
        if any(counts.values()):
            dominant = max(counts, key=counts.get)
            top_count = counts[dominant]
            if list(counts.values()).count(top_count) > 1:
                dominant = 'hold'

        return {
            'dominant': dominant,
            'buy_count': counts['buy'],
            'hold_count': counts['hold'],
            'sell_count': counts['sell'],
        }

    @staticmethod
    def _infer_provider_stance(text: str) -> str:
        lowered = (text or '').lower()

        sell_markers = ['vente', 'vendre', 'baiss', 'allègement', 'allégement', 'prudence élevée']
        buy_markers = ['achat', 'acheter', 'haussi', 'momentum positif', 'poursuite de la tendance']
        hold_markers = ['attente', 'neutre', 'surveiller', 'vigilance', 'prudent']

        sell_score = sum(marker in lowered for marker in sell_markers)
        buy_score = sum(marker in lowered for marker in buy_markers)
        hold_score = sum(marker in lowered for marker in hold_markers)

        if buy_score > sell_score and buy_score >= hold_score:
            return 'buy'
        if sell_score > buy_score and sell_score >= hold_score:
            return 'sell'
        return 'hold'

    @staticmethod
    def _first_meaningful_line(text: str) -> str:
        for line in text.splitlines():
            stripped = line.strip().lstrip('-•').strip()
            if stripped:
                return stripped
        return ''

    @staticmethod
    def _to_positive_float(value):
        try:
            parsed = float(value)
            return parsed if parsed > 0 else None
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _pct_change(current, previous):
        if current is None or previous is None or previous == 0:
            return None
        return ((current - previous) / previous) * 100

    @staticmethod
    def _fmt(value):
        if value is None:
            return '-'
        return f'{value:.2f}'


llm_analysis_service = MultiLlmAnalysisService()
