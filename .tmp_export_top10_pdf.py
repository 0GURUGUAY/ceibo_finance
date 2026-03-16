import json
import urllib.request
from datetime import datetime
from pathlib import Path

url = 'http://127.0.0.1:18000/api/v1/analysis/opportunities?days=7&limit=10'
with urllib.request.urlopen(url, timeout=120) as resp:
    data = json.loads(resp.read().decode('utf-8'))

criteria = data.get('criteria') or []
picks = data.get('top_picks') or []
rankings = data.get('llm_rankings') or []
providers_ok = [r.get('provider') for r in rankings if r.get('status') == 'ok' and r.get('provider')]

lines = []
lines.append('RESULTAT RECHERCHE IA - TOP 10 OPPORTUNITES (7 JOURS)')
lines.append('')
lines.append(f"Date export: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
lines.append(f"Univers scanne: {data.get('candidates_considered', '-')}")
lines.append(f"Preselection: {len(data.get('preselected_candidates') or [])}")
lines.append(f"Providers LLM OK: {', '.join(providers_ok) if providers_ok else 'aucun'}")
lines.append('')

if criteria:
    lines.append('CRITERES UTILISES')
    for item in criteria:
        lines.append(f"- {item}")
    lines.append('')

if not picks:
    lines.append('Aucune opportunite retournee.')
else:
    lines.append('TOP OPPORTUNITES')
    for i, item in enumerate(picks, start=1):
        symbol = item.get('symbol') or '-'
        name = item.get('company_name') or '-'
        final_score = item.get('final_score', '-')
        market = item.get('market', '-')
        activity = item.get('activity') or 'Activite non renseignee.'
        providers = ', '.join(item.get('providers') or []) or '-'
        votes = item.get('provider_votes', 0)
        market_score = item.get('market_score', '-')
        lines.append(f"{i}. {symbol} - {name}")
        lines.append(f"   Score final: {final_score} | Marche: {market}")
        lines.append(f"   Votes LLM: {votes} | Providers: {providers} | Score marche: {market_score}")
        lines.append(f"   Activite: {activity}")

        rationales = item.get('rationales') or []
        evidence = item.get('evidence') or []
        if rationales:
            lines.append('   Rationales IA:')
            for r in rationales:
                lines.append(f"    - {r}")
        if evidence:
            lines.append('   Elements quantitatifs:')
            for e in evidence:
                lines.append(f"    - {e}")
        lines.append('')

Path('RESULTAT_RECHERCHE_TOP10_7J.txt').write_text('\n'.join(lines), encoding='utf-8')
print('OK: RESULTAT_RECHERCHE_TOP10_7J.txt')
