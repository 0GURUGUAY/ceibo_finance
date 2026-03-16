# ceibo_finance

Base Python/FastAPI pour une app finance avec connecteur Alpaca (paper trading + market data).

## Setup

```bash
cd /Users/maxpatissier/Downloads/ceibo_finance
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env
```

## Run

```bash
source .venv/bin/activate
uvicorn ceibo_finance.main:app --reload --app-dir src
```

Docs: http://127.0.0.1:8000/docs
Interface web MVP: http://127.0.0.1:8000/

## Endpoints initiaux

- `GET /health`
- `GET /api/v1/account`
- `GET /api/v1/positions`
- `GET /api/v1/orders?status=open&limit=50`
- `POST /api/v1/orders`
- `GET /api/v1/marketdata/latest?symbol=AAPL`
- `GET /api/v1/analysis/providers`
- `GET /api/v1/analysis/position?symbol=META`
- `POST /api/v1/strategy/antifragile/backtest`

## V2 - Intelligence quote privée (US)

Objectif V2: collecter des signaux de quote multi-sources (Yahoo Finance, Google Finance, Boursorama), générer un résumé via le moteur LLM configuré, stocker localement, puis sortir une shortlist d’actions intéressantes.

Cadence recommandée: toutes les `15` minutes (configurable).

Endpoints V2:

- `GET /api/v1/analysis/v2/quote?symbol=AAPL&force_refresh=false`
- `POST /api/v1/analysis/v2/collect-us?limit=30&force_refresh=false`
- `GET /api/v1/analysis/v2/opportunities?limit=10&max_age_minutes=1440`
- `POST /api/v1/analysis/v2/auto/start?limit=30&force_refresh=false`
- `POST /api/v1/analysis/v2/auto/stop`
- `GET /api/v1/analysis/v2/auto/status`

Configuration `.env`:

- `QUOTE_INTEL_REFRESH_MINUTES=15`
- `QUOTE_INTEL_DB_PATH=data/quote_intel.db`

Notes importantes:

- Le stockage V2 est local (SQLite), prêt à être remplacé par ton moteur privé Ceibo si besoin.
- Certaines pages publiques (Google Finance/Boursorama) peuvent varier; la collecte est en mode best-effort avec fallback Yahoo.

Utilisation rapide (API):

```bash
# 1) collecter une quote + résumé
curl "http://127.0.0.1:8000/api/v1/analysis/v2/quote?symbol=AAPL&force_refresh=true"

# 2) collecter un sous-univers US
curl -X POST "http://127.0.0.1:8000/api/v1/analysis/v2/collect-us?limit=30&force_refresh=false"

# 3) afficher la shortlist du jour
curl "http://127.0.0.1:8000/api/v1/analysis/v2/opportunities?limit=10&max_age_minutes=1440"
```

Mode auto 15 min (API):

```bash
# démarrer
curl -X POST "http://127.0.0.1:8000/api/v1/analysis/v2/auto/start?limit=30&force_refresh=false"

# statut
curl "http://127.0.0.1:8000/api/v1/analysis/v2/auto/status"

# arrêter
curl -X POST "http://127.0.0.1:8000/api/v1/analysis/v2/auto/stop"
```

Utilisation depuis l’UI:

- Ouvrir l’onglet `IA` dans l’interface web.
- Bloc `V2 · LLM privé (US, 15 min)`:
	- `Résumé quote V2`
	- `Collecter US`
	- `Top opportunités V2`
	- `Démarrer auto 15m` / `Arrêter auto`
- Le statut auto affiche `running`, dernier run et prochaine exécution.

## Antifragile Asset Allocation

Backtest mensuel du modèle `Antifragile Asset Allocation`:

- `POST /api/v1/strategy/antifragile/backtest`

Exemple:

```bash
curl -X POST "http://127.0.0.1:8000/api/v1/strategy/antifragile/backtest" \
	-H "Content-Type: application/json" \
	-d '{
		"start": "2024-01-01",
		"end": "2025-12-31",
		"cash_symbol": "BIL",
		"weights": {
			"momentum": 1.0,
			"volatility": 1.0,
			"correlation": 1.0,
			"trend": 1.0
		}
	}'
```

La réponse retourne:

- `monthly_allocation`
- `historical_backtest`
- `equity_curve`
- `drawdown`
- `sharpe_ratio`

## Interface de trading (MVP)

Une page web simple est servie par FastAPI à la racine `/`.

Fonctions disponibles:

- Vue compte Alpaca
- Dernière quote par symbole
- Liste des positions
- Analyse IA concise d'une position (multi-LLM si clés API configurées)
- Liste des ordres ouverts
- Envoi d'un ordre market (`buy`/`sell`)

## Partage live (spectator read-only)

Objectif: partager en temps réel la liste des positions avec un invité via un lien temporaire, sans accès trading.

Nouveaux endpoints:

- `POST /api/v1/strategy/trend-following/viewer/invite`
- `WS /api/v1/strategy/ws/trend-following/viewer?token=...`
- `GET /spectator?t=...`

Variables `.env` à définir pour la prod:

- `VIEWER_TOKEN_SECRET` (secret long et aléatoire)
- `VIEWER_ADMIN_KEY` (clé admin requise pour générer les liens invités)
- `VIEWER_TOKEN_DEFAULT_TTL_MINUTES` (durée par défaut d'un lien)

Exemple de génération d'un secret fort (macOS/Linux):

```bash
python3 - <<'PY'
import secrets
print(secrets.token_urlsafe(48))
PY
```

Créer un lien invité temporaire:

```bash
curl -X POST "http://127.0.0.1:8000/api/v1/strategy/trend-following/viewer/invite" \
	-H "Content-Type: application/json" \
	-H "x-viewer-admin-key: VOTRE_VIEWER_ADMIN_KEY" \
	-d '{"expires_minutes": 120}'
```

La réponse contient `spectator_url`: c'est ce lien que vous partagez à l'invité.

### Checklist prod minimale (5 min)

1. Déployer l'app derrière HTTPS (obligatoire pour un accès internet fiable).
2. Définir `VIEWER_TOKEN_SECRET` et `VIEWER_ADMIN_KEY` dans les variables d'environnement du service.
3. Vérifier que les routes de trading restent privées (ne partager que `/spectator?...`).
4. Générer un lien invité avec TTL court (ex: 60 à 180 min).
5. Régénérer un lien si besoin (les anciens expirent automatiquement).

## Service recommandé (facile): Render

Pourquoi Render ici:

- Déploiement FastAPI simple (sans Docker obligatoire)
- HTTPS automatique
- Support WebSocket (nécessaire pour le spectator live)
- Configuration as-code via `render.yaml`

Le repo contient déjà `render.yaml` prêt à l'emploi.

### Déploiement rapide

1. Pousser ce projet sur GitHub.
2. Sur Render: `New +` → `Blueprint` → sélectionner le repo.
3. Render lit `render.yaml` et crée le service web.
4. Dans Render, compléter les variables sensibles manquantes:
	 - `ALPACA_API_KEY`
	 - `ALPACA_API_SECRET`
	 - (optionnel) clés LLM selon usage
5. Déployer puis ouvrir l'URL Render du service.

### Partager l'expérience live à un ami

Générer un lien invité temporaire:

```bash
curl -X POST "https://VOTRE-SERVICE.onrender.com/api/v1/strategy/trend-following/viewer/invite" \
	-H "Content-Type: application/json" \
	-H "x-viewer-admin-key: VOTRE_VIEWER_ADMIN_KEY" \
	-d '{"expires_minutes": 120}'
```

Puis envoyer `spectator_url` à ton ami (lecture seule, temps réel).

## Gestion des clés API (LLM)

Configurer les clés dans `.env` (ne jamais les commiter):

- `OPENAI_API_KEY`
- `ANTHROPIC_API_KEY`
- `OPENROUTER_API_KEY`
- `GEMINI_API_KEY`

Vous pouvez vérifier la configuration sans exposer les secrets via:

- `GET /api/v1/analysis/providers`

L'endpoint ne retourne que l'état `configuré/non configuré` et le modèle utilisé.
