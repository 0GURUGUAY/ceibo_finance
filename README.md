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

## Interface de trading (MVP)

Une page web simple est servie par FastAPI à la racine `/`.

Fonctions disponibles:

- Vue compte Alpaca
- Dernière quote par symbole
- Liste des positions
- Analyse IA concise d'une position (multi-LLM si clés API configurées)
- Liste des ordres ouverts
- Envoi d'un ordre market (`buy`/`sell`)

## Gestion des clés API (LLM)

Configurer les clés dans `.env` (ne jamais les commiter):

- `OPENAI_API_KEY`
- `ANTHROPIC_API_KEY`
- `OPENROUTER_API_KEY`
- `GEMINI_API_KEY`

Vous pouvez vérifier la configuration sans exposer les secrets via:

- `GET /api/v1/analysis/providers`

L'endpoint ne retourne que l'état `configuré/non configuré` et le modèle utilisé.
