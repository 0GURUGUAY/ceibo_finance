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

## Endpoints initiaux

- `GET /health`
- `GET /api/v1/account`
- `GET /api/v1/positions`
- `GET /api/v1/orders?status=open&limit=50`
- `POST /api/v1/orders`
- `GET /api/v1/marketdata/latest?symbol=AAPL`
