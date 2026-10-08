# Real-Time Bidding / Collaboration Backend

Python FastAPI + WebSocket + SQLite project for a live multi-client auction.

## Requirements covered
- WebSocket live updates to all connected clients
- Race-condition-safe bidding
- Persistent auction state
- Correct reconnect/disconnect behavior
- REST API plus browser demo client

## Run on Windows

```powershell
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt
uvicorn app.main:app --reload
```

Then open http://127.0.0.1:8000/ in multiple browser tabs.

## WebSocket message
```json
{"type":"place_bid","bidder":"Alice","amount":1500}
```

A bid is accepted only when it is greater than the persisted highest bid. SQLite `BEGIN IMMEDIATE` serializes the write transaction, preventing stale/lower concurrent bids from overwriting the winner.
