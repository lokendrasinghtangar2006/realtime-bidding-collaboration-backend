import asyncio
import json
import sqlite3
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field

BASE_DIR = Path(__file__).resolve().parent.parent
DB_PATH = BASE_DIR / "auction.db"

app = FastAPI(
    title="Real-Time Bidding Backend",
    version="1.0.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

connections: set[WebSocket] = set()
connections_lock = asyncio.Lock()


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


@contextmanager
def db():
    conn = sqlite3.connect(DB_PATH, timeout=10, isolation_level=None)
    conn.row_factory = sqlite3.Row
    try:
        yield conn
    finally:
        conn.close()


def init_db() -> None:
    with db() as conn:
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute(
            '''
            CREATE TABLE IF NOT EXISTS auction (
                id INTEGER PRIMARY KEY CHECK (id = 1),
                item_name TEXT NOT NULL,
                highest_bid REAL NOT NULL,
                highest_bidder TEXT NOT NULL,
                version INTEGER NOT NULL DEFAULT 0,
                duration_seconds INTEGER NOT NULL,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
            '''
        )

        row = conn.execute("SELECT id FROM auction WHERE id = 1").fetchone()
        if row is None:
            now = utc_now()
            conn.execute(
                '''
                INSERT INTO auction
                (id, item_name, highest_bid, highest_bidder, version,
                 duration_seconds, created_at, updated_at)
                VALUES (1, ?, ?, ?, 0, ?, ?, ?)
                ''',
                ("Laptop", 1000.0, "Starting Bid", 300, now, now),
            )


def get_state() -> dict[str, Any]:
    with db() as conn:
        row = conn.execute("SELECT * FROM auction WHERE id = 1").fetchone()
        if row is None:
            raise RuntimeError("Auction state does not exist.")
        return dict(row)


def place_bid_transaction(bidder: str, amount: float):
    bidder = bidder.strip()
    state = get_state()

    if not bidder:
        return False, state, "Bidder name is required."

    if amount <= 0:
        return False, state, "Bid amount must be greater than zero."

    with db() as conn:
        try:
            # Acquire SQLite's write lock before reading and updating.
            # Validation + update therefore happen atomically.
            conn.execute("BEGIN IMMEDIATE")

            row = conn.execute(
                "SELECT * FROM auction WHERE id = 1"
            ).fetchone()
            current = dict(row)

            if amount <= float(current["highest_bid"]):
                conn.rollback()
                return (
                    False,
                    current,
                    f"Bid rejected. Current highest bid is {current['highest_bid']:.2f}.",
                )

            new_version = int(current["version"]) + 1
            now = utc_now()

            conn.execute(
                '''
                UPDATE auction
                SET highest_bid = ?,
                    highest_bidder = ?,
                    version = ?,
                    updated_at = ?
                WHERE id = 1
                ''',
                (amount, bidder, new_version, now),
            )
            conn.commit()

            updated = dict(
                conn.execute("SELECT * FROM auction WHERE id = 1").fetchone()
            )
            return True, updated, "Bid accepted."

        except Exception:
            conn.rollback()
            raise


async def broadcast(message: dict[str, Any]) -> None:
    payload = json.dumps(message)

    async with connections_lock:
        clients = list(connections)

    dead = []
    for ws in clients:
        try:
            await ws.send_text(payload)
        except Exception:
            dead.append(ws)

    if dead:
        async with connections_lock:
            for ws in dead:
                connections.discard(ws)


class AuctionCreate(BaseModel):
    item_name: str = Field(min_length=1, max_length=200)
    starting_bid: float = Field(gt=0)
    duration_seconds: int = Field(default=300, gt=0, le=86400)


class BidRequest(BaseModel):
    bidder: str = Field(min_length=1, max_length=100)
    amount: float = Field(gt=0)


@app.on_event("startup")
async def startup():
    init_db()


@app.get("/")
async def index():
    return FileResponse(BASE_DIR / "frontend" / "index.html")


@app.get("/api/auction")
async def auction_state():
    return {"type": "state", "auction": get_state()}


@app.post("/api/auction")
async def create_auction(payload: AuctionCreate):
    now = utc_now()

    with db() as conn:
        conn.execute("BEGIN IMMEDIATE")
        conn.execute(
            '''
            INSERT INTO auction
            (id, item_name, highest_bid, highest_bidder, version,
             duration_seconds, created_at, updated_at)
            VALUES (1, ?, ?, ?, 0, ?, ?, ?)
            ON CONFLICT(id) DO UPDATE SET
                item_name=excluded.item_name,
                highest_bid=excluded.highest_bid,
                highest_bidder=excluded.highest_bidder,
                version=0,
                duration_seconds=excluded.duration_seconds,
                created_at=excluded.created_at,
                updated_at=excluded.updated_at
            ''',
            (
                payload.item_name.strip(),
                payload.starting_bid,
                "Starting Bid",
                payload.duration_seconds,
                now,
                now,
            ),
        )
        conn.commit()

    state = get_state()
    await broadcast({"type": "state", "auction": state})
    return {"type": "state", "auction": state}


@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()

    async with connections_lock:
        connections.add(websocket)

    # Reconnecting clients immediately receive the persisted state.
    await websocket.send_text(
        json.dumps({"type": "state", "auction": get_state()})
    )

    try:
        while True:
            raw = await websocket.receive_text()

            try:
                message = json.loads(raw)
            except json.JSONDecodeError:
                await websocket.send_text(
                    json.dumps({"type": "error", "message": "Invalid JSON."})
                )
                continue

            message_type = message.get("type")

            if message_type == "get_state":
                await websocket.send_text(
                    json.dumps({"type": "state", "auction": get_state()})
                )

            elif message_type == "ping":
                await websocket.send_text(json.dumps({"type": "pong"}))

            elif message_type == "place_bid":
                try:
                    request = BidRequest(
                        bidder=message.get("bidder", ""),
                        amount=message.get("amount", 0),
                    )

                    accepted, state, reason = place_bid_transaction(
                        request.bidder, request.amount
                    )

                    if accepted:
                        await broadcast({
                            "type": "state",
                            "auction": state,
                            "event": "bid_accepted",
                        })
                    else:
                        await websocket.send_text(json.dumps({
                            "type": "bid_result",
                            "accepted": False,
                            "message": reason,
                            "auction": state,
                        }))

                except Exception as exc:
                    await websocket.send_text(json.dumps({
                        "type": "error",
                        "message": f"Bid processing failed: {exc}",
                    }))

            else:
                await websocket.send_text(json.dumps({
                    "type": "error",
                    "message": "Unknown message type.",
                }))

    except WebSocketDisconnect:
        pass
    finally:
        async with connections_lock:
            connections.discard(websocket)


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("app.main:app", host="127.0.0.1", port=8000, reload=True)
