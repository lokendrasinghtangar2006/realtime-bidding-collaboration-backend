# Submission Notes

This implementation matches the assignment requirements.

- **Live updates:** FastAPI WebSockets broadcast accepted bid changes to all connected clients.
- **Concurrency:** SQLite `BEGIN IMMEDIATE` serializes write transactions. The highest-bid check and update occur inside the same transaction.
- **No last-write-wins bug:** A lower/stale bid is rejected after the latest persisted highest bid is read under the transaction lock.
- **Persistence:** Auction state is stored in SQLite.
- **Reconnect:** A newly connected client immediately receives the persisted current state.
- **Disconnect:** Broken WebSocket connections are removed from the active connection set.
- **Python:** Backend is implemented with FastAPI.
- **Demo:** Open the frontend in multiple tabs and submit different bids.
