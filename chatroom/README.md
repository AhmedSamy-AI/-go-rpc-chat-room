# Go RPC Chat Room (Polling)

Concurrent chat using `net/rpc`. Clients **poll** the server (default every 500 ms) for pending messages.

## Run
```bash
go run ./server                 # terminal 1  (flag: -addr :1234)
go run ./client                 # terminals 2..N (flags: -addr localhost:1234 -name alice -poll 500ms)
```
Client commands: `/users` (list online users), `/quit` (leave). Ctrl+C also leaves cleanly.

## Design
- `shared/` - RPC arg/reply types.
- `server/` - `ChatService` with a per-user message queue guarded by a `sync.Mutex`.
  RPC methods: `Join`, `Leave`, `Send`, `Poll`, `ListUsers`.
  - Duplicate (case-insensitive) / invalid usernames are rejected; the client asks again.
  - Chat messages go to everyone except the sender; join/leave notices go to everyone except the subject.
  - A reaper goroutine removes clients that stop polling for 10 s (unexpected disconnect) and notifies others.
  - Ctrl+C closes the listener and all connections, then waits for handlers.
- `client/` - goroutines for polling and stdin; Ctrl+C or `/quit` calls `Leave`; detects server loss.
