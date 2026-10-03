## System Architecture

`messageGO` is built using an event-driven, concurrent hub-and-spoke architecture powered by Go goroutines and channels.

```text
               ┌─────────────────────────────────────────┐
               │       Client (Postman/Frontend)         │
               └────────────────────┬────────────────────┘
                                    │ WebSocket Protocol
                                    ▼
               ┌─────────────────────────────────────────┐
               │    HTTP Upgrader (cmd/server/main.go)   │
               └────────────────────┬────────────────────┘
                                    │
                         Spawns per-connection
                                    │
             ┌──────────────────────┴──────────────────────┐
             ▼                                             ▼
  ┌─────────────────────┐                       ┌─────────────────────┐
  │   Client.ReadPump   │                       │  Client.WritePump   │
  │     (Goroutine)     │                       │     (Goroutine)     │
  └──────────┬──────────┘                       └──────────▲──────────┘
             │                                             │
             │ Inbound Envelope                            │ Outbound Envelope
             │ (broadcast chan)                            │ (client.Send chan)
             ▼                                             │
┌──────────────────────────────────────────────────────────┴─────────────────┐
│                           Hub (internal/chat/hub.go)                       │
│                                                                            │
│   - Event Loop (`go hub.Run()`)                                            │
│   - Session Registry: map[UserID]*Client                                   │
│   - Routes envelopes based on recipient_id                                 │
└─────────────────────────────────────┬──────────────────────────────────────┘
                                      │
                        Recipient Online / Offline Check
                                      │
                   ┌──────────────────┴──────────────────┐
        Recipient Online?                         Recipient Offline?
                   │                                     │
                   ▼                                     ▼
        Deliver via client.Send                     Persist to Store
                                                         │
                                                         ▼
                                                ┌──────────────────┐
                                                │   MemoryStore    │
                                                │   (RAM Queue)    │
                                                └──────────────────┘

```

# What Connects to What
## HTTP to WebSocket Upgrader (cmd/server/main.go)

- Intercepts incoming HTTP requests on /ws?user_id=<id>.

- Upgrades the HTTP connection to a persistent WebSocket protocol connection using gorilla/websocket.

- Instantiates a Client struct and passes it to the Hub.

## Per-Connection I/O Pumps (internal/chat/client.go)

- Every connected user gets two dedicated goroutines:

  - ReadPump: Listens continuously for raw WebSocket frames from the socket, unmarshals JSON into a models.Envelope, and pushes it to hub.broadcast.

  - WritePump: Listens on the client’s private Send channel `(chan *models.Envelope)`, encodes outgoing envelopes into JSON, and writes them down the WebSocket socket. Handles connection heartbeats (Ping/Pong).

## Central Event Router (internal/chat/hub.go)

- Runs a single background goroutine `(go hub.Run())` executing a select loop over three main channels:

  - register: Adds new active clients to the clients map `(map[string]*Client)`.

  - unregister: Removes disconnected clients and closes their Send channels.

  - broadcast: Inspects `RecipientID` in the envelope payload and routes it:

- If Recipient is Online: Sends envelope directly into `recipient.Send`.

- If Recipient is Offline: Hands envelope off to `offlineStore.Save()`.

- Storage Layer Interface `(internal/storage/store.go)`

- Decouples storage logic from routing via the OfflineStore interface `(Save and GetAndClear)`.

# Where Queued Messages Are Stored
Currently, queued offline messages are stored strictly in RAM inside `MemoryStore`.
