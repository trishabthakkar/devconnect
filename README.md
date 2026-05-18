# DevConnect

A real-time CS student networking and collaboration platform. Register a profile, broadcast collab and debug requests to the live feed, build a peer network through the friend graph, and chat via private direct messages — all streamed over WebSocket with zero page reloads.

---

## Features

- **Live Collaboration Feed** — post project shoutouts (`Collab`) or debugging requests (`Debug`); every connected peer sees new cards appear instantly
- **Peer Network** — send, accept, and decline friend requests; online presence indicators update in real time
- **Direct Messages** — slide-up DM drawer with per-thread chat history, delivered live to both participants
- **Profile Hub** — username, bio, avatar URL, skills list; partial updates broadcast to all peers immediately
- **Race-condition-safe backend** — serialized event queue ensures concurrent mutations never corrupt state
- **Persistent state** — all data survives server restarts via `data.json`; new clients receive a full snapshot on connect
- **Auto-reconnect** — the client retries the WebSocket connection every 3 seconds after any drop

---

## Design

The UI uses a **Chic Purple & Dark Blue** minimalist palette — matte dark slate base (`#0B0F19`), flat charcoal panels (`#111625`), and a sophisticated violet accent (`#8B5CF6`). No neon glows, no glassmorphism — crisp borders, clean typography, and restrained motion.

| Role | Value |
|---|---|
| Base background | `#0B0F19` |
| Panel / card | `#111625` |
| Border | `#1e2235` |
| Accent (violet-500) | `#8B5CF6` |
| Primary text | `#F1F5F9` |
| Secondary text (lavender) | `#C4B5FD` |
| Muted / metadata | `#64748b` |

---

## Tech Stack

| Layer | Technology |
|---|---|
| Runtime | Node.js 18+ (no framework) |
| WebSocket | `ws` v8 |
| IDs | `uuid` v11 |
| Persistence | `data.json` — flat file, atomic writes via serialized event queue |
| Frontend | Vanilla HTML/CSS/JS, Tailwind CSS via CDN, Inter + JetBrains Mono fonts |

---

## Prerequisites

- **Node.js 18+** — verify with `node --version`
- No database, no Docker, no build step required

---

## Quick Start

```bash
# 1. Clone and install
git clone https://github.com/trishabthakkar/devconnect.git
cd devconnect
npm install

# 2. Start the server
npm start
# → DevConnect  →  http://localhost:3000

# 3. Open two browser tabs to simulate two students
open http://localhost:3000
```

Custom port:

```bash
PORT=8080 npm start
```

Live-reload during development:

```bash
npm run dev   # uses node --watch (Node 18+)
```

---

## Project Structure

```
devconnect/
├── client/
│   └── index.html          # Single-page app — all UI, styles, and client logic
├── server/
│   ├── index.js            # HTTP static server + WebSocket server (491 lines)
│   └── stress-test.js      # Concurrent correctness test suite (6 suites, 25 assertions)
├── data.json               # Auto-created on first run; gitignored
├── package.json
└── README.md
```

The server serves `client/index.html` over HTTP and handles all WebSocket traffic on the same port. There is no separate build process — the single HTML file is the complete frontend.

---

## Demo Walkthrough

Open **two browser tabs** side by side at `http://localhost:3000`.

### Tab 1 — Alice

1. Fill in **Username** (`alice`), **Bio**, a **Skills** list (`TypeScript, React, Solana`), and an optional avatar URL (e.g. `https://i.pravatar.cc/150?u=alice`).
2. Click **Create Account**.
3. The dashboard appears. Left panel shows your profile; center feed is empty.

### Tab 2 — Bob

1. Register as `bob` with skills `Python, ML`.
2. Bob's dashboard appears. The **Peers** panel (right column) already lists Alice.

### Friending

- In Bob's Peers panel, click **+ Connect** next to Alice.
- Alice's tab shows a toast: *"bob wants to connect"*.
- In Alice's Peers panel, click **Accept** — both tabs update instantly and the connection count increments.

### Posting

- Alice selects **⚡ Collab Request** or **🐛 Debug Call**, types a title, and clicks **Publish →**.
- The card appears in both feeds in real time.
- Alice can delete her own post with the **×** button; the card disappears from all open tabs.

### Direct Messages

- Click **DM** next to any connected peer to open the slide-up chat drawer.
- Type a message and press Enter — it appears in both tabs immediately.
- Click the drawer header to minimize; click **×** to close.

### Profile Editing

- Click the pencil icon on the left panel to expand the edit form.
- Update bio, avatar URL, or skills and click **Save Changes** — the peer list in all open tabs refreshes.

---

## Architecture

### Event Queue

Node.js is single-threaded but async I/O creates re-entrancy windows: two handlers that both call `await saveData()` can each read the same pre-save state, mutate it, and write back — last write wins, silently dropping the other. The queue chains every incoming mutation onto a single promise tail, giving deterministic sequential execution regardless of how many clients fire simultaneously.

```
client A  →  message  →  queue.enqueue(handlerA)
client B  →  message  →  queue.enqueue(handlerB)
                              ↓  handlerA runs fully (including fs.write)
                              ↓  handlerB runs fully (including fs.write)
```

### State Synchronization

On every login or register the server sends a `state_snapshot` containing:

- All registered user profiles
- All posts
- DM threads for this user only
- Friend list, outgoing requests, incoming requests
- Currently authenticated user IDs (for online presence indicators)

The client replaces its entire local state from this snapshot, so a browser refresh or network drop and reconnect always produces a consistent view — no partial state is possible.

### Bidirectional Friendship Invariant

Every friendship mutation goes through `addFriendship(a, b)` or `removeFriendship(a, b)`, which always update both `friendships[a]` and `friendships[b]` atomically within the same synchronous call before any `await`. This guarantees the graph can never end up in a split state where A considers B a friend but B does not.

### Mutual Friend-Request Auto-Accept

If A sends a request to B while B already has a pending request to A (possible under concurrent load), the server detects the mutual intent and immediately establishes the friendship without leaving either request in a pending state.

### Security

- **Path traversal protection** — the HTTP static file handler resolves the requested path and verifies it starts with `CLIENT_ROOT` before reading; requests like `../../etc/passwd` return 403.
- **Input sanitization** — all string fields are trimmed and length-capped server-side; arrays are validated; unknown message types return `UNKNOWN_TYPE`.
- **No client-to-client routing** — all messages pass through the server; clients only receive data the server explicitly sends them (DMs are filtered per-user in `dmsForUser`).
- **Disconnected-client safety** — all handlers guard against `clients.get(clientId)` returning `undefined` when a client disconnects between message receipt and queue execution.

---

## Stress Test

Six suites, 25 assertions, run against a live server:

| Suite | What it tests |
|---|---|
| 0 | Four concurrent registrations |
| 1 | Friend-request race condition (A→B and B→A in the same tick) |
| 2 | Unfriend + simultaneous re-request collision |
| 3 | Five concurrent post broadcasts observed by a single neutral client |
| 4 | DM delivery under concurrent message load |
| 5 | Profile update propagation |
| 6 | Input validation and error codes |

```bash
# Terminal 1 — server must be running
npm start

# Terminal 2
node server/stress-test.js
```

Expected output:

```
══════════════════════════════════════════════
  DevConnect Backend Stress Test
══════════════════════════════════════════════

[ Suite 0 ] Concurrent connections + registration
  ✓  four clients connected simultaneously
  ✓  alice profile has skills
  ✓  bob   profile has skills
  ✓  carol profile has skills
  ✓  dave  profile has skills
  ✓  avatarUrl persisted on alice

[ Suite 1 ] Friend-request race condition (A→B simultaneous B→A)
  ✓  A received friendship_changed action=added
  ✓  B received friendship_changed action=added
  ✓  friendship_changed userA/userB match A and B

...

══════════════════════════════════════════════
  Results: 25 passed, 0 failed
══════════════════════════════════════════════
```

Each run uses timestamp-scoped usernames (`alice_<runId>`) so tests are hermetic against leftover `data.json` state and can be re-run without resetting the server.

---

## WebSocket Message Reference

### Client → Server

| Type | Key Fields | Description |
|---|---|---|
| `register` | `username`, `bio?`, `avatarUrl?`, `skills[]?` | Create a new account |
| `login` | `username` | Resume an existing account |
| `update_profile` | `bio?`, `avatarUrl?`, `skills[]?` | Partial profile update |
| `post` | `title`, `body?`, `tags[]?`, `kind` (`collab`\|`debug`) | Publish a feed post |
| `delete_post` | `postId` | Delete own post |
| `friend_request` | `toUserId` | Send friend request (auto-accepts if mutual) |
| `friend_respond` | `fromUserId`, `accept` (bool) | Accept or decline an incoming request |
| `unfriend` | `userId` | Remove a friendship |
| `send_dm` | `toUserId`, `text` | Send a direct message |

### Server → Client

| Type | Payload highlights | Description |
|---|---|---|
| `welcome` | `clientId`, `onlineCount` | Sent immediately on WebSocket connect |
| `registered` | `user` | Account created successfully |
| `logged_in` | `user` | Login successful |
| `state_snapshot` | `users`, `posts`, `dms`, `friends`, `outgoingRequests`, `incomingRequests`, `onlineUserIds` | Full state for this user |
| `user_joined` | `user` | Another user authenticated |
| `user_left` | `userId`, `username` | A user disconnected |
| `user_updated` | `user` | A user updated their profile |
| `online_count` | `count` | Total connected clients |
| `post_new` | `post` | A post was published |
| `post_deleted` | `postId` | A post was deleted |
| `friendship_changed` | `action` (`added`\|`removed`), `userA`, `userB` | Friendship established or removed |
| `friend_request_sent` | `toUserId` | Echoed back to the requester |
| `friend_request_received` | `fromUserId`, `fromUsername` | Delivered to the target |
| `friend_request_declined` | `fromUserId` / `byUserId` | Delivered to both parties |
| `dm_received` | `dm` (`dmId`, `fromUserId`, `toUserId`, `text`, `sentAt`) | Delivered to sender (echo) and recipient |
| `profile_updated` | `user` | Echoed to the updater |
| `error` | `code`, `text` | See error codes below |

### Error Codes

| Code | Meaning |
|---|---|
| `BAD_INPUT` | Missing or invalid field (e.g. empty post title) |
| `BAD_JSON` | Message body could not be parsed as JSON |
| `USERNAME_TAKEN` | Registration attempted with an existing username |
| `NOT_FOUND` | Referenced user, post, or request does not exist |
| `UNAUTHED` | Action requires authentication |
| `ALREADY_FRIENDS` | Friend request sent to an existing friend |
| `DUPLICATE` | Identical pending request already exists |
| `NOT_FRIENDS` | Unfriend attempted on a non-friend |
| `FORBIDDEN` | Action not permitted (e.g. deleting another user's post) |
| `UNKNOWN_TYPE` | Message `type` field not recognised |

---

## Data File

`data.json` is created automatically on first run and is gitignored. Schema:

```json
{
  "users":          { "<userId>": { "userId", "username", "bio", "avatarUrl", "skills", "createdAt" } },
  "posts":          [ { "postId", "authorId", "title", "body", "tags", "kind", "createdAt" } ],
  "dms":            { "<userA>:<userB>": [ { "dmId", "fromUserId", "toUserId", "text", "sentAt" } ] },
  "friendships":    { "<userId>": ["<friendId>", ...] },
  "friendRequests": { "<fromUserId>": ["<toUserId>", ...] }
}
```

DM channel keys are always the two user IDs sorted lexicographically and joined with `:`, so `A:B` and `B:A` always resolve to the same thread.

To reset all data:

```bash
rm data.json && npm start
```

---

## License

MIT
