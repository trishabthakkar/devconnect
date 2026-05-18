'use strict';

const http = require('http');
const fs   = require('fs');
const path = require('path');
const { WebSocketServer } = require('ws');
const { v4: uuidv4 } = require('uuid');

// ─── Persistence ──────────────────────────────────────────────────────────────

const DATA_FILE = path.join(__dirname, '..', 'data.json');

function loadData() {
  try {
    if (fs.existsSync(DATA_FILE)) {
      const raw = JSON.parse(fs.readFileSync(DATA_FILE, 'utf8'));
      // Schema migration: upgrade from v1 shape (projects/messages → posts/dms)
      return {
        users:          raw.users          || {},
        posts:          raw.posts          || raw.projects || [],
        dms:            raw.dms            || {},
        friendships:    raw.friendships    || {},
        friendRequests: raw.friendRequests || {},
      };
    }
  } catch (e) {
    console.error('[store] load failed, starting fresh:', e.message);
  }
  return { users: {}, posts: [], dms: {}, friendships: {}, friendRequests: {} };
}

const store = loadData();

// Returns a Promise so callers inside the event queue can await it.
function saveData() {
  return fs.promises
    .writeFile(DATA_FILE, JSON.stringify(store, null, 2))
    .catch(e => console.error('[store] save failed:', e.message));
}

// ─── Serialised Event Queue ───────────────────────────────────────────────────
//
// Node.js is single-threaded, but async I/O creates re-entrancy windows:
// two handlers that both do `await saveData()` can each read the same pre-save
// state, mutate it, and then write back — last write wins, dropping the other.
//
// The queue chains every incoming mutation onto a single promise tail.  Each
// job fully completes (including the awaited fs.write) before the next job
// starts, giving us deterministic, sequential execution regardless of how many
// clients fire at the same microsecond.

class EventQueue {
  constructor() { this._tail = Promise.resolve(); }

  enqueue(fn) {
    this._tail = this._tail
      .then(fn)
      .catch(err => console.error('[queue] handler threw:', err));
    return this._tail;
  }
}

const queue = new EventQueue();

// ─── Store helpers ─────────────────────────────────────────────────────────────

function ensureFriendList(uid)   { if (!store.friendships[uid])    store.friendships[uid]    = []; return store.friendships[uid]; }
function ensureRequestList(uid)  { if (!store.friendRequests[uid]) store.friendRequests[uid] = []; return store.friendRequests[uid]; }

function areFriends(a, b)         { return (store.friendships[a]    || []).includes(b); }
function hasPendingRequest(from, to) { return (store.friendRequests[from] || []).includes(to); }

// Atomically establish a bidirectional friendship edge.
function addFriendship(a, b) {
  const fa = ensureFriendList(a);
  const fb = ensureFriendList(b);
  if (!fa.includes(b)) fa.push(b);
  if (!fb.includes(a)) fb.push(a);
}

// Atomically remove a bidirectional friendship edge.
function removeFriendship(a, b) {
  if (store.friendships[a]) store.friendships[a] = store.friendships[a].filter(x => x !== b);
  if (store.friendships[b]) store.friendships[b] = store.friendships[b].filter(x => x !== a);
}

function removeRequest(from, to) {
  if (store.friendRequests[from])
    store.friendRequests[from] = store.friendRequests[from].filter(x => x !== to);
}

// Canonical DM channel key: always sorted so A↔B and B↔A share one thread.
function dmKey(a, b) { return [a, b].sort().join(':'); }

// Return only the DM threads that involve this user.
function dmsForUser(userId) {
  const result = {};
  for (const [key, msgs] of Object.entries(store.dms)) {
    if (key.split(':').includes(userId)) result[key] = msgs;
  }
  return result;
}

// Sanitised public projection — never expose internal fields.
function publicUser(u) {
  return {
    userId:    u.userId,
    username:  u.username,
    bio:       u.bio,
    avatarUrl: u.avatarUrl,
    skills:    u.skills,
    createdAt: u.createdAt,
  };
}

// Full state snapshot tailored to the authenticated user.
function buildSnapshot(userId) {
  const publicUsers = {};
  for (const [id, u] of Object.entries(store.users)) publicUsers[id] = publicUser(u);

  return {
    type:             'state_snapshot',
    users:            publicUsers,
    posts:            store.posts,
    dms:              dmsForUser(userId),
    friends:          store.friendships[userId]    || [],
    outgoingRequests: store.friendRequests[userId] || [],
    incomingRequests: Object.entries(store.friendRequests)
      .filter(([, targets]) => targets.includes(userId))
      .map(([from]) => from),
  };
}

// ─── WebSocket transport helpers ──────────────────────────────────────────────

// clientId → { ws, userId }
const clients = new Map();

function send(ws, payload) {
  if (ws.readyState === ws.OPEN) ws.send(JSON.stringify(payload));
}

function broadcast(payload, excludeClientId = null) {
  const msg = JSON.stringify(payload);
  for (const [id, { ws }] of clients) {
    if (id !== excludeClientId && ws.readyState === ws.OPEN) ws.send(msg);
  }
}

// Deliver to every active session for a given userId (a user may have
// multiple browser tabs open).
function sendToUser(userId, payload) {
  for (const { ws, userId: uid } of clients.values()) {
    if (uid === userId) send(ws, payload);
  }
}

// ─── Message handlers ─────────────────────────────────────────────────────────
// Every handler is an async function.  They are always invoked through
// queue.enqueue(), so they execute one at a time.

const handlers = {};

// ── register ──────────────────────────────────────────────────────────────────
handlers.register = async (ws, clientId, msg) => {
  const { username, bio = '', avatarUrl = '', skills = [] } = msg;
  if (!username || typeof username !== 'string')
    return send(ws, { type: 'error', code: 'BAD_INPUT', text: 'username required' });

  const clean = username.trim().slice(0, 32);
  if (!clean)
    return send(ws, { type: 'error', code: 'BAD_INPUT', text: 'invalid username' });

  // Check uniqueness inside the queue — safe from TOCTOU races.
  if (Object.values(store.users).some(u => u.username === clean))
    return send(ws, { type: 'error', code: 'USERNAME_TAKEN', text: 'username taken' });

  const userId = uuidv4();
  store.users[userId] = {
    userId,
    username:  clean,
    bio:       String(bio).slice(0, 300),
    avatarUrl: String(avatarUrl).slice(0, 500),
    skills:    Array.isArray(skills) ? skills.map(String).slice(0, 20) : [],
    createdAt: Date.now(),
  };
  clients.get(clientId).userId = userId;
  await saveData();

  send(ws, { type: 'registered', user: publicUser(store.users[userId]) });
  send(ws, buildSnapshot(userId));
  broadcast({ type: 'user_joined', user: publicUser(store.users[userId]) }, clientId);
  broadcast({ type: 'online_count', count: clients.size });
  console.log(`[auth] register "${clean}" (${userId})`);
};

// ── login ─────────────────────────────────────────────────────────────────────
handlers.login = async (ws, clientId, msg) => {
  const { username } = msg;
  if (!username || typeof username !== 'string')
    return send(ws, { type: 'error', code: 'BAD_INPUT', text: 'username required' });

  const user = Object.values(store.users).find(u => u.username === username.trim());
  if (!user)
    return send(ws, { type: 'error', code: 'NOT_FOUND', text: 'user not found' });

  clients.get(clientId).userId = user.userId;

  send(ws, { type: 'logged_in', user: publicUser(user) });
  send(ws, buildSnapshot(user.userId));
  broadcast({ type: 'user_joined',   user: publicUser(user) }, clientId);
  broadcast({ type: 'online_count',  count: clients.size });
  console.log(`[auth] login "${user.username}" (${user.userId})`);
};

// ── update_profile ────────────────────────────────────────────────────────────
handlers.update_profile = async (ws, clientId, msg) => {
  const { userId } = clients.get(clientId);
  if (!userId) return send(ws, { type: 'error', code: 'UNAUTHED', text: 'not authenticated' });

  const user = store.users[userId];
  if (!user) return send(ws, { type: 'error', code: 'NOT_FOUND', text: 'user not found' });

  if (msg.bio       !== undefined) user.bio       = String(msg.bio).slice(0, 300);
  if (msg.avatarUrl !== undefined) user.avatarUrl = String(msg.avatarUrl).slice(0, 500);
  if (msg.skills    !== undefined)
    user.skills = Array.isArray(msg.skills) ? msg.skills.map(String).slice(0, 20) : user.skills;

  await saveData();
  const pub = publicUser(user);
  send(ws, { type: 'profile_updated', user: pub });
  broadcast({ type: 'user_updated', user: pub }, clientId);
};

// ── post (collab request / debug call) ───────────────────────────────────────
handlers.post = async (ws, clientId, msg) => {
  const { userId } = clients.get(clientId);
  if (!userId) return send(ws, { type: 'error', code: 'UNAUTHED', text: 'not authenticated' });

  const { title, body = '', tags = [], kind = 'collab' } = msg;
  if (!title || typeof title !== 'string')
    return send(ws, { type: 'error', code: 'BAD_INPUT', text: 'title required' });
  if (!['collab', 'debug'].includes(kind))
    return send(ws, { type: 'error', code: 'BAD_INPUT', text: 'kind must be collab or debug' });

  const post = {
    postId:    uuidv4(),
    authorId:  userId,
    title:     title.trim().slice(0, 120),
    body:      String(body).slice(0, 1000),
    tags:      Array.isArray(tags) ? tags.map(String).slice(0, 10) : [],
    kind,
    createdAt: Date.now(),
  };
  store.posts.push(post);
  await saveData();

  const payload = { type: 'post_new', post };
  send(ws, payload);
  broadcast(payload, clientId);
  console.log(`[feed] post "${post.title}" kind=${kind} by ${userId}`);
};

// ── delete_post ───────────────────────────────────────────────────────────────
handlers.delete_post = async (ws, clientId, msg) => {
  const { userId } = clients.get(clientId);
  if (!userId) return send(ws, { type: 'error', code: 'UNAUTHED', text: 'not authenticated' });

  const idx = store.posts.findIndex(p => p.postId === msg.postId);
  if (idx === -1)
    return send(ws, { type: 'error', code: 'NOT_FOUND', text: 'post not found' });
  if (store.posts[idx].authorId !== userId)
    return send(ws, { type: 'error', code: 'FORBIDDEN', text: 'not your post' });

  store.posts.splice(idx, 1);
  await saveData();

  const payload = { type: 'post_deleted', postId: msg.postId };
  send(ws, payload);
  broadcast(payload, clientId);
};

// ── friend_request ────────────────────────────────────────────────────────────
//
// Invariant enforcement:
//   • Cannot request yourself
//   • Cannot request someone you're already friends with
//   • Cannot send a duplicate pending request
//   • If the target has already sent YOU a request, auto-accept (mutual intent)
//     instead of creating an orphaned pair of pending requests.

handlers.friend_request = async (ws, clientId, msg) => {
  const { userId: from } = clients.get(clientId);
  if (!from) return send(ws, { type: 'error', code: 'UNAUTHED', text: 'not authenticated' });

  const to = msg.toUserId;
  if (!to || !store.users[to])
    return send(ws, { type: 'error', code: 'NOT_FOUND', text: 'user not found' });
  if (from === to)
    return send(ws, { type: 'error', code: 'BAD_INPUT', text: 'cannot friend yourself' });
  if (areFriends(from, to))
    return send(ws, { type: 'error', code: 'ALREADY_FRIENDS', text: 'already friends' });
  if (hasPendingRequest(from, to))
    return send(ws, { type: 'error', code: 'DUPLICATE', text: 'request already sent' });

  // Mutual simultaneous request → auto-accept both, no redundant pending state.
  if (hasPendingRequest(to, from)) {
    removeRequest(to, from);
    addFriendship(from, to);
    await saveData();
    const payload = { type: 'friendship_changed', action: 'added', userA: from, userB: to };
    send(ws, payload);
    sendToUser(to, payload);
    console.log(`[friends] mutual-accept ${from} ↔ ${to}`);
    return;
  }

  ensureRequestList(from).push(to);
  await saveData();

  send(ws, { type: 'friend_request_sent', toUserId: to });
  sendToUser(to, {
    type:         'friend_request_received',
    fromUserId:   from,
    fromUsername: store.users[from].username,
  });
  console.log(`[friends] request ${from} → ${to}`);
};

// ── friend_respond ────────────────────────────────────────────────────────────
handlers.friend_respond = async (ws, clientId, msg) => {
  const { userId: to } = clients.get(clientId);
  if (!to) return send(ws, { type: 'error', code: 'UNAUTHED', text: 'not authenticated' });

  const { fromUserId: from, accept } = msg;
  if (!from || !store.users[from])
    return send(ws, { type: 'error', code: 'NOT_FOUND', text: 'user not found' });

  // Guard: the request must still exist (it may have been rescinded via
  // a concurrent unfriend/cancel that arrived just ahead of us in the queue).
  if (!hasPendingRequest(from, to))
    return send(ws, { type: 'error', code: 'NOT_FOUND', text: 'no pending request from that user' });

  removeRequest(from, to);

  if (accept) {
    addFriendship(from, to);
    await saveData();
    const payload = { type: 'friendship_changed', action: 'added', userA: from, userB: to };
    send(ws, payload);
    sendToUser(from, payload);
    console.log(`[friends] accepted ${from} ↔ ${to}`);
  } else {
    await saveData();
    send(ws, { type: 'friend_request_declined', fromUserId: from });
    sendToUser(from, { type: 'friend_request_declined', byUserId: to });
    console.log(`[friends] declined ${from} → ${to}`);
  }
};

// ── unfriend ──────────────────────────────────────────────────────────────────
handlers.unfriend = async (ws, clientId, msg) => {
  const { userId: self } = clients.get(clientId);
  if (!self) return send(ws, { type: 'error', code: 'UNAUTHED', text: 'not authenticated' });

  const other = msg.userId;
  if (!other || !store.users[other])
    return send(ws, { type: 'error', code: 'NOT_FOUND', text: 'user not found' });
  if (!areFriends(self, other))
    return send(ws, { type: 'error', code: 'NOT_FRIENDS', text: 'not friends' });

  removeFriendship(self, other);
  await saveData();

  const payload = { type: 'friendship_changed', action: 'removed', userA: self, userB: other };
  send(ws, payload);
  sendToUser(other, payload);
  console.log(`[friends] unfriend ${self} ✕ ${other}`);
};

// ── send_dm ───────────────────────────────────────────────────────────────────
handlers.send_dm = async (ws, clientId, msg) => {
  const { userId: from } = clients.get(clientId);
  if (!from) return send(ws, { type: 'error', code: 'UNAUTHED', text: 'not authenticated' });

  const { toUserId, text } = msg;
  if (!toUserId || !text || typeof text !== 'string')
    return send(ws, { type: 'error', code: 'BAD_INPUT', text: 'toUserId and text required' });
  if (!store.users[toUserId])
    return send(ws, { type: 'error', code: 'NOT_FOUND', text: 'recipient not found' });

  const dm = {
    dmId:       uuidv4(),
    fromUserId: from,
    toUserId,
    text:       text.slice(0, 2000),
    sentAt:     Date.now(),
  };

  const key = dmKey(from, toUserId);
  if (!store.dms[key]) store.dms[key] = [];
  store.dms[key].push(dm);
  await saveData();

  const payload = { type: 'dm_received', dm };
  send(ws, payload);
  sendToUser(toUserId, payload);
};

// ─── HTTP server (serves /client static files) ────────────────────────────────

const MIME = {
  '.html': 'text/html',
  '.js':   'application/javascript',
  '.css':  'text/css',
  '.json': 'application/json',
  '.png':  'image/png',
  '.svg':  'image/svg+xml',
  '.ico':  'image/x-icon',
};

const CLIENT_ROOT = path.resolve(path.join(__dirname, '..', 'client'));

const httpServer = http.createServer((req, res) => {
  const urlPath  = req.url === '/' ? '/index.html' : req.url.split('?')[0];
  const filePath = path.resolve(path.join(CLIENT_ROOT, urlPath));

  // Prevent path-traversal attacks (e.g. ../../etc/passwd).
  if (!filePath.startsWith(CLIENT_ROOT)) {
    res.writeHead(403); res.end('Forbidden'); return;
  }

  fs.readFile(filePath, (err, data) => {
    if (err) { res.writeHead(404); res.end('Not found'); return; }
    const ext = path.extname(filePath);
    res.writeHead(200, { 'Content-Type': MIME[ext] || 'text/plain' });
    res.end(data);
  });
});

// ─── WebSocket server ─────────────────────────────────────────────────────────

const wss = new WebSocketServer({ server: httpServer });

wss.on('connection', (ws) => {
  const clientId = uuidv4();
  clients.set(clientId, { ws, userId: null });
  console.log(`[ws] connect    ${clientId}  (total: ${clients.size})`);

  send(ws, { type: 'welcome', clientId, onlineCount: clients.size });
  broadcast({ type: 'online_count', count: clients.size });

  ws.on('message', (raw) => {
    let msg;
    try { msg = JSON.parse(raw); }
    catch { return send(ws, { type: 'error', code: 'BAD_JSON', text: 'invalid JSON' }); }

    const handler = handlers[msg.type];
    if (!handler)
      return send(ws, { type: 'error', code: 'UNKNOWN_TYPE', text: `unknown type: ${msg.type}` });

    // All state mutations are serialised through the queue.
    queue.enqueue(() => handler(ws, clientId, msg));
  });

  ws.on('close', () => {
    const { userId } = clients.get(clientId) || {};
    clients.delete(clientId);
    console.log(`[ws] disconnect ${clientId}  (total: ${clients.size})`);
    broadcast({ type: 'online_count', count: clients.size });
    if (userId && store.users[userId])
      broadcast({ type: 'user_left', userId, username: store.users[userId].username });
  });

  ws.on('error', err => console.error(`[ws] error ${clientId}:`, err.message));
});

// ─── Boot ──────────────────────────────────────────────────────────────────────

const PORT = process.env.PORT || 3000;
httpServer.listen(PORT, () => {
  const u = Object.keys(store.users).length;
  const p = store.posts.length;
  const d = Object.keys(store.dms).length;
  const f = Math.round(
    Object.values(store.friendships).reduce((n, arr) => n + arr.length, 0) / 2
  );
  console.log(`\n  DevConnect  →  http://localhost:${PORT}`);
  console.log(`  Loaded: ${u} users · ${p} posts · ${d} DM channels · ${f} friendships\n`);
});
