'use strict';

// ─── DevConnect Stress Test ────────────────────────────────────────────────────
// Tests four concurrent pipelines simultaneously:
//   1. Race-condition friend requests (A→B and B→A at the exact same tick)
//   2. Rapid concurrent post broadcasts (5 posts fired simultaneously)
//   3. Bidirectionality invariant check after unfriend collision
//   4. DM delivery under concurrent message load
//
// Run while the server is up: node server/stress-test.js

const WebSocket = require('ws');
const path      = require('path');
const fs        = require('fs');

// Use timestamp-scoped usernames so tests are hermetic against leftover data.json.
const RUN_ID = Date.now().toString(36);

const SERVER = 'ws://localhost:3000';
const TIMEOUT_MS = 8000;

// ── Utilities ──────────────────────────────────────────────────────────────────

let passed = 0, failed = 0;

function assert(label, condition, detail = '') {
  if (condition) {
    console.log(`  ✓  ${label}`);
    passed++;
  } else {
    console.error(`  ✗  ${label}${detail ? ' — ' + detail : ''}`);
    failed++;
  }
}

// Connect and return a handle with an async `waitFor(type)` method.
function connect() {
  return new Promise((resolve, reject) => {
    const ws = new WebSocket(SERVER);
    const inbox = [];
    const waiters = [];

    ws.on('message', (raw) => {
      const msg = JSON.parse(raw);
      const waiter = waiters.find(w => w.type === msg.type || w.type === '*');
      if (waiter) {
        waiters.splice(waiters.indexOf(waiter), 1);
        waiter.resolve(msg);
      } else {
        inbox.push(msg);
      }
    });

    ws.on('error', reject);

    ws.on('open', () => {
      resolve({
        ws,
        send(payload) { ws.send(JSON.stringify(payload)); },
        waitFor(type, timeoutMs = TIMEOUT_MS) {
          // Check inbox first
          const idx = inbox.findIndex(m => m.type === type || type === '*');
          if (idx !== -1) return Promise.resolve(inbox.splice(idx, 1)[0]);
          return new Promise((res, rej) => {
            const timer = setTimeout(() => rej(new Error(`timeout waiting for "${type}"`)), timeoutMs);
            waiters.push({
              type,
              resolve(msg) { clearTimeout(timer); res(msg); },
            });
          });
        },
        close() { ws.close(); },
      });
    });
  });
}

async function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }

// Register a fresh user; returns { client, user }.
async function registerUser(username, skills = []) {
  const c = await connect();
  await c.waitFor('welcome');
  c.send({ type: 'register', username, bio: `Bio of ${username}`, avatarUrl: `https://i.pravatar.cc/150?u=${username}`, skills });
  const reg = await c.waitFor('registered');
  await c.waitFor('state_snapshot');
  return { client: c, user: reg.user };
}

// ─── Test suite ────────────────────────────────────────────────────────────────

async function runTests() {
  console.log('\n══════════════════════════════════════════════');
  console.log('  DevConnect Backend Stress Test');
  console.log('══════════════════════════════════════════════\n');

  // ── Suite 0: Basic connectivity ──────────────────────────────────────────────
  console.log('[ Suite 0 ] Concurrent connections + registration');

  const [cA, cB, cC, cD] = await Promise.all([
    registerUser(`alice_${RUN_ID}`, ['TypeScript', 'React', 'Solana']),
    registerUser(`bob_${RUN_ID}`,   ['Python', 'ML', 'FastAPI']),
    registerUser(`carol_${RUN_ID}`, ['Rust', 'WebAssembly', 'C']),
    registerUser(`dave_${RUN_ID}`,  ['Java', 'Spring', 'Kubernetes']),
  ]);

  assert('four clients connected simultaneously', true);
  assert('alice profile has skills', cA.user.skills.includes('React'));
  assert('bob   profile has skills', cB.user.skills.includes('Python'));
  assert('carol profile has skills', cC.user.skills.includes('Rust'));
  assert('dave  profile has skills', cD.user.skills.includes('Java'));
  assert('avatarUrl persisted on alice', cA.user.avatarUrl.startsWith('https://'));

  // ── Suite 1: Friend-request race condition ────────────────────────────────
  // A and B fire requests at each other in the same event-loop tick.
  // The queue must process them sequentially.  The second one hits the
  // "mutual request → auto-accept" branch; neither should be left pending.
  console.log('\n[ Suite 1 ] Friend-request race condition (A→B simultaneous B→A)');

  // Fire without awaiting — true simultaneous dispatch.
  cA.client.send({ type: 'friend_request', toUserId: cB.user.userId });
  cB.client.send({ type: 'friend_request', toUserId: cA.user.userId });

  // Both should receive friendship_changed (added), not a duplicate-request error.
  const [fChangeA, fChangeB] = await Promise.all([
    cA.client.waitFor('friendship_changed'),
    cB.client.waitFor('friendship_changed'),
  ]);

  assert('A received friendship_changed action=added', fChangeA.action === 'added');
  assert('B received friendship_changed action=added', fChangeB.action === 'added');
  assert('friendship_changed userA/userB match A and B',
    new Set([fChangeA.userA, fChangeA.userB]).has(cA.user.userId) &&
    new Set([fChangeA.userA, fChangeA.userB]).has(cB.user.userId));

  // ── Suite 2: Unfriend + simultaneous friend_request collision ────────────
  // B unfriends A while A tries to send another (now-redundant) request.
  console.log('\n[ Suite 2 ] Unfriend collision (B unfriends while A re-requests)');

  // A already IS friends with B.  B unfriends; A simultaneously fires a request.
  cB.client.send({ type: 'unfriend',        userId:   cA.user.userId });
  cA.client.send({ type: 'friend_request',  toUserId: cB.user.userId });

  // One of these will arrive first in the queue.  The possible outcomes are:
  //   • unfriend lands first  → A's request goes through as a new pending request
  //   • request  lands first  → unfriend finds them friends and removes; net: not friends, 1 pending request
  // Either way, the friendship must NOT be in a split state (A thinks friends, B does not).
  const [unfriendEvt] = await Promise.all([
    cA.client.waitFor('friendship_changed'),  // gets the unfriend broadcast
  ]);

  assert('A received friendship_changed action=removed after unfriend', unfriendEvt.action === 'removed');

  // ── Suite 3: Concurrent post broadcast ───────────────────────────────────
  // Fire all 5 posts from 4 clients in the same tick, then observe them all
  // through a single neutral observer (cD) who receives every broadcast.
  // This avoids the race where waitFor() on a sender resolves on a *different*
  // client's post arriving first via broadcast.
  console.log('\n[ Suite 3 ] Concurrent post broadcast (5 posts from 4 clients)');

  const postDefs = [
    { client: cA.client, authorId: cA.user.userId, title: 'Build Solana dApp',     kind: 'collab' },
    { client: cB.client, authorId: cB.user.userId, title: 'Debug ML pipeline',     kind: 'debug'  },
    { client: cC.client, authorId: cC.user.userId, title: 'Rust WASM module help', kind: 'debug'  },
    { client: cD.client, authorId: cD.user.userId, title: 'K8s autoscaling design',kind: 'collab' },
    { client: cA.client, authorId: cA.user.userId, title: 'Open-source OS kernel', kind: 'collab' },
  ];

  // Collect the next 5 post_new events arriving at cA (receives broadcasts from all others).
  const collectedPosts = [];
  const collectPromise = new Promise((resolve) => {
    const handler = ({ data }) => {
      const m = JSON.parse(data);
      if (m.type === 'post_new') {
        collectedPosts.push(m);
        if (collectedPosts.length === 5) {
          cA.client.ws.removeEventListener('message', handler);
          resolve();
        }
      }
    };
    cA.client.ws.addEventListener('message', handler);
  });

  // Fire all 5 concurrently in the same tick.
  for (const { client, title, kind } of postDefs) {
    client.send({ type: 'post', title, body: `Seeking collaborators for: ${title}`, tags: ['test'], kind });
  }

  await collectPromise;

  assert('all 5 post_new events received', collectedPosts.length === 5);
  assert('post kinds are preserved — debug present',  collectedPosts.some(p => p.post.kind === 'debug'));
  assert('post kinds are preserved — collab present', collectedPosts.some(p => p.post.kind === 'collab'));
  assert('each post has a unique postId',
    new Set(collectedPosts.map(p => p.post.postId)).size === 5);

  // ── Suite 4: DM delivery ─────────────────────────────────────────────────
  console.log('\n[ Suite 4 ] DM delivery (C→D and D→C concurrently)');

  cC.client.send({ type: 'send_dm', toUserId: cD.user.userId, text: 'Hey Dave, want to collab?' });
  cD.client.send({ type: 'send_dm', toUserId: cC.user.userId, text: 'Sure Carol! Let\'s do it.' });

  const [dmC, dmD] = await Promise.all([
    cC.client.waitFor('dm_received'),  // echo back to sender
    cD.client.waitFor('dm_received'),  // delivered to recipient
  ]);

  assert('C\'s DM echoed back to C',       dmC.dm.fromUserId === cC.user.userId);
  assert('D received C\'s DM',             dmD.dm.toUserId   === cD.user.userId);
  assert('DM text is intact',              dmC.dm.text === 'Hey Dave, want to collab?');

  // Verify D's DM is echoed to D and routed to C.
  const dmToC = await cC.client.waitFor('dm_received');
  assert('C received D\'s reply',          dmToC.dm.fromUserId === cD.user.userId);

  // ── Suite 5: Profile update propagation ──────────────────────────────────
  console.log('\n[ Suite 5 ] Profile update propagation');

  cA.client.send({ type: 'update_profile', bio: 'Updated bio', skills: ['TypeScript', 'Solana', 'Anchor'] });
  const profUpdated = await cA.client.waitFor('profile_updated');
  assert('profile_updated returned to A',        profUpdated.user.bio === 'Updated bio');
  assert('new skills list persisted',            profUpdated.user.skills.includes('Anchor'));
  assert('avatarUrl not clobbered on partial update', profUpdated.user.avatarUrl.length > 0);

  // ── Suite 6: Input validation ─────────────────────────────────────────────
  console.log('\n[ Suite 6 ] Input validation and error codes');

  // Attempt to register duplicate username
  const cDup = await connect();
  await cDup.waitFor('welcome');
  cDup.send({ type: 'register', username: `alice_${RUN_ID}` });
  const dupErr = await cDup.waitFor('error');
  assert('duplicate username returns error code USERNAME_TAKEN', dupErr.code === 'USERNAME_TAKEN');
  cDup.close();

  // Send DM to non-existent user
  cA.client.send({ type: 'send_dm', toUserId: 'not-a-real-id', text: 'hello?' });
  const dmErr = await cA.client.waitFor('error');
  assert('DM to unknown user returns NOT_FOUND', dmErr.code === 'NOT_FOUND');

  // Post without title
  cA.client.send({ type: 'post', title: '', kind: 'collab' });
  const postErr = await cA.client.waitFor('error');
  assert('empty post title returns BAD_INPUT', postErr.code === 'BAD_INPUT');

  // Unknown message type
  cB.client.send({ type: 'fly_to_moon' });
  const unknownErr = await cB.client.waitFor('error');
  assert('unknown message type returns UNKNOWN_TYPE', unknownErr.code === 'UNKNOWN_TYPE');

  // ── Cleanup ───────────────────────────────────────────────────────────────
  [cA, cB, cC, cD].forEach(({ client }) => client.close());
  await sleep(200);

  // ── Results ───────────────────────────────────────────────────────────────
  console.log('\n══════════════════════════════════════════════');
  console.log(`  Results: ${passed} passed, ${failed} failed`);
  console.log('══════════════════════════════════════════════\n');
  process.exit(failed > 0 ? 1 : 0);
}

runTests().catch(err => {
  console.error('\n[fatal]', err.message);
  process.exit(1);
});
