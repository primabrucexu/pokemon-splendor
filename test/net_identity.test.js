/* Transport identity is stable per tab, but distinct across tabs in one browser. */
const assert = require('assert');
const vm = require('vm');
const fs = require('fs');
function storage(seed) {
  const values = new Map(Object.entries(seed || {}));
  return {
    getItem(k) { return values.has(k) ? values.get(k) : null; },
    setItem(k, v) { values.set(k, String(v)); },
    removeItem(k) { values.delete(k); },
  };
}
function load(session, local) {
  const sent = [], sockets = [];
  class Socket {
    constructor() { this.readyState = 1; sockets.push(this); }
    send(msg) { sent.push(JSON.parse(msg)); }
    close() {}
  }
  const ctx = { window: {}, WebSocket: Socket, crypto: require('crypto').webcrypto,
    location: { protocol: 'https:', host: 'example.test' }, sessionStorage: session, localStorage: local,
    setInterval: () => 1, clearInterval() {}, setTimeout, clearTimeout, console };
  vm.runInNewContext(fs.readFileSync(require.resolve('../js/net.js'), 'utf8'), ctx);
  const join = (code) => {
    ctx.window.Net.connect(code, 'A'); sockets.at(-1).onopen();
    return sent.at(-1).token;
  };
  return { ctx, join };
}

const sharedLocal = storage();
const tabAStorage = storage(), tabBStorage = storage();
const tabA = load(tabAStorage, sharedLocal), tabB = load(tabBStorage, sharedLocal);
const first = tabA.join('ABC');
tabA.ctx.window.Net.close();
assert.strictEqual(tabA.join('ABC'), first, 'same tab reconnect keeps its seat');
assert.notStrictEqual(tabB.join('ABC'), first, 'another tab gets another player identity');
assert.notStrictEqual(tabA.join('DEF'), first, 'another room gets another identity');

const reloadedA = load(tabAStorage, sharedLocal);
assert.strictEqual(reloadedA.join('ABC'), first, 'refresh in the same tab reclaims its seat');
assert.match(first, /^tok-[0-9a-f]{48}$/);

const legacy = 'tok-' + 'a'.repeat(48), legacyLocal = storage({ pkmn_net_token_LEG: legacy });
const migrated = load(storage(), legacyLocal);
assert.strictEqual(migrated.join('LEG'), legacy, 'legacy identity migrates without losing its seat');
assert.notStrictEqual(load(storage(), legacyLocal).join('LEG'), legacy, 'legacy token is consumed only once');
console.log('PASS per-tab identity, reload recovery, separate rooms, and legacy migration');
