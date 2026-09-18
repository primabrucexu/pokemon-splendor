'use strict';

// One bridge process owns one active room. Python owns networking, persistence,
// and scheduling; this process only executes the existing, tested JS authority.
const readline = require('readline');
const { Room } = require('../js/room.js');
const AI = require('../js/ai.js');
const DB = require('../data/cards.json');
const MEGA_DB = require('../data/megas.json');
const POKEMART_DB = require('../data/pokemart.json');

let authority = null;
let outbound = [];

function buildAuthority() {
  outbound = [];
  authority = new Room({
    cardDB: DB,
    megaDB: MEGA_DB,
    pokemartDB: POKEMART_DB,
    ai: AI,
    send: (connId, msg) => outbound.push({ connId, msg }),
  });
}

function roomStatus() {
  return {
    aiPending: authority.aiPending(),
    humansConnected: authority.humansConnected(),
    turnStartedAt: authority.turnStartedAt,
    seq: authority.seq,
  };
}

function handle(req) {
  if (!req || typeof req.op !== 'string') throw new Error('invalid bridge request');
  if (req.op === 'init') {
    buildAuthority();
    if (req.snapshot) authority.restore(req.snapshot);
  } else {
    if (!authority) throw new Error('bridge is not initialized');
    outbound = [];
    authority.now = Number.isFinite(req.now) ? req.now : Date.now();
    if (req.op === 'message') authority.onMessage(req.connId, req.message);
    else if (req.op === 'leave') authority.leave(req.connId);
    else if (req.op === 'stepAI') authority.stepAI(req.attempt || 0);
    else if (req.op !== 'inspect') throw new Error('unknown bridge operation');
  }
  return {
    outbound,
    snapshot: req.persist ? authority.snapshot() : undefined,
    status: roomStatus(),
  };
}

buildAuthority();
const input = readline.createInterface({ input: process.stdin, crlfDelay: Infinity });
input.on('line', (line) => {
  let req;
  try {
    req = JSON.parse(line);
    const result = handle(req);
    process.stdout.write(JSON.stringify({ id: req.id, ok: true, ...result }) + '\n');
  } catch (err) {
    process.stdout.write(JSON.stringify({
      id: req && req.id,
      ok: false,
      error: err && err.message ? err.message : String(err),
    }) + '\n');
  }
});
