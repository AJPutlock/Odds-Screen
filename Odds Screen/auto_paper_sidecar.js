// Paper auto-betting sidecar (10/14): prices exchange quotes with the REAL
// static/index.html functions, so model-priced lines (bridged / model / alt
// strikes / FG shift / NHL goal model) need no Python twin. auto_paper.py
// spawns one of these lazily and talks JSON lines over stdin/stdout.
//
// Extraction: every top-level declaration in index.html's main <script> is
// indexed (function / const / let / var); the closure of ENTRY is pulled in
// by identifier, in source order, and evaluated in a vm context that has NO
// network or host access (no fetch, XMLHttpRequest, WebSocket, require,
// process, document, window). Side-effecting page helpers are replaced by
// STUBS. Date.now() / new Date() inside the context return the scan's `now`.
//
// Lifetime: exits when stdin closes (parent died or restarted), so a Flask
// reloader restart can't leave it orphaned.
//
// Requests:  {"id": n, "op": "hello"}
//            {"id": n, "op": "price", "sk": ..., "now": ms, "rows": [...],
//             "source_times": {...}, "books": ["novig", "kalshi", "polymarket"]}
// Responses: {"id": n, "ok": true, ...} or {"id": n, "ok": false, "error": "..."}
'use strict';
const fs = require('fs');
const vm = require('vm');
const readline = require('readline');

const INDEX = process.argv[2];
const ENTRY = ['softSideFair', 'altFairParts', 'evMinEdge', 'getSharpNoVig', 'sharpCurve', 'bookSides',
  'americanToImplied', 'passesOddsFloor', 'altTooLong', 'isTotalsType', 'isH2HType', 'isSourceStale'];
// Page helpers that report or send; never extracted, replaced by inert stubs.
const STUBS = {
  reportPageError: (msg, where) => { pageErrors.push(`${msg} @ ${where}`); },
};
const BANNED = /\b(fetch|XMLHttpRequest|WebSocket|require|import|document|window|localStorage|sessionStorage|navigator|process)\b\s*[.(]/;

const pageErrors = [];
const state = { now: Date.now() };

// ── declaration index ──────────────────────────────────────────────────────
function mainScript(html) {
  let best = '';
  for (const m of html.matchAll(/<script(?![^>]*\bsrc=)[^>]*>([\s\S]*?)<\/script>/g)) if (m[1].length > best.length) best = m[1];
  return best;
}

// End of a statement starting at i: the ';' (or newline after a closing
// bracket) at bracket depth 0, skipping strings, templates, comments.
function statementEnd(src, i) {
  let depth = 0;
  for (; i < src.length; i++) {
    const c = src[i], n = src[i + 1];
    if (c === '/' && n === '/') { i = src.indexOf('\n', i); if (i < 0) return src.length; if (depth === 0) return i; continue; }
    if (c === '/' && n === '*') { i = src.indexOf('*/', i + 2) + 1; continue; }
    if (c === '"' || c === "'" || c === '`') {
      for (i++; i < src.length && src[i] !== c; i++) if (src[i] === '\\') i++;
      continue;
    }
    if (c === '(' || c === '[' || c === '{') depth++;
    else if (c === ')' || c === ']' || c === '}') depth--;
    else if (c === ';' && depth === 0) return i + 1;
  }
  return src.length;
}

function indexDeclarations(src) {
  const decls = [];               // {names, start, end, kind}
  const re = /^(async\s+function|function|const|let|var)\s+([A-Za-z_$][\w$]*)/gm;
  let m;
  while ((m = re.exec(src))) {
    const start = m.index;
    let end, names = [m[2]];
    if (m[1].endsWith('function')) {
      const j = src.indexOf('\n}', start);
      end = j < 0 ? src.length : j + 2;
    } else {
      end = statementEnd(src, start);
      // `const A = 1, B = 2;`: more names at depth 0
      const text = src.slice(start, end);
      let depth = 0;
      for (let k = 0; k < text.length; k++) {
        const c = text[k];
        if ('([{'.includes(c)) depth++;
        else if (')]}'.includes(c)) depth--;
        else if (c === ',' && depth === 0) {
          const mm = /^,\s*([A-Za-z_$][\w$]*)\s*=/.exec(text.slice(k));
          if (mm) names.push(mm[1]);
        }
      }
    }
    decls.push({ names, start, end, kind: m[1] });
    re.lastIndex = end;
  }
  return decls;
}

function identifiers(code) {
  const out = new Set();
  const stripped = code.replace(/\/\/[^\n]*|\/\*[\s\S]*?\*\//g, ' ');
  for (const m of stripped.matchAll(/(^|[^.\w$])([A-Za-z_$][\w$]*)/g)) out.add(m[2]);
  return out;
}

function extract(html) {
  const src = mainScript(html.split('\r').join(''));
  const decls = indexDeclarations(src);
  const byName = new Map();
  decls.forEach(d => d.names.forEach(n => { if (!byName.has(n)) byName.set(n, d); }));
  const missing = ENTRY.filter(n => !byName.has(n));
  const take = new Set(), queue = ENTRY.filter(n => byName.has(n));
  while (queue.length) {
    const d = byName.get(queue.pop());
    if (!d || take.has(d)) continue;
    take.add(d);
    for (const id of identifiers(src.slice(d.start, d.end))) {
      if (STUBS[id]) continue;
      const dd = byName.get(id);
      if (dd && !take.has(dd)) queue.push(id);
    }
  }
  const ordered = [...take].sort((a, b) => a.start - b.start);
  const code = ordered.map(d => src.slice(d.start, d.end)).join('\n');
  const banned = BANNED.exec(code.replace(/\/\/[^\n]*|\/\*[\s\S]*?\*\//g, ' '));
  const names = ordered.flatMap(d => d.names);
  return { code, missing, banned: banned ? banned[0] : null, names,
           functions: ordered.filter(d => d.kind.endsWith('function')).length };
}

// ── the pricing loop (mirrors scanEV for the paper books) ───────────────────
const PRICE_ALL = `
// tabState may be the page's own top-level \`let\` (lexical, not a property of
// the sandbox object), so it is set and cleared from inside the context.
function __priceAll(sk, books, nowMs, rows, sourceTimes) {
  tabState[sk] = { loaded: true, data: rows, sourceTimes: sourceTimes || {} };
  const out = [];
  try {
    for (const row of rows) {
      try { __priceRow(sk, books, nowMs, row, out); }
      catch (e) { out.push({ game_id: row.game_id, market: row.market, error: String(e && e.message || e) }); }
    }
  } finally {
    delete tabState[sk];
  }
  return out;
}
function __priceRow(sk, books, nowMs, row, out) {
  const mkt = row.market, isTotals = isTotalsType(mkt);
  const hrs = (new Date(row.commence_time).getTime() - nowMs) / 3.6e6;
  if (!(hrs > 0)) return;                                   // started: never
  const sv = getSharpNoVig(row, sk);
  if (!sv) return;
  const fair = { upper: sv.nv.pUpper, lower: sv.nv.pLower };
  const anchorPt = { upper: sv.bolUpt, lower: sv.bolLpt };
  const curve = sharpCurve(row, sk);
  const quotes = [], seen = new Set();
  const add = (bk, side, e, bs, isAlt) => {
    const u = side === 'upper';
    const odds = u ? bs.u : bs.l, pt = u ? bs.uPt : bs.lPt, k = bk + '|' + side + '|' + pt;
    if (!odds || seen.has(k)) return;
    seen.add(k);
    quotes.push({ bk, side, odds, pt, isAlt, prob: u ? bs.uProb : bs.lProb, ref: u ? bs.uRef : bs.lRef,
                  other_ref: u ? bs.lRef : bs.uRef, usd: u ? bs.uUsd : bs.lUsd, coef: bs.coef });
  };
  for (const bk of books) {
    if (isSourceStale(sk, bk, mkt, row)) continue;
    const e = row.books && row.books[bk];
    if (e) { const bs = bookSides(e, isTotals); add(bk, 'upper', e, bs, false); add(bk, 'lower', e, bs, false); }
  }
  for (const bk of books) {
    if (isSourceStale(sk, bk, mkt, row)) continue;
    ((row.exchange_ladders || {})[bk] || []).forEach(e => {
      const bs = bookSides(e, isTotals); add(bk, 'upper', e, bs, true); add(bk, 'lower', e, bs, true);
    });
  }
  for (const q of quotes) {
    if (!passesOddsFloor(q.odds)) continue;
    const impl = americanToImplied(q.odds);
    if (impl == null) continue;
    const s = softSideFair(row, sk, mkt, q.side, q.pt, anchorPt[q.side], fair[q.side], q.bk);
    const parts = q.isAlt ? altFairParts(row, sk, mkt, q.side, q.pt, anchorPt[q.side], s) : null;
    const trusted = q.isAlt ? parts.trusted : s.fair;
    const min = evMinEdge(mkt, s.backing, q.isAlt, sk, hrs, q.side);
    const fg = curve && curve.fgAdj ? (q.side === 'upper' ? curve.fgAdj.pp : -curve.fgAdj.pp) : null;
    out.push({ game_id: row.game_id, market: mkt, bk: q.bk, side: q.side, pt: q.pt ?? null, odds: String(q.odds),
               prob: q.prob ?? null, ref: q.ref ?? null, other_ref: q.other_ref ?? null, usd: q.usd ?? null,
               is_alt: q.isAlt, hrs, impl, fair: trusted ?? null, sharp_fair: s.fair ?? null,
               fair_est: s.fairEst ?? null, backing: s.backing ?? null, exact: !!s.exact,
               from_pt: s.fromPt ?? null, floored_at: s.flooredAt ?? null, keys: s.keys || [],
               model_fair: parts ? parts.model ?? null : null, why: parts ? parts.why ?? null : null,
               anchor_pt: anchorPt[q.side] ?? null, fg_adj: fg,
               min_edge: Number.isFinite(min) ? min : null, blocked: !Number.isFinite(min),
               too_long: q.isAlt && altTooLong(q.odds) });
  }
}
`;

// ── context ────────────────────────────────────────────────────────────────
const RealDate = Date;
class ScanDate extends RealDate {
  constructor(...a) { if (a.length) super(...a); else super(state.now); }
  static now() { return state.now; }
}

let ctx = null, info = null;
function load() {
  const html = fs.readFileSync(INDEX, 'utf8');
  const mtime = fs.statSync(INDEX).mtimeMs;
  const ex = extract(html);
  info = { ready: false, missing: ex.missing, banned: ex.banned, functions: ex.functions,
           declarations: ex.names.length, index_mtime: mtime, error: null };
  if (ex.missing.length || ex.banned) return info;
  const sandbox = { tabState: {}, Date: ScanDate, console: { log() {}, warn() {}, error() {} }, ...STUBS };
  ctx = vm.createContext(sandbox, { codeGeneration: { strings: false, wasm: false } });
  try {
    vm.runInContext(ex.code + '\n' + PRICE_ALL, ctx, { filename: 'index.html#extracted', timeout: 10000 });
    const probe = vm.runInContext('typeof softSideFair + typeof altFairParts + typeof evMinEdge + typeof __priceAll', ctx);
    if (probe !== 'functionfunctionfunctionfunction') throw new Error('entry functions not defined: ' + probe);
    info.ready = true;
  } catch (e) {
    info.error = String(e && e.stack || e).slice(0, 600);
  }
  return info;
}

function handle(req) {
  if (req.op === 'hello') return { ok: true, ...info };
  if (req.op !== 'price') return { ok: false, error: 'unknown op' };
  if (!info.ready) return { ok: false, error: 'not ready', ...info };
  const t0 = RealDate.now();
  state.now = req.now;
  pageErrors.length = 0;
  Object.assign(ctx, { __sk: req.sk, __books: req.books, __now: req.now, __rows: req.rows, __st: req.source_times });
  try {
    const quotes = vm.runInContext('__priceAll(__sk, __books, __now, __rows, __st)', ctx, { timeout: 10000 });
    return { ok: true, quotes, page_errors: pageErrors.slice(0, 10), ms: RealDate.now() - t0 };
  } finally {
    ctx.__rows = null;
  }
}

load();
const rl = readline.createInterface({ input: process.stdin });
rl.on('line', line => {
  let req = null, res;
  try { req = JSON.parse(line); res = handle(req); }
  catch (e) { res = { ok: false, error: String(e && e.message || e).slice(0, 400) }; }
  process.stdout.write(JSON.stringify({ id: req && req.id, ...res }) + '\n');
});
rl.on('close', () => process.exit(0));          // parent gone: never linger
process.stdin.on('end', () => process.exit(0));
