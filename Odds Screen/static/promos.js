// ═══════════════════════════════════════════════════════
// PROMO / BOOST CALCULATOR
// ═══════════════════════════════════════════════════════
// Self-contained: injects its own BOOST button (tab bar, left of NO-VIG),
// panel and styles, so index.html only carries the <script> tag.
//
// Shift+click any price in a sport grid, an alt row, or a +EV row to load it.
// The fair win probability comes from the same sharp-fair engine the +EV tab
// uses (getSharpNoVig + softSideFair / altTrustedFair, read from index.html's
// globals), so a boosted EV here is measured exactly like any other edge.
//
// Promo math, with p = fair win probability and d = the book's decimal odds:
//   profit boost X   d' = 1 + (d-1)(1+X)          EV/$ = p·d' - 1
//   boosted odds     d' = the boosted price        EV/$ = p·d' - 1
//   no-sweat         refund = bonus bet worth r/$  EV/$ = p·d - 1 + (1-p)·r
//   bonus bet        stake is not returned         EV/$ of credit = p·(d-1)
(() => {
'use strict';

// ── Math (pure — also used by the promo finder) ──────────────────────────
const PromoMath = {
  amToDec(am) {
    const n = Number(String(am).replace('+', ''));
    if (!isFinite(n) || (n > -100 && n < 100)) return null;
    return n > 0 ? 1 + n / 100 : 1 + 100 / -n;
  },
  decToAm(d) {
    if (!(d > 1)) return null;
    return d >= 2 ? Math.round((d - 1) * 100) : Math.round(-100 / (d - 1));
  },
  probToAm(p) {
    if (!(p > 0 && p < 1)) return null;
    return p > 0.5 ? Math.round(-100 * p / (1 - p)) : Math.round(100 / p - 100);
  },
  boostedDec(d, boost) { return 1 + (d - 1) * (1 + boost); },

  // EV per $1 staked (per $1 of credit for a bonus bet). opt: {boost, boostedDec, refundRate}
  ev(type, p, d, opt = {}) {
    if (!(p > 0 && p < 1) || !(d > 1)) return null;
    switch (type) {
      case 'profit':  return p * PromoMath.boostedDec(d, opt.boost || 0) - 1;
      case 'odds':    return opt.boostedDec > 1 ? p * opt.boostedDec - 1 : null;
      case 'nosweat': return p * d - 1 + (1 - p) * (opt.refundRate ?? 0.7);
      case 'bonus':   return p * (d - 1);
      default:        return p * d - 1;
    }
  },

  // Smallest profit boost that makes this price break even (<= 0: already +EV).
  breakevenBoost(p, d) {
    if (!(p > 0 && p < 1) || !(d > 1)) return null;
    return (1 / p - 1) / (d - 1) - 1;
  },

  // Full-Kelly fraction at decimal odds dd (null when not +EV).
  kelly(p, dd) {
    const b = dd - 1;
    if (!(b > 0)) return null;
    const f = p - (1 - p) / b;
    return f > 0 ? f : null;
  },
};
window.PromoMath = PromoMath;

// ── State (per-viewer settings kept in localStorage) ─────────────────────
const LS_KEY = 'promoCalc.v1';
const TYPES = [
  { key: 'profit',  label: 'PROFIT BOOST' },
  { key: 'odds',    label: 'BOOSTED ODDS' },
  { key: 'nosweat', label: 'NO-SWEAT' },
  { key: 'bonus',   label: 'BONUS BET' },
];
const BOOST_MAX_ODDS = 400;   // best uses: book odds no longer than this unless a boost sets its own max
const state = { type: 'profit', boostPct: 30, boostedOdds: '', refundPct: 70, maxStake: 25 };
try { Object.assign(state, JSON.parse(localStorage.getItem(LS_KEY) || '{}')); } catch (e) { /* storage blocked */ }
const save = () => {
  try {
    const { type, boostPct, refundPct, maxStake } = state;
    localStorage.setItem(LS_KEY, JSON.stringify({ type, boostPct, refundPct, maxStake }));
  } catch (e) { /* storage blocked */ }
};
let bet = null;   // { label, bookOdds, fairProb, fairSrc, warn }

// ── Styles ───────────────────────────────────────────────────────────────
const css = `
.pr-tab-btn {
  flex-shrink:0; display:flex; align-items:center; gap:5px; padding:0 14px;
  font-family:var(--mono); font-size:10px; font-weight:700; letter-spacing:.08em;
  text-transform:uppercase; color:var(--t3); white-space:nowrap;
  border:none; border-left:1px solid var(--border2); background:none; cursor:pointer;
  transition:color .15s, background .15s;
}
.pr-tab-btn:hover { color:var(--t2); background:rgba(255,255,255,.02); }
.pr-tab-btn.open  { color:var(--amber); background:var(--amber-dim); }
#boost-add-btn { color:var(--amber); }
.pr-tab-btn .cnt { margin-left:2px; padding:0 5px; border-radius:8px; background:var(--amber-dim); color:var(--amber); font-size:9px; }
#promo-panel {
  display:none; position:fixed; right:12px; top:calc(var(--hh) + var(--th) + 6px);
  background:var(--surface2); border:1px solid var(--border2); border-radius:10px;
  padding:14px 16px; z-index:491; box-shadow:0 10px 32px rgba(0,0,0,.5);
  width:340px; max-width:calc(100vw - 24px);
}
#promo-panel.open { display:block; }
.pr-title { display:flex; justify-content:space-between; align-items:center;
  font-family:var(--mono); font-size:9px; font-weight:700; letter-spacing:.12em;
  text-transform:uppercase; color:var(--t4); margin-bottom:10px; }
.pr-x { background:none; border:none; color:var(--t4); cursor:pointer; font-size:12px; }
.pr-x:hover { color:var(--t2); }
.pr-bet { font-family:var(--mono); font-size:11px; color:var(--t1); line-height:1.4;
  background:var(--surface); border:1px solid var(--border2); border-radius:5px;
  padding:6px 8px; margin-bottom:10px; min-height:30px; }
.pr-bet.empty { color:var(--t3); font-size:10px; }
.pr-src { font-size:9px; color:var(--t3); margin-top:2px; }
.pr-warn { font-size:9px; color:var(--amber); margin-top:2px; }
.pr-row { display:grid; grid-template-columns:1fr 1fr; gap:8px; margin-bottom:8px; }
.pr-field label { display:block; font-family:var(--mono); font-size:8px; font-weight:700;
  letter-spacing:.1em; text-transform:uppercase; color:var(--t4); margin-bottom:3px; }
.pr-field input { width:100%; background:var(--surface); border:1px solid var(--border2);
  border-radius:5px; padding:5px 8px; color:var(--t1); font-family:var(--mono);
  font-size:13px; font-weight:600; text-align:center; }
.pr-field input:focus { outline:none; border-color:var(--amber); }
.pr-field .pr-sub { font-family:var(--mono); font-size:9px; color:var(--t3); text-align:center; margin-top:2px; }
.pr-types { display:grid; grid-template-columns:repeat(4,1fr); gap:3px; margin:4px 0 8px; }
.pr-type { font-family:var(--mono); font-size:8px; font-weight:700; letter-spacing:.05em;
  padding:6px 2px; border-radius:4px; border:1px solid var(--border2);
  background:var(--surface); color:var(--t3); cursor:pointer; }
.pr-type.active { color:var(--amber); border-color:var(--amber); background:var(--amber-dim); }
.pr-out { border-top:1px solid var(--border2); margin-top:4px; padding-top:8px;
  display:grid; grid-template-columns:auto 1fr; gap:4px 10px; font-family:var(--mono); font-size:11px; }
.pr-out .k { color:var(--t3); font-size:9px; letter-spacing:.06em; text-transform:uppercase; align-self:center; }
.pr-out .v { color:var(--t1); text-align:right; font-weight:600; }
.pr-out .v.pos { color:var(--green); } .pr-out .v.neg { color:#f87171; }
.pr-out .v.big { font-size:15px; }
.pr-note { font-family:var(--mono); font-size:8px; color:var(--t4); margin-top:8px; line-height:1.5; }
.pr-row-btn { display:block; margin:4px 0 0 auto; font-family:var(--mono); font-size:8px; font-weight:700;
  letter-spacing:.06em; padding:2px 6px; border-radius:3px; border:1px solid var(--border2);
  background:var(--surface); color:var(--t3); cursor:pointer; }
.pr-row-btn:hover { color:var(--amber); border-color:var(--amber); background:var(--amber-dim); }
#promo-panel { width:390px; max-height:calc(100vh - var(--hh) - var(--th) - 20px); overflow-y:auto; }
.pr-log { display:none; width:100%; margin-top:10px; padding:7px; font-family:var(--mono); font-size:10px; font-weight:700;
  letter-spacing:.08em; border-radius:5px; border:1px solid var(--green); background:var(--green-dim); color:var(--green); cursor:pointer; }
.pr-log.show { display:block; }
.pr-plusev { color:var(--green); font-weight:700; }
.pr-mini, .pr-use button { font-family:var(--mono); font-size:8px; font-weight:700; padding:2px 6px; border-radius:3px;
  border:1px solid var(--border2); background:var(--surface); color:var(--t2); cursor:pointer; }
.pr-mini:hover, .pr-use button:hover { color:var(--amber); border-color:var(--amber); }
.pr-promo { background:var(--surface); border:1px solid var(--border2); border-radius:6px; padding:6px 8px; margin-bottom:6px;
  font-family:var(--mono); font-size:10px; color:var(--t2); }
.pr-promo-h { display:flex; justify-content:space-between; gap:6px; align-items:center; }
.pr-promo-h b { color:var(--t1); }
.pr-crit { font-size:9px; color:var(--t3); margin:2px 0 4px; }
.pr-use { display:flex; justify-content:space-between; gap:6px; align-items:center; padding:4px 0; border-top:1px dashed var(--border); font-size:10px; line-height:1.35; }
.pr-use .pos { color:var(--green); font-weight:700; } .pr-use .neg { color:#f87171; }
.pr-none { font-size:9px; color:var(--t4); padding:3px 0; }
#boosts-panel {
  display:none; position:fixed; right:12px; top:calc(var(--hh) + var(--th) + 6px);
  background:var(--surface2); border:1px solid var(--border2); border-radius:10px;
  padding:14px 16px; z-index:491; box-shadow:0 10px 32px rgba(0,0,0,.5);
  width:460px; max-width:calc(100vw - 24px); max-height:calc(100vh - var(--hh) - var(--th) - 20px); overflow-y:auto;
}
#boosts-panel.open { display:block; }
.pr-sub-h { font-family:var(--mono); font-size:8px; font-weight:700; letter-spacing:.1em; text-transform:uppercase; color:var(--t4); margin:8px 0 2px; }
.pr-legs { margin:3px 0 3px 16px; padding:0; color:var(--t2); }
.pr-legs li { margin:1px 0; }
.pr-copy-btn { font-family:var(--mono); font-size:8px; font-weight:700; padding:2px 6px; border-radius:3px;
  border:1px solid var(--border2); background:var(--surface); color:var(--t2); cursor:pointer; align-self:flex-start; }
.pr-copy-btn:hover { color:var(--amber); border-color:var(--amber); }
.pr-btns { display:flex; flex-direction:column; gap:4px; align-self:flex-start; }
#parlay-log { display:none; position:fixed; inset:0; background:rgba(0,0,0,.55); z-index:600; align-items:center; justify-content:center; }
#parlay-log.open { display:flex; }
#parlay-log .pl-box { background:var(--surface2); border:1px solid var(--border2); border-radius:10px; padding:16px 18px; width:min(420px, calc(100vw - 32px));
  box-shadow:0 10px 32px rgba(0,0,0,.5); font-family:var(--mono); font-size:11px; color:var(--t1); }
#parlay-log .pl-h { font-family:var(--display); font-size:15px; font-weight:700; letter-spacing:.04em; margin-bottom:8px; }
#parlay-log ol { margin:0 0 10px 18px; padding:0; line-height:1.6; }
#parlay-log .pl-row { display:grid; grid-template-columns:1fr 1fr; gap:8px; margin-bottom:8px; }
#parlay-log label { display:block; font-size:9px; font-weight:700; letter-spacing:.1em; text-transform:uppercase; color:var(--t3); margin-bottom:3px; }
#parlay-log input { width:100%; box-sizing:border-box; background:var(--surface); border:1px solid var(--border2); border-radius:4px; color:var(--t1);
  font-family:var(--mono); font-size:12px; padding:5px 7px; }
#parlay-log .pl-sum { color:var(--t2); margin:4px 0 12px; }
#parlay-log .pl-act { display:flex; justify-content:flex-end; gap:8px; }
#parlay-log .pl-act button { font-family:var(--mono); font-size:10px; font-weight:700; letter-spacing:.06em; padding:6px 12px; border-radius:4px;
  border:1px solid var(--border2); background:var(--surface3); color:var(--t2); cursor:pointer; }
#parlay-log .pl-act .pl-ok { background:var(--amber); border-color:var(--amber); color:#000; }
#bw-overlay { display:none; position:fixed; inset:0; background:rgba(0,0,0,.55); z-index:600; align-items:center; justify-content:center; }
#bw-overlay.open { display:flex; }
#bw-card { background:var(--surface2); border:1px solid var(--border2); border-radius:12px; width:420px; max-width:calc(100vw - 32px);
  padding:16px 20px 18px; box-shadow:0 16px 48px rgba(0,0,0,.6); font-family:var(--mono); }
.bw-top { display:flex; justify-content:space-between; align-items:center; font-size:9px; font-weight:700; letter-spacing:.12em; text-transform:uppercase; color:var(--t4); }
.bw-dots { display:flex; gap:4px; margin:8px 0 16px; }
.bw-dot { flex:1; height:3px; border-radius:2px; background:var(--border2); }
.bw-dot.done { background:var(--amber); }
#bw-q { font-size:15px; font-weight:700; color:var(--t1); margin-bottom:12px; }
#bw-body select, #bw-body input { width:100%; box-sizing:border-box; background:var(--surface); border:1px solid var(--border2); border-radius:6px;
  color:var(--t1); font-family:var(--mono); font-size:13px; padding:8px 10px; color-scheme:dark; }
#bw-body input:focus, #bw-body select:focus { outline:none; border-color:var(--amber); }
.bw-opts { display:grid; gap:6px; }
.bw-opts.two { grid-template-columns:1fr 1fr; }
.bw-opt { text-align:left; padding:10px 12px; border-radius:7px; border:1px solid var(--border2); background:var(--surface);
  color:var(--t1); font-family:var(--mono); font-size:12px; font-weight:700; cursor:pointer; }
.bw-opt small { display:block; font-weight:400; font-size:9px; color:var(--t3); margin-top:3px; }
.bw-opt:hover { border-color:var(--t3); }
.bw-opt.sel { border-color:var(--amber); background:var(--amber-dim); color:var(--amber); }
.bw-field { margin-bottom:10px; }
.bw-field label { display:block; font-size:9px; font-weight:700; letter-spacing:.1em; text-transform:uppercase; color:var(--t3); margin-bottom:4px; }
.bw-hint { font-size:9px; color:var(--t4); margin-top:4px; line-height:1.45; }
.bw-load { display:flex; flex-wrap:wrap; gap:4px; margin-top:4px; }
.bw-err { color:#f87171; font-size:10px; min-height:14px; margin-top:8px; }
.bw-nav { display:flex; justify-content:space-between; align-items:center; margin-top:8px; }
.bw-next { font-family:var(--mono); font-size:11px; font-weight:700; letter-spacing:.08em; padding:7px 18px; border-radius:6px;
  border:1px solid var(--amber); background:var(--amber-dim); color:var(--amber); cursor:pointer; }
.bw-next:disabled { opacity:.5; cursor:default; }
.bw-review { display:grid; grid-template-columns:auto 1fr; gap:6px 12px; font-size:11px; }
.bw-review .k { color:var(--t3); font-size:9px; text-transform:uppercase; letter-spacing:.08em; align-self:center; }
.bw-review .v { color:var(--t1); text-align:right; font-weight:600; }
`;
document.head.insertAdjacentHTML('beforeend', `<style>${css}</style>`);

// ── DOM ──────────────────────────────────────────────────────────────────
// Tab bar: + ADD BOOST (the wizard) and BOOSTS (saved boosts and their best
// uses). The calculator opens from shift+click, the ⚡ row buttons, or BOOSTS.
const tabBtn = (id, title, html) => {
  const b = document.createElement('button');
  b.id = id; b.className = 'pr-tab-btn'; b.title = title; b.innerHTML = html;
  return b;
};
const addBtn = tabBtn('boost-add-btn', 'Add a sportsbook boost, bonus bet or no-sweat bet', '+ ADD BOOST');
const viewBtn = tabBtn('boost-view-btn', 'Saved boosts and the best ways to use them (straight bets and parlays)',
  `<svg width="13" height="13" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.2" stroke-linecap="round" stroke-linejoin="round"><polyline points="23 6 13.5 15.5 8.5 10.5 1 18"/><polyline points="17 6 23 6 23 12"/></svg>BOOSTS<span class="cnt" id="boost-count" style="display:none"></span>`);
const nvBtn = document.getElementById('nv-conv-btn');
const tabRow = document.getElementById('tab-bar-row');
if (nvBtn) { nvBtn.before(addBtn); nvBtn.before(viewBtn); }
else if (tabRow) { tabRow.appendChild(addBtn); tabRow.appendChild(viewBtn); }

const panel = document.createElement('div');
panel.id = 'promo-panel';
panel.innerHTML = `
  <div class="pr-title">Promo / Boost Calculator <span><button class="pr-mini" id="pr-clear" title="Clear the loaded bet, odds, boost and stake">CLEAR</button> <button class="pr-x" title="Close">✕</button></span></div>
  <div class="pr-bet empty" id="pr-bet">Shift+click any price in the table (grid, alt lines or +EV tab) to load it — or type the odds below.</div>
  <div class="pr-row">
    <div class="pr-field"><label>Book odds</label><input id="pr-book" type="text" placeholder="+120"><div class="pr-sub" id="pr-book-sub">&nbsp;</div></div>
    <div class="pr-field"><label>Fair odds</label><input id="pr-fair" type="text" placeholder="+105"><div class="pr-sub" id="pr-fair-sub">&nbsp;</div></div>
  </div>
  <div class="pr-types" id="pr-types">${TYPES.map(t => `<button class="pr-type" data-type="${t.key}">${t.label}</button>`).join('')}</div>
  <div class="pr-row">
    <div class="pr-field" id="pr-param-wrap"><label id="pr-param-lbl">Boost %</label><input id="pr-param" type="text"></div>
    <div class="pr-field"><label id="pr-stake-lbl">Max stake $</label><input id="pr-stake" type="text"></div>
  </div>
  <div class="pr-out" id="pr-out"></div>
  <div class="pr-note" id="pr-note"></div>
  <button class="pr-log" id="pr-log" title="Opens the bet form with the boosted price and stake filled in">LOG THIS BET AT THE BOOSTED PRICE</button>
</div>`;
document.body.appendChild(panel);

const $ = id => document.getElementById(id);
let logBoost = null;     // { effDec, useStake, p, bookAm } when the loaded bet can be logged boosted
const inBook = $('pr-book'), inFair = $('pr-fair'), inParam = $('pr-param'), inStake = $('pr-stake');

// ── Formatting ───────────────────────────────────────────────────────────
const fmtAm = n => n == null || isNaN(n) ? '—' : (n > 0 ? '+' + n : String(n));
const fmtPct = (x, dp = 1) => x == null || isNaN(x) ? '—' : (x > 0 ? '+' : '') + (x * 100).toFixed(dp) + '%';
const fmtUsd = x => x == null || isNaN(x) ? '—' : (x < 0 ? '−$' : '$') + Math.abs(x).toFixed(2);
const cls = x => x == null ? '' : x > 1e-9 ? 'pos' : x < -1e-9 ? 'neg' : '';
const num = s => { const n = parseFloat(String(s).replace(/[$%+,\s]/g, '')); return isFinite(n) ? n : null; };
const escH = s => String(s ?? '').replace(/[&<>"']/g, c => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]));

// ── Render ───────────────────────────────────────────────────────────────
function syncParamField() {
  const lbl = $('pr-param-lbl'), wrap = $('pr-param-wrap');
  wrap.style.visibility = state.type === 'bonus' ? 'hidden' : 'visible';
  if (state.type === 'profit')  { lbl.textContent = 'Boost %';            inParam.value = state.boostPct;    inParam.placeholder = '30'; }
  if (state.type === 'odds')    { lbl.textContent = 'Boosted odds';       inParam.value = state.boostedOdds; inParam.placeholder = '+150'; }
  if (state.type === 'nosweat') { lbl.textContent = 'Refund converts %';  inParam.value = state.refundPct;   inParam.placeholder = '70'; }
  $('pr-stake-lbl').textContent = state.type === 'bonus' ? 'Bonus bet $' : state.type === 'nosweat' ? 'Max refund $' : 'Max stake $';
  inStake.value = state.maxStake;
  document.querySelectorAll('#pr-types .pr-type').forEach(b => b.classList.toggle('active', b.dataset.type === state.type));
}

function compute() {
  const bookAm = num(inBook.value), fairAm = num(inFair.value);
  const d = bookAm == null ? null : PromoMath.amToDec(bookAm);
  // A loaded bet keeps its exact fair probability until the fair box is edited
  // (the box shows it rounded to whole American odds).
  const p = fairAm == null ? null
    : bet && bet.fairProb != null && fairAm === PromoMath.probToAm(bet.fairProb) ? bet.fairProb
    : (() => { const fd = PromoMath.amToDec(fairAm); return fd ? 1 / fd : null; })();
  $('pr-book-sub').textContent = d ? `${(100 / d).toFixed(1)}% implied` : ' ';
  $('pr-fair-sub').textContent = p ? `${(p * 100).toFixed(1)}% to win` : ' ';

  const out = $('pr-out'), note = $('pr-note');
  if (!d || !p) {
    out.innerHTML = `<div class="k">EV</div><div class="v">—</div>`;
    note.textContent = 'Needs both the book price and a fair price.';
    return;
  }

  const stake = num(inStake.value);
  const base = PromoMath.ev('none', p, d);
  logBoost = null;
  const rows = [];
  let evX = null, effDec = null, dollars = null, extra = '';

  if (state.type === 'profit' || state.type === 'odds') {
    effDec = state.type === 'profit'
      ? PromoMath.boostedDec(d, (num(state.boostPct) ?? 0) / 100)
      : PromoMath.amToDec(num(state.boostedOdds));
    evX = effDec ? p * effDec - 1 : null;
    const impliedBoost = effDec ? (effDec - 1) / (d - 1) - 1 : null;
    rows.push(['Boosted price', `${fmtAm(PromoMath.decToAm(effDec))}${state.type === 'odds' && impliedBoost != null ? ` <span style="color:var(--t3);font-size:9px">(${(impliedBoost * 100).toFixed(0)}% boost)</span>` : ''}`, '']);
    rows.push(['EV per $', fmtPct(evX), cls(evX) + ' big']);
    rows.push(['Without boost', fmtPct(base), cls(base)]);
    // Stake: Kelly at the boosted price, capped at the promo's max.
    const f = evX > 0 ? PromoMath.kelly(p, effDec) : null;
    const bank = typeof kellyBankroll !== 'undefined' ? kellyBankroll : null;
    const frac = typeof kellyFraction !== 'undefined' ? kellyFraction : 0.5;
    let useStake = stake;
    if (f != null && bank > 0) {
      const k = bank * Math.min(f, 1) * frac;
      useStake = stake != null ? Math.min(stake, k) : k;
      extra = k > (stake ?? Infinity) ? `Kelly says $${Math.round(k)} — the $${stake} max binds.` : `Kelly-sized below the $${stake} max.`;
    }
    if (effDec && bet && bet.ctx) logBoost = { effDec, useStake, p, bookAm };
    if (useStake != null && evX != null) {
      dollars = evX * useStake;
      rows.push(['Stake', `$${useStake.toFixed(useStake >= 20 ? 0 : 2)}`, '']);
      rows.push(['Expected profit', fmtUsd(dollars), cls(dollars) + ' big']);
    }
    const be = PromoMath.breakevenBoost(p, d);
    rows.push(['Breakeven boost', be == null ? '—' : be < -5e-4 ? 'already +EV' : be < 5e-4 ? '0% (priced at fair)' : `${(be * 100).toFixed(1)}%`, '']);
    note.innerHTML = 'A profit boost multiplies the winnings, so it is worth most on longer odds that are already close to fair. Check the promo’s eligible markets, odds limits and max stake.';
  } else if (state.type === 'nosweat') {
    const r = (num(state.refundPct) ?? 70) / 100;
    evX = PromoMath.ev('nosweat', p, d, { refundRate: r });
    rows.push(['EV per $', fmtPct(evX), cls(evX) + ' big']);
    rows.push(['Without promo', fmtPct(base), cls(base)]);
    if (stake != null) {
      dollars = evX * stake;
      rows.push(['Stake', `$${stake.toFixed(0)}`, '']);
      rows.push(['Refund value', fmtUsd((1 - p) * r * stake), '']);
      rows.push(['Expected profit', fmtUsd(dollars), cls(dollars) + ' big']);
    }
    note.innerHTML = `The refund is only paid on a loss, so longer odds make it worth more (${(100 - p * 100).toFixed(0)}% chance to lose here). A bonus bet refund is typically worth 65–80% of face value when you use it on a longshot.`;
  } else if (state.type === 'bonus') {
    evX = PromoMath.ev('bonus', p, d);
    rows.push(['Converts at', fmtPct(evX).replace('+', ''), cls(evX) + ' big']);
    if (stake != null) {
      dollars = evX * stake;
      rows.push(['Bonus bet', `$${stake.toFixed(0)}`, '']);
      rows.push(['Expected cash', fmtUsd(dollars), cls(dollars) + ' big']);
    }
    rows.push(['Max possible', `${((1 - 1 / d) * 100).toFixed(1)}% <span style="color:var(--t3);font-size:9px">at fair</span>`, '']);
    note.innerHTML = 'The stake is not returned, so only the winnings count. Longer odds that are close to fair convert best — a fair +400 converts at 80%.';
  }
  out.innerHTML = rows.map(([k, v, c]) => `<div class="k">${k}</div><div class="v ${c}">${v}</div>`).join('');
  if (extra) note.innerHTML = escH(extra) + ' ' + note.innerHTML;
  $('pr-log').classList.toggle('show', !!logBoost);
}

function renderBet() {
  const el = $('pr-bet');
  if (!bet) return;
  el.classList.remove('empty');
  el.innerHTML = `${escH(bet.label)}${bet.fairSrc ? `<div class="pr-src">Fair: ${escH(bet.fairSrc)}</div>` : ''}${bet.warn ? `<div class="pr-warn">${escH(bet.warn)}</div>` : ''}`;
}

// ── Open / close ─────────────────────────────────────────────────────────
function openPanel() {
  closeBoosts();
  panel.classList.add('open');
  if (typeof nvClosePanel === 'function') nvClosePanel();
  syncParamField(); compute();
}
function closePanel() { panel.classList.remove('open'); }
panel.querySelector('.pr-x').addEventListener('click', closePanel);

// CLEAR: drop the loaded bet and this boost's numbers (odds, fair, boost,
// stake) so the next one starts blank. The promo type and the no-sweat
// refund rate are kept; saved boosts below are untouched.
function clearCalc() {
  bet = null; logBoost = null;
  inBook.value = ''; inFair.value = '';
  state.boostPct = ''; state.boostedOdds = ''; state.maxStake = '';
  save();
  const el = $('pr-bet');
  el.classList.add('empty');
  el.textContent = 'Shift+click any price in the table (grid, alt lines or +EV tab) to load it — or type the odds below.';
  syncParamField(); compute();
}
$('pr-clear').addEventListener('click', clearCalc);

// ── Inputs ───────────────────────────────────────────────────────────────
$('pr-types').addEventListener('click', e => {
  const b = e.target.closest('.pr-type');
  if (!b) return;
  state.type = b.dataset.type; save(); syncParamField(); compute();
});
inParam.addEventListener('input', () => {
  if (state.type === 'profit')  state.boostPct = inParam.value;
  if (state.type === 'odds')    state.boostedOdds = inParam.value;
  if (state.type === 'nosweat') state.refundPct = inParam.value;
  save(); compute();
});
inStake.addEventListener('input', () => { state.maxStake = inStake.value; save(); compute(); });
inBook.addEventListener('input', compute);
inFair.addEventListener('input', () => {
  if (bet && bet.fairProb != null && num(inFair.value) !== PromoMath.probToAm(bet.fairProb)) {
    bet.fairProb = null; bet.fairSrc = 'typed in'; bet.warn = ''; renderBet();
  }
  compute();
});

// Load a bet from outside (the promo finder and +EV rows will call this too).
// info: { label, bookOdds (American), fairProb (0-1), fairSrc, warn }
function load(info) {
  bet = info;
  inBook.value = info.bookOdds != null ? fmtAm(info.bookOdds) : '';
  const fairAm = info.fairProb != null ? PromoMath.probToAm(info.fairProb) : null;
  inFair.value = fairAm != null ? fmtAm(fairAm) : '';
  renderBet(); openPanel();
}
window.promoCalcLoad = load;

// ── Log at the boosted price ─────────────────────────────────────────────
// Opens index.html's bet form (position warnings, correlation sizing, slip
// link) with the boosted price and the boost-capped stake filled in; the
// tracker records it as source 'boost' with the boost in its notes.
$('pr-log').addEventListener('click', () => {
  if (!logBoost || !bet || !bet.ctx || typeof showBetModal !== 'function') return;
  const boostedAm = PromoMath.decToAm(logBoost.effDec);
  const kind = state.type === 'profit' ? `${num(state.boostPct)}% profit boost` : 'boosted odds';
  showBetModal({ ...bet.ctx, odds: fmtAm(boostedAm), fair: logBoost.p, source: 'boost',
                 stakePrefill: logBoost.useStake,
                 boostNote: `${kind}: ${fmtAm(logBoost.bookAm)} → ${fmtAm(boostedAm)} at ${bet.ctx.book}`,
                 promoId: bet.promoId || null });
});

// ── Saved boosts: Add Boost wizard, View Boosts panel, best uses ─────────
// Boosts are stored server-side (/api/promos, promos_store.py) so they persist
// all week. For each active one, every price at its book on the loaded boards
// that meets its terms (sport, game, odds, not started, not a stale scrape) is
// priced at the sharp fair value (same engine as the +EV tab).
//  - Straight boosts rank single bets by expected $ at the stake the boost allows.
//  - Parlay boosts search cross-game parlays: one leg per game, so the legs are
//    close to independent and the fair hit rate is the product of the legs'.
//    The book's parlay price is taken as the product of the leg decimals.
let promos = [];
const SPORT_CHOICES = () => (typeof SPORTS_LIST !== 'undefined' ? SPORTS_LIST : [])
  .map(sk => [sk, (typeof SPORT_META !== 'undefined' && SPORT_META[sk] && SPORT_META[sk].label) || sk]);
const sportLbl = sk => (SPORT_CHOICES().find(x => x[0] === sk) || [, sk])[1];
const MARKET_CHOICES = [['h2h', 'ML'], ['spreads', 'Spread'], ['totals', 'Total']];
const PERIOD_CHOICES = [['full', 'Full'], ['h1', '1H'], ['q1', 'Q1']];
const bookName = bk => (typeof BM_DISPLAY !== 'undefined' && BM_DISPLAY[bk]) || bk;

// The wizard's sport choices. NBA covers the preseason board too.
const SPORT_GROUPS = [
  { key: 'any',   label: 'Any sport', sports: [] },
  { key: 'nfl',   label: 'NFL',   sports: ['americanfootball_nfl'] },
  { key: 'nba',   label: 'NBA',   sports: ['basketball_nba', 'basketball_nba_preseason'] },
  { key: 'nhl',   label: 'NHL',   sports: ['icehockey_nhl'] },
  { key: 'ncaaf', label: 'NCAAF', sports: ['americanfootball_ncaaf'] },
  { key: 'mlb',   label: 'MLB',   sports: ['baseball_mlb'] },
  { key: 'cbb',   label: 'CBB',   sports: ['basketball_ncaab'] },
];
const groupSports = g => {
  const have = typeof SPORTS_LIST !== 'undefined' ? SPORTS_LIST : [];
  const grp = SPORT_GROUPS.find(x => x.key === g) || SPORT_GROUPS[0];
  const list = grp.sports.length ? grp.sports : SPORT_GROUPS.flatMap(x => x.sports);
  return list.filter(sk => have.includes(sk));
};
const EXCHANGES = new Set(['novig', 'kalshi', 'polymarket']);
const recBooks = () => (typeof BOOKMAKERS !== 'undefined' ? BOOKMAKERS : [])
  .filter(bk => !EXCHANGES.has(bk) && !(typeof SHARP_BOOKS !== 'undefined' && SHARP_BOOKS.has(bk)));
const KIND_LABEL = { straight: 'Straight bet', parlay: 'Parlay', any: 'Straight or parlay' };
const promoKind = pr => pr.bet_kind || 'straight';   // boosts saved before 10/10 were straight-bet boosts
const PARLAY_LEGS_MAX = 6;

// Kelly at decimal odds dd, capped at the promo's max stake (the cap alone without a bankroll).
function kellyStake(p, dd, cap) {
  const f = PromoMath.kelly(p, dd);
  const bank = typeof kellyBankroll !== 'undefined' ? kellyBankroll : null;
  const frac = typeof kellyFraction !== 'undefined' ? kellyFraction : 0.5;
  return f != null && bank > 0 ? Math.min(cap || Infinity, bank * Math.min(f, 1) * frac) : (cap || 0);
}

// Logging a bet from a saved boost (USE -> LOG) marks that boost used; the
// bet form calls this after the bet is saved.
window.promoMarkUsed = async id => {
  try {
    await fetch(`/api/promos/${id}`, { method: 'PATCH', headers: { 'Content-Type': 'application/json' },
                                       body: JSON.stringify({ status: 'used' }) });
  } catch (e) { /* stays in the list; USED by hand still works */ }
  loadPromos();
  if (typeof clearCalc === 'function') clearCalc();
};

async function loadPromos() {
  try {
    const r = await fetch('/api/promos');
    promos = r.ok ? await r.json() : [];
  } catch (e) { promos = []; }
  const cnt = document.getElementById('boost-count');
  if (cnt) { cnt.textContent = promos.length || ''; cnt.style.display = promos.length ? '' : 'none'; }
  if (vpanel.classList.contains('open')) renderPromos();
}

function promoTitle(pr) {
  const t = pr.type === 'profit' ? `${pr.value}% profit boost` : pr.type === 'nosweat' ? 'No-sweat' : pr.type === 'bonus' ? `$${pr.max_stake || '?'} bonus bet` : 'Boost';
  return `${bookName(pr.book_key)} · ${pr.title || t}`;
}
function promoCrit(pr) {
  const parts = [], kind = promoKind(pr);
  parts.push(kind === 'straight' ? 'straight' : `${KIND_LABEL[kind].toLowerCase()}${pr.min_legs ? ` (${pr.min_legs}+ legs)` : ''}`);
  if (pr.max_stake && pr.type !== 'bonus') parts.push(`$${pr.max_stake} ${pr.type === 'nosweat' ? 'max return' : 'max wager'}`);
  if (pr.type === 'nosweat') parts.push(`refund as ${pr.refund_rate >= 0.999 ? 'cash' : 'bonus bet'}`);
  if (pr.sports && pr.sports.length) parts.push([...new Set(pr.sports.map(sportLbl))].join('/'));
  if (pr.games && pr.games.length) parts.push(pr.games.map(g => g.label).join(', ') + ' only');
  if (pr.markets && pr.markets.length) parts.push(pr.markets.map(m => (MARKET_CHOICES.find(x => x[0] === m) || [, m])[1]).join('/'));
  if (pr.periods && pr.periods.length) parts.push(pr.periods.map(m => (PERIOD_CHOICES.find(x => x[0] === m) || [, m])[1]).join('/'));
  if (pr.min_odds != null) parts.push(`${fmtAm(pr.min_odds)} or longer${kind !== 'straight' ? ' (whole bet)' : ''}`);
  if (pr.min_leg_odds != null && kind !== 'straight') parts.push(`each leg ${fmtAm(pr.min_leg_odds)} or longer`);
  if (pr.max_odds != null) parts.push(`up to ${fmtAm(pr.max_odds)}`);
  if (pr.expires_at) parts.push(`use by ${new Date(pr.expires_at).toLocaleString([], { weekday: 'short', month: 'short', day: 'numeric', hour: 'numeric', minute: '2-digit' })}`);
  return parts.join(' · ');
}
const evText = (pr, ev) => pr.type === 'bonus' ? `converts ${fmtPct(ev).replace('+', '')}` : fmtPct(ev);

// Every price at the promo's book on the loaded boards that meets its terms,
// with the sharp fair win probability at its own line.
function eligibleLegs(pr) {
  if (typeof tabState === 'undefined' || typeof bookSides !== 'function') return [];
  const now = Date.now(), out = [];
  const gameIds = (pr.games || []).map(g => g.id);
  (typeof SPORTS_LIST !== 'undefined' ? SPORTS_LIST : []).forEach(sk => {
    if (pr.sports && pr.sports.length && !pr.sports.includes(sk)) return;
    const ts = tabState[sk];
    if (!ts || !ts.loaded || !ts.data) return;
    const sn = n => typeof shortTeam === 'function' ? shortTeam(n, sk) : n;
    ts.data.forEach(row => {
      if (gameIds.length && !gameIds.includes(row.game_id)) return;
      const m = String(row.market).match(/^(h2h|spreads|totals)(?:_(h1|q1))?$/);
      if (!m) return;
      if (pr.markets && pr.markets.length && !pr.markets.includes(m[1])) return;
      if (pr.periods && pr.periods.length && !pr.periods.includes(m[2] || 'full')) return;
      if (row.commence_time && new Date(row.commence_time).getTime() <= now) return;
      if (typeof isSourceStale === 'function' && isSourceStale(sk, pr.book_key, row.market)) return;
      const entry = row.books && row.books[pr.book_key];
      if (!entry) return;
      const isTot = m[1] === 'totals';
      const bs = bookSides(entry, isTot);
      [['upper', bs.u, bs.uPt], ['lower', bs.l, bs.lPt]].forEach(([side, oddsS, pt]) => {
        const am = parseAm(oddsS), d = am != null ? PromoMath.amToDec(am) : null;
        if (!d) return;
        const f = fairAtLine(row, sk, side, pt, pr.book_key, false);
        if (!f) return;
        const sideName = isTot ? (side === 'upper' ? 'Over' : 'Under') : (side === 'upper' ? row.away_team : row.home_team);
        const ptLbl = isTot ? (pt != null ? ' ' + pt : '') : (pt != null && m[1] !== 'h2h' ? ' ' + (pt > 0 ? '+' + pt : pt) : '');
        const per = m[2] ? m[2].toUpperCase() + ' ' : '';
        out.push({ am, d, p: f.p, model: f.model, row, sk, side, pt,
                   label: `${per}${isTot ? sideName : sn(sideName)}${ptLbl}${m[1] === 'h2h' ? ' ML' : ''}`,
                   game: `${sn(row.away_team)} @ ${sn(row.home_team)}`,
                   ctx: { sport: sportLbl(sk), away: row.away_team, home: row.home_team,
                          time: row.commence_time, market: row.market, side: sideName, line: ptLbl.trim(), odds: fmtAm(am),
                          book: bookName(pr.book_key), sk, gid: row.game_id,
                          sideKey: isTot ? (side === 'upper' ? 'over' : 'under') : (side === 'upper' ? 'away' : 'home'),
                          point: pt, bk: pr.book_key, fair: f.p, isAlt: false } });
      });
    });
  });
  return out;
}

// Straight bets: every eligible price ranked by expected $ at the allowed stake.
function bestUses(pr, limit = 3) {
  const decMin = pr.min_odds != null ? PromoMath.amToDec(pr.min_odds) : null;
  // Book odds are capped at +400 unless the boost sets its own limit: a boosted
  // longshot is worth a lot on paper but rarely cashes.
  const decMax = PromoMath.amToDec(pr.max_odds != null ? pr.max_odds : BOOST_MAX_ODDS);
  const out = [];
  eligibleLegs(pr).forEach(l => {
    if ((decMin && l.d < decMin - 1e-9) || (decMax && l.d > decMax + 1e-9)) return;
    let ev, stake, effDec = null;
    if (pr.type === 'profit') {
      effDec = PromoMath.boostedDec(l.d, (pr.value || 0) / 100);
      ev = l.p * effDec - 1;
      stake = ev > 0 ? kellyStake(l.p, effDec, pr.max_stake) : (pr.max_stake || 0);
    } else if (pr.type === 'nosweat') {
      ev = PromoMath.ev('nosweat', l.p, l.d, { refundRate: pr.refund_rate ?? 0.7 }); stake = pr.max_stake || 0;
    } else if (pr.type === 'bonus') {
      ev = PromoMath.ev('bonus', l.p, l.d); stake = pr.max_stake || 0;
    } else return;
    const edgeAlone = l.p - 1 / l.d;    // already +EV before the boost (the +EV tab's 1pp floor)
    out.push({ ...l, ev, dollars: ev * stake, stake, effDec, fair: l.p,
               evAlone: edgeAlone >= 0.01 ? l.p * l.d - 1 : null });
  });
  out.sort((a, b) => b.dollars - a.dollars || b.ev - a.ev);
  // The top picks by expected $, plus up to two bets that are +EV before the
  // boost (boosting an edge you already have) if they missed the top.
  const top = out.slice(0, limit);
  out.slice(limit).filter(u => u.evAlone != null).slice(0, 2).forEach(u => top.push(u));
  return top;
}

// Parlays: the best parlay of each length from the boost's minimum legs up to
// one past it (at least 4), one leg per game, searched exhaustively over the
// lowest-hold legs (p·d closest to 1) at the book. With a profit boost X:
//   EV/$ = P·(1 + (D-1)(1+X)) - 1,  P = Π p_i,  D = Π d_i
// Legs priced only by the outcome model are left out: a leg's fair-value
// error multiplies through every parlay it is in.
function bestParlays(pr) {
  const nMin = Math.max(2, pr.min_legs || 2);
  const nMax = Math.min(PARLAY_LEGS_MAX, Math.max(nMin + 1, 4));
  // Min odds per leg: books void a boosted parlay with a leg shorter than the
  // terms allow (e.g. -200), so those legs never enter the search. Applied
  // before the per-game cut so a dropped leg doesn't use up its game's slots.
  const dLegMin = pr.min_leg_odds != null ? PromoMath.amToDec(pr.min_leg_odds) : null;
  const legs = eligibleLegs(pr).filter(l => !l.model && (!dLegMin || l.d >= dLegMin - 1e-9));
  legs.forEach(l => { l.r = l.p * l.d; });
  legs.sort((a, b) => b.r - a.r);
  const perGame = new Map(), pool = [];
  for (const l of legs) {             // the 3 best prices per game keep some odds variety
    const n = perGame.get(l.row.game_id) || 0;
    if (n >= 3) continue;
    perGame.set(l.row.game_id, n + 1);
    pool.push(l);
  }
  const P0 = pool.slice(0, nMax <= 4 ? 40 : nMax === 5 ? 28 : 22);
  const res = { list: [], games: perGame.size, nMin, nMax };
  if (perGame.size < nMin) return res;

  const dMin = pr.min_odds != null ? PromoMath.amToDec(pr.min_odds) : null;
  const dMax = pr.max_odds != null ? PromoMath.amToDec(pr.max_odds) : null;
  const b = (pr.value || 0) / 100, rr = pr.refund_rate ?? 0.7, cap = pr.max_stake || 0;
  const evOf = (P, D) => pr.type === 'profit' ? P * PromoMath.boostedDec(D, b) - 1
                       : pr.type === 'bonus'  ? P * (D - 1)
                       : P * D - 1 + (1 - P) * rr;      // no-sweat: a loss refunds the stake (up to the max return)
  const best = {}, pick = [];
  (function dfs(start, P, D) {
    const n = pick.length;
    if (n >= nMin && (!dMin || D >= dMin - 1e-9) && (!dMax || D <= dMax + 1e-9)) {
      const ev = evOf(P, D);
      const effDec = pr.type === 'profit' ? PromoMath.boostedDec(D, b) : null;
      const stake = pr.type === 'profit' && ev > 0 ? kellyStake(P, effDec, cap) : cap;
      const dollars = ev * stake, cur = best[n];
      if (!cur || dollars > cur.dollars + 1e-9 || (Math.abs(dollars - cur.dollars) <= 1e-9 && ev > cur.ev))
        best[n] = { legs: pick.slice(), P, D, effDec, ev, stake, dollars };
    }
    if (n === nMax) return;
    for (let i = start; i < P0.length; i++) {
      const l = P0[i];
      if (pick.some(x => x.row.game_id === l.row.game_id)) continue;
      pick.push(l); dfs(i + 1, P * l.p, D * l.d); pick.pop();
    }
  })(0, 1, 1);
  res.list = Object.values(best).sort((x, y) => y.dollars - x.dollars || y.ev - x.ev);
  return res;
}

function parlayText(pr, pl) {
  const boosted = pl.effDec ? ` → ${fmtAm(PromoMath.decToAm(pl.effDec))} boosted` : '';
  return `${bookName(pr.book_key)} ${pl.legs.length}-leg parlay ${fmtAm(PromoMath.decToAm(pl.D))}${boosted}, $${Math.round(pl.stake)}\n` +
    pl.legs.map((l, i) => `${i + 1}. ${l.ctx.sport}: ${l.label} ${fmtAm(l.am)} (${l.game})`).join('\n');
}

function renderPromos() {
  const box = $('pr-promos');
  if (!promos.length) {
    box.innerHTML = '<div class="pr-none">No saved boosts. Use + ADD BOOST to enter one as it arrives; the best ways to use it show up here.</div>';
    return;
  }
  const loaded = sk => typeof tabState !== 'undefined' && tabState[sk] && tabState[sk].loaded;
  box.innerHTML = promos.map(pr => {
    const kind = promoKind(pr);
    const want = pr.sports && pr.sports.length ? pr.sports : groupSports('any');
    const unloaded = [...new Set(want.filter(sk => !loaded(sk)).map(sportLbl))];
    const anyLoaded = want.some(loaded);
    let html = '';
    if (!anyLoaded) {
      html = `<div class="pr-none">Load ${escH(unloaded.join(' / ') || 'a sport')} to see where to use it.</div>`;
    } else {
      if (kind !== 'parlay') {
        const uses = bestUses(pr);
        pr._uses = uses;
        html += kind === 'any' ? '<div class="pr-sub-h">Best straight bets</div>' : '';
        html += !uses.length ? '<div class="pr-none">No straight bet on the loaded boards fits these terms.</div>'
          : uses.map((u, i) => `<div class="pr-use"><span>${escH(u.label)} ${fmtAm(u.am)}${u.effDec ? ` → ${fmtAm(PromoMath.decToAm(u.effDec))}` : ''}
              <span style="color:var(--t3)">· ${escH(u.game)}</span><br>
              ${u.evAlone != null ? `<span class="pr-plusev" title="Already +EV before the boost (${fmtPct(u.evAlone)}) — the boost stacks on a real edge">★ +EV alone ${fmtPct(u.evAlone)}</span><br>` : ''}<span class="${u.ev > 0 ? 'pos' : 'neg'}">${evText(pr, u.ev)}</span> · ${fmtUsd(u.dollars)} on $${Math.round(u.stake)}${u.model ? ' <span style="color:var(--amber)" title="Fair value from the model, not a real sharp price">(model)</span>' : ''}</span>
              <button data-pid="${pr.id}" data-i="${i}" class="pr-use-btn">USE</button></div>`).join('');
      }
      if (kind !== 'straight') {
        const pl = bestParlays(pr);
        pr._parlays = pl.list;
        html += '<div class="pr-sub-h">Best parlays · one leg per game</div>';
        html += !pl.list.length
          ? `<div class="pr-none">${pl.games < pl.nMin ? `Needs ${pl.nMin} games with sharp-priced lines at ${escH(bookName(pr.book_key))}; the loaded boards have ${pl.games}.` : 'No parlay on the loaded boards fits these terms.'}</div>`
          : pl.list.map((p, i) => `<div class="pr-use"><span><b>${p.legs.length} legs</b> ${fmtAm(PromoMath.decToAm(p.D))}${p.effDec ? ` → ${fmtAm(PromoMath.decToAm(p.effDec))}` : ''}
              <span style="color:var(--t3)">· hits ${(p.P * 100).toFixed(1)}%</span>
              <ol class="pr-legs">${p.legs.map(l => `<li>${escH(l.label)} ${fmtAm(l.am)} <span style="color:var(--t3)">· ${escH(l.game)}${want.length > 1 ? ' · ' + escH(l.ctx.sport) : ''} · fair ${fmtAm(PromoMath.probToAm(l.p))}</span></li>`).join('')}</ol>
              <span class="${p.ev > 0 ? 'pos' : 'neg'}">${evText(pr, p.ev)}</span> · ${fmtUsd(p.dollars)} on $${Math.round(p.stake)}</span>
              <span class="pr-btns"><button data-pid="${pr.id}" data-i="${i}" class="pr-copy-btn" title="Copy the legs to paste or read off at the book">COPY</button>
              <button data-pid="${pr.id}" data-i="${i}" class="pr-copy-btn pr-log-btn" title="Log this parlay in the bet tracker (graded leg by leg)">LOG</button></span></div>`).join('');
      }
      if (unloaded.length) html += `<div class="pr-none">Not searched yet: ${escH(unloaded.join(' / '))} (board not loaded).</div>`;
    }
    return `<div class="pr-promo"><div class="pr-promo-h"><b>${escH(promoTitle(pr))}</b>
        <span><button class="pr-mini pr-used" data-pid="${pr.id}" title="Mark used">USED</button>
        <button class="pr-mini pr-del" data-pid="${pr.id}" title="Delete">✕</button></span></div>
      <div class="pr-crit">${escH(promoCrit(pr))}</div>${html}</div>`;
  }).join('');
}

// ── View Boosts panel ────────────────────────────────────────────────────
const vpanel = document.createElement('div');
vpanel.id = 'boosts-panel';
vpanel.innerHTML = `
  <div class="pr-title">Saved boosts · best uses <span>
    <button class="pr-mini" id="bv-refresh" title="Re-rank against the boards as loaded now">↻</button>
    <button class="pr-mini" id="bv-calc" title="Boost calculator (also: shift+click any price)">CALCULATOR</button>
    <button class="pr-mini" id="bv-add">+ ADD</button>
    <button class="pr-x" id="bv-x" title="Close">✕</button></span></div>
  <div id="pr-promos"></div>
  <div class="pr-note">Ranked by expected $ at the sharp fair value (same engine as the +EV tab), staked at Kelly capped by the boost’s max. Parlays use one leg per game, priced as the product of the legs; the book’s own parlay price can round slightly. Legs priced only by the model are left out of parlays.</div>`;
document.body.appendChild(vpanel);

function openBoosts() {
  closePanel(); vpanel.classList.add('open'); viewBtn.classList.add('open');
  if (typeof nvClosePanel === 'function') nvClosePanel();
  renderPromos(); loadPromos();
}
function closeBoosts() { vpanel.classList.remove('open'); viewBtn.classList.remove('open'); }
viewBtn.addEventListener('click', e => { e.stopPropagation(); vpanel.classList.contains('open') ? closeBoosts() : openBoosts(); });
$('bv-x').addEventListener('click', closeBoosts);
$('bv-refresh').addEventListener('click', renderPromos);
$('bv-calc').addEventListener('click', () => { closeBoosts(); openPanel(); });
$('bv-add').addEventListener('click', () => openWizard());

$('pr-promos').addEventListener('click', async e => {
  const logP = e.target.closest('.pr-log-btn');
  if (logP) {
    const pr = promos.find(p => String(p.id) === logP.dataset.pid), pl = pr && pr._parlays && pr._parlays[+logP.dataset.i];
    if (pl) openParlayLog(pr, pl);
    return;
  }
  const use = e.target.closest('.pr-use-btn'), copy = e.target.closest('.pr-copy-btn');
  const used = e.target.closest('.pr-used'), del = e.target.closest('.pr-del');
  if (use) {
    const pr = promos.find(p => String(p.id) === use.dataset.pid), u = pr && pr._uses && pr._uses[+use.dataset.i];
    if (!u) return;
    if (pr.type === 'profit') { state.type = 'profit'; state.boostPct = pr.value; }
    else if (pr.type === 'nosweat') { state.type = 'nosweat'; state.refundPct = Math.round((pr.refund_rate ?? 0.7) * 100); }
    else if (pr.type === 'bonus') state.type = 'bonus';
    if (pr.max_stake) state.maxStake = pr.max_stake;
    save();
    closeBoosts();
    load({ label: `${u.label} ${fmtAm(u.am)} @ ${bookName(pr.book_key)}, ${u.game}`, bookOdds: u.am, fairProb: u.fair,
           fairSrc: u.model ? 'outcome model past the last sharp price' : 'sharp fair at this line',
           warn: u.model ? 'Fair comes from the model, not a real sharp price — treat with care.' : '', ctx: u.ctx,
           promoId: pr.id });
    return;
  }
  if (copy) {
    const pr = promos.find(p => String(p.id) === copy.dataset.pid), pl = pr && pr._parlays && pr._parlays[+copy.dataset.i];
    if (!pl) return;
    const txt = parlayText(pr, pl);
    try { await navigator.clipboard.writeText(txt); copy.textContent = 'COPIED'; }
    catch (err) { window.prompt('Copy the parlay:', txt); }
    setTimeout(() => { copy.textContent = 'COPY'; }, 1500);
    return;
  }
  const pid = (used || del) && (used || del).dataset.pid;
  if (!pid) return;
  if (del && !confirm('Delete this boost?')) return;
  await fetch(`/api/promos/${pid}`, used ? { method: 'PATCH', headers: { 'Content-Type': 'application/json' },
                                             body: JSON.stringify({ status: 'used' }) } : { method: 'DELETE' });
  loadPromos();
});

// ── Log a boosted parlay in the tracker ──────────────────────────────────
// One bets.db row (market 'parlay') carrying every leg, so the grader can
// settle it leg by leg. The book's parlay price can round off the product of
// the legs, so it's editable; a profit boost's price is recomputed from it.
const plog = document.createElement('div');
plog.id = 'parlay-log';
plog.innerHTML = `<div class="pl-box"><div class="pl-h" id="pl-h"></div><ol id="pl-legs"></ol>
  <div class="pl-row"><div><label for="pl-odds">Book parlay price</label><input id="pl-odds"></div>
  <div><label for="pl-stake">Stake $</label><input id="pl-stake" type="number" step="0.01" min="0"></div></div>
  <div class="pl-sum" id="pl-sum"></div>
  <div class="pl-act"><button id="pl-cancel">CANCEL</button><button class="pl-ok" id="pl-ok">LOG PARLAY</button></div></div>`;
document.body.appendChild(plog);
let plCur = null;

function plCalc() {
  if (!plCur) return null;
  const { pr, pl } = plCur;
  const am = parseAm($('pl-odds').value), stake = num($('pl-stake').value);
  const D = am != null ? PromoMath.amToDec(am) : null;
  if (!D) return { err: 'Odds should look like +450.' };
  const b = (pr.value || 0) / 100, rr = pr.refund_rate ?? 0.7;
  const payDec = pr.type === 'profit' ? PromoMath.boostedDec(D, b) : D;
  const ev = pr.type === 'profit' ? pl.P * payDec - 1 : pr.type === 'bonus' ? pl.P * (D - 1) : pl.P * D - 1 + (1 - pl.P) * rr;
  return { am, D, payAm: PromoMath.decToAm(payDec), payDec, stake, ev };
}
function plRender() {
  const c = plCalc();
  if (!c || c.err) { $('pl-sum').textContent = c ? c.err : ''; return; }
  const { pr } = plCur;
  $('pl-sum').innerHTML = `${pr.type === 'profit' ? `Boosted ${pr.value}%: pays <b>${fmtAm(c.payAm)}</b> · ` : pr.type === 'bonus' ? 'Bonus bet: winnings only · ' : 'No-sweat · '}` +
    `hits ${(plCur.pl.P * 100).toFixed(1)}% · EV <span class="${c.ev > 0 ? 'pos' : 'neg'}">${fmtPct(c.ev)}</span>` +
    (c.stake > 0 ? ` · ${fmtUsd(c.ev * c.stake)} expected` : '');
}
function openParlayLog(pr, pl) {
  plCur = { pr, pl };
  $('pl-h').textContent = `Log ${pl.legs.length}-leg parlay · ${bookName(pr.book_key)}`;
  $('pl-legs').innerHTML = pl.legs.map(l => `<li>${escH(l.label)} ${fmtAm(l.am)} <span style="color:var(--t3)">· ${escH(l.game)}</span></li>`).join('');
  $('pl-odds').value = fmtAm(PromoMath.decToAm(pl.D));
  $('pl-stake').value = Math.round(pl.stake * 100) / 100 || '';
  plRender();
  plog.classList.add('open');
  setTimeout(() => $('pl-stake').focus(), 0);
}
function closeParlayLog() { plog.classList.remove('open'); plCur = null; }
async function confirmParlayLog() {
  const c = plCalc();
  if (!c || c.err) return;
  if (!(c.stake > 0)) { $('pl-sum').textContent = 'Enter the stake.'; return; }
  const { pr, pl } = plCur;
  const legs = pl.legs.map(l => ({
    sport_key: l.sk, event_id: l.row.game_id, away: l.row.away_team, home: l.row.home_team,
    commence_time: l.row.commence_time || null, market: l.row.market, side: l.ctx.sideKey,
    point: String(l.row.market).startsWith('h2h') ? null : (l.pt ?? null),
    team: l.ctx.sideKey === 'away' ? l.row.away_team : l.ctx.sideKey === 'home' ? l.row.home_team : null,
    odds: l.am, label: l.label, game: l.game, fair: l.p }));
  const starts = legs.map(l => l.commence_time).filter(Boolean).sort();
  const sks = [...new Set(legs.map(l => l.sport_key))];
  const boostTxt = pr.type === 'profit' ? `${pr.value}% profit boost` : pr.type === 'bonus' ? 'bonus bet' : 'no-sweat';
  const body = {
    sport_key: sks.length === 1 ? sks[0] : null, sport_label: [...new Set(sks.map(sportLbl))].join('/'),
    away: 'Parlay', home: `${legs.length} legs`, commence_time: starts[0] || null,
    market: 'parlay', side: 'parlay', odds: c.payAm, book_key: pr.book_key, book: bookName(pr.book_key),
    stake: c.stake, fair_prob: pl.P, ev: c.ev, kelly_rec: pl.stake, source: 'boost',
    notes: `${legs.length}-leg parlay at ${fmtAm(c.am)} · ${boostTxt}`,
    legs: { legs, boost_type: pr.type, boost_value: pr.value ?? null, book_odds: c.am },
  };
  $('pl-ok').disabled = true;
  try {
    const r = await fetch('/api/bets', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(body) });
    const j = await r.json().catch(() => ({}));
    if (!r.ok) { $('pl-sum').textContent = 'Not logged: ' + (j.error || r.status); return; }
  } catch (e) { $('pl-sum').textContent = 'Could not reach the server.'; return; }
  finally { $('pl-ok').disabled = false; }
  closeParlayLog();
  if (typeof showToast === 'function') showToast('Parlay logged · boost marked used', 'ok');
  if (typeof loadOpenBets === 'function') loadOpenBets();
  window.promoMarkUsed(pr.id);
}
$('pl-odds').addEventListener('input', plRender);
$('pl-stake').addEventListener('input', plRender);
$('pl-cancel').addEventListener('click', closeParlayLog);
$('pl-ok').addEventListener('click', confirmParlayLog);
plog.addEventListener('mousedown', e => { if (e.target === plog) closeParlayLog(); });
plog.addEventListener('keydown', e => {
  if (e.key === 'Escape') { e.stopPropagation(); closeParlayLog(); }
  else if (e.key === 'Enter') { e.preventDefault(); confirmParlayLog(); }
});

// ── Add Boost wizard ─────────────────────────────────────────────────────
// One question per screen: book → sport → specific game? (→ which game) →
// boost type → straight or parlay → the type's terms → use-by date → review.
const wiz = document.createElement('div');
wiz.id = 'bw-overlay';
wiz.innerHTML = `<div id="bw-card">
  <div class="bw-top"><span id="bw-step"></span><button class="pr-x" id="bw-x" title="Close (Esc)">✕</button></div>
  <div class="bw-dots" id="bw-dots"></div>
  <div id="bw-q"></div><div id="bw-body"></div><div class="bw-err" id="bw-err"></div>
  <div class="bw-nav"><button class="pr-mini" id="bw-back">‹ BACK</button><button class="bw-next" id="bw-next">NEXT ›</button></div>
</div>`;
document.body.appendChild(wiz);

const WZ_LS = 'boostWizard.last';
const pad2 = n => String(n).padStart(2, '0');
const ymd = dt => `${dt.getFullYear()}-${pad2(dt.getMonth() + 1)}-${pad2(dt.getDate())}`;
let wz = null, wzStep = 0;

function openWizard() {
  let last = {};
  try { last = JSON.parse(localStorage.getItem(WZ_LS) || '{}'); } catch (e) { /* storage blocked */ }
  const books = recBooks();
  wz = { book: books.includes(last.book) ? last.book : books[0], sport: 'any', specific: null, game: null,
         type: null, kind: 'straight', minLegs: 3, value: '', minOdds: '', maxStake: '', refundCash: false, date: '' };
  wzStep = 0;
  closeBoosts();
  wiz.classList.add('open');
  renderWizard();
}
function closeWizard() { wiz.classList.remove('open'); }

const wzSteps = () => ['book', 'sport', 'specific', ...(wz.specific ? ['game'] : []), 'type', 'kind', ...(wz.kind !== 'straight' ? ['legs'] : []), 'details', 'date', 'review'];
const TYPE_LABEL = { profit: 'Profit boost', bonus: 'Bonus bet', nosweat: 'No-sweat bet' };

// Not-yet-started games on the loaded boards of the chosen sport.
function wzGames() {
  const now = Date.now(), games = new Map(), unloaded = [];
  groupSports(wz.sport).forEach(sk => {
    const ts = typeof tabState !== 'undefined' && tabState[sk];
    if (!ts || !ts.loaded || !ts.data) { unloaded.push(sk); return; }
    const nm = n => typeof shortTeam === 'function' ? shortTeam(n, sk) : n;
    ts.data.forEach(r => {
      if (games.has(r.game_id) || (r.commence_time && new Date(r.commence_time).getTime() <= now)) return;
      const when = r.commence_time ? new Date(r.commence_time).toLocaleString([], { weekday: 'short', month: 'short', day: 'numeric', hour: 'numeric', minute: '2-digit' }) : '';
      games.set(r.game_id, { id: r.game_id, sk, label: `${nm(r.away_team)} @ ${nm(r.home_team)}`, when, commence_time: r.commence_time });
    });
  });
  return { list: [...games.values()].sort((a, b) => String(a.commence_time).localeCompare(String(b.commence_time))), unloaded };
}

const opt = (val, cur, label, sub = '') =>
  `<button class="bw-opt${val === cur ? ' sel' : ''}" data-v="${escH(val)}">${escH(label)}${sub ? `<small>${escH(sub)}</small>` : ''}</button>`;
const field = (id, label, val, ph, hint = '', type = 'text') =>
  `<div class="bw-field"><label for="${id}">${label}</label><input id="${id}" type="${type}" value="${escH(val)}" placeholder="${escH(ph)}" autocomplete="off">${hint ? `<div class="bw-hint">${hint}</div>` : ''}</div>`;

function renderWizard() {
  const steps = wzSteps(), step = steps[wzStep];
  $('bw-step').textContent = `Add boost · ${wzStep + 1} of ${steps.length}`;
  $('bw-dots').innerHTML = steps.map((s, i) => `<div class="bw-dot${i <= wzStep ? ' done' : ''}"></div>`).join('');
  $('bw-err').textContent = '';
  $('bw-back').style.visibility = wzStep ? 'visible' : 'hidden';
  $('bw-next').textContent = step === 'review' ? 'SAVE BOOST' : 'NEXT ›';
  const q = $('bw-q'), body = $('bw-body');
  const parlayish = wz.kind !== 'straight';

  if (step === 'book') {
    q.textContent = 'Which book is the boost at?';
    body.innerHTML = `<select id="bw-book">${recBooks().map(bk => `<option value="${bk}"${bk === wz.book ? ' selected' : ''}>${escH(bookName(bk))}</option>`).join('')}</select>`;
  } else if (step === 'sport') {
    q.textContent = 'Which sport?';
    body.innerHTML = `<select id="bw-sport">${SPORT_GROUPS.filter(g => g.key === 'any' || groupSports(g.key).length)
      .map(g => `<option value="${g.key}"${g.key === wz.sport ? ' selected' : ''}>${escH(g.label)}</option>`).join('')}</select>`;
  } else if (step === 'specific') {
    q.textContent = 'Is it for a specific game?';
    const cur = wz.specific == null ? null : wz.specific ? 'yes' : 'no';
    body.innerHTML = `<div class="bw-opts two" data-key="specific">${opt('yes', cur, 'Yes')}${opt('no', cur, 'No')}</div>`;
  } else if (step === 'game') {
    q.textContent = 'Which game?';
    const { list, unloaded } = wzGames();
    body.innerHTML = (list.length
      ? `<select id="bw-game"><option value="">Pick a game…</option>${list.map(g => `<option value="${escH(g.id)}"${wz.game && wz.game.id === g.id ? ' selected' : ''}>${escH(g.label)}${g.when ? ' · ' + escH(g.when) : ''}${wz.sport === 'any' ? ' · ' + escH(sportLbl(g.sk)) : ''}</option>`).join('')}</select>`
      : '<div class="bw-hint">No upcoming games on the loaded boards for this sport.</div>') +
      (unloaded.length ? `<div class="bw-hint" style="margin-top:8px">Not loaded yet (loading pulls fresh odds):</div><div class="bw-load">${unloaded.map(sk => `<button class="pr-mini bw-load-btn" data-sk="${sk}">LOAD ${escH(sportLbl(sk))}</button>`).join('')}</div>` : '');
  } else if (step === 'type') {
    q.textContent = 'What type of boost?';
    body.innerHTML = `<div class="bw-opts" data-key="type">${opt('profit', wz.type, 'Profit boost', 'Winnings paid X% higher')}${opt('bonus', wz.type, 'Bonus bet', 'Free bet credit; the stake is not returned')}${opt('nosweat', wz.type, 'No-sweat bet', 'Stake refunded if the bet loses')}</div>`;
  } else if (step === 'kind') {
    q.textContent = 'Straight bet or parlay?';
    body.innerHTML = `<div class="bw-opts" data-key="kind">${opt('straight', wz.kind, 'Straight bet')}${opt('parlay', wz.kind, 'Parlay', 'Searches cross-game parlays (no same-game parlays)')}${opt('any', wz.kind, 'Either', 'Shows the best straight bets and parlays')}</div>`;
  } else if (step === 'legs') {
    q.textContent = 'Minimum number of legs?';
    body.innerHTML = field('bw-legs', 'Legs', wz.minLegs, '3', `Press Enter to keep ${wz.minLegs}. The finder also tries parlays a leg or two longer.`);
  } else if (step === 'details') {
    const oddsHint = parlayish ? 'The whole bet’s odds (for a parlay, the combined price). Blank = no minimum.' : 'Blank = no minimum.';
    const legField = parlayish ? field('bw-legmin', 'Min odds per leg', wz.minLegOdds, '-200',
      'Shortest price allowed on any one leg (books void the boost below it). Blank = no limit.') : '';
    q.textContent = `${TYPE_LABEL[wz.type]} terms`;
    if (wz.type === 'profit') {
      body.innerHTML = field('bw-value', 'Boost %', wz.value, '50') + field('bw-min', 'Min odds', wz.minOdds, '-200', oddsHint) + legField + field('bw-stake', 'Max wager $', wz.maxStake, '25');
    } else if (wz.type === 'bonus') {
      body.innerHTML = field('bw-stake', 'Bonus bet amount $', wz.maxStake, '25') + field('bw-min', 'Min odds', wz.minOdds, '-200', oddsHint) + legField;
    } else {
      body.innerHTML = field('bw-stake', 'Max return $', wz.maxStake, '25', 'The most the book refunds on a loss.') + field('bw-min', 'Min odds', wz.minOdds, '-200', oddsHint) + legField +
        `<div class="bw-field"><label>Refund paid as</label><div class="bw-opts two" data-key="refund">${opt('bonus', wz.refundCash ? 'cash' : 'bonus', 'Bonus bet', 'Valued at 70%')}${opt('cash', wz.refundCash ? 'cash' : 'bonus', 'Cash')}</div></div>`;
    }
  } else if (step === 'date') {
    q.textContent = 'Use by what date?';
    const today = ymd(new Date());
    if (!wz.date) wz.date = wz.game && wz.game.commence_time ? ymd(new Date(wz.game.commence_time)) : today;
    body.innerHTML = `<input id="bw-date" type="date" min="${today}" value="${wz.date}">` +
      (wz.game ? `<div class="bw-hint">The boost also drops off at kickoff of ${escH(wz.game.label)}.</div>` : '<div class="bw-hint">It drops off the list after 11:59 pm that day.</div>');
  } else if (step === 'review') {
    q.textContent = 'Save this boost?';
    const b = wzBody();
    const rows = [['Book', bookName(b.book_key)], ['Sport', (SPORT_GROUPS.find(g => g.key === wz.sport) || {}).label],
      ['Game', wz.game ? wz.game.label : 'Any'], ['Type', TYPE_LABEL[wz.type] + (wz.type === 'profit' ? ` ${b.value}%` : '')],
      ['Bet', KIND_LABEL[wz.kind] + (wz.kind !== 'straight' ? ` · ${b.min_legs}+ legs` : '')],
      [wz.type === 'profit' ? 'Max wager' : wz.type === 'bonus' ? 'Amount' : 'Max return', b.max_stake != null ? `$${b.max_stake}` : '—'],
      ...(wz.type === 'nosweat' ? [['Refund as', wz.refundCash ? 'Cash' : 'Bonus bet']] : []),
      ['Min odds', b.min_odds != null ? fmtAm(b.min_odds) : 'None'],
      ...(wz.kind !== 'straight' ? [['Min odds per leg', b.min_leg_odds != null ? fmtAm(b.min_leg_odds) : 'None']] : []),
      ['Use by', new Date(b.expires_at).toLocaleString([], { weekday: 'short', month: 'short', day: 'numeric', hour: 'numeric', minute: '2-digit' })]];
    body.innerHTML = `<div class="bw-review">${rows.map(([k, v]) => `<div class="k">${escH(k)}</div><div class="v">${escH(v)}</div>`).join('')}</div>`;
  }
  const first = body.querySelector('input, select');
  if (first) setTimeout(() => { first.focus(); if (first.select && first.type !== 'date') first.select(); }, 0);   // typing replaces a preloaded value
}

// Reads the current screen into wz; returns an error message or ''.
function wzRead() {
  const step = wzSteps()[wzStep], v = id => ($(id) ? $(id).value.trim() : '');
  if (step === 'book') wz.book = v('bw-book');
  if (step === 'sport') {
    if (v('bw-sport') !== wz.sport) { wz.sport = v('bw-sport'); wz.game = null; }
  }
  if (step === 'specific' && wz.specific == null) return 'Pick Yes or No.';
  if (step === 'game') {
    const id = v('bw-game');
    if (!id) return 'Pick the game (load its sport first if it isn’t listed).';
    const g = wzGames().list.find(x => x.id === id);
    if (wz.game?.id !== id) wz.date = '';
    wz.game = g || null;
  }
  if (step === 'type' && !wz.type) return 'Pick a boost type.';
  if (step === 'legs') {
    const n = parseInt(v('bw-legs'), 10);
    if (!(n >= 2 && n <= PARLAY_LEGS_MAX)) return `Minimum legs must be 2–${PARLAY_LEGS_MAX}.`;
    wz.minLegs = n;
  }
  if (step === 'details') {
    wz.minOdds = v('bw-min'); wz.maxStake = v('bw-stake'); wz.minLegOdds = v('bw-legmin');
    if (wz.minLegOdds && parseAm(wz.minLegOdds) == null) return 'Min odds per leg should look like -200.';
    if (wz.type === 'profit') {
      wz.value = v('bw-value');
      if (!(num(wz.value) > 0)) return 'Enter the boost %.';
    }
    if (wz.minOdds && parseAm(wz.minOdds) == null) return 'Min odds should look like -200 or +100.';
    if (!(num(wz.maxStake) > 0)) return wz.type === 'profit' ? 'Enter the max wager.' : wz.type === 'bonus' ? 'Enter the bonus bet amount.' : 'Enter the max return.';
  }
  if (step === 'date') {
    wz.date = v('bw-date');
    if (!/^\d{4}-\d{2}-\d{2}$/.test(wz.date)) return 'Pick a date.';
    if (wz.date < ymd(new Date())) return 'That date has passed.';
  }
  return '';
}

function wzBody() {
  let exp = new Date(wz.date + 'T23:59:59');
  if (wz.game && wz.game.commence_time && new Date(wz.game.commence_time) < exp) exp = new Date(wz.game.commence_time);
  const sports = wz.game ? [wz.game.sk] : wz.sport === 'any' ? [] : groupSports(wz.sport);
  return {
    book_key: wz.book, type: wz.type,
    value: wz.type === 'profit' ? num(wz.value) : null,
    refund_rate: wz.type === 'nosweat' ? (wz.refundCash ? 1 : 0.7) : null,
    max_stake: num(wz.maxStake),
    min_odds: wz.minOdds ? parseAm(wz.minOdds) : null,
    min_leg_odds: wz.kind !== 'straight' && wz.minLegOdds ? parseAm(wz.minLegOdds) : null,
    sports, markets: [], periods: [],
    games: wz.game ? [{ id: wz.game.id, label: wz.game.label, commence_time: wz.game.commence_time }] : [],
    bet_kind: wz.kind, min_legs: wz.kind !== 'straight' ? wz.minLegs : null,
    expires_at: exp.toISOString(),
  };
}

async function wzNext() {
  const err = wzRead();
  if (err) { $('bw-err').textContent = err; return; }
  const steps = wzSteps();
  if (steps[wzStep] !== 'review') { wzStep++; renderWizard(); return; }
  const btnSave = $('bw-next');
  btnSave.disabled = true;
  try {
    const r = await fetch('/api/promos', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(wzBody()) });
    if (!r.ok) { $('bw-err').textContent = 'Could not save: ' + (((await r.json().catch(() => ({}))).error) || r.status); return; }
  } catch (e) { $('bw-err').textContent = 'Could not reach the server.'; return; }
  finally { btnSave.disabled = false; }
  try { localStorage.setItem(WZ_LS, JSON.stringify({ book: wz.book })); } catch (e) { /* storage blocked */ }
  closeWizard();
  if (typeof showToast === 'function') showToast('Boost saved', 'ok');
  openBoosts();
}

$('bw-next').addEventListener('click', wzNext);
$('bw-back').addEventListener('click', () => { wzRead(); if (wzStep) { wzStep--; renderWizard(); } });
$('bw-x').addEventListener('click', closeWizard);
wiz.addEventListener('mousedown', e => { if (e.target === wiz) closeWizard(); });
wiz.addEventListener('keydown', e => {
  if (e.key === 'Escape') { e.stopPropagation(); closeWizard(); }
  else if (e.key === 'Enter' && !e.target.closest('button')) { e.preventDefault(); wzNext(); }
});
$('bw-body').addEventListener('click', async e => {
  const loadBtn = e.target.closest('.bw-load-btn');
  if (loadBtn) {
    if (typeof loadOdds !== 'function') return;
    loadBtn.disabled = true; loadBtn.textContent = 'LOADING…';
    await loadOdds(loadBtn.dataset.sk);
    renderWizard();
    return;
  }
  const b = e.target.closest('.bw-opt'), grp = b && b.closest('.bw-opts');
  if (!b) return;
  const key = grp.dataset.key, val = b.dataset.v;
  if (key === 'specific') { wz.specific = val === 'yes'; if (!wz.specific) wz.game = null; wzStep++; renderWizard(); return; }
  if (key === 'type') { if (wz.type !== val) { wz.type = val; wz.value = ''; } wzStep++; renderWizard(); return; }
  if (key === 'kind') {
    wz.kind = val; wzStep++; renderWizard();   // parlay / either go on to the min-legs question
    return;
  }
  if (key === 'refund') {
    wz.minOdds = $('bw-min').value; wz.maxStake = $('bw-stake').value;    // keep what's typed across the redraw
    if ($('bw-legmin')) wz.minLegOdds = $('bw-legmin').value;
    wz.refundCash = val === 'cash'; renderWizard();
  }
});
addBtn.addEventListener('click', e => { e.stopPropagation(); openWizard(); });
loadPromos();

// ── Shift+click resolution ───────────────────────────────────────────────
const parseAm = s => { const n = parseInt(String(s || '').replace(/[^\d+-]/g, ''), 10); return isFinite(n) && Math.abs(n) >= 100 ? n : null; };
const parsePt = s => { const m = String(s || '').match(/[-+]?\d+(\.\d+)?/); return m ? parseFloat(m[0]) : null; };
const bookKeyFromDisplay = name => {
  if (typeof BM_DISPLAY === 'undefined' || !name) return null;
  const hit = Object.entries(BM_DISPLAY).find(([, v]) => String(v).toLowerCase() === String(name).toLowerCase());
  return hit ? hit[0] : null;
};

// Fair win probability at the bet's own line, from the same engine as scanEV.
function fairAtLine(row, sk, side, pt, bk, isAlt) {
  if (typeof getSharpNoVig !== 'function' || typeof softSideFair !== 'function') return null;
  const sv = getSharpNoVig(row, sk);
  if (!sv) return null;
  const anchorPt = side === 'upper' ? sv.bolUpt : sv.bolLpt;
  const anchorFair = side === 'upper' ? sv.nv.pUpper : sv.nv.pLower;
  const s = softSideFair(row, sk, row.market, side, pt, anchorPt, anchorFair, bk);
  let p = s.fair;
  if (isAlt && typeof altTrustedFair === 'function') p = altTrustedFair(row, sk, row.market, side, pt, anchorPt, s);
  const how = s.backing === 'real' ? (s.exact ? 'sharp price at this line' : 'interpolated between sharp prices')
                                   : 'outcome model past the last sharp price';
  return p == null ? null : { p, how, model: s.backing !== 'real' };
}

function resolveGridClick(target) {
  const tr = target.closest('tr');
  const line = target.closest('.odds-row, .bc-line, .ev-line');
  if (!tr || !line || !tr.dataset.market) return null;
  const td = target.closest('td');
  if (td && td.classList.contains('novig-odds-cell')) return { error: 'That column is the fair line itself — click a book’s price.' };
  if (tr.dataset.prop) return { error: 'Props aren’t supported in the boost calculator yet.' };

  const pair = line.parentElement;
  const siblings = Array.from(pair ? pair.querySelectorAll(':scope > .odds-row, :scope > .bc-line, :scope > .ev-line') : []);
  const isTop = line.dataset.side ? line.dataset.side === 'top' : siblings.indexOf(line) <= 0;
  const side = isTop ? 'upper' : 'lower';

  const oddsEl = line.querySelector('.odds-num:not(.na), .bc-odds:not(.na)');
  const odds = parseAm(oddsEl && oddsEl.textContent);
  if (odds == null) return { error: 'No price in that cell.' };
  const ptEl = line.querySelector('.best-pt, .bc-pt');
  const pt = ptEl ? parsePt(ptEl.textContent) : null;
  const tagEl = line.querySelector('.book-tag, .ev-book');
  // Book cells carry the DISPLAY name in data-book; best/+EV cells a display-name tag.
  const bkName = (td && td.dataset.book) || (tagEl && tagEl.textContent.trim()) || '';
  const bk = bookKeyFromDisplay(bkName) || bkName || null;

  const sk = typeof activeTab !== 'undefined' ? activeTab : null;
  const data = sk && typeof tabState !== 'undefined' && tabState[sk] ? tabState[sk].data || [] : [];
  const isAlt = tr.classList.contains('alt-row');
  const mkt = tr.dataset.market;
  const row = isAlt
    ? data.find(r => r.game_id === tr.dataset.ev && r.market === mkt)
    : data.find(r => r.away_team === tr.dataset.away && r.home_team === tr.dataset.home && r.market === mkt);

  const isTotals = typeof isTotalsType === 'function' && isTotalsType(mkt);
  const sideName = isTotals ? (isTop ? 'Over' : 'Under') : (isTop ? tr.dataset.away : tr.dataset.home);
  const mktLbl = (typeof MKT_LABELS !== 'undefined' && MKT_LABELS[mkt]) || mkt;
  const ptLbl = pt == null ? '' : isTotals ? ' ' + pt : ' ' + (pt > 0 ? '+' + pt : pt);
  const bookName = (typeof BM_DISPLAY !== 'undefined' && BM_DISPLAY[bk]) || bkName || '?';
  const label = `${sideName}${ptLbl} ${fmtAm(odds)} @ ${bookName} — ${mktLbl}${isAlt ? ' (alt)' : ''}, ${tr.dataset.away} @ ${tr.dataset.home}`;

  const f = row ? fairAtLine(row, sk, side, pt, bk, isAlt) : null;
  return {
    ctx: { sport: tr.dataset.sport, away: tr.dataset.away, home: tr.dataset.home, time: tr.dataset.time, market: mkt,
           side: sideName, line: ptEl ? ptEl.textContent.trim() : '', odds: fmtAm(odds), book: bookName,
           sk, gid: row ? row.game_id : null, sideKey: isTotals ? (isTop ? 'over' : 'under') : (isTop ? 'away' : 'home'),
           point: pt, bk, fair: f ? f.p : null, isAlt },
    label, bookOdds: odds,
    fairProb: f ? f.p : null,
    fairSrc: f ? f.how : '',
    warn: !row ? 'Couldn’t find this game’s data — type the fair odds.'
        : !f ? 'No trusted fair value at this line — type the fair odds.'
        : f.model ? 'Fair comes from the model, not a real sharp price — treat with care.' : '',
  };
}

function resolveEvRow(tr) {
  const ds = tr.dataset;
  const odds = parseAm(ds.odds);
  if (odds == null) return null;
  let p = ds.fair ? parseFloat(ds.fair) : null;
  if (!(p > 0 && p < 1) && ds.nvOdds) { const fd = PromoMath.amToDec(ds.nvOdds); p = fd ? 1 / fd : null; }
  const mktLbl = (typeof MKT_LABELS !== 'undefined' && MKT_LABELS[ds.market]) || ds.market;
  return {
    ctx: { sport: ds.sport, away: ds.away, home: ds.home, time: ds.time, market: ds.market, side: ds.side, line: ds.line,
           odds: fmtAm(odds), book: ds.book, sk: ds.sk, gid: ds.gid, sideKey: ds.sideKey, point: ds.point, bk: ds.bk,
           fair: p > 0 && p < 1 ? p : null, isAlt: ds.alt === '1' },
    label: `${ds.side}${ds.line ? ' ' + ds.line : ''} ${fmtAm(odds)} @ ${ds.book} — ${mktLbl}${ds.alt === '1' ? ' (alt)' : ''}, ${ds.away} @ ${ds.home}`,
    bookOdds: odds,
    fairProb: p > 0 && p < 1 ? p : null,
    fairSrc: 'FAIR @ LINE from the +EV scan',
    warn: p > 0 && p < 1 ? '' : 'No fair value on this row — type the fair odds.',
  };
}

// ⚡ button on every +EV tab row. It's added after each render by watching the table,
// so index.html's row template stays untouched.
const tbl = document.getElementById('tbl-outer');
const addRowButtons = () => {
  tbl.querySelectorAll('tr.opp-row-ev:not([data-player]), tr.opp-row-ev[data-player=""]').forEach(tr => {
    if (tr.querySelector('.pr-row-btn')) return;
    const td = tr.querySelector('td.opp-value') || tr.lastElementChild;
    if (td) td.insertAdjacentHTML('beforeend', '<button class="pr-row-btn" title="Price this bet with a promo (boost calculator)">⚡ BOOST</button>');
  });
};
if (tbl) {
  new MutationObserver(addRowButtons).observe(tbl, { childList: true, subtree: true });
  addRowButtons();
}

// Capture phase on document: runs before index.html's own table handlers
// (bet modal, no-vig "set best price"), and only for shift+clicks or the ⚡ button.
document.addEventListener('mousedown', e => {
  if (e.shiftKey && e.target.closest && e.target.closest('#tbl-outer')) e.preventDefault();   // no text selection
}, true);
document.addEventListener('click', e => {
  if (!e.target.closest) return;
  const rowBtn = e.target.closest('.pr-row-btn');
  if (rowBtn) {
    e.stopPropagation(); e.preventDefault();
    const info = resolveEvRow(rowBtn.closest('tr'));
    if (info) load(info);
    return;
  }
  if (!e.shiftKey || !e.target.closest('#tbl-outer')) return;
  const evRow = e.target.closest('.opp-row-ev');
  let info = null;
  if (evRow && !evRow.dataset.player) info = resolveEvRow(evRow);
  else if (!evRow) info = resolveGridClick(e.target);
  if (!info) return;
  e.stopPropagation(); e.preventDefault();
  if (info.error) {
    bet = { label: info.error }; renderBet(); openPanel();
    return;
  }
  load(info);
}, true);
})();
