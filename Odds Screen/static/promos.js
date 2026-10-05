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
#promo-btn {
  flex-shrink:0; display:flex; align-items:center; gap:5px; padding:0 14px;
  font-family:var(--mono); font-size:10px; font-weight:700; letter-spacing:.08em;
  text-transform:uppercase; color:var(--t3); white-space:nowrap;
  border:none; border-left:1px solid var(--border2); background:none; cursor:pointer;
  transition:color .15s, background .15s;
}
#promo-btn:hover { color:var(--t2); background:rgba(255,255,255,.02); }
#promo-btn.open  { color:var(--amber); background:var(--amber-dim); }
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
.pr-sec { border-top:1px solid var(--border2); margin-top:12px; padding-top:10px; }
.pr-sec-h { display:flex; justify-content:space-between; align-items:center; font-family:var(--mono); font-size:9px;
  font-weight:700; letter-spacing:.12em; text-transform:uppercase; color:var(--t3); margin-bottom:6px; }
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
.pr-form { display:none; background:var(--surface); border:1px solid var(--border2); border-radius:6px; padding:8px; margin-bottom:8px; }
.pr-form.open { display:block; }
.pr-form label { display:flex; justify-content:space-between; align-items:center; gap:8px; font-family:var(--mono); font-size:9px; color:var(--t3); margin:4px 0; }
.pr-form input, .pr-form select { width:160px; background:var(--surface2); border:1px solid var(--border2); border-radius:4px;
  color:var(--t1); font-family:var(--mono); font-size:10px; padding:3px 5px; }
.pr-chks { display:flex; flex-wrap:wrap; gap:4px 8px; justify-content:flex-end; max-width:220px; }
.pr-chks label { margin:0; gap:3px; color:var(--t2); }
.pr-chks input { width:auto; }
`;
document.head.insertAdjacentHTML('beforeend', `<style>${css}</style>`);

// ── DOM ──────────────────────────────────────────────────────────────────
const btn = document.createElement('button');
btn.id = 'promo-btn';
btn.title = 'Promo / boost EV calculator — shift+click any price to load it';
btn.innerHTML = `<svg width="13" height="13" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.2" stroke-linecap="round" stroke-linejoin="round"><polyline points="23 6 13.5 15.5 8.5 10.5 1 18"/><polyline points="17 6 23 6 23 12"/></svg>BOOST`;
const nvBtn = document.getElementById('nv-conv-btn');
const tabRow = document.getElementById('tab-bar-row');
if (nvBtn) nvBtn.before(btn); else if (tabRow) tabRow.appendChild(btn);

const panel = document.createElement('div');
panel.id = 'promo-panel';
panel.innerHTML = `
  <div class="pr-title">Promo / Boost Calculator <button class="pr-x" title="Close">✕</button></div>
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
  <div class="pr-sec">
    <div class="pr-sec-h"><span>Saved boosts · best uses</span><button class="pr-mini" id="pr-add-btn">+ ADD BOOST</button></div>
    <div class="pr-form" id="pr-form">
      <label>Book <select id="pf-book"></select></label>
      <label>Type <select id="pf-type"><option value="profit">Profit boost %</option><option value="nosweat">No-sweat (refund as bonus bet)</option><option value="bonus">Bonus bet</option></select></label>
      <label><span id="pf-value-lbl">Boost %</span> <input id="pf-value" type="number" step="1" placeholder="30"></label>
      <label><span id="pf-stake-lbl">Max stake $</span> <input id="pf-stake" type="number" step="1" placeholder="25"></label>
      <label>Sports <span class="pr-chks" id="pf-sports"></span></label>
      <label>Markets <span class="pr-chks" id="pf-markets"></span></label>
      <label>Periods <span class="pr-chks" id="pf-periods"></span></label>
      <label>Odds this or longer <input id="pf-min" type="text" placeholder="-200 (blank = any)"></label>
      <label>Odds this or shorter <input id="pf-max" type="text" placeholder="+500 (blank = any)"></label>
      <label>Expires <input id="pf-exp" type="date"></label>
      <label>Name / notes <input id="pf-title" type="text" placeholder="e.g. Sunday NFL 30%"></label>
      <div style="display:flex;gap:6px;justify-content:flex-end;margin-top:6px">
        <button class="pr-mini" id="pf-cancel">CANCEL</button><button class="pr-mini" id="pf-save" style="color:var(--green);border-color:var(--green)">SAVE BOOST</button>
      </div>
    </div>
    <div id="pr-promos"><div class="pr-none">No saved boosts yet. Add one as it arrives; the best places to use it show up here.</div></div>
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
  panel.classList.add('open'); btn.classList.add('open');
  if (typeof nvClosePanel === 'function') nvClosePanel();
  syncParamField(); compute();
  loadPromos();
}
function closePanel() { panel.classList.remove('open'); btn.classList.remove('open'); }
btn.addEventListener('click', e => { e.stopPropagation(); panel.classList.contains('open') ? closePanel() : openPanel(); });
panel.querySelector('.pr-x').addEventListener('click', closePanel);

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
                 boostNote: `${kind}: ${fmtAm(logBoost.bookAm)} → ${fmtAm(boostedAm)} at ${bet.ctx.book}` });
});

// ── Saved boosts + best uses ─────────────────────────────────────────────
// Boosts are stored server-side (/api/promos, promos_store.py) so they persist
// all week. For each active one, every price at its book on the loaded boards
// that meets its terms (sport, market, period, odds range, not started) is
// priced at the sharp fair value (same engine as the +EV tab) and ranked by
// expected dollars at the stake the boost allows.
let promos = [];
const SPORT_CHOICES = () => (typeof SPORTS_LIST !== 'undefined' ? SPORTS_LIST : [])
  .map(sk => [sk, (typeof SPORT_META !== 'undefined' && SPORT_META[sk] && SPORT_META[sk].label) || sk]);
const MARKET_CHOICES = [['h2h', 'ML'], ['spreads', 'Spread'], ['totals', 'Total']];
const PERIOD_CHOICES = [['full', 'Full'], ['h1', '1H'], ['q1', 'Q1']];
const bookName = bk => (typeof BM_DISPLAY !== 'undefined' && BM_DISPLAY[bk]) || bk;

async function loadPromos() {
  try {
    const r = await fetch('/api/promos');
    promos = r.ok ? await r.json() : [];
  } catch (e) { promos = []; }
  renderPromos();
}

function promoTitle(pr) {
  const t = pr.type === 'profit' ? `${pr.value}% profit boost` : pr.type === 'nosweat' ? 'No-sweat' : pr.type === 'bonus' ? 'Bonus bet' : 'Boost';
  return `${bookName(pr.book_key)} · ${pr.title || t}`;
}
function promoCrit(pr) {
  const parts = [];
  if (pr.max_stake) parts.push(`$${pr.max_stake} ${pr.type === 'bonus' ? 'bet' : pr.type === 'nosweat' ? 'max refund' : 'max'}`);
  if (pr.sports && pr.sports.length) parts.push(pr.sports.map(sk => (SPORT_CHOICES().find(x => x[0] === sk) || [, sk])[1]).join('/'));
  if (pr.markets && pr.markets.length) parts.push(pr.markets.map(m => (MARKET_CHOICES.find(x => x[0] === m) || [, m])[1]).join('/'));
  if (pr.periods && pr.periods.length) parts.push(pr.periods.map(m => (PERIOD_CHOICES.find(x => x[0] === m) || [, m])[1]).join('/'));
  if (pr.min_odds != null) parts.push(`${fmtAm(pr.min_odds)} or longer`);
  if (pr.max_odds != null) parts.push(`up to ${fmtAm(pr.max_odds)}`);
  if (pr.expires_at) parts.push(`expires ${new Date(pr.expires_at).toLocaleDateString([], { month: 'short', day: 'numeric' })}`);
  return parts.join(' · ') || 'any bet';
}

// Every eligible price at the promo's book, ranked by expected $ at the allowed stake.
function bestUses(pr, limit = 3) {
  if (typeof tabState === 'undefined' || typeof bookSides !== 'function') return [];
  const now = Date.now(), out = [];
  const decMin = pr.min_odds != null ? PromoMath.amToDec(pr.min_odds) : null;
  const decMax = pr.max_odds != null ? PromoMath.amToDec(pr.max_odds) : null;
  (typeof SPORTS_LIST !== 'undefined' ? SPORTS_LIST : []).forEach(sk => {
    if (pr.sports && pr.sports.length && !pr.sports.includes(sk)) return;
    const ts = tabState[sk];
    if (!ts || !ts.loaded || !ts.data) return;
    ts.data.forEach(row => {
      const m = String(row.market).match(/^(h2h|spreads|totals)(?:_(h1|q1))?$/);
      if (!m) return;
      if (pr.markets && pr.markets.length && !pr.markets.includes(m[1])) return;
      if (pr.periods && pr.periods.length && !pr.periods.includes(m[2] || 'full')) return;
      if (row.commence_time && new Date(row.commence_time).getTime() <= now) return;
      const entry = row.books && row.books[pr.book_key];
      if (!entry) return;
      const isTot = m[1] === 'totals';
      const bs = bookSides(entry, isTot);
      [['upper', bs.u, bs.uPt], ['lower', bs.l, bs.lPt]].forEach(([side, oddsS, pt]) => {
        const am = parseAm(oddsS), d = am != null ? PromoMath.amToDec(am) : null;
        if (!d || (decMin && d < decMin - 1e-9) || (decMax && d > decMax + 1e-9)) return;
        const f = fairAtLine(row, sk, side, pt, pr.book_key, false);
        if (!f) return;
        let ev, stake, effDec = null;
        if (pr.type === 'profit') {
          effDec = PromoMath.boostedDec(d, (pr.value || 0) / 100);
          ev = f.p * effDec - 1;
          const kf = ev > 0 ? PromoMath.kelly(f.p, effDec) : null;
          const bank = typeof kellyBankroll !== 'undefined' ? kellyBankroll : null;
          const frac = typeof kellyFraction !== 'undefined' ? kellyFraction : 0.5;
          stake = kf != null && bank > 0 ? Math.min(pr.max_stake || Infinity, bank * Math.min(kf, 1) * frac) : (pr.max_stake || 0);
        } else if (pr.type === 'nosweat') {
          ev = PromoMath.ev('nosweat', f.p, d, { refundRate: pr.refund_rate ?? 0.7 }); stake = pr.max_stake || 0;
        } else if (pr.type === 'bonus') {
          ev = PromoMath.ev('bonus', f.p, d); stake = pr.max_stake || 0;
        } else return;
        const sideName = isTot ? (side === 'upper' ? 'Over' : 'Under') : (side === 'upper' ? row.away_team : row.home_team);
        const ptLbl = isTot ? (pt != null ? ' ' + pt : '') : (pt != null && !m[1].startsWith('h2h') ? ' ' + (pt > 0 ? '+' + pt : pt) : '');
        const per = m[2] ? m[2].toUpperCase() + ' ' : '';
        out.push({ ev, dollars: ev * stake, stake, effDec, fair: f.p, am, row, sk, side, pt, model: f.model,
                   label: `${per}${sideName}${ptLbl}`, game: `${row.away_team} @ ${row.home_team}`,
                   ctx: { sport: (SPORT_CHOICES().find(x => x[0] === sk) || [, sk])[1], away: row.away_team, home: row.home_team,
                          time: row.commence_time, market: row.market, side: sideName, line: ptLbl.trim(), odds: fmtAm(am),
                          book: bookName(pr.book_key), sk, gid: row.game_id,
                          sideKey: isTot ? (side === 'upper' ? 'over' : 'under') : (side === 'upper' ? 'away' : 'home'),
                          point: pt, bk: pr.book_key, fair: f.p, isAlt: false } });
      });
    });
  });
  out.sort((a, b) => b.dollars - a.dollars || b.ev - a.ev);
  return out.slice(0, limit);
}

function renderPromos() {
  const box = $('pr-promos');
  if (!promos.length) {
    box.innerHTML = '<div class="pr-none">No saved boosts yet. Add one as it arrives; the best places to use it show up here.</div>';
    return;
  }
  const anyLoaded = typeof SPORTS_LIST !== 'undefined' && SPORTS_LIST.some(sk => tabState[sk] && tabState[sk].loaded);
  box.innerHTML = promos.map(pr => {
    const uses = anyLoaded ? bestUses(pr) : [];
    pr._uses = uses;
    const list = !anyLoaded ? '<div class="pr-none">Load a sport to see where to use it.</div>'
      : !uses.length ? '<div class="pr-none">Nothing on the loaded boards fits these terms.</div>'
      : uses.map((u, i) => `<div class="pr-use"><span>${escH(u.label)} ${fmtAm(u.am)}${u.effDec ? ` → ${fmtAm(PromoMath.decToAm(u.effDec))}` : ''}
          <span style="color:var(--t3)">· ${escH(u.game)}</span><br>
          <span class="${u.ev > 0 ? 'pos' : 'neg'}">${fmtPct(u.ev)}</span> · ${fmtUsd(u.dollars)} on $${Math.round(u.stake)}${u.model ? ' <span style="color:var(--amber)" title="Fair value from the model, not a real sharp price">(model)</span>' : ''}</span>
          <button data-pid="${pr.id}" data-i="${i}" class="pr-use-btn">USE</button></div>`).join('');
    return `<div class="pr-promo"><div class="pr-promo-h"><b>${escH(promoTitle(pr))}</b>
        <span><button class="pr-mini pr-used" data-pid="${pr.id}" title="Mark used">USED</button>
        <button class="pr-mini pr-del" data-pid="${pr.id}" title="Delete">✕</button></span></div>
      <div class="pr-crit">${escH(promoCrit(pr))}</div>${list}</div>`;
  }).join('');
}

$('pr-promos').addEventListener('click', async e => {
  const use = e.target.closest('.pr-use-btn'), used = e.target.closest('.pr-used'), del = e.target.closest('.pr-del');
  if (use) {
    const pr = promos.find(p => String(p.id) === use.dataset.pid), u = pr && pr._uses[+use.dataset.i];
    if (!u) return;
    if (pr.type === 'profit') { state.type = 'profit'; state.boostPct = pr.value; }
    else if (pr.type === 'nosweat') { state.type = 'nosweat'; state.refundPct = Math.round((pr.refund_rate ?? 0.7) * 100); }
    else if (pr.type === 'bonus') state.type = 'bonus';
    if (pr.max_stake) state.maxStake = pr.max_stake;
    save();
    load({ label: `${u.label} ${fmtAm(u.am)} @ ${bookName(pr.book_key)}, ${u.game}`, bookOdds: u.am, fairProb: u.fair,
           fairSrc: u.model ? 'outcome model past the last sharp price' : 'sharp fair at this line',
           warn: u.model ? 'Fair comes from the model, not a real sharp price — treat with care.' : '', ctx: u.ctx });
    return;
  }
  const pid = (used || del) && (used || del).dataset.pid;
  if (!pid) return;
  if (del && !confirm('Delete this boost?')) return;
  await fetch(`/api/promos/${pid}`, used ? { method: 'PATCH', headers: { 'Content-Type': 'application/json' },
                                             body: JSON.stringify({ status: 'used' }) } : { method: 'DELETE' });
  loadPromos();
});

// Add-boost form
function fillPromoForm() {
  const books = (typeof BOOKMAKERS !== 'undefined' ? BOOKMAKERS : []).filter(bk => !(typeof SHARP_BOOKS !== 'undefined' && SHARP_BOOKS.has(bk)));
  $('pf-book').innerHTML = books.map(bk => `<option value="${bk}">${escH(bookName(bk))}</option>`).join('');
  const chk = (id, list, name) => { $(id).innerHTML = list.map(([v, l]) => `<label><input type="checkbox" name="${name}" value="${v}">${escH(l)}</label>`).join(''); };
  chk('pf-sports', SPORT_CHOICES(), 'pf-sport'); chk('pf-markets', MARKET_CHOICES, 'pf-market'); chk('pf-periods', PERIOD_CHOICES, 'pf-period');
}
function syncPromoForm() {
  const t = $('pf-type').value;
  $('pf-value-lbl').textContent = t === 'profit' ? 'Boost %' : t === 'nosweat' ? 'Refund converts %' : 'Bonus amount $';
  $('pf-value').placeholder = t === 'profit' ? '30' : t === 'nosweat' ? '70' : '25';
  $('pf-stake-lbl').parentElement.style.display = t === 'bonus' ? 'none' : '';
  $('pf-stake-lbl').textContent = t === 'nosweat' ? 'Max refund $' : 'Max stake $';
}
$('pr-add-btn').addEventListener('click', () => { fillPromoForm(); syncPromoForm(); $('pr-form').classList.toggle('open'); });
$('pf-type').addEventListener('change', syncPromoForm);
$('pf-cancel').addEventListener('click', () => $('pr-form').classList.remove('open'));
$('pf-save').addEventListener('click', async () => {
  const t = $('pf-type').value, v = num($('pf-value').value);
  const picked = name => [...document.querySelectorAll(`input[name="${name}"]:checked`)].map(i => i.value);
  const exp = $('pf-exp').value;
  const body = {
    book_key: $('pf-book').value, type: t, title: $('pf-title').value.trim() || null,
    value: t === 'profit' ? v : null,
    refund_rate: t === 'nosweat' ? (v != null ? v / 100 : 0.7) : null,
    max_stake: t === 'bonus' ? v : num($('pf-stake').value),
    sports: picked('pf-sport'), markets: picked('pf-market'), periods: picked('pf-period'),
    min_odds: parseAm($('pf-min').value), max_odds: parseAm($('pf-max').value),
    expires_at: exp ? new Date(exp + 'T23:59:59').toISOString() : null,
  };
  if (t === 'profit' && !(v > 0)) { alert('Enter the boost %'); return; }
  const r = await fetch('/api/promos', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(body) });
  if (!r.ok) { alert('Could not save: ' + ((await r.json()).error || r.status)); return; }
  $('pr-form').classList.remove('open');
  ['pf-value', 'pf-stake', 'pf-min', 'pf-max', 'pf-exp', 'pf-title'].forEach(id => { $(id).value = ''; });
  loadPromos();
});

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
