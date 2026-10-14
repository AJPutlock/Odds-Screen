/* CFB Live hub - scoreboard grid.
 *
 * Auto-refreshes every 30s while the tab is visible, and on the Refresh button.
 * This is free: /api/scoreboard reads the local database, which the background
 * loops keep current - it does not call ESPN or The Odds API, so polling it
 * costs no quota.
 */

const POLL_MS = 30000;

let activeState = 'in';
let lastPayload = null;
let lastLoadedAt = null;
let lastError = null;
let pollTimer = null;

// ---- formatting helpers ----

function fmtOdds(price) {
  if (price === null || price === undefined) return '';
  return price > 0 ? '+' + price : String(price);
}

function fmtPoint(point) {
  if (point === null || point === undefined) return '';
  return point > 0 ? '+' + point : String(point);
}

function fmtKick(iso) {
  if (!iso) return '';
  const d = new Date(iso);
  return d.toLocaleTimeString([], { hour: 'numeric', minute: '2-digit' });
}

function esc(s) {
  if (s === null || s === undefined) return '';
  return String(s).replace(/[&<>"']/g, c => (
    { '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]
  ));
}

// ---- card rendering ----

// Small football shown beside whichever team has possession. Same shape and
// palette as the app's desktop icon, so the two read as one family.
const FOOTBALL_SVG =
  '<svg class="poss-ball" viewBox="0 0 16 10" width="14" height="9" aria-label="has possession">' +
  '<ellipse cx="8" cy="5" rx="7.1" ry="4.1" fill="#96522d" stroke="#6c381d" stroke-width="1"/>' +
  '<line x1="5.2" y1="5" x2="10.8" y2="5" stroke="#fff" stroke-width="1.1"/>' +
  '<line x1="6.6" y1="3.7" x2="6.6" y2="6.3" stroke="#fff" stroke-width="1"/>' +
  '<line x1="8" y1="3.6" x2="8" y2="6.4" stroke="#fff" stroke-width="1"/>' +
  '<line x1="9.4" y1="3.7" x2="9.4" y2="6.3" stroke="#fff" stroke-width="1"/>' +
  '</svg>';

function teamRow(game, side) {
  const t = {
    name: game[side + '_team'],
    abbr: game[side + '_abbr'],
    score: game[side + '_score'],
    rank: game[side + '_rank'],
    rec: game[side + '_record'],
    logo: game[side + '_logo'],
    id: game[side + '_id'],
  };
  const live = game.state === 'in';
  const other = side === 'home' ? game.away_score : game.home_score;
  const leading = live || game.state === 'post'
    ? (t.score !== null && other !== null && t.score > other)
    : false;
  const hasPoss = live && game.possession_id && String(game.possession_id) === String(t.id);

  return `
    <div class="team-row">
      <img class="team-logo" src="${esc(t.logo || '')}" alt="" onerror="this.style.visibility='hidden'">
      <div class="team-name ${leading ? 'leading' : ''}">
        <span class="poss ${hasPoss ? '' : 'hidden'}">${FOOTBALL_SVG}</span>
        ${t.rank ? `<span class="rank">#${t.rank}</span>` : ''}
        <span class="nm">${esc(t.name || t.abbr || '')}</span>
        ${t.rec ? `<span class="rec">${esc(t.rec)}</span>` : ''}
      </div>
      <div class="score ${game.state === 'pre' ? 'dim' : ''}">${t.score === null || t.score === undefined ? '-' : t.score}</div>
    </div>`;
}

function oddsStrip(game) {
  const o = game.odds || {};
  const bits = [];

  if (o.spreads && o.spreads.sides) {
    const s = o.spreads.sides.home;
    if (s) {
      bits.push(`<div class="odds-item">
        <span class="odds-label">Spread</span>
        <span class="odds-val">${esc(game.home_abbr || 'H')} ${fmtPoint(s.point)} ${fmtOdds(s.price)}</span>
        <span class="odds-book">${esc(s.book || '')}</span>
        ${s.is_live ? '<span class="live-tag">LIVE</span>' : ''}
      </div>`);
    }
  }

  if (o.totals && o.totals.sides) {
    const ov = o.totals.sides.over;
    if (ov) {
      bits.push(`<div class="odds-item">
        <span class="odds-label">Total</span>
        <span class="odds-val">o${ov.point} ${fmtOdds(ov.price)}</span>
        <span class="odds-book">${esc(ov.book || '')}</span>
      </div>`);
    }
  }

  if (o.h2h && o.h2h.sides) {
    const h = o.h2h.sides.home, a = o.h2h.sides.away;
    if (h && a) {
      bits.push(`<div class="odds-item">
        <span class="odds-label">ML</span>
        <span class="odds-val">${esc(game.away_abbr)} ${fmtOdds(a.price)} / ${esc(game.home_abbr)} ${fmtOdds(h.price)}</span>
      </div>`);
    }
  }

  if (!bits.length) return '';
  return `<div class="odds-strip">${bits.join('')}</div>`;
}

// Absolute yard line (0 = home goal, 100 = away goal) -> the way a broadcast
// says it: "BYU 43", "ISU 3", or "50". Verified against ESPN's own text on live
// games (yardLine 57 = "ISU 43", 97 = "ISU 3", 25 = "BYU 25").
function yardMarker(game, yl) {
  if (yl === null || yl === undefined) return '';
  if (yl === 50) return '50';
  if (yl < 50) return `${game.home_abbr || 'HOME'} ${yl}`;
  return `${game.away_abbr || 'AWAY'} ${100 - yl}`;
}

function fieldMarker(game) {
  // A 100-yard field with the ball on it: who has it, where, and how far to go.
  //
  // ESPN's yardLine is an ABSOLUTE coordinate - 0 is the home team's goal line,
  // 100 is the away team's - and does not flip with possession. Verified against
  // downDistanceText on live games (an away team on its own 25 reads 75).
  //
  // So the field is drawn with the home end zone on the left and the away end
  // zone on the right, and the ball sits at yardLine% from the left. The home
  // team attacks rightward, the away team leftward.
  if (game.state !== 'in') return '';

  const yl = game.yard_line;
  if (yl === null || yl === undefined) {
    // Distinguish "ESPN has stopped sending play-by-play for this game" from
    // "nothing has happened yet". ESPN's own playByPlayAvailable flag is an
    // honest signal here - when it goes false, situation and drives both vanish
    // across every ESPN endpoint (site.api, core.api and cdn.espn.com alike).
    const msg = game.pbp_available
      ? 'Waiting on first snap'
      : 'ESPN play-by-play offline for this game';
    return `<div class="field-none">${msg}</div>`;
  }

  const possId = game.possession_id ? String(game.possession_id) : null;
  const homeHasBall = possId && possId === String(game.home_id);
  const awayHasBall = possId && possId === String(game.away_id);
  const possAbbr = homeHasBall ? game.home_abbr : (awayHasBall ? game.away_abbr : null);

  // Home drives toward 100 (right), away drives toward 0 (left).
  const dir = homeHasBall ? 1 : (awayHasBall ? -1 : 0);

  // First-down line, when there is a live down and it isn't goal-to-go.
  let firstDown = null;
  if (dir !== 0 && game.down >= 1 && game.down <= 4 && game.distance > 0) {
    const target = yl + dir * game.distance;
    if (target > 0 && target < 100) firstDown = target;
  }

  // ESPN's own text already reads "1st & 10 at BYU 28", spot included, so it is
  // the single line that carries everything. When a repaired spot replaced it
  // (ESPN's text would describe the stale position), rebuild it in the same
  // shape rather than dropping the yard marker.
  const hasDown = game.down >= 1 && game.down <= 4
    && game.distance !== null && game.distance !== undefined;
  let label = game.down_distance_text || '';
  if (!label && hasDown) {
    const spot = yardMarker(game, yl);
    label = `${['', '1st', '2nd', '3rd', '4th'][game.down]} & ${game.distance}`
      + (spot ? ` at ${spot}` : '');
  }

  const arrow = dir === 1 ? '&#9654;' : (dir === -1 ? '&#9664;' : '');
  const stale = game.situation_at
    ? (Date.now() - new Date(game.situation_at).getTime()) / 1000 > 90
    : false;

  return `
    <div class="field ${game.is_red_zone ? 'rz' : ''} ${stale ? 'stale' : ''}">
      <div class="field-ez left">${esc(game.home_abbr || '')}</div>
      <div class="field-strip">
        ${[20, 80].map(p => `<div class="field-tick rz-line" style="left:${p}%"></div>`).join('')}
        <div class="field-50"></div>
        ${[[20, '20'], [50, '50'], [80, '20']].map(
          ([p, n]) => `<div class="field-num" style="left:${p}%">${n}</div>`
        ).join('')}
        ${firstDown !== null ? `<div class="field-fd" style="left:${firstDown}%"></div>` : ''}
        <div class="field-ball" style="left:${Math.max(0, Math.min(100, yl))}%">
          ${dir === -1 ? '<span class="field-arrow left"></span>' : ''}
          <span class="field-ball-dot ${dir === 0 ? 'no-poss' : ''}"></span>
          ${dir === 1 ? '<span class="field-arrow right"></span>' : ''}
        </div>
      </div>
      <div class="field-ez right">${esc(game.away_abbr || '')}</div>
    </div>
    <div class="field-legend">
      <span class="field-poss">${possAbbr ? `${arrow} ${esc(possAbbr)}` : '&mdash;'}</span>
      <span>${esc(label)}${game.is_red_zone ? ' <span class="rz-tag">RZ</span>' : ''}</span>
    </div>`;
}

function card(game) {
  const live = game.state === 'in';
  const cls = live ? 'is-live' : (game.state === 'post' ? 'is-final' : '');
  const clock = live
    ? `<span class="clock live">${esc(game.status_detail || '')}</span>`
    : (game.state === 'post'
      ? `<span class="clock">${esc(game.status_detail || 'Final')}</span>`
      : `<span class="clock">${esc(fmtKick(game.start_time))}</span>`);

  return `
    <div class="card ${cls}" onclick="location.href='/game/${esc(game.game_id)}'">
      <div class="card-top">
        ${clock}
        <span class="tv">${esc(game.broadcast || '')}</span>
      </div>
      ${teamRow(game, 'away')}
      ${teamRow(game, 'home')}
      ${fieldMarker(game)}
      ${live && game.last_play ? `<div class="lastplay">${esc(game.last_play)}</div>` : ''}
      ${oddsStrip(game)}
    </div>`;
}

// ---- data flow ----

function render(payload) {
  lastPayload = payload;
  const counts = payload.counts || {};
  document.getElementById('c-in').textContent = counts.in || 0;
  document.getElementById('c-pre').textContent = counts.pre || 0;
  document.getElementById('c-post').textContent = counts.post || 0;
  document.getElementById('c-all').textContent =
    (counts.in || 0) + (counts.pre || 0) + (counts.post || 0);

  let games = payload.games || [];
  if (activeState !== 'all') games = games.filter(g => g.state === activeState);

  // Live games first and closest-to-finishing on top; otherwise by kickoff.
  games.sort((a, b) => {
    if (a.state !== b.state) {
      const order = { in: 0, pre: 1, post: 2 };
      return (order[a.state] ?? 3) - (order[b.state] ?? 3);
    }
    if (a.state === 'in') return (b.elapsed_seconds || 0) - (a.elapsed_seconds || 0);
    return String(a.start_time || '').localeCompare(String(b.start_time || ''));
  });

  const board = document.getElementById('board');
  const empty = document.getElementById('empty');
  if (!games.length) {
    board.innerHTML = '';
    empty.style.display = 'block';
    empty.textContent = activeState === 'in'
      ? 'No games in progress right now.'
      : 'Nothing to show for this filter.';
  } else {
    empty.style.display = 'none';
    board.innerHTML = games.map(card).join('');
  }

  lastLoadedAt = Date.now();
  paintFreshness();
}

// The indicator must report when THIS PAGE last pulled, not when the server
// last polled ESPN. It used to show the server's timestamp, which meant the
// header read "just now" while the cards on screen were minutes old - the
// server was refreshing fine, the page simply had not re-fetched.
function paintFreshness() {
  const pulse = document.getElementById('pulse');
  const label = document.getElementById('updated');

  // A failed fetch must survive this ticker. Without this branch the catch in
  // load() would write "error: ..." and the 1s repaint would immediately
  // replace it with a bare "not loaded", hiding why it failed - which is
  // exactly what happens during a server restart.
  if (lastError) {
    pulse.className = 'dot dead';
    label.textContent = lastLoadedAt === null
      ? 'offline - ' + lastError
      : 'stale (' + Math.round((Date.now() - lastLoadedAt) / 1000) + 's) - retrying';
    return;
  }

  if (lastLoadedAt === null) {
    pulse.className = 'dot dead';
    label.textContent = 'loading...';
    return;
  }
  const age = Math.round((Date.now() - lastLoadedAt) / 1000);
  pulse.className = 'dot' + (age > 120 ? ' dead' : (age > 75 ? ' stale' : ''));
  label.textContent = age < 5 ? 'just now' : age + 's ago';

  // Odds run on a 3-minute cadence while the board refreshes every 30s, so the
  // two ages are shown separately - otherwise the prices look stale when they
  // are simply on a slower (metered) clock.
  const oddsEl = document.getElementById('odds-age');
  if (oddsEl && lastPayload && lastPayload.last_odds_at) {
    const oa = Math.round((Date.now() - new Date(lastPayload.last_odds_at).getTime()) / 1000);
    oddsEl.textContent = 'odds ' + (oa < 60 ? oa + 's' : Math.floor(oa / 60) + 'm ' + (oa % 60) + 's') + ' ago';
    oddsEl.className = 'odds-age' + (oa > 420 ? ' stale' : '');
  }
}

async function load() {
  try {
    const r = await fetch('/api/scoreboard');
    if (!r.ok) throw new Error('HTTP ' + r.status);
    render(await r.json());
    lastError = null;          // recovered
  } catch (e) {
    // Keep whatever is already on screen rather than blanking the board - a
    // brief server restart should not wipe the games you are watching. The
    // 30s poll retries on its own.
    lastError = e.message;
  }
  paintFreshness();
}

async function loadStatus() {
  try {
    const s = await (await fetch('/api/status')).json();
    const q = s.odds_quota || {};
    document.getElementById('quota').textContent =
      q.remaining ? 'odds quota ' + q.remaining : '';
  } catch (e) { /* status is decorative; ignore */ }
}

document.getElementById('tabs').addEventListener('click', e => {
  const btn = e.target.closest('.tab');
  if (!btn) return;
  activeState = btn.dataset.state;
  document.querySelectorAll('.tab').forEach(t => t.classList.toggle('active', t === btn));
  if (lastPayload) render(lastPayload);
});

// Manual refresh forces BOTH sides at once: a fresh ESPN scoreboard pull and a
// fresh bulk odds poll, ahead of whatever the background loops had scheduled.
// The bulk poll is 3 credits; the ESPN pull is free.
document.getElementById('refresh').addEventListener('click', async e => {
  const btn = e.target;
  btn.disabled = true;
  btn.textContent = 'Refreshing...';
  try {
    await fetch('/api/refresh', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ what: 'all' }),   // scoreboard + bulk odds
    });
    await load();
    await loadStatus();
  } finally {
    btn.disabled = false;
    btn.textContent = 'Refresh';
  }
});

function startPolling() {
  if (pollTimer) clearInterval(pollTimer);
  pollTimer = setInterval(load, POLL_MS);
}

// Pause while the tab is hidden, and pull immediately on return so you never
// look at a frozen board. This costs nothing extra: /api/scoreboard reads the
// database, it does not call ESPN or The Odds API.
document.addEventListener('visibilitychange', () => {
  if (document.hidden) {
    clearInterval(pollTimer);
    pollTimer = null;
  } else {
    load();
    startPolling();
  }
});

load();
loadStatus();
startPolling();
// Tick the age label every second so it counts up between polls.
setInterval(paintFreshness, 1000);
setInterval(loadStatus, 60000);
