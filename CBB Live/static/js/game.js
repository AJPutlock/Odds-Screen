/* CFB Live hub - single game detail.
 *
 * Game state (score, clock, drives, movement history) auto-refreshes every 30s
 * from the local database - free. The full odds board is NOT on a timer,
 * because it costs an API credit per market; it reloads only on an explicit
 * Refresh / "Reload odds" click.
 */

const GAME_ID = window.GAME_ID;
let allOdds = null;
let openGroups = null;   // null until first render, then a Set of open titles
let currentGame = null;  // latest /api/game payload, for team-name abbreviation

// The Odds API labels rows with full team names ("Alabama Crimson Tide"), which
// wrap badly in a table that already has one column per book. Swap in ESPN's
// abbreviation where we can match it.
function shortTeam(label) {
  if (!currentGame || !label) return label;
  if (label === currentGame.home_team) return currentGame.home_abbr || label;
  if (label === currentGame.away_team) return currentGame.away_abbr || label;
  return label;
}

function esc(s) {
  if (s === null || s === undefined) return '';
  return String(s).replace(/[&<>"']/g, c => (
    { '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]
  ));
}

function fmtOdds(p) {
  if (p === null || p === undefined) return '-';
  return p > 0 ? '+' + p : String(p);
}

function fmtPoint(p) {
  if (p === null || p === undefined) return '';
  return p > 0 ? '+' + p : String(p);
}

function fmtTime(iso) {
  if (!iso) return '';
  return new Date(iso).toLocaleTimeString([], { hour: 'numeric', minute: '2-digit', second: '2-digit' });
}

// ---- sections ----

// ESPN reports down: -1 (or 0) on kickoffs, PATs and between periods. There is
// no live down in those situations, so the clause is suppressed entirely.
function downDistance(g) {
  return '';   // basketball: no downs (kept so head() matches CFB Live)
  if (g.state !== 'in') return '';
  const hasDown = g.down >= 1 && g.down <= 4 && g.distance !== null && g.distance !== undefined;
  if (!hasDown) {
    return g.is_red_zone ? '<div class="detail-status">RED ZONE</div>' : '';
  }
  const ord = ['', '1st', '2nd', '3rd', '4th'][g.down];
  return `<div class="detail-status">${ord} &amp; ${g.distance}${g.is_red_zone ? ' &middot; RED ZONE' : ''}</div>`;
}

function head(g) {
  const statusLine = g.state === 'in'
    ? g.status_detail
    : (g.state === 'post' ? (g.status_detail || 'Final') : new Date(g.start_time).toLocaleString());

  const side = (which) => `
    <div class="detail-team ${which}">
      <img src="${esc(g[which + '_logo'] || '')}" alt="" onerror="this.style.visibility='hidden'">
      <div>
        <div class="t-name">${g[which + '_rank'] ? '#' + g[which + '_rank'] + ' ' : ''}${esc(g[which + '_team'] || '')}</div>
        <div class="t-rec">${esc(g[which + '_record'] || '')}</div>
      </div>
    </div>`;

  return `
    <div class="detail-head">
      <div class="detail-teams">
        ${side('away')}
        <div class="detail-mid">
          <div class="detail-score">${g.away_score ?? '-'} &ndash; ${g.home_score ?? '-'}</div>
          <div class="detail-status">${esc(statusLine)}</div>
          ${downDistance(g)}
        </div>
        ${side('home')}
      </div>
      ${g.venue ? `<div class="detail-status" style="text-align:center;margin-top:10px">${esc(g.venue)}${g.broadcast ? ' &middot; ' + esc(g.broadcast) : ''}</div>` : ''}
    </div>`;
}

function linescore(g) {
  let home = [], away = [];
  try { home = JSON.parse(g.home_linescores || '[]'); } catch (e) { home = []; }
  try { away = JSON.parse(g.away_linescores || '[]'); } catch (e) { away = []; }
  if (!home.length && !away.length) return '';

  const periods = Math.max(home.length, away.length);
  const cols = [];
  for (let i = 0; i < periods; i++) cols.push(i < 2 ? 'H' + (i + 1) : (periods === 3 ? 'OT' : 'OT' + (i - 1)));

  const row = (label, arr, total) => `
    <tr>
      <td>${esc(label)}</td>
      ${cols.map((_, i) => `<td>${arr[i] ?? '-'}</td>`).join('')}
      <td class="tot">${total ?? '-'}</td>
    </tr>`;

  return `
    <div class="panel">
      <h3>Line Score</h3>
      <table class="linescore">
        <thead><tr><th></th>${cols.map(c => `<th>${c}</th>`).join('')}<th>T</th></tr></thead>
        <tbody>
          ${row(g.away_abbr, away, g.away_score)}
          ${row(g.home_abbr, home, g.home_score)}
        </tbody>
      </table>
    </div>`;
}

function consensusPanel(g) {
  // Two readings side by side: where the game actually IS right now, expressed
  // in the same units as a betting line, and where the market currently has it.
  // Putting them next to each other is the whole point - "ALA -12, total 68"
  // against "ALA -17.5, total 81.5" is the comparison you'd otherwise do in
  // your head on every glance.
  const home = g.home_score, away = g.away_score;
  const hasScore = home !== null && home !== undefined && away !== null && away !== undefined;

  let nowSpread = '-', nowTotal = '-';
  if (hasScore) {
    const margin = home - away;
    if (margin === 0) {
      nowSpread = 'PK';
    } else {
      const leader = margin > 0 ? (g.home_abbr || 'HOME') : (g.away_abbr || 'AWAY');
      nowSpread = `${leader} -${Math.abs(margin)}`;
    }
    nowTotal = String(home + away);
  }

  const best = g.best_lines || {};
  const mktSpreadPt = best.spreads ? best.spreads.median_point : null;
  const mktTotalPt = best.totals ? best.totals.median_point : null;

  // median_point is the HOME side, so a negative number means home is favoured.
  let mktSpread = '-';
  if (mktSpreadPt !== null && mktSpreadPt !== undefined) {
    if (mktSpreadPt === 0) {
      mktSpread = 'PK';
    } else {
      const fav = mktSpreadPt < 0 ? (g.home_abbr || 'HOME') : (g.away_abbr || 'AWAY');
      mktSpread = `${fav} -${Math.abs(mktSpreadPt)}`;
    }
  }
  const mktTotal = (mktTotalPt !== null && mktTotalPt !== undefined) ? String(mktTotalPt) : '-';

  const wp = [];
  if (g.market_home_wp !== null && g.market_home_wp !== undefined) {
    wp.push(`market ${esc(g.home_abbr)} ${Math.round(g.market_home_wp * 100)}%`);
  }
  if (g.espn_home_wp !== null && g.espn_home_wp !== undefined) {
    wp.push(`ESPN ${esc(g.home_abbr)} ${Math.round(g.espn_home_wp * 100)}%`);
  }

  const bookNote = best.spreads && best.spreads.book_count
    ? `${best.spreads.book_count} books` : '';

  return `
    <div class="state-boxes">
      <div class="state-box now">
        <div class="state-title">Scoreboard</div>
        <div class="state-line"><span class="state-k">Spread</span><span class="state-v">${esc(nowSpread)}</span></div>
        <div class="state-line"><span class="state-k">Total</span><span class="state-v">${esc(nowTotal)}</span></div>
      </div>
      <div class="state-box">
        <div class="state-title">Market${bookNote ? ' <span class="rec">' + bookNote + '</span>' : ''}</div>
        <div class="state-line"><span class="state-k">Spread</span><span class="state-v">${esc(mktSpread)}</span></div>
        <div class="state-line"><span class="state-k">Total</span><span class="state-v">${esc(mktTotal)}</span></div>
      </div>
      ${wp.length ? `<div class="state-box">
        <div class="state-title">Win probability</div>
        ${wp.map(w => `<div class="state-line"><span class="state-k">${w.split(' ').slice(0, 2).join(' ')}</span><span class="state-v">${w.split(' ').pop()}</span></div>`).join('')}
      </div>` : ''}
    </div>`;
}

function teamStats(g) {
  const s = g.summary;
  if (!s || !s.boxscore || !s.boxscore.teams) return '';
  const teams = s.boxscore.teams;
  const ids = Object.keys(teams);
  if (ids.length < 2) return '';

  // Order away-then-home to match the rest of the page.
  const away = teams[String(g.away_id)] || teams[ids[0]];
  const home = teams[String(g.home_id)] || teams[ids[1]];
  if (!away || !home) return '';

  const keys = Array.from(new Set([
    ...Object.keys(away.stats || {}),
    ...Object.keys(home.stats || {}),
  ]));
  if (!keys.length) return '';

  const label = k => k.replace(/([A-Z])/g, ' $1').replace(/^./, c => c.toUpperCase());

  return `
    <div class="panel">
      <h3>Team Stats</h3>
      <div class="scroll">
        <table>
          <thead><tr><th>Stat</th><th class="num">${esc(away.abbr)}</th><th class="num">${esc(home.abbr)}</th></tr></thead>
          <tbody>
            ${keys.map(k => `
              <tr>
                <td class="name">${esc(label(k))}</td>
                <td class="num">${esc((away.stats || {})[k] ?? '-')}</td>
                <td class="num">${esc((home.stats || {})[k] ?? '-')}</td>
              </tr>`).join('')}
          </tbody>
        </table>
      </div>
    </div>`;
}

// ---- model panel (score-only null v1; xScore later) ----

let replayAt = null;   // elapsed seconds being replayed on a finished game, or null

function fmtSpread(home, abbrH, abbrA) {
  if (home === null || home === undefined) return '-';
  if (Math.abs(home) < 0.05) return 'PK';
  return home < 0 ? `${abbrH} ${home.toFixed(1)}` : `${abbrA} ${(-home).toFixed(1)}`;
}

function fmtElapsed(e) {
  if (e === null || e === undefined) return '-';
  const half = e < 1200 ? 1 : (e < 2400 ? 2 : 3);
  const left = half === 1 ? 1200 - e : (half === 2 ? 2400 - e : 0);
  return (half === 3 ? 'OT' : 'H' + half) + ' ' + Math.floor(left / 60) + ':' + String(left % 60).padStart(2, '0');
}

function modelPanel(g) {
  const v = g.pace;
  const canReplay = g.state === 'post' && g.summary && g.summary.plays && g.summary.plays.length;
  if (!v && !canReplay) return '';
  const H = esc(g.home_abbr || 'HOME'), A = esc(g.away_abbr || 'AWAY');
  const r = v && v.replay;
  const at = r ? r.elapsed_seconds : (replayAt ?? 1200);
  const slider = canReplay ? `
      <div class="state-line" style="gap:10px;align-items:center">
        <span class="state-k">Replay at</span>
        <input type="range" id="replay" min="60" max="2340" step="60" value="${at}" style="flex:1">
        <span class="state-v">${fmtElapsed(at)}${r ? ` &middot; ${A} ${r.away_score}-${r.home_score} ${H}` : ''}</span>
      </div>` : '';
  if (!v || (!r && g.state === 'post')) {
    return `<div class="panel"><h3>Model</h3>${slider}<div class="detail-status">Drag to price this game at any point (pregame line + the score at that moment).</div></div>`;
  }
  const pre = `${fmtSpread(v.pregame_spread_home, H, A)} &middot; ${v.pregame_total ?? '-'}`;
  const pct = x => (x === null || x === undefined) ? '-' : Math.round(x * 100) + '%';
  const live = v.live_spread_home !== null && v.live_spread_home !== undefined;
  return `
    <div class="panel">
      <h3>Model <span class="rec">${esc(v.model || '')}</span></h3>
      ${slider}
      <div class="state-boxes">
        <div class="state-box now">
          <div class="state-title">Fair now</div>
          <div class="state-line"><span class="state-k">Spread</span><span class="state-v">${fmtSpread(v.fair_spread_home, H, A)}</span></div>
          <div class="state-line"><span class="state-k">Total</span><span class="state-v">${v.fair_total ?? '-'}</span></div>
          <div class="state-line"><span class="state-k">${H} win</span><span class="state-v">${pct(v.home_wp)}</span></div>
        </div>
        <div class="state-box">
          <div class="state-title">Uncertainty (1 SD)</div>
          <div class="state-line"><span class="state-k">Margin</span><span class="state-v">&plusmn;${v.margin_sd ?? '-'}</span></div>
          <div class="state-line"><span class="state-k">Total</span><span class="state-v">&plusmn;${v.total_sd ?? '-'}</span></div>
          <div class="state-line"><span class="state-k">Pregame</span><span class="state-v">${pre}</span></div>
        </div>
        ${live ? `<div class="state-box">
          <div class="state-title">Vs live median</div>
          <div class="state-line"><span class="state-k">${H} covers ${v.live_spread_home}</span><span class="state-v">${pct(v.p_home_cover_live)}</span></div>
          <div class="state-line"><span class="state-k">Over ${v.live_total ?? '-'}</span><span class="state-v">${pct(v.p_over_live)}</span></div>
        </div>` : ''}
      </div>
      <div class="detail-status" style="margin-top:8px">Score-only model: about as good as DK/FD live lines in testing, not better. Differences shown are model disagreement, not bet signals, until the xScore layer lands.</div>
      ${(v.notes || []).map(n => `<div class="detail-status">${esc(n)}</div>`).join('')}
    </div>`;
}

function wireReplay() {
  const el = document.getElementById('replay');
  if (!el) return;
  el.addEventListener('change', e => { replayAt = parseInt(e.target.value, 10); load(); });
}

function playFeed(g) {
  const s = g.summary;
  if (!s || !s.plays || !s.plays.length) return '';
  const list = s.plays.slice(-60).reverse();   // most recent first
  return `
    <div class="panel">
      <h3>Plays <span class="rec">last ${list.length} of ${s.plays.length}</span></h3>
      <div class="scroll">
        ${list.map(p => `
          <div class="drive">
            <div class="drive-head">
              <span class="drive-team">${p.period > 2 ? 'OT' : 'H' + (p.period ?? '')} ${esc(p.clock || '')}</span>
              <span class="drive-result ${p.scoring_play ? 'score' : ''}">${p.away_score ?? ''}-${p.home_score ?? ''}</span>
            </div>
            <div class="drive-desc">${esc(p.text || '')}</div>
          </div>`).join('')}
      </div>
    </div>`;
}

function drives(g) {
  const s = g.summary;
  if (!s || !s.drives || !s.drives.length) return '';
  const list = s.drives.slice().reverse();   // most recent first
  return `
    <div class="panel">
      <h3>Drives (${s.drives.length})</h3>
      <div class="scroll">
        ${list.map(d => `
          <div class="drive">
            <div class="drive-head">
              <span class="drive-team">${esc(d.team_abbr || '')}</span>
              <span class="drive-result ${d.is_score ? 'score' : ''}">${esc(d.result || '')}</span>
            </div>
            <div class="drive-desc">
              ${esc(d.description || '')}
              ${d.start_text ? ' &middot; from ' + esc(d.start_text) : ''}
              ${d.start_period ? ' &middot; Q' + d.start_period + ' ' + esc(d.start_clock || '') : ''}
            </div>
          </div>`).join('')}
      </div>
    </div>`;
}

function scoringPlays(g) {
  const s = g.summary;
  if (!s || !s.scoring_plays || !s.scoring_plays.length) return '';
  const list = s.scoring_plays.slice().reverse();
  return `
    <div class="panel">
      <h3>Scoring Plays</h3>
      <div class="scroll">
        <table>
          <thead><tr><th>Q</th><th>Clock</th><th>Team</th><th>Play</th><th class="num">Score</th></tr></thead>
          <tbody>
            ${list.map(p => `
              <tr>
                <td>${p.period ?? ''}</td>
                <td>${esc(p.clock || '')}</td>
                <td>${esc(p.team_abbr || '')}</td>
                <td class="name">${esc(p.text || '')}</td>
                <td class="num">${p.away_score ?? ''}-${p.home_score ?? ''}</td>
              </tr>`).join('')}
          </tbody>
        </table>
      </div>
    </div>`;
}

const movementBook = { spreads: 'all', totals: 'all' };

function lineMovement(g, market) {
  // market: 'spreads' (tracked on the home side) or 'totals' (tracked on Over).
  // One side is enough for either - the other is its mirror, so showing both
  // would double the rows without adding information.
  const cfg = market === 'totals'
    ? { side: 'over', title: 'Total Movement &mdash; Over', fmt: p => (p === null || p === undefined ? '-' : String(p)) }
    : { side: 'home', title: 'Spread Movement &mdash; home side', fmt: fmtPoint };

  const hist = (g.odds_history || []).filter(r => r.market === market && r.side === cfg.side);
  if (hist.length < 2) return '';

  const byBook = {};
  hist.forEach(r => { (byBook[r.book] = byBook[r.book] || []).push(r); });
  const books = Object.keys(byBook).sort((a, b) => byBook[b].length - byBook[a].length);

  // Per-book summary first. The chronological list alone was misleading: books
  // move at very different rates (DraftKings logged 66 moves on one game while
  // BetRivers logged 2), so a flat time-ordered feed gets dominated by the
  // busiest one or two books and the rest look absent when they are not.
  const summary = books.map(b => {
    const rows = byBook[b];
    const first = rows[0], last = rows[rows.length - 1];
    const moved = last.point !== first.point;
    return `
      <tr>
        <td class="name">${esc((allOdds && allOdds.book_display && allOdds.book_display[b]) || b)}</td>
        <td>${cfg.fmt(first.point)} ${fmtOdds(first.price)}</td>
        <td>${cfg.fmt(last.point)} ${fmtOdds(last.price)}</td>
        <td class="${moved ? 'moved' : ''}">${moved ? (last.point > first.point ? '+' : '') + (last.point - first.point).toFixed(1) : '&mdash;'}</td>
        <td class="num">${rows.length}</td>
      </tr>`;
  }).join('');

  const sel = movementBook[market];
  const shown = sel === 'all' ? hist : byBook[sel] || [];
  const CAP = 150;
  const clipped = shown.length > CAP;

  const opts = ['all'].concat(books)
    .map(b => `<option value="${esc(b)}" ${b === sel ? 'selected' : ''}>${
      b === 'all' ? `All books (${hist.length})` : esc(b) + ` (${byBook[b].length})`
    }</option>`).join('');

  return `
    <div class="panel">
      <h3>${cfg.title}</h3>
      <table>
        <thead><tr><th>Book</th><th>Opened</th><th>Latest</th><th>Move</th><th class="num">Logged</th></tr></thead>
        <tbody>${summary}</tbody>
      </table>

      <div class="movement-head">
        <span class="rec">Every logged change${clipped ? `, showing latest ${CAP} of ${shown.length}` : ''}</span>
        <select id="movement-book-${market}" class="btn" data-market="${market}">${opts}</select>
      </div>
      <div class="scroll">
        <table>
          <thead><tr><th>Time</th><th>Book</th><th>Line</th><th>Price</th><th>Score</th><th>Q/Clock</th></tr></thead>
          <tbody>
            ${shown.slice().reverse().slice(0, CAP).map(r => `
              <tr>
                <td>${esc(fmtTime(r.captured_at))}</td>
                <td class="name">${esc(r.book)}</td>
                <td>${cfg.fmt(r.point)}</td>
                <td>${fmtOdds(r.price)}</td>
                <td>${r.away_score ?? '-'}-${r.home_score ?? '-'}</td>
                <td>${r.period ? 'Q' + r.period : ''} ${r.clock_seconds !== null && r.clock_seconds !== undefined ? Math.floor(r.clock_seconds / 60) + ':' + String(r.clock_seconds % 60).padStart(2, '0') : ''}</td>
              </tr>`).join('')}
          </tbody>
        </table>
      </div>
    </div>`;
}

// ---- data flow ----

// ---- full odds board ----

function allOddsPanel() {
  if (!allOdds) {
    return '<div class="panel"><h3>All Odds</h3><div class="empty">Loading full board...</div></div>';
  }
  if (allOdds.note) {
    return `<div class="panel"><h3>All Odds</h3><div class="empty">${esc(allOdds.note)}</div></div>`;
  }
  if (!allOdds.groups || !allOdds.groups.length) {
    return '<div class="panel"><h3>All Odds</h3><div class="empty">No markets currently posted for this game.</div></div>';
  }

  // First render opens the standard lines and leaves the long ladders collapsed.
  if (openGroups === null) {
    openGroups = new Set(allOdds.groups.map(g => g.title).filter(t => t !== 'Alternate Lines'));
  }

  const disp = allOdds.book_display || {};

  const marketTable = m => {
    const books = m.books || [];
    const head = books.map(b => `<th class="num">${esc(disp[b] || b)}</th>`).join('');
    const body = m.rows.map(row => {
      const label = row.sublabel
        ? `${esc(shortTeam(row.label))} <span class="rec">${esc(row.sublabel)}</span>`
        : esc(shortTeam(row.label));
      const rowPoint = row.point !== null && row.point !== undefined
        ? ` <span class="rec">${fmtPoint(row.point)}</span>` : '';
      const cells = books.map(b => {
        const c = row.cells[b];
        if (!c) return '<td class="num">-</td>';
        const pt = c.point !== null && c.point !== undefined ? fmtPoint(c.point) + ' ' : '';
        return `<td class="num">${pt}${fmtOdds(c.price)}</td>`;
      }).join('');
      return `<tr><td class="name">${label}${rowPoint}</td>${cells}</tr>`;
    }).join('');

    return `
      <div class="market">
        <div class="market-head">
          <span class="market-name">${esc(m.label)}</span>
          <span class="rec">${m.rows.length} line${m.rows.length === 1 ? '' : 's'} &middot; ${books.length} book${books.length === 1 ? '' : 's'}</span>
        </div>
        <div class="market-scroll ${m.is_ladder ? 'scroll' : ''}">
          <table>
            <thead><tr><th>Line</th>${head}</tr></thead>
            <tbody>${body}</tbody>
          </table>
        </div>
      </div>`;
  };

  const groups = allOdds.groups.map(g => {
    const open = openGroups.has(g.title);
    return `
      <div class="panel">
        <h3 class="group-toggle" data-group="${esc(g.title)}">
          <span>${open ? '&#9662;' : '&#9656;'}</span> ${esc(g.title)}
          <span class="rec">${g.markets.length} market${g.markets.length === 1 ? '' : 's'}</span>
        </h3>
        <div style="display:${open ? 'block' : 'none'}">
          ${g.markets.map(marketTable).join('')}
        </div>
      </div>`;
  }).join('');

  const meta = allOdds.cached
    ? 'cached'
    : `${allOdds.market_count} markets &middot; ${allOdds.credits_spent} credits`;

  return `
    <div class="odds-board-head">
      <span class="rec">Recreational books only &middot; ${meta}</span>
      <button class="btn" id="reload-odds">Reload odds</button>
    </div>
    ${groups}`;
}

function wireGroupToggles() {
  document.querySelectorAll('.group-toggle').forEach(h => {
    h.addEventListener('click', () => {
      const title = h.dataset.group;
      if (openGroups.has(title)) openGroups.delete(title); else openGroups.add(title);
      renderAllOdds();
    });
  });
  const btn = document.getElementById('reload-odds');
  if (btn) {
    btn.addEventListener('click', async () => {
      btn.disabled = true;
      btn.textContent = 'Loading...';
      await loadAllOdds(true);
    });
  }
}

function renderAllOdds() {
  const host = document.getElementById('all-odds');
  if (!host) return;
  host.innerHTML = allOddsPanel();
  wireGroupToggles();
}

async function loadAllOdds(force) {
  try {
    const url = '/api/game/' + encodeURIComponent(GAME_ID) + '/all_odds' + (force ? '?force=1' : '');
    const r = await fetch(url);
    if (r.status === 404) {
      allOdds = { groups: [], note: 'Odds are off (CBB_ODDS=0 in CBB Live/.env) - turn on once the season starts.' };
      renderAllOdds();
      return;
    }
    if (!r.ok) throw new Error('HTTP ' + r.status);
    allOdds = await r.json();
  } catch (e) {
    allOdds = { groups: [], note: 'Could not load the full odds board: ' + e.message };
  }
  renderAllOdds();
}

// ---- page load ----

async function load() {
  try {
    const r = await fetch('/api/game/' + encodeURIComponent(GAME_ID) + (replayAt !== null ? '?at=' + replayAt : ''));
    if (!r.ok) throw new Error('HTTP ' + r.status);
    const g = await r.json();
    currentGame = g;
    document.title = `${g.away_abbr} @ ${g.home_abbr} - CBB Live`;
    document.getElementById('content').innerHTML = [
      head(g),
      consensusPanel(g),
      modelPanel(g),
      `<div id="all-odds"></div>`,
      `<div class="grid-2"><div>${linescore(g)}${scoringPlays(g)}</div><div>${playFeed(g)}</div></div>`,
      teamStats(g),
      lineMovement(g, 'spreads'),
      lineMovement(g, 'totals'),
    ].join('');
    renderAllOdds();
    wireReplay();

    document.querySelectorAll('[id^="movement-book-"]').forEach(el => {
      el.addEventListener('change', e => {
        movementBook[e.target.dataset.market] = e.target.value;
        load();   // cheap: re-reads the cached game payload, no odds spend
      });
    });

    const age = g.updated_at ? Math.round((Date.now() - new Date(g.updated_at).getTime()) / 1000) : null;
    const pulse = document.getElementById('pulse');
    pulse.className = 'dot' + (age === null || age > 120 ? ' dead' : (age > 60 ? ' stale' : ''));
    document.getElementById('updated').textContent = age === null ? 'unknown' : age + 's ago';
  } catch (e) {
    document.getElementById('content').innerHTML =
      `<div class="empty">Could not load this game: ${esc(e.message)}</div>`;
    document.getElementById('pulse').className = 'dot dead';
  }
}

document.getElementById('refresh').addEventListener('click', async e => {
  const btn = e.target;
  btn.disabled = true;
  btn.textContent = '...';
  try {
    // Same contract as the board's Refresh: force a fresh ESPN pull AND a fresh
    // bulk odds poll, then re-render. Also re-pulls this game's full odds board,
    // since that is the main thing on screen here.
    await fetch('/api/refresh', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ what: 'all' }),   // scoreboard + bulk odds
    });
    await load();
    await loadAllOdds(true);
  } finally {
    btn.disabled = false;
    btn.textContent = 'Refresh';
  }
});

// Game state auto-refreshes every 30s (free - it reads the local database).
// The full odds board does NOT, because it costs an API credit per market;
// that only reloads on the Refresh button or "Reload odds".
(async () => {
  await load();
  await loadAllOdds(false);
})();

let pollTimer = setInterval(load, 30000);
document.addEventListener('visibilitychange', () => {
  if (document.hidden) {
    clearInterval(pollTimer);
    pollTimer = null;
  } else {
    load();
    pollTimer = setInterval(load, 30000);
  }
});
