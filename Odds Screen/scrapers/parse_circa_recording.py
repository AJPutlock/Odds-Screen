"""
Circa Sports Screen-Recording OCR Importer
===========================================
Extracts frames from an iPhone screen recording of the Circa Sports app's
odds board and OCRs them into game dicts matching the same output shape as
parse_bookmaker_har.games_from_bookmaker_har().

Must be run with the isolated venv at Odds Screen/.venv-ocr — pytesseract /
opencv are deliberately NOT installed in the main project environment
because opencv-python-headless forces a numpy upgrade that breaks numba/
scipy used by other projects sharing the global Python install.

Usage (from the venv):
    .venv-ocr\\Scripts\\python.exe scrapers\\parse_circa_recording.py <video_or_image_path>

Output format: same as parse_bookmaker_har — list of game dicts:
    {
      "away_team": str, "home_team": str, "commence_time": iso8601|None,
      "markets": {"h2h": {...}, "spreads": {...}, "totals": {...}},
      "_rotation": {"away": int, "home": int},
      "_neutral_site": bool,
      "_warnings": [str, ...],   # low-confidence / ambiguous fields worth a human glance
    }
"""

import os
import re
import sys
import logging
import concurrent.futures
from collections import Counter, defaultdict

import cv2
import pytesseract
from pytesseract import Output
from PIL import Image

logger = logging.getLogger(__name__)

pytesseract.pytesseract.tesseract_cmd = r"C:\Program Files\Tesseract-OCR\tesseract.exe"

# Per-cell OCR calls each spawn their own tesseract.exe subprocess and are
# dispatched concurrently via one shared thread pool spanning every frame in
# the video at once (see games_from_circa_recording) — not just the rows
# within a single frame — so the whole video's OCR workload runs across all
# available cores together instead of finishing one frame before starting
# the next. Sized a bit past raw core count: each call is short and has real
# I/O/startup gaps (subprocess spawn), so mild oversubscription keeps cores
# busier than a strict 1-worker-per-core cap would.
_OCR_MAX_WORKERS = min(24, (os.cpu_count() or 4) * 2)

# ── Column x-boundaries (pixels, calibrated against a 1180-wide iPhone
#    screen recording of Circa's odds board — see memory: odds_screen_recording_ingestion) ──
COL_LEFT_MAX   = 400   # rotation # / team name / date-time header
COL_SPREAD_MAX = 750
COL_TOTAL_MAX  = 950
# anything >= COL_TOTAL_MAX is moneyline

TITLE_ROW_Y_MAX   = 250   # "Sports  <  NCAA FB  >  ☰" title bar only
HEADER_ROW_Y_MAX  = 400   # + Account/Balance strip below the title bar — skip both for row data

_MONTHS = {"Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"}
ROW_ANCHOR_RE = re.compile(r"^\d{2,4}$")

# Section headers on a game's "Props" detail page (list view has none of these —
# rows there default to the "game" section). Period rows use rotation number
# = parent game's rotation + 1000 (confirmed against a real Circa NCAAF sample:
# game 147/148 -> period 1147/1148) — a reliable structural link, used instead
# of fuzzy team-name matching to attach 1H markets to the right game.
_SECTION_HEADER_RE = re.compile(r"\b(game|periods|propositions)\b", re.IGNORECASE)
PERIOD_ROTATION_OFFSET = 1000    # 1H:  e.g. game 147/148 -> period 1147/1148
QUARTER_ROTATION_OFFSET = 3000   # 1Q:  e.g. game 153/154 -> period 3153/3154 (confirmed 2026-09-04)
_PERIOD_NAME_PREFIX_RE = re.compile(r"^1[HQ]\s+", re.IGNORECASE)

# Restricted whitelist re-OCR config for the fiddly spread/total cells —
# forces Tesseract to choose among these glyphs instead of free-form guessing.
# ½ is deliberately excluded: Tesseract reads it as a stray digit (often "2")
# far too often to trust; half-points are detected geometrically instead
# (see _half_point_info) and the digit crop is re-OCR'd without that glyph.
_CELL_CONFIG = '--psm 7 -c tessedit_char_whitelist=0123456789+-ouOU'


# ── Frame extraction ──────────────────────────────────────────────────────

def extract_frames(video_path: str, every_n_seconds: float = 0.75) -> list:
    """Return a list of (timestamp_sec, PIL.Image) sampled from the video."""
    cap = cv2.VideoCapture(video_path)
    fps = cap.get(cv2.CAP_PROP_FPS) or 30.0
    frame_interval = max(1, int(round(fps * every_n_seconds)))

    frames = []
    idx = 0
    while True:
        ok, frame_bgr = cap.read()
        if not ok:
            break
        if idx % frame_interval == 0:
            rgb = cv2.cvtColor(frame_bgr, cv2.COLOR_BGR2RGB)
            frames.append((idx / fps, Image.fromarray(rgb)))
        idx += 1
    cap.release()
    return frames


def extract_stable_frames(video_path: str, diff_threshold: float = 3.0,
                           min_stable_duration: float = 0.5,
                           frames_per_stable_run: int = 2,
                           sharpness_candidates: int = 8) -> list:
    """
    Motion-based frame selection, preferred over fixed-interval extract_frames()
    for the "tap into a game's Props page" recording style. Rather than sample
    every N seconds regardless of what's on screen, detect stretches where the
    picture isn't changing (a page has loaded and settled) vs. where it is
    (scrolling, a slide transition between games, or the "Please wait" spinner
    — which still counts as motion since it animates, so it's excluded here
    too, not just by the text-based filter in parse_frame). Only stable
    stretches get OCR'd: cheaper, since it skips both redundant near-duplicate
    frames and the transition frames that were producing garbled rows, and
    more accurate for the same reason.

    Returns (timestamp, PIL.Image) tuples — up to frames_per_stable_run
    readability-ranked samples per stable stretch (see _readability_score),
    so cross-frame consensus voting still has more than one independent read
    to work with, just fewer than before by default (2, was 3) — this is a
    deliberate speed/accuracy tradeoff, not free; see this function's
    docstring history in memory: odds_screen_recording_ingestion.
    """
    import numpy as np
    cap = cv2.VideoCapture(video_path)
    fps = cap.get(cv2.CAP_PROP_FPS) or 30.0
    min_stable_raw_frames = max(2, int(min_stable_duration * fps))

    prev_small = None
    run: list = []
    runs: list = []
    idx = 0
    while True:
        ok, frame_bgr = cap.read()
        if not ok:
            break
        small = cv2.resize(cv2.cvtColor(frame_bgr, cv2.COLOR_BGR2GRAY), (60, 130)).astype(int)
        if prev_small is not None and float(np.abs(small - prev_small).mean()) < diff_threshold:
            run.append((idx, frame_bgr))
        else:
            if len(run) >= min_stable_raw_frames:
                runs.append(run)
            run = [(idx, frame_bgr)]
        prev_small = small
        idx += 1
    if len(run) >= min_stable_raw_frames:
        runs.append(run)
    cap.release()

    # Screen-recording "blur" isn't optical — it's the video encoder still
    # catching up on bitrate right after motion stops (confirmed: Laplacian
    # sharpness measured ~5x lower in the first ~0.1s of a stable stretch
    # than a few frames later in the same stretch, on a real sample).
    # Sharpness alone is a cheap first cut, but doesn't perfectly predict
    # whether a frame will actually OCR well — a visually sharp frame can
    # still read badly. So: narrow to the sharpest `sharpness_candidates`
    # per run first (avoids a cheap OCR pass on every single frame in a long
    # stable run), then rank by how much a cheap whole-frame OCR pass on
    # each actually recognized. Only the winners get the expensive per-cell
    # re-OCR pass later in games_from_circa_recording — this is the lever
    # that controls how much of that expensive work happens at all, not
    # just which frames look sharpest.
    #
    # This scoring OCR pass was itself confirmed to dominate total pipeline
    # time on a real video (47 of 71 seconds, ~two-thirds) when run
    # sequentially one candidate at a time — so, same fix as the per-cell
    # OCR: every run's candidates are scored through one shared pool at
    # once, not run-by-run.
    candidates = []   # (run_index, fidx, PIL.Image)
    for run_idx, r in enumerate(runs):
        by_sharpness = sorted(r, key=lambda fr: _sharpness(fr[1]), reverse=True)[:sharpness_candidates]
        for fidx, frame_bgr in by_sharpness:
            rgb = cv2.cvtColor(frame_bgr, cv2.COLOR_BGR2RGB)
            candidates.append((run_idx, fidx, Image.fromarray(rgb)))

    with concurrent.futures.ThreadPoolExecutor(max_workers=_OCR_MAX_WORKERS) as pool:
        word_futs = [pool.submit(_ocr_words, img) for _, _, img in candidates]
        scores = [_readability_score(f.result()) for f in word_futs]

    by_run: dict = defaultdict(list)
    for (run_idx, fidx, img), score in zip(candidates, scores):
        by_run[run_idx].append((score, fidx, img))

    out = []
    for items in by_run.values():
        items.sort(key=lambda t: t[0], reverse=True)
        for _, fidx, img in items[:frames_per_stable_run]:
            out.append((fidx / fps, img))
    return out


def _readability_score(words: list) -> float:
    """How much a cheap whole-frame OCR pass actually recognized — used to
    pick which frames are worth the expensive per-cell re-OCR pass, since
    raw image sharpness doesn't reliably predict this on its own."""
    score = 0.0
    for w in words:
        txt = w["text"]
        if ROW_ANCHOR_RE.match(txt) or _PERIOD_NAME_PREFIX_RE.match(txt):
            score += 3       # a clean rotation number or "1H "/"1Q " read is a strong signal
        elif txt.upper() in ("HALF", "QUARTER", "GAME", "PERIODS", "PROPOSITIONS"):
            score += 1
        if w.get("conf", -1) >= 80:
            score += 0.1     # mild bonus for generally high-confidence reads
    return score


def _sharpness(frame_bgr) -> float:
    """Laplacian variance — a standard, cheap focus/sharpness metric."""
    gray = cv2.cvtColor(frame_bgr, cv2.COLOR_BGR2GRAY)
    return cv2.Laplacian(gray, cv2.CV_64F).var()


# ── Word/line extraction ──────────────────────────────────────────────────

# A single OCR call can hang indefinitely rather than error or complete —
# confirmed in production: a tesseract subprocess sat at ~0% CPU for minutes,
# neither progressing nor crashing, blocking the entire watcher queue behind
# it (one stuck frame stalled every video still waiting to be processed).
# pytesseract's own `timeout` kwarg turns a hang into a clean RuntimeError
# instead, which these wrappers treat as "no result" so one bad frame can't
# take down a whole run.
_OCR_TIMEOUT = 10  # seconds


def _safe_image_to_data(img: Image.Image) -> dict:
    try:
        return pytesseract.image_to_data(img, output_type=Output.DICT, timeout=_OCR_TIMEOUT)
    except RuntimeError:
        return {"text": [], "left": [], "top": [], "width": [], "height": [], "conf": []}


def _safe_image_to_string(img: Image.Image, config: str) -> str:
    try:
        return pytesseract.image_to_string(img, config=config, timeout=_OCR_TIMEOUT)
    except RuntimeError:
        return ""


def _ocr_words(img: Image.Image) -> list:
    data = _safe_image_to_data(img)
    words = []
    for i in range(len(data["text"])):
        txt = data["text"][i].strip()
        if not txt:
            continue
        conf_raw = str(data["conf"][i])
        conf = int(float(conf_raw)) if re.match(r"^-?\d+(\.\d+)?$", conf_raw) else -1
        words.append({
            "text": txt,
            "left": data["left"][i],
            "top":  data["top"][i],
            "width": data["width"][i],
            "height": data["height"][i],
            "conf": conf,
        })
    return words


def _cluster_lines(words: list, band_px: int = 16) -> list:
    """
    Group words (already filtered to one column/region) into visual lines by
    y-proximity, then order each line strictly left-to-right. Returns
    [{"top": int, "text": str, "words": [word, ...]}, ...] sorted top to bottom.

    Sorting must cluster-then-sort-by-left rather than a single sort by
    (top, left): two words on the same visual line routinely differ by a
    pixel or two in OCR'd top, which a naive (top, left) sort can flip.
    """
    ws = sorted(words, key=lambda w: w["top"])
    clusters: list = []
    for w in ws:
        if clusters and abs(w["top"] - clusters[-1]["top0"]) <= band_px:
            clusters[-1]["words"].append(w)
        else:
            clusters.append({"top0": w["top"], "words": [w]})

    lines = []
    for c in clusters:
        line_words = sorted(c["words"], key=lambda w: w["left"])
        texts = [w["text"] for w in line_words]
        # Join tight (no space) when fragments look like split sign/short-token
        # (e.g. "+" "10" -> "+10"); otherwise treat as separate words.
        joined = "".join(texts) if all(len(t) <= 3 for t in texts) else " ".join(texts)
        lines.append({"top": min(w["top"] for w in line_words), "text": joined, "words": line_words})
    lines.sort(key=lambda l: l["top"])
    return lines


# ── Half-point detection (geometric, not character-based) ─────────────────
#
# Tesseract reads this font's "½" as a stray digit (often "2") too often to
# trust as a character. But the glyph's *shape* is reliable: the fraction's
# denominator renders distinctly lower than this font's digit baseline,
# where a real 2nd/3rd digit never does. So: find contours, establish the
# baseline from the tallest ("real digit") contours, and flag a half-point
# whenever something past the last real digit sits well below that baseline.
# The digit-only OCR pass is then re-cropped to exclude the fraction glyph
# entirely, so it can't contaminate the number.

def _half_point_info(crop_img: Image.Image) -> tuple:
    """Returns (has_half: bool, digit_right_edge_px: int|None) in crop-local pixels.

    The leading contour is always the sign ('+'/'-') or totals side-char
    ('o'/'u'), which sits at its own natural height/offset — excluded from
    both the baseline and the half-point check so it can't be confused with
    a lowered fraction denominator. Threshold is proportional to digit
    height so it holds regardless of the crop's upscale factor.
    """
    import numpy as np
    gray = cv2.cvtColor(np.array(crop_img.convert("RGB")), cv2.COLOR_RGB2GRAY)
    _, thresh = cv2.threshold(gray, 0, 255, cv2.THRESH_BINARY_INV + cv2.THRESH_OTSU)
    contours, _ = cv2.findContours(thresh, cv2.RETR_EXTERNAL, cv2.CHAIN_APPROX_SIMPLE)
    boxes = [cv2.boundingRect(c) for c in contours]
    boxes = [b for b in boxes if b[2] > 2 and b[3] > 2]
    if not boxes:
        return False, None
    boxes.sort(key=lambda b: b[0])   # left to right

    digit_candidates = boxes[1:] if len(boxes) > 1 else boxes   # drop leading sign/side-char
    if not digit_candidates:
        return False, None

    max_h = max(b[3] for b in digit_candidates)
    digit_boxes = [b for b in digit_candidates if b[3] >= 0.75 * max_h]
    baseline_top = sorted(b[1] for b in digit_boxes)[len(digit_boxes) // 2]   # median top
    thresh_px = 0.3 * max_h

    has_half = any(b[1] > baseline_top + thresh_px for b in digit_candidates)
    last_digit_right = max(b[0] + b[2] for b in digit_boxes)
    return has_half, last_digit_right


def _ocr_number_cell(img: Image.Image, x0: int, y0: int, x1: int, y1: int,
                      pad: int = 6, scale: int = 4) -> tuple:
    """
    Crop a single cell, geometrically detect a trailing half-point, then OCR
    just the clean digit region. Returns (raw_text, has_half, warning|None).
    """
    w, h = img.size
    box = (max(0, x0 - pad), max(0, y0 - pad), min(w, x1 + pad), min(h, y1 + pad))
    crop = img.crop(box)
    if crop.width == 0 or crop.height == 0:
        return "", False, "empty cell crop"
    crop = crop.resize((crop.width * scale, crop.height * scale), Image.LANCZOS)

    has_half, digit_right = _half_point_info(crop)
    digit_crop = crop.crop((0, 0, digit_right + 8, crop.height)) if digit_right else crop

    text = _safe_image_to_string(digit_crop, _CELL_CONFIG).strip()
    warning = None
    if has_half and not re.match(r"^[+-]?\d+$", text):
        warning = f"half-point detected but digit OCR unclean: raw={text!r}"
    return text, has_half, warning


# Juice (the odds attached to a spread/total line, e.g. "-110", "-105") never
# carries a half-point, so it gets a plain whitelisted OCR pass rather than
# the half-point contour machinery in _ocr_number_cell — simpler and avoids
# that machinery's geometry assumptions where they don't apply. American
# odds always carry an explicit sign, so a signless read (e.g. "105" instead
# of "-105") is treated as a failed read, not a valid-but-different value —
# it's cheaper to fall back to the -110 default than to ship a sign-dropped
# number as if it were trustworthy.
_JUICE_RE = re.compile(r"^[+-]\d{2,4}$")
# Spread/total juice on a standard game line is essentially never this
# extreme in practice (confirmed real range across every sample so far:
# -105 to -140ish) — a read outside this is far more likely a digit misread
# (confirmed on a real sample: "-130" came back "-614") than a genuine value,
# so it's rejected rather than trusted. Deliberately generous vs. the ~100-140
# norm to avoid false-rejecting a real outlier line, just not ~600.
_JUICE_MAX_MAGNITUDE = 350

# Whole-frame OCR's line clustering frequently fails to detect a real 2nd
# line for these cells (merges it into the first line's bounding box, or
# misses it) even when it's clearly rendered — confirmed against a real
# sample where the correct 2-line detection only fired in 3 of 18 frames.
# So the juice line isn't looked for conditionally on that detection; it's
# always probed at this calibrated fixed offset below the point line
# (measured ~39px apart on a 1180px-wide recording) and validated by regex
# instead — if nothing juice-shaped is there, no harm done, defaults apply.
_JUICE_LINE_OFFSET = 39

# Vertical distance from a row's ANCHOR top (the rotation-number line's own
# top) down to that row's point-line (spread/total first line) — calibrated
# from a real sample (rotation "149" top=739, its point line "+10" top=774).
# Only applies to anchor-kind rows; name-kind (synthesized) rows use the name
# line's own top directly, since name and point text render at roughly the
# same height there (unlike the anchor, which sits well above both).
#
# This exists because whole-frame OCR's own reported top for the point line
# (used previously) is occasionally badly wrong even when the anchor and
# name both read fine — confirmed on a real sample where a garbled "-6½"
# fragment reported a top ~46px off from where the text actually is,
# cascading into a wildly wrong juice-cell crop position ("-130" misread as
# "-614"). Deriving the position from the reliable anchor instead of trusting
# that per-line OCR position removes the dependency on it being right.
POINT_LINE_OFFSET = 35

# Moneylines never got a dedicated whitelisted re-OCR pass — they were read
# straight from whole-frame OCR's raw line text, the same unreliable source
# that produces things like "ae" or "os" for spread cells (see _CELL_CONFIG's
# comment). Confirmed on a real sample: "-550" came back "0.550" (the minus
# misread as a decimal point). ML magnitude ranges from 100 up to 5 digits.
_ML_RE = re.compile(r"^[+-]\d{3,6}$")


def _ocr_ml_cell(img: Image.Image, x0: int, y0: int, x1: int, y1: int,
                  pad: int = 6, scale: int = 4) -> str | None:
    w, h = img.size
    box = (max(0, x0 - pad), max(0, y0 - pad), min(w, x1 + pad), min(h, y1 + pad))
    crop = img.crop(box)
    if crop.width == 0 or crop.height == 0:
        return None
    crop = crop.resize((crop.width * scale, crop.height * scale), Image.LANCZOS)
    text = _safe_image_to_string(crop, _CELL_CONFIG).strip()
    return text if _ML_RE.match(text) else None


def _ocr_juice_cell(img: Image.Image, x0: int, y0: int, x1: int, y1: int,
                     pad: int = 6, scale: int = 4) -> str | None:
    w, h = img.size
    box = (max(0, x0 - pad), max(0, y0 - pad), min(w, x1 + pad), min(h, y1 + pad))
    crop = img.crop(box)
    if crop.width == 0 or crop.height == 0:
        return None
    crop = crop.resize((crop.width * scale, crop.height * scale), Image.LANCZOS)
    text = _safe_image_to_string(crop, _CELL_CONFIG).strip()
    if not _JUICE_RE.match(text):
        return None
    if abs(int(text)) > _JUICE_MAX_MAGNITUDE:
        return None
    return text


def _parse_spread_cell(img: Image.Image, point_line_top: int, x0: int, x1: int) -> dict:
    out = {"point": None, "juice": None, "warning": None}
    pt_box = (x0, point_line_top - 20, x1, point_line_top + 30)
    digits, has_half, warning = _ocr_number_cell(img, *pt_box)
    out["point"] = f"{digits}.5" if (digits and has_half) else (digits or None)
    out["warning"] = warning

    j_box = (x0, point_line_top + _JUICE_LINE_OFFSET - 10, x1, point_line_top + _JUICE_LINE_OFFSET + 28)
    out["juice"] = _ocr_juice_cell(img, *j_box) or "-110"
    return out


def _parse_total_cell(img: Image.Image, point_line_top: int, x0: int, x1: int) -> dict:
    out = {"side": None, "point": None, "juice": None, "warning": None}
    box = (x0, point_line_top - 20, x1, point_line_top + 30)
    raw, has_half, warning = _ocr_number_cell(img, *box)

    # Juice is an independent crop read at a fixed offset (see _JUICE_LINE_OFFSET)
    # — it must be attempted regardless of whether the point read above
    # succeeded, not skipped via an early return on point-read failure. A
    # failed point read otherwise silently loses a perfectly good juice read
    # too (and worse, drops it to None instead of the -110 default, corrupting
    # the cross-frame vote for rows that read fine in other frames).
    j_box = (x0, point_line_top + _JUICE_LINE_OFFSET - 10, x1, point_line_top + _JUICE_LINE_OFFSET + 28)
    out["juice"] = _ocr_juice_cell(img, *j_box) or "-110"

    if not raw:
        out["warning"] = "empty totals cell OCR"
        return out
    first, digits = raw[0], raw[1:]
    if first in ("0", "o", "O"):
        side = "over"
    elif first in ("u", "U"):
        side = "under"
    else:
        out["warning"] = f"unrecognized totals side char: raw={raw!r}"
        return out
    out["side"] = side
    out["point"] = f"{digits}.5" if (digits and has_half) else (digits or None)
    out["warning"] = warning
    return out


# ── Frame → structured rows ────────────────────────────────────────────────

# Row-name sanity bounds — a transition frame (mid-navigation slide, or a
# "Please wait" loading overlay) smears two screens' text into one garbled
# row instead of a clean team name; reject rows that look like that instead
# of trusting them into the cross-frame vote.
_MAX_NAME_LEN = 22
_NAME_GARBAGE_RE = re.compile(r"\d{1,2}:\d{2}|[,“”]")


def _prepare_frame(img: Image.Image) -> dict:
    """
    Phase 1 (cheap): whole-frame OCR + row/section/geometry detection. Does
    NOT run the expensive per-cell re-OCR (spread/total/moneyline) — that's
    deferred to _finish_frame so games_from_circa_recording can pool those
    calls across every frame in the video at once instead of one frame's
    worth at a time (see _OCR_MAX_WORKERS). parse_frame() below is a
    single-frame convenience wrapper around both phases, for the CLI debug
    mode and any other single-image caller.
    """
    words = _ocr_words(img)

    if any(w["text"].lower() in ("wait", "please") for w in words):
        return {"img": img, "header_text": None, "rows": [], "date_headers": [], "skipped": "loading screen"}

    title_words = [w for w in words if w["top"] < TITLE_ROW_Y_MAX]
    title_lines = _cluster_lines(title_words)
    header_text = " ".join(
        l["text"] for l in title_lines
        if re.search(r"[A-Za-z]{2,}", l["text"])
    )
    header_text = re.sub(r"\bSports\b", "", header_text, flags=re.IGNORECASE).strip()
    header_text = re.sub(r"^[^A-Za-z0-9]+", "", header_text).strip() or None

    body = [w for w in words if w["top"] >= HEADER_ROW_Y_MAX]
    left_lines = _cluster_lines([w for w in body if w["left"] < COL_LEFT_MAX])

    rows, date_headers = [], []
    cur_row = None
    cur_header = None          # most recent date/time header text, carried onto each row
    cur_section = "game"       # "game" | "periods" | "propositions" — list view never
                                # switches this; a game's Props page does via section headers
    pending_annotation = []    # left-column line(s) seen between a header and the next
                                # rotation-number row — Circa shows a neutral-site location
                                # line here when applicable (unvalidated against a real
                                # neutral-site sample — flag for a human glance either way)
    # Period rotation numbers (1153, 3153, ...) get OCR'd far less reliably
    # than the 3-digit game rotations they're derived from — confirmed on a
    # real sample where "1153"/"1154" were misread as complete garbage
    # ("te8", "He", 0% confidence) in a frame where everything else, name
    # lines included, read perfectly. Rather than trust that OCR at all, the
    # period rotation is *derived* from the game's own (reliably-read)
    # rotation + the confirmed per-level offset — this also means a name line
    # with no valid digit-anchor at all (the garbled-anchor case) can still
    # become a row instead of silently losing an otherwise-perfect read.
    cur_period_suffix = None   # "h1" | "q1" | None, set by a "...1ST HALF"/"...1ST QUARTER" subheader
    period_slot = 0            # 0 = next row is away, 1 = next row is home

    # The "1ST HALF"/"1ST QUARTER" subheader is detected by *position*, not by
    # text content of the (possibly column-truncated) clustered line — Circa
    # renders that subheader with the FULL team names for halves ("MIAMI FL/
    # STANFORD 1ST HALF") but ABBREVIATED names for quarters ("MIA/STAN 1ST
    # QUARTER"); the full-name version is long enough that "1ST HALF" itself
    # lands past COL_LEFT_MAX and gets excluded from the left-column word set
    # entirely — confirmed on a real sample where this silently dropped the
    # entire 1H section. Scanning the whole frame (not just the left column)
    # for the "HALF"/"QUARTER" keyword and matching by row position sidesteps
    # that column-width dependency altogether.
    period_markers = sorted(
        (w["top"], "h1" if w["text"].upper() == "HALF" else "q1")
        for w in body if w["text"].upper() in ("HALF", "QUARTER")
    )

    def _structural_period_rotation(slot: int) -> str | None:
        if cur_period_suffix is None:
            return None
        offset = PERIOD_ROTATION_OFFSET if cur_period_suffix == "h1" else QUARTER_ROTATION_OFFSET
        game_rows_so_far = [r for r in rows if r["section"] == "game"]
        if len(game_rows_so_far) < 2:
            return None
        base = game_rows_so_far[0 if slot == 0 else 1]["rotation"]
        return str(int(base) + offset)

    for ln in left_lines:
        txt = ln["text"]
        section_match = _SECTION_HEADER_RE.search(txt) if len(txt) <= 16 else None
        if section_match:
            cur_section = section_match.group(1).lower()
            cur_row = None
            pending_annotation = []
            continue
        if cur_section == "periods":
            for mtop, mlevel in period_markers:
                if abs(mtop - ln["top"]) <= 20:
                    cur_period_suffix, period_slot = mlevel, 0
                    break
        first_tok = txt.split()[0].rstrip(",") if txt.split() else ""
        if first_tok in _MONTHS:
            cur_header = txt
            date_headers.append({"top": ln["top"], "text": txt})
            cur_row = None
            pending_annotation = []
            continue
        if ROW_ANCHOR_RE.match(txt):
            rotation = txt
            if cur_section == "periods":
                rotation = _structural_period_rotation(period_slot) or txt
                period_slot += 1
            cur_row = {
                "rotation": rotation, "top": ln["top"], "name_lines": [],
                "date_header": cur_header, "section": cur_section,
                "neutral_site_text": " ".join(pending_annotation) or None,
                "top_kind": "anchor",
            }
            rows.append(cur_row)
            pending_annotation = []
            continue
        # A "1H "/"1Q " prefixed line always carries the team's full name in
        # one shot (never needs a continuation line) — EXCEPT when a digit
        # anchor just fired cleanly right before it (e.g. "3153" -> next line
        # "1Q MIAMI FL" is that anchor's name, not a second team). Only treat
        # it as starting a fresh synthesized row when cur_row isn't already a
        # freshly-anchored, still-nameless row waiting for exactly this line.
        just_anchored = cur_row is not None and cur_section == "periods" and not cur_row["name_lines"]
        if cur_section == "periods" and _PERIOD_NAME_PREFIX_RE.match(txt) and not just_anchored:
            rotation = _structural_period_rotation(period_slot)
            if rotation is not None:
                period_slot += 1
                cur_row = {
                    "rotation": rotation, "top": ln["top"], "name_lines": [txt],
                    "date_header": cur_header, "section": cur_section,
                    "neutral_site_text": None, "top_kind": "name",
                }
                rows.append(cur_row)
                cur_row = None   # self-contained — don't let a stray garbled line after it get appended
                continue
        if cur_row is not None:
            cur_row["name_lines"].append(txt)
        elif cur_section == "game":
            pending_annotation.append(txt)   # e.g. neutral-site line; ignore free text in other sections

    # Cheap geometry pass first (line clustering, no OCR) for every row —
    # the position-sanity-check comment below applies to _spread_top/_total_top.
    row_tops = [r["top"] for r in rows] + [10 ** 9]
    for r, next_top in zip(rows, row_tops[1:]):
        r["spread_lines"] = _cluster_lines(_band(body, r["top"], next_top, COL_LEFT_MAX, COL_SPREAD_MAX))
        r["total_lines"]  = _cluster_lines(_band(body, r["top"], next_top, COL_SPREAD_MAX, COL_TOTAL_MAX))
        r["ml_lines"]     = _cluster_lines(_band(body, r["top"], next_top, COL_TOTAL_MAX, 10 ** 9))
        # The detected line's own top is usually fine, but occasionally badly
        # wrong even when the anchor and name both read cleanly (confirmed on
        # a real sample). One cheap, reliable sanity check exists for anchor
        # rows: the point line can never legitimately be at or above the
        # anchor's own top — a real failure case had the detected top 11px
        # *above* the anchor. When that impossible case is hit, don't guess a
        # replacement position (tried a fixed anchor+offset estimate first;
        # it "fixed" that one case but broke totals on several other rows
        # elsewhere, since the true offset isn't actually constant across
        # rows) — just treat it as no usable line found, same as the row
        # having none at all. An honest gap beats confidently reading from a
        # guessed-wrong position.
        # Tried two "smarter" position-correction heuristics here (a fixed
        # anchor+offset estimate, then a narrower "reject only the impossible
        # case" version) — both demonstrably caused more regressions across
        # other rows than they fixed on the one row they targeted (a specific
        # frame where whole-frame OCR badly mispositioned a line despite the
        # anchor and name both reading fine). Reverted to trusting the
        # detected line's own top outright, same as originally. That one row
        # remains a known, narrow residual gap — see memory:
        # odds_screen_recording_ingestion for the specific case and why it
        # resisted a general fix.
        r["_spread_top"] = r["spread_lines"][0]["top"] if r["spread_lines"] else r["top"]
        r["_total_top"]  = r["total_lines"][0]["top"] if r["total_lines"] else r["top"]

    return {"img": img, "header_text": header_text, "rows": rows, "date_headers": date_headers}


def _submit_row_ocr(pool: concurrent.futures.ThreadPoolExecutor, img: Image.Image, r: dict) -> list:
    """Submits one row's per-cell re-OCR calls (spread/total/moneyline) to
    the given pool — the expensive part of this whole pipeline, each
    spawning its own tesseract.exe subprocess. Returns [(row, field, future)]
    for the caller to resolve once every row across every frame has been
    submitted, so the pool runs the whole video's OCR workload concurrently
    rather than one frame (or one row) at a time."""
    tasks = [
        (r, "spread", pool.submit(_parse_spread_cell, img, r["_spread_top"], COL_LEFT_MAX, COL_SPREAD_MAX)),
        (r, "total",  pool.submit(_parse_total_cell, img, r["_total_top"], COL_SPREAD_MAX, COL_TOTAL_MAX)),
    ]
    if r["ml_lines"]:
        ml_top = r["ml_lines"][0]["top"]
        ml_box = (COL_TOTAL_MAX, ml_top - 20, COL_TOTAL_MAX + 350, ml_top + 30)
        tasks.append((r, "ml", pool.submit(_ocr_ml_cell, img, *ml_box)))
    else:
        r["ml"] = None
    return tasks


def _finish_frame(prepared: dict) -> dict:
    """Phase 2 finalize: called after every row's futures (submitted via
    _submit_row_ocr) have been resolved onto r["spread"]/r["total"]/r["ml"] —
    assembles names and applies the garbage-row filter."""
    for r in prepared["rows"]:
        r["name"] = " ".join(r["name_lines"])
    clean_rows = [r for r in prepared["rows"]
                  if r["name"] and len(r["name"]) <= _MAX_NAME_LEN and not _NAME_GARBAGE_RE.search(r["name"])]
    return {"header_text": prepared["header_text"], "rows": clean_rows, "date_headers": prepared["date_headers"]}


def parse_frame(img: Image.Image) -> dict:
    """Single-frame convenience wrapper (CLI debug mode, or any other
    single-image caller) — runs both phases with its own pool. Processing a
    whole video's worth of frames should use _prepare_frame/_submit_row_ocr/
    _finish_frame directly with ONE shared pool across every frame instead
    (see games_from_circa_recording) — much better parallelism than a fresh
    pool per frame."""
    prepared = _prepare_frame(img)
    if prepared.get("skipped"):
        return {"header_text": None, "rows": [], "date_headers": [], "skipped": prepared["skipped"]}
    with concurrent.futures.ThreadPoolExecutor(max_workers=_OCR_MAX_WORKERS) as pool:
        tasks = []
        for r in prepared["rows"]:
            tasks.extend(_submit_row_ocr(pool, img, r))
        for r, field, fut in tasks:
            r[field] = fut.result()
    return _finish_frame(prepared)


def _band(words: list, top0: int, top1: int, x0: int, x1: int) -> list:
    return [w for w in words if x0 <= w["left"] < x1 and top0 - 15 <= w["top"] < top1 - 20]


# ── Cross-frame consensus merge ────────────────────────────────────────────
#
# A slow scroll captures each game across several overlapping frames. Rather
# than trust any single frame's OCR, vote per field across every frame that
# saw a given rotation number and keep only values with a real majority.
# Anything without a clean majority (including rows seen in only one frame —
# no corroboration at all) is flagged rather than guessed, per user decision.


def _vote(values: list) -> tuple:
    """Majority vote over non-null values. Returns (value, confident: bool)."""
    clean = [v for v in values if v]
    if not clean:
        return None, False
    counts = Counter(clean)
    best_val, best_n = counts.most_common(1)[0]
    tied = sum(1 for _, n in counts.items() if n == best_n) > 1
    confident = (not tied) and (best_n > 1 or len(clean) == 1)
    return best_val, confident


def _merge_rotation_rows(occurrences: list) -> dict:
    """occurrences: every parsed row (across all frames) sharing one rotation number."""
    name, name_ok       = _vote([o["name"] for o in occurrences])
    date_header, _       = _vote([o["date_header"] for o in occurrences])
    neutral_text, _       = _vote([o["neutral_site_text"] for o in occurrences])
    sp_point, sp_ok      = _vote([o["spread"]["point"] for o in occurrences])
    sp_juice, sp_juice_ok = _vote([o["spread"]["juice"] for o in occurrences])
    tot_side, tot_side_ok = _vote([o["total"]["side"] for o in occurrences])
    tot_point, tot_ok     = _vote([o["total"]["point"] for o in occurrences])
    tot_juice, tot_juice_ok = _vote([o["total"].get("juice") for o in occurrences])
    ml, ml_ok            = _vote([o["ml"] for o in occurrences])

    warnings = []
    if len(occurrences) == 1:
        warnings.append("seen in only 1 frame — no cross-frame corroboration")
    if not name_ok:
        warnings.append(f"team name disagreement across frames: {[o['name'] for o in occurrences]}")
    if not sp_ok:
        warnings.append(f"spread point disagreement/uncertain: {[o['spread']['point'] for o in occurrences]}")
    if not tot_ok:
        warnings.append(f"total point disagreement/uncertain: {[o['total']['point'] for o in occurrences]}")
    if not ml_ok:
        warnings.append(f"moneyline disagreement/uncertain: {[o['ml'] for o in occurrences]}")
    for o in occurrences:
        if o["spread"].get("warning"):
            warnings.append(o["spread"]["warning"])
        if o["total"].get("warning"):
            warnings.append(o["total"]["warning"])

    return {
        "name": name, "date_header": date_header, "neutral_site_text": neutral_text,
        "spread_point": sp_point, "spread_juice": sp_juice,
        "total_side": tot_side, "total_point": tot_point, "total_juice": tot_juice,
        "ml": ml, "warnings": warnings,
    }


def _parse_date_header(text: str | None, ref_year: int) -> str | None:
    """'Sep 4, 8:00 PM' -> ISO8601 UTC. Circa shows local (ET) time; treated as
    naive/local here — caller should adjust for timezone if precision matters."""
    if not text:
        return None
    m = re.match(r"^([A-Za-z]{3})\.?\s+(\d{1,2}),?\s+(\d{1,2}):(\d{2})\s*(AM|PM)$", text.strip())
    if not m:
        return None
    mon, day, hh, mm, ampm = m.groups()
    month_num = {"Jan": 1, "Feb": 2, "Mar": 3, "Apr": 4, "May": 5, "Jun": 6,
                 "Jul": 7, "Aug": 8, "Sep": 9, "Oct": 10, "Nov": 11, "Dec": 12}.get(mon)
    if not month_num:
        return None
    h = int(hh) % 12
    if ampm == "PM":
        h += 12
    try:
        from datetime import datetime, timezone
        return datetime(ref_year, month_num, int(day), h, int(mm), tzinfo=timezone.utc).isoformat()
    except ValueError:
        return None


# Very small, deliberately conservative abbreviation-expansion table — Circa's
# team-name abbreviations vs. the Odds API's fuller names are matched later in
# app.py's existing _match_teams() fuzzy matcher; this just improves that
# matcher's odds by expanding a few extremely common patterns. Anything not
# covered here is passed through as-is (raw Circa text), not invented.
_PREFIX_EXPAND = {"E ": "EASTERN ", "W ": "WESTERN ", "N ": "NORTHERN ", "S ": "SOUTHERN "}
_SUFFIX_EXPAND = {" ST": " STATE", " COLL": " COLLEGE", " CAR": " CAROLINA"}


def _expand_team_name(raw: str) -> str:
    name = raw.strip().upper()
    for pre, full in _PREFIX_EXPAND.items():
        if name.startswith(pre):
            name = full + name[len(pre):]
            break
    for suf, full in _SUFFIX_EXPAND.items():
        if name.endswith(suf):
            name = name[: -len(suf)] + full
            break
    return name.title()


def games_from_circa_recording(video_path: str, sport_key: str = "",
                                every_n_seconds: float = 0.4, ref_year: int | None = None) -> list:
    """
    Parse a Circa screen recording end to end: extract frames, OCR each one,
    cross-frame-vote every field, pair rows into games. Output matches
    parse_bookmaker_har.games_from_bookmaker_har()'s shape so it can be
    matched against Odds API events the same way (via app.py's _match_teams).

    Sampling more frequently (smaller every_n_seconds) gives more redundant
    reads per game and thus better consensus — but only helps if the
    recording itself scrolls slowly/steadily; a fast flick-scroll still only
    shows each row once or twice no matter how densely frames are sampled.
    """
    from datetime import datetime, timezone
    ref_year = ref_year or datetime.now(timezone.utc).year

    frames = extract_stable_frames(video_path)
    if not frames:
        return []

    # Phase 1 (cheap, sequential is fine): row/geometry detection for every
    # frame in the video.
    prepared_list = [_prepare_frame(img) for _, img in frames]

    # Phase 2 (expensive): submit every frame's per-cell re-OCR tasks into
    # ONE shared pool, so the whole video's OCR workload runs concurrently
    # across all available cores at once — not one frame's worth at a time,
    # which left most of a many-core machine idle between frames.
    with concurrent.futures.ThreadPoolExecutor(max_workers=_OCR_MAX_WORKERS) as pool:
        all_tasks = []
        for p in prepared_list:
            for r in p["rows"]:
                all_tasks.extend(_submit_row_ocr(pool, p["img"], r))
        for r, field, fut in all_tasks:
            r[field] = fut.result()

    parsed = [_finish_frame(p) for p in prepared_list]

    header_votes = Counter(p["header_text"] for p in parsed if p["header_text"])
    detected_header = header_votes.most_common(1)[0][0] if header_votes else None

    by_rotation: dict = defaultdict(list)
    for p in parsed:
        for r in p["rows"]:
            by_rotation[r["rotation"]].append(r)

    merged = {rot: _merge_rotation_rows(occ) for rot, occ in by_rotation.items()}
    # section is structural (a rotation number belongs to exactly one section), but a
    # transition frame can occasionally mis-tag one occurrence — vote rather than
    # trust any single occurrence (e.g. the first).
    for rot, occ in by_rotation.items():
        merged[rot]["section"] = Counter(o["section"] for o in occ).most_common(1)[0][0]

    # Trust the numeric rotation threshold over the OCR'd "Game"/"Periods" section
    # label alone — a period row occasionally gets text-misclassified as "game" by
    # a noisy frame, and pairing it into the main game list produces a bogus game.
    # A rotation < 1000 can never legitimately be a period row (see offset above).
    game_rows = sorted(
        ((rot, row) for rot, row in merged.items()
         if row["section"] == "game" and int(rot) < PERIOD_ROTATION_OFFSET),
        key=lambda kv: int(kv[0]),
    )

    games = []
    pending = None
    for rot, row in game_rows:
        if pending is None:
            pending = (rot, row)
            continue
        away_rot, away = pending
        home_rot, home = rot, row
        pending = None

        warnings = list(away["warnings"]) + list(home["warnings"])
        if away["date_header"] != home["date_header"]:
            warnings.append(
                f"date-header mismatch within pair #{away_rot}/#{home_rot}: "
                f"{away['date_header']!r} vs {home['date_header']!r} — pairing may be wrong"
            )

        neutral = bool(away.get("neutral_site_text") or home.get("neutral_site_text"))

        markets = _build_markets(away, home, warnings, f"#{away_rot}/#{home_rot}")

        # Period markets (1H, 1Q, ...): each period level uses its own fixed
        # rotation-number offset from the parent game — a reliable structural
        # link (confirmed against real samples for both) used instead of
        # fuzzy team-name matching across sections. Each level's plausibility
        # baseline is the *previous* level (1H vs full game; 1Q vs 1H) since
        # the compression pattern is monotonic — confirmed on a real sample:
        # Miami FL spread -24 (full) -> -14 (1H) -> -6.5 (1Q), ML -3200 ->
        # -1050 -> -550, same for the total.
        prior_markets = markets
        for offset, suffix, level_label in ((PERIOD_ROTATION_OFFSET, "h1", "1H"),
                                              (QUARTER_ROTATION_OFFSET, "q1", "1Q")):
            p_away_rot, p_home_rot = str(int(away_rot) + offset), str(int(home_rot) + offset)
            p_away, p_home = merged.get(p_away_rot), merged.get(p_home_rot)
            if p_away and p_home and p_away["section"] == "periods" and p_home["section"] == "periods":
                p_warnings = []
                p_markets = _build_markets(p_away, p_home, p_warnings, f"#{p_away_rot}/#{p_home_rot} ({level_label})")
                _resolve_h1_spread_by_plausibility(prior_markets.get("spreads"), p_markets.get("spreads"),
                                                    p_warnings, f"#{away_rot}/#{home_rot} ({level_label})")
                _check_h1_ml_plausibility(prior_markets.get("h2h"), p_markets.get("h2h"),
                                           p_warnings, f"#{away_rot}/#{home_rot} ({level_label})")
                for k, v in p_markets.items():
                    markets[f"{k}_{suffix}"] = v
                warnings.extend(p_warnings)
                prior_markets = p_markets   # next level's baseline is this level, not the full game
            elif p_away or p_home:
                warnings.append(f"{level_label} rows partially found for pair #{away_rot}/#{home_rot} "
                                 f"(rotation {p_away_rot}/{p_home_rot}) — skipped, incomplete")

        games.append({
            "away_team":     _expand_team_name(away["name"]) if away["name"] else None,
            "home_team":     _expand_team_name(home["name"]) if home["name"] else None,
            "commence_time": _parse_date_header(away["date_header"] or home["date_header"], ref_year),
            "markets":       markets,
            "_rotation":     {"away": int(away_rot), "home": int(home_rot)},
            "_neutral_site":  neutral,
            "_raw_names":    {"away": away["name"], "home": home["name"]},
            "_sport_header": detected_header,
            "_warnings":     warnings,
        })

    if pending is not None:
        rot, row = pending
        logger.warning(f"circa parse: rotation #{rot} ({row['name']}) has no pair — dropped "
                        "(recording likely ended mid-game; re-record with a bit more scroll)")

    return games


def _reconcile_symmetric(a, b, opposite: bool, label: str, warnings: list):
    """
    A game's two spread points are always exact opposites; a total's two
    points are always the identical number. Use that to recover a value one
    side failed to read cleanly, and to catch a disagreement between two
    values that both look "clean" but can't both be right. Never silently
    picks a winner when they actively disagree — flags instead, per policy.
    """
    expected_b = -a if (a is not None and opposite) else a
    if a is not None and b is not None:
        if abs((b if b is not None else 0) - (expected_b if expected_b is not None else 0)) > 0.01:
            warnings.append(f"{label}: sides disagree ({a} vs {b}, expected exact "
                             f"{'opposite' if opposite else 'match'}) — not auto-corrected")
        return a, b
    if a is not None and b is None:
        warnings.append(f"{label}: home/away side missing, inferred {expected_b} from the other side")
        return a, expected_b
    if b is not None and a is None:
        inferred_a = -b if opposite else b
        warnings.append(f"{label}: home/away side missing, inferred {inferred_a} from the other side")
        return inferred_a, b
    return None, None


def _resolve_h1_spread_by_plausibility(full_spreads: dict | None, h1_spreads: dict | None,
                                        warnings: list, pair_label: str) -> None:
    """
    A team's 1H point-spread edge can never exceed their full-game edge — the
    full game strictly contains the first half. Mutates h1_spreads in place
    when exactly one side of a disagreeing 1H spread pair is plausible
    relative to the full-game spread and the other clearly isn't (this is
    what actually happened on a real sample: a digit-OCR error read "-1½" as
    "-17.5", wildly implausible against a full-game spread of -3, while the
    other side's clean "1.5" wasn't). Leaves it alone — still flagged
    upstream — when the check itself is inconclusive; never guesses between
    two similarly-plausible values.
    """
    if not full_spreads or not h1_spreads:
        return
    full_away = full_spreads.get("away_point")
    a, h = h1_spreads.get("away_point"), h1_spreads.get("home_point")
    if full_away is None or a is None or h is None:
        return
    if abs(a + h) < 0.01:
        return   # sides already agree, nothing to resolve
    limit = abs(full_away) * 1.25 + 1   # small buffer over "can't exceed full-game edge"
    a_ok, h_ok = abs(a) <= limit, abs(h) <= limit
    if a_ok and not h_ok:
        h1_spreads["home_point"] = -a
        warnings.append(f"1H spread {pair_label}: resolved disagreement ({a} vs {h}) to {a}/{-a} — "
                         f"{h} exceeds the full-game spread ({full_away}), which a 1H edge can't do")
    elif h_ok and not a_ok:
        h1_spreads["away_point"] = -h
        warnings.append(f"1H spread {pair_label}: resolved disagreement ({a} vs {h}) to {-h}/{h} — "
                         f"{a} exceeds the full-game spread ({full_away}), which a 1H edge can't do")
    # else: both or neither plausible — leave flagged, don't guess


def _check_h1_ml_plausibility(full_h2h: dict | None, h1_h2h: dict | None,
                               warnings: list, pair_label: str) -> None:
    """
    A 1H moneyline is never more extreme than the same team's full-game line —
    less time on the clock means more uncertainty, so odds compress toward
    even, not away from it. Held on every one of 4 real games checked (e.g.
    +1300/-2600 full -> +675/-1000 1H). No independent second read to resolve
    a violation against (unlike the spread checks), so this only flags.
    """
    if not full_h2h or not h1_h2h:
        return
    for side in ("away_odds", "home_odds"):
        full_raw, h1_raw = full_h2h.get(side), h1_h2h.get(side)
        if not full_raw or not h1_raw:
            continue
        try:
            full_val = abs(int(full_raw.replace("+", "")))
            h1_val = abs(int(h1_raw.replace("+", "")))
        except (ValueError, AttributeError):
            continue
        if h1_val > full_val * 1.15 + 20:   # small buffer for legitimate edge cases
            warnings.append(f"moneyline {pair_label} ({side}): 1H line ({h1_raw}) is more extreme "
                             f"than the full-game line ({full_raw}) — 1H should compress toward even, "
                             f"likely a misread")


def _build_markets(away: dict, home: dict, warnings: list, pair_label: str) -> dict:
    """Build h2h/spreads/totals for one away/home row pair (full-game or period)."""
    markets = {}
    if away["ml"] or home["ml"]:
        markets["h2h"] = {"away_odds": away["ml"], "home_odds": home["ml"],
                           "away_point": None, "home_point": None}

    if away["spread_point"] or home["spread_point"]:
        a_pt, h_pt = _reconcile_symmetric(_to_float(away["spread_point"]), _to_float(home["spread_point"]),
                                           opposite=True, label=f"spread {pair_label}", warnings=warnings)
        markets["spreads"] = {
            "away_odds": away["spread_juice"], "home_odds": home["spread_juice"],
            "away_point": a_pt, "home_point": h_pt,
        }

    if away["total_point"] or home["total_point"]:
        # normalize to home=Over/away=Under, matching parse_bookmaker_har's convention
        over_row  = away if away["total_side"] == "over" else home if home["total_side"] == "over" else None
        under_row = away if away["total_side"] == "under" else home if home["total_side"] == "under" else None
        if not (over_row and under_row):
            warnings.append(f"totals over/under side unclear for {pair_label}")
        h_pt, a_pt = _reconcile_symmetric(
            _to_float(over_row["total_point"]) if over_row else None,
            _to_float(under_row["total_point"]) if under_row else None,
            opposite=False, label=f"total {pair_label}", warnings=warnings,
        )
        markets["totals"] = {
            "home_odds": (over_row or {}).get("total_juice"),
            "away_odds": (under_row or {}).get("total_juice"),
            "home_point": h_pt, "away_point": a_pt,
        }

    _check_ml_sign_consistency(markets.get("h2h"), markets.get("spreads"), warnings, pair_label)
    return markets


def _check_ml_sign_consistency(h2h: dict | None, spreads: dict | None, warnings: list, pair_label: str) -> None:
    """The spread favorite (negative points) must have the negative moneyline —
    there's no read where a team gets both points *and* better ML odds than
    their opponent. Flags only; there's no independent signal here to know
    which of the two (spread sign vs. ML sign) is the one that's actually
    wrong, so this isn't auto-corrected like the spread/total symmetry checks."""
    if not h2h or not spreads:
        return
    a_ml, h_ml = h2h.get("away_odds"), h2h.get("home_odds")
    a_pt, h_pt = spreads.get("away_point"), spreads.get("home_point")
    if a_ml is None or h_ml is None or a_pt is None or h_pt is None or a_pt == h_pt:
        return
    try:
        a_ml_val, h_ml_val = int(a_ml.replace("+", "")), int(h_ml.replace("+", ""))
    except (ValueError, AttributeError):
        return
    favorite_is_away = a_pt < h_pt
    if favorite_is_away and not (a_ml_val < 0 < h_ml_val):
        warnings.append(f"moneyline {pair_label}: spread favors away ({a_pt}) but ML doesn't "
                         f"reflect that (away={a_ml}, home={h_ml}) — one of these is likely misread")
    elif not favorite_is_away and not (h_ml_val < 0 < a_ml_val):
        warnings.append(f"moneyline {pair_label}: spread favors home ({h_pt}) but ML doesn't "
                         f"reflect that (away={a_ml}, home={h_ml}) — one of these is likely misread")


def _to_float(s):
    if s is None:
        return None
    try:
        return float(s)
    except (ValueError, TypeError):
        return None


if __name__ == "__main__":
    # --json <video_path>: run the full pipeline and print result games as a
    # JSON array to stdout — nothing else on stdout in this mode. Meant to be
    # invoked as an isolated subprocess (see circa_watcher.py) with an outer
    # timeout, since a single OCR/CV call has been observed to hang
    # indefinitely in practice (root cause not fully pinned down — tried both
    # a pytesseract-level timeout and forcing local file hydration first,
    # neither reliably fixed it) and a long-running watcher process must not
    # be able to freeze entirely because of it.
    if sys.argv[1] == "--json":
        import json
        games = games_from_circa_recording(sys.argv[2])
        print(json.dumps(games))
        sys.exit(0)

    path = sys.argv[1]
    if path.lower().endswith((".png", ".jpg", ".jpeg")):
        img = Image.open(path)
    else:
        frames = extract_frames(path)
        print(f"extracted {len(frames)} frames")
        img = frames[0][1]

    result = parse_frame(img)
    print(f"header: {result['header_text']!r}")
    print(f"date headers: {[h['text'] for h in result['date_headers']]}")
    for r in result["rows"]:
        print(f"  #{r['rotation']:>4}  {r['name']:<16}  spread={r['spread']}  total={r['total']}  ml={r['ml']}")
