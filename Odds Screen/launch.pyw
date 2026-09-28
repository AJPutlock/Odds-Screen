"""
Odds Screen Launcher
Double-click or run via the Desktop shortcut.
- If the server is already running, just opens the browser.
- Otherwise starts Flask silently in the background, waits for it, then opens the browser.
"""
import subprocess
import sys
import os
import time
import webbrowser
import socket

APP_DIR = os.path.dirname(os.path.abspath(__file__))
PORT    = 5000

def is_server_running():
    try:
        with socket.create_connection(('localhost', PORT), timeout=1):
            return True
    except OSError:
        return False

PYTHON = r'C:\Users\ajput\AppData\Local\Programs\Python\Python311\python.exe'

# The server runs without a console, so its output goes to a log file —
# otherwise nothing records why a Bookmaker pull or login failed. Started
# fresh once it passes LOG_MAX_BYTES.
LOG_PATH      = os.path.join(APP_DIR, 'data', 'odds_screen.log')
LOG_MAX_BYTES = 5_000_000

def start_server():
    os.makedirs(os.path.dirname(LOG_PATH), exist_ok=True)
    if os.path.exists(LOG_PATH) and os.path.getsize(LOG_PATH) > LOG_MAX_BYTES:
        os.replace(LOG_PATH, LOG_PATH + '.old')
    log = open(LOG_PATH, 'a', encoding='utf-8', buffering=1)
    log.write(f"\n===== started {time.strftime('%Y-%m-%d %H:%M:%S')} =====\n")
    subprocess.Popen(
        [PYTHON, '-u', 'app.py'],   # -u: unbuffered, so lines land as they happen
        cwd=APP_DIR,
        stdout=log, stderr=subprocess.STDOUT,
        env={**os.environ, 'PYTHONIOENCODING': 'utf-8'},
        creationflags=0x08000000,  # CREATE_NO_WINDOW — no console popup
    )

if not is_server_running():
    start_server()
    # Wait up to 10 seconds for Flask to be ready
    for _ in range(20):
        time.sleep(0.5)
        if is_server_running():
            break

webbrowser.open(f'http://localhost:{PORT}')
