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

def start_server():
    subprocess.Popen(
        [PYTHON, 'app.py'],
        cwd=APP_DIR,
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
