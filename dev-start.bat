@echo off
setlocal

set "ROOT=%~dp0"

start "Adamant Backend" cmd /k "cd /d %ROOT%backend && call venv\Scripts\activate && set FLASK_APP=api.py && set FLASK_ENV=development && python -m flask run --no-debugger --host 127.0.0.1 --port 5000"
start "Adamant Frontend" cmd /k "cd /d %ROOT% && npm run dev"
start "Adamant DB-UI" cmd /k "cd /d %ROOT%db-ui && npm run dev"
