@echo off
setlocal

set "ROOT=%~dp0.."
set "PY=%ROOT%\backend\venv\Scripts\python.exe"
if not exist "%PY%" set "PY=python"

rem Runs every 5 seconds
%PY% "%ROOT%\bin\insert_data2db_win.py" --watch --interval 5 --delete-missing
