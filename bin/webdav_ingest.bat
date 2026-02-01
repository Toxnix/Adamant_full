@echo off
setlocal

set "PYTHON="
if defined CONDA_PREFIX if exist "%CONDA_PREFIX%\python.exe" set "PYTHON=%CONDA_PREFIX%\python.exe"
if not defined PYTHON if exist "C:\Users\Torsten\miniconda3\python.exe" set "PYTHON=C:\Users\Torsten\miniconda3\python.exe"
if not defined PYTHON (
  for /f "delims=" %%P in ('where python 2^>nul') do set "PYTHON=%%P" & goto :found
  for /f "delims=" %%P in ('where py 2^>nul') do set "PYTHON=%%P" & goto :found
)

:found
if not defined PYTHON (
  echo [ERROR] Python not found. Install Python or activate Conda.
  pause
  exit /b 1
)

"%PYTHON%" "%~dp0webdav_ingest.py" %*
echo.
echo [DONE] Press any key to close.
pause >nul
