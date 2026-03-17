@echo off
setlocal EnableDelayedExpansion
cd /d "%~dp0"

set "APP_EXE=%~dp0wenjian-chuli.exe"
set "SCRIPT=%~dp0main.py"
set "CHECK=import importlib.util, sys; mods=('pandas','openpyxl','tkinter','tkinterdnd2'); sys.exit(0 if all(importlib.util.find_spec(m) for m in mods) else 1)"

if exist "%APP_EXE%" (
    start "" "%APP_EXE%"
    exit /b 0
)

call :try_python "%USERPROFILE%\miniconda3\python.exe" "%USERPROFILE%\miniconda3\pythonw.exe"
if %errorlevel%==0 exit /b 0

call :try_python "C:\Python314\python.exe" "C:\Python314\pythonw.exe"
if %errorlevel%==0 exit /b 0

call :try_python "%LOCALAPPDATA%\Programs\Python\Python310\python.exe" "%LOCALAPPDATA%\Programs\Python\Python310\pythonw.exe"
if %errorlevel%==0 exit /b 0

for /f "delims=" %%I in ('where python.exe 2^>nul') do (
    call :try_python "%%I" ""
    if !errorlevel!==0 exit /b 0
)

echo No packaged EXE or usable Python environment was found.
echo Required modules for source mode: pandas, openpyxl, tkinter, tkinterdnd2.
pause
exit /b 1

:try_python
setlocal EnableDelayedExpansion
set "PY=%~1"
set "PYW=%~2"

if not exist "!PY!" exit /b 1

"!PY!" -c "%CHECK%" >nul 2>nul
if errorlevel 1 exit /b 1

if exist "!PYW!" (
    start "" "!PYW!" "%SCRIPT%"
    exit /b 0
)

"!PY!" "%SCRIPT%"
set "RC=%errorlevel%"
if not "%RC%"=="0" pause
exit /b %RC%
