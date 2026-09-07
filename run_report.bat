@echo off
REM run_report.bat -- Generate the PDF SITREP and open the HTML report console.
setlocal

set PROJECT_ROOT=%~dp0
set PYTHON=%PROJECT_ROOT%.venv\Scripts\python.exe
if not exist "%PYTHON%" set PYTHON=python

echo [report] Generating PDF SITREP...
"%PYTHON%" "war_tracker_v2\scripts2\generate_report.py"
if errorlevel 1 (
    echo [report] PDF generation failed with exit code %ERRORLEVEL%.
    exit /b 1
)

echo [report] Exporting artifacts (report payload)...
"%PYTHON%" "scripts\generate_output.py"
if errorlevel 1 (
    echo [report] Artifact export failed with exit code %ERRORLEVEL%.
    exit /b 1
)

start "" "%PROJECT_ROOT%report.html"
echo [report] Done.
exit /b 0
