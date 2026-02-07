@echo off
echo ===================================================
echo   OSINT TRACKER - DAILY REPORT GENERATOR
echo ===================================================
echo.
echo Running generation script...
.venv\Scripts\python.exe scripts\generate_daily_report.py
echo.
echo Done! Opening report...
start report.html
pause
