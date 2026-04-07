@echo off
title PDI System Starter
echo ==============================================
echo        STARTING PDI SYSTEM SERVICES
echo ==============================================

cd /d "%~dp0"

echo [1/6] Starting Telegram Bot (main.py)...
start "Telegram Bot" cmd /k "python main.py"

echo [2/6] Starting PDI Web (Port 9000)...
start "PDI Web (9000)" cmd /k "python pdi_web.py"

echo [3/6] Starting Inventory Web (Port 9111)...
start "Inventory Web (9111)" cmd /k "uvicorn inventory_web:app --host 0.0.0.0 --port 9111"

echo [4/6] Starting Report Service (Port 9112)...
start "Report Service (9112)" cmd /k "uvicorn report:app --host 0.0.0.0 --port 9112"

echo [5/6] Starting Claim Management (Port 9120)...
start "Claim Management (9120)" cmd /k "python cm_web.py"

echo [6/6] Starting EDC Battery PDF (Port 7100)...
start "EDC Battery PDF (7100)" cmd /k "python EDC.py"

echo.
echo ==============================================
echo All 6 services have been launched in separate windows!
echo Please keep those black CMD windows open to keep the system running.
echo ==============================================
pause
