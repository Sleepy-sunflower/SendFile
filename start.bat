@echo off
cd /d "%~dp0"
where node >nul 2>&1
if errorlevel 1 (
  echo 未找到 Node.js。请先安装: https://nodejs.org
  pause
  exit /b 1
)
node server.js
