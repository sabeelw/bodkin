@echo off
rem bodkin board: the sniper behind http://127.0.0.1:4663. Dry run. Ctrl+C to stop.
cd /d "%~dp0"
if not exist .env copy .env.example .env >nul
where cargo >nul 2>&1 || (echo install Rust from https://rustup.rs && pause && exit /b 1)
if not exist target\release\bodkin.exe (
  echo building bodkin...
  cargo build --release || (pause && exit /b 1)
)
target\release\bodkin.exe board
