@echo off
rem bodkin snipe: dry run, min score 55. Nothing is bought until you start it with --live. Ctrl+C to stop.
cd /d "%~dp0"
if not exist .env copy .env.example .env >nul
where cargo >nul 2>&1 || (echo install Rust from https://rustup.rs && pause && exit /b 1)
if not exist target\release\bodkin.exe (
  echo building bodkin...
  cargo build --release || (pause && exit /b 1)
)
target\release\bodkin.exe snipe --min-score 55
