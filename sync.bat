@echo off
echo Starting Database Sync Tool...
echo.

REM Check if sync.exe exists
if not exist "sync.exe" (
    echo ERROR: sync.exe not found!
    echo Please ensure sync.exe is in the same folder as this batch file.
    pause
    exit /b 1
)

REM Check if config.json exists  
if not exist "config.json" (
    echo ERROR: config.json not found!
    echo Please ensure config.json is in the same folder as this batch file.
    pause
    exit /b 1
)

REM Run the executable - will automatically close on success
start /wait sync.exe

REM This part will only run if sync failed (since successful sync exits directly)
if %errorlevel% neq 0 (
    echo.
    echo SYNC FAILED - Process encountered errors.
    echo Please check the sync.log file for details.
    echo.
    timeout /t 10
) else (
    echo.
    echo SYNC COMPLETED SUCCESSFULLY!
    echo.
    timeout /t 3
)