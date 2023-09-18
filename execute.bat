@echo off

setlocal enabledelayedexpansion

:: convert all \ to / if exists
set "args=%*"
echo %args%
set args=!args:\=/!

:: Run your Docker command with the cleaned arguments
docker container prune -f && docker pull therealspring/gwsc-executor:latest && docker run -it --rm -v "%CD%":/usr/local/gwsc_app therealspring/gwsc-executor:latest python3 %args%
