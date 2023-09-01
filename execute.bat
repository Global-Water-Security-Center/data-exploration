@echo off
setlocal enabledelayedexpansion

:: Initialize an empty string to hold the cleaned arguments
set "clean_args="

:loop
if "%~1"=="" goto endloop

:: Replace \ with / in the current argument and append it to clean_args
set "arg=%~1"
set "arg=!arg:\=/!"
set "clean_args=!clean_args! !arg!"

:: Move to the next argument
shift
goto loop

:endloop

:: Run your Docker command with the cleaned arguments
docker container prune -f && docker pull therealspring/gwsc-executor:latest && docker run -it -v "%CD%":/usr/local/gwsc_app therealspring/gwsc-executor:latest python3 !clean_args!
