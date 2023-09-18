@echo off

:: Run your Docker command with the cleaned arguments
docker container prune -f && docker pull therealspring/gwsc-executor:latest && docker run -it -v "%CD%":/usr/local/gwsc_app therealspring/gwsc-executor:latest python3 %*
