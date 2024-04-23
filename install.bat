@echo off

REM Check if Python 3.9 is installed
where python3.9 >nul 2>nul
if %ERRORLEVEL% NEQ 0 (
    echo Python 3.9 is not installed. Please download and install Python 3.9 from https://www.python.org/downloads/
    exit /b 1
)

REM Install pip for Python 3.9
curl https://bootstrap.pypa.io/get-pip.py -o get-pip.py
python3.9 get-pip.py
del get-pip.py

REM Install libraries from requirements.txt
python3.9 -m pip install -r requirements.txt

echo Installation complete.
