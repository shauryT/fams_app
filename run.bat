@echo off
echo Installing dependencies...
pip install flask flask-cors

echo.
echo Starting FAMS...
python app.py
pause
