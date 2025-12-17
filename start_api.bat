@echo off
REM Переходим в директорию проекта
cd /d "%~dp0"

REM Активируем виртуальное окружение
call .venv\Scripts\activate.bat

REM Запускаем FastAPI с uvicorn
python -m uvicorn qt_pvp.api:app --host 0.0.0.0 --port 8001 --env-file .env

