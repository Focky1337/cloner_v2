#!/bin/bash
echo "Установка зависимостей..."
pip install flask

echo ""
echo "Запуск веб-сервера..."
echo "Сайт будет доступен по адресу: http://127.0.0.1:5000"
echo ""
python3 app.py

