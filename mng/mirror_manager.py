import os
import sys
import json
import shutil
import asyncio
import logging
import sqlite3
from pathlib import Path
from datetime import datetime
from typing import Optional, Dict, List
from aiogram import Bot, Dispatcher, types
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.filters import Command, CommandStart
from aiogram.types import (
    Message,
    InlineKeyboardMarkup,
    InlineKeyboardButton,
    CallbackQuery
)
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
import aiohttp

try:
    from manager_config import (
        MANAGER_BOT_TOKEN,
        ADMIN_ID,
        CRYPTOBOT_TOKEN,
        PAYMENT_AMOUNT,
        CLONER_DIR,
        MIRROR_BASE_DIR
    )
except ImportError:
    import os
    BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    MANAGER_BOT_TOKEN = "YOUR_MANAGER_BOT_TOKEN"
    ADMIN_ID = 6995119648
    CRYPTOBOT_TOKEN = "YOUR_CRYPTOBOT_TOKEN"
    PAYMENT_AMOUNT = 7.0
    if os.name == 'nt':
        CLONER_DIR = BASE_DIR
        MIRROR_BASE_DIR = BASE_DIR
    else:
        CLONER_DIR = "/root/cloner"
        MIRROR_BASE_DIR = "/root/cloner"

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

bot = Bot(token=MANAGER_BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
storage = MemoryStorage()
dp = Dispatcher(storage=storage)

class MirrorStates(StatesGroup):
    WAITING_BOT_TOKEN = State()
    WAITING_PAYMENT = State()

try:
    from manager_config import MANAGER_DIR
except ImportError:
    import os
    BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    if os.name == 'nt':
        MANAGER_DIR = os.path.join(BASE_DIR, "mng")
    else:
        MANAGER_DIR = "/root/cloner/mng"

os.makedirs(MANAGER_DIR, exist_ok=True)
DB_PATH = os.path.join(MANAGER_DIR, "mirror_manager.db")

def init_manager_db():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS users (
            user_id INTEGER PRIMARY KEY,
            username TEXT,
            first_name TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            mirrors_count INTEGER DEFAULT 0
        )
    ''')
    
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS mirrors (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER,
            username TEXT,
            mirror_name TEXT,
            bot_token TEXT,
            directory_path TEXT,
            status TEXT DEFAULT 'pending',
            payment_status TEXT DEFAULT 'unpaid',
            invoice_id TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            activated_at TIMESTAMP,
            FOREIGN KEY (user_id) REFERENCES users (user_id)
        )
    ''')
    
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS payments (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            mirror_id INTEGER,
            invoice_id TEXT UNIQUE,
            amount REAL,
            currency TEXT DEFAULT 'USD',
            status TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            paid_at TIMESTAMP,
            FOREIGN KEY (mirror_id) REFERENCES mirrors (id)
        )
    ''')
    
    conn.commit()
    conn.close()

async def send_to_admin(message: str):
    try:
        await bot.send_message(ADMIN_ID, message, parse_mode=ParseMode.HTML)
    except Exception as e:
        logging.error(f"Ошибка отправки админу: {e}")

def get_user_info(user: types.User) -> tuple:
    username = user.username or "без_username"
    user_id = user.id
    first_name = user.first_name or "Пользователь"
    return username, user_id, first_name

def save_user_to_db(user_id: int, username: str, first_name: str):
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute('''
        INSERT OR REPLACE INTO users (user_id, username, first_name)
        VALUES (?, ?, ?)
    ''', (user_id, username, first_name))
    conn.commit()
    conn.close()

def create_mirror_directory(username: str, user_id: int) -> str:
    mirror_name = f"{username}_{user_id}"
    mirror_dir = os.path.join(MIRROR_BASE_DIR, mirror_name)
    
    if os.path.exists(mirror_dir):
        counter = 1
        while os.path.exists(f"{mirror_dir}_{counter}"):
            counter += 1
        mirror_dir = f"{mirror_dir}_{counter}"
        mirror_name = f"{username}_{user_id}_{counter}"
    
    os.makedirs(mirror_dir, exist_ok=True)
    os.makedirs(os.path.join(mirror_dir, "DataBase"), exist_ok=True)
    
    return mirror_dir, mirror_name

def copy_bot_files(source_dir: str, target_dir: str):
    files_to_copy = ['main.py', 'requirements.txt']
    
    for file in files_to_copy:
        source_path = os.path.join(source_dir, file)
        target_path = os.path.join(target_dir, file)
        
        if os.path.exists(source_path):
            shutil.copy2(source_path, target_path)
            logging.info(f"Скопирован файл {file} в {target_dir}")

def create_config_file(mirror_dir: str, bot_token: str, user_id: int):
    config_content = f'''import os

API_ID = 24670035
API_HASH = "f5f000a0f88b93ee5abea430945a94c8"

BOT_TOKEN = "{bot_token}"

ADMIN_IDS = [{user_id}]

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
UPLOAD_DIR = os.path.join(BASE_DIR, "DataBase")
os.makedirs(UPLOAD_DIR, exist_ok=True)

DB_PATH = os.path.join(UPLOAD_DIR, 'sessions.db')

DELAY_SECONDS = 7

MAX_FILE_SIZE = 10 * 1024 * 1024
'''
    
    config_path = os.path.join(mirror_dir, "config.py")
    with open(config_path, 'w', encoding='utf-8') as f:
        f.write(config_content)

def create_systemd_service(mirror_name: str, mirror_dir: str):
    if os.name == 'nt':
        logging.info(f"Пропуск создания systemd сервиса на Windows для {mirror_name}")
        return None
    
    service_content = f'''[Unit]
Description=Telegram Cloner Bot Mirror - {mirror_name}
After=network.target

[Service]
Type=simple
User=root
WorkingDirectory={mirror_dir}
Environment="PATH={mirror_dir}/venv/bin"
ExecStart={mirror_dir}/venv/bin/python3 {mirror_dir}/main.py
Restart=always
RestartSec=10
StandardOutput=journal
StandardError=journal

[Install]
WantedBy=multi-user.target
'''
    
    service_file = f"/etc/systemd/system/cloner-mirror-{mirror_name}.service"
    try:
        with open(service_file, 'w', encoding='utf-8') as f:
            f.write(service_content)
        
        # Перезагружаем systemd (используем полный путь)
        systemctl_paths = ["/usr/bin/systemctl", "/bin/systemctl", "systemctl"]
        systemctl_cmd = None
        
        for path in systemctl_paths:
            if os.path.exists(path) or path == "systemctl":
                systemctl_cmd = path
                break
        
        if systemctl_cmd:
            try:
                os.system(f"{systemctl_cmd} daemon-reload")
                logging.info(f"Systemd перезагружен через {systemctl_cmd}")
            except Exception as e:
                logging.warning(f"Не удалось перезагрузить systemd: {e}")
        else:
            logging.warning("systemctl не найден, сервис создан, но systemd не перезагружен")
        
        return service_file
    except Exception as e:
        logging.error(f"Ошибка создания systemd сервиса: {e}")
        return None

def save_mirror_to_db(user_id: int, username: str, mirror_name: str, 
                      bot_token: str, directory_path: str, invoice_id: str = None) -> int:
    """Сохранение информации о зеркале в БД"""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute('''
        INSERT INTO mirrors (user_id, username, mirror_name, bot_token, 
                           directory_path, invoice_id)
        VALUES (?, ?, ?, ?, ?, ?)
    ''', (user_id, username, mirror_name, bot_token, directory_path, invoice_id))
    mirror_id = cursor.lastrowid
    conn.commit()
    conn.close()
    return mirror_id

# ============================================
# CRYPTOBOT API
# ============================================
async def create_invoice(amount: float, currency: str = "USD", 
                        description: str = "Активация зеркала бота") -> Optional[Dict]:
    """Создание инвойса в CryptoBot"""
    url = f"https://pay.crypt.bot/api/createInvoice"
    headers = {
        "Crypto-Pay-API-Token": CRYPTOBOT_TOKEN,
        "Content-Type": "application/json"
    }
    
    # CryptoBot требует asset (валюту) вместо currency
    # Поддерживаемые валюты: USDT, BTC, ETH, BNB, TRX, TON, USDC
    asset_map = {
        "USD": "USDT",  # По умолчанию используем USDT для USD
        "USDT": "USDT",
        "BTC": "BTC",
        "ETH": "ETH"
    }
    asset = asset_map.get(currency, "USDT")
    
    data = {
        "asset": asset,
        "amount": str(amount),  # CryptoBot требует строку
        "description": description
    }
    
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(url, json=data, headers=headers) as response:
                result = await response.json()
                if result.get("ok"):
                    return result.get("result")
                else:
                    logging.error(f"Ошибка создания инвойса: {result}")
                    return None
    except Exception as e:
        logging.error(f"Ошибка при создании инвойса: {e}")
        return None

async def check_invoice_status(invoice_id: int) -> Optional[str]:
    """Проверка статуса инвойса"""
    url = f"https://pay.crypt.bot/api/getInvoices"
    headers = {
        "Crypto-Pay-API-Token": CRYPTOBOT_TOKEN
    }
    
    params = {
        "invoice_ids": str(invoice_id)
    }
    
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url, params=params, headers=headers) as response:
                result = await response.json()
                if result.get("ok"):
                    invoices = result.get("result", {}).get("items", [])
                    if invoices:
                        return invoices[0].get("status")  # paid, active, expired
                return None
    except Exception as e:
        logging.error(f"Ошибка при проверке инвойса: {e}")
        return None

# ============================================
# ОБРАБОТЧИКИ КОМАНД
# ============================================
@dp.message(CommandStart())
async def cmd_start(message: Message):
    """Обработчик команды /start"""
    username, user_id, first_name = get_user_info(message.from_user)
    save_user_to_db(user_id, username, first_name)
    
    welcome_text = f"""👋 <b>Добро пожаловать, {first_name}!</b>

🤖 <b>Это бот-менеджер для создания зеркал клонирующего бота</b>

📋 <b>Возможности зеркала:</b>
• Авторизация аккаунтов по номеру телефона
• Поддержка двухфакторной аутентификации (2FA)
• Сохранение сессий в SQLite базе данных
• Добавление аккаунтов через команду
• Удаление аккаунтов через меню
• Просмотр списка аккаунтов
• Установка чата-источника для каждого аккаунта
• Установка чатов назначения для каждого аккаунта
• Массовая установка источника для всех аккаунтов
• Массовая установка чатов назначения для всех аккаунтов
• Копирование сообщений из указанного чата в реальном времени
• Отправка сообщений из текстовых файлов
• Загрузка текстовых файлов с сообщениями в базу данных
• Удаление загруженных файлов сообщений
• Выбор файла сообщений для аккаунта
• Массовая установка файла сообщений для всех аккаунтов
• Включение/выключение режима копирования для аккаунта
• Настройка задержки между сообщениями
• Настройка задержки между аккаунтами
• Настройка множителя FloodWait
• Добавление прокси в базу данных
• Удаление прокси
• Назначение прокси аккаунту
• Удаление прокси из аккаунта
• Присоединение к чатам по ID, username или инвайт-ссылке
• Логирование действий в файл и консоль
• Отправка файла логов админам по команде /logs
• Удаление файла логов после отправки
• Корректировка текста сообщений по полу аккаунта (мужской/женский)
• Установка пола аккаунта (мужской/женский)
• Остановка копирования через меню
• Сохранение состояния копирования (указатели сообщений и аккаунтов)
• Интерактивное меню с inline-кнопками
• Проверка прав администратора
• Отправка уведомлений админам об ошибках
• Обработка ошибок Pyrogram (FloodWait, PeerIdInvalid, др.)
• Блокировка множественных запусков через portalocker
• Поддержка инвайт-ссылок формата +XXXXX
• Сохранение последнего ID сообщения для каждого аккаунта
• Управление персистентными клиентами для чат-режима
• Инициализация последнего ID сообщения для источников в чат-режиме
• Хранение чатов назначения в JSON-формате
• Отображение статуса (аккаунты, файлы, прокси, задержки)
• Команда /start для главного меню

💰 <b>Стоимость активации зеркала: ${PAYMENT_AMOUNT}</b>"""
    
    if PAYMENT_AMOUNT > 0:
        welcome_text += """
💳 Оплата через CryptoBot (криптовалюта)

После оплаты зеркало будет активировано и готово к работе!"""
    else:
        welcome_text += """
✅ <b>БЕСПЛАТНО ДЛЯ ТЕСТИРОВАНИЯ</b>

Зеркало будет активировано сразу после создания!"""
    
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🪞 Создать зеркало", callback_data="create_mirror")]
    ])
    
    await message.answer(welcome_text, reply_markup=keyboard)
    
    # Уведомление админу о новом пользователе
    await send_to_admin(
        f"👤 <b>Новый пользователь</b>\n"
        f"ID: {user_id}\n"
        f"Username: @{username}\n"
        f"Имя: {first_name}"
    )

@dp.callback_query(lambda c: c.data == "create_mirror")
async def create_mirror_handler(callback: CallbackQuery, state: FSMContext):
    """Обработчик создания зеркала"""
    await callback.answer()
    await state.set_state(MirrorStates.WAITING_BOT_TOKEN)
    
    await callback.message.edit_text(
        "🤖 <b>Создание зеркала</b>\n\n"
        "📝 Пожалуйста, отправьте токен бота для зеркала.\n\n"
        "💡 <i>Получить токен можно у @BotFather в Telegram</i>",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="❌ Отмена", callback_data="cancel")]
        ])
    )

@dp.callback_query(lambda c: c.data == "cancel")
async def cancel_handler(callback: CallbackQuery, state: FSMContext):
    """Отмена создания зеркала"""
    await state.clear()
    await callback.message.edit_text("❌ Создание зеркала отменено")
    await cmd_start(callback.message)

@dp.message(MirrorStates.WAITING_BOT_TOKEN)
async def process_bot_token(message: Message, state: FSMContext):
    """Обработка токена бота"""
    bot_token = message.text.strip()
    
    # Базовая проверка формата токена
    if not bot_token or ":" not in bot_token:
        await message.answer(
            "❌ Неверный формат токена!\n\n"
            "Токен должен быть в формате: 123456789:ABCdefGHIjklMNOpqrsTUVwxyz\n\n"
            "Попробуйте еще раз:"
        )
        return
    
    # Получаем username бота через API
    bot_username = None
    try:
        import aiohttp
        async with aiohttp.ClientSession() as session:
            async with session.get(f"https://api.telegram.org/bot{bot_token}/getMe", timeout=aiohttp.ClientTimeout(total=5)) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    if data.get('ok') and data.get('result'):
                        bot_username = data['result'].get('username')
                        if bot_username:
                            await message.answer(f"✅ Бот найден: @{bot_username}")
    except Exception as e:
        logging.warning(f"Не удалось получить username бота: {e}")
        await message.answer("⚠️ Не удалось определить username бота, но продолжаем...")
    
    username, user_id, first_name = get_user_info(message.from_user)
    
    try:
        # Создаем директорию для зеркала
        mirror_dir, mirror_name = create_mirror_directory(username, user_id)
        
        # Копируем файлы
        copy_bot_files(CLONER_DIR, mirror_dir)
        
        # Создаем config.py
        create_config_file(mirror_dir, bot_token, user_id)
        
        # Создаем виртуальное окружение (только на Linux)
        if os.name != 'nt':  # Linux
            os.system(f"cd {mirror_dir} && python3 -m venv venv")
            
            # Устанавливаем зависимости
            os.system(f"cd {mirror_dir} && {mirror_dir}/venv/bin/pip install --quiet --upgrade pip")
            os.system(f"cd {mirror_dir} && {mirror_dir}/venv/bin/pip install --quiet -r {mirror_dir}/requirements.txt")
            
            # Создаем systemd сервис (но не запускаем)
            service_file = create_systemd_service(mirror_name, mirror_dir)
        else:  # Windows
            logging.info(f"Пропуск создания venv и systemd сервиса на Windows для {mirror_name}")
            service_file = None
        
        # Если цена 0, сразу активируем зеркало без оплаты
        if PAYMENT_AMOUNT == 0:
            # Сохраняем зеркало в БД без invoice_id
            mirror_id = save_mirror_to_db(
                user_id, username, mirror_name, bot_token, 
                mirror_dir, None
            )
            
            # Сохраняем bot_username в БД
            if bot_username:
                conn = sqlite3.connect(DB_PATH)
                cursor = conn.cursor()
                # Добавляем колонку bot_username, если её нет
                try:
                    cursor.execute('ALTER TABLE mirrors ADD COLUMN bot_username TEXT')
                except sqlite3.OperationalError:
                    pass  # Колонка уже существует
                cursor.execute('UPDATE mirrors SET bot_username = ? WHERE id = ?', (bot_username, mirror_id))
                conn.commit()
                conn.close()
            
            # Сохраняем платеж как оплаченный
            conn = sqlite3.connect(DB_PATH)
            cursor = conn.cursor()
            cursor.execute('''
                INSERT INTO payments (mirror_id, invoice_id, amount, currency, status)
                VALUES (?, ?, ?, ?, ?)
            ''', (mirror_id, None, 0, "USD", "paid"))
            conn.commit()
            conn.close()
            
            await state.clear()
            
            # Сразу активируем зеркало
            await activate_mirror(mirror_id, mirror_name, mirror_dir, user_id, bot_username)
            return
        
        # Если цена > 0, создаем инвойс для оплаты
        invoice = await create_invoice(
            PAYMENT_AMOUNT,
            "USD",
            f"Активация зеркала {mirror_name}"
        )
        
        if not invoice:
            await message.answer(
                "❌ Ошибка при создании платежа. Попробуйте позже."
            )
            return
        
        invoice_id = invoice.get("invoice_id")
        invoice_url = invoice.get("pay_url")
        
        # Сохраняем зеркало в БД
        mirror_id = save_mirror_to_db(
            user_id, username, mirror_name, bot_token, 
            mirror_dir, invoice_id
        )
        
        # Сохраняем bot_username в БД
        if bot_username:
            conn = sqlite3.connect(DB_PATH)
            cursor = conn.cursor()
            # Добавляем колонку bot_username, если её нет
            try:
                cursor.execute('ALTER TABLE mirrors ADD COLUMN bot_username TEXT')
            except sqlite3.OperationalError:
                pass  # Колонка уже существует
            cursor.execute('UPDATE mirrors SET bot_username = ? WHERE id = ?', (bot_username, mirror_id))
            conn.commit()
            conn.close()
        
        # Сохраняем платеж в БД
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        cursor.execute('''
            INSERT INTO payments (mirror_id, invoice_id, amount, currency, status)
            VALUES (?, ?, ?, ?, ?)
        ''', (mirror_id, invoice_id, PAYMENT_AMOUNT, "USD", "active"))
        conn.commit()
        conn.close()
        
        await state.clear()
        
        # Отправляем сообщение с кнопкой оплаты
        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text=f"💳 Оплатить ${PAYMENT_AMOUNT}", url=invoice_url)],
            [InlineKeyboardButton(text="🔄 Проверить оплату", callback_data=f"check_payment_{mirror_id}")]
        ])
        
        await message.answer(
            f"✅ <b>Зеркало создано!</b>\n\n"
            f"📁 Директория: <code>{mirror_name}</code>\n"
            f"🤖 Токен: <code>{bot_token[:10]}...</code>\n\n"
            f"💰 <b>Для активации необходимо оплатить ${PAYMENT_AMOUNT}</b>\n\n"
            f"💳 Нажмите кнопку ниже для оплаты:",
            reply_markup=keyboard
        )
        
        # Уведомление админу
        await send_to_admin(
            f"🪞 <b>Создано новое зеркало</b>\n"
            f"👤 Пользователь: @{username} ({user_id})\n"
            f"📁 Имя: {mirror_name}\n"
            f"📂 Путь: {mirror_dir}\n"
            f"💰 Сумма: ${PAYMENT_AMOUNT}\n"
            f"📊 Статус: Ожидание оплаты"
        )
        
    except Exception as e:
        logging.error(f"Ошибка при создании зеркала: {e}")
        await message.answer(
            f"❌ Произошла ошибка при создании зеркала:\n<code>{str(e)}</code>"
        )

@dp.callback_query(lambda c: c.data.startswith("check_payment_"))
async def check_payment_handler(callback: CallbackQuery):
    """Проверка статуса оплаты"""
    await callback.answer("Проверяю статус оплаты...")
    
    mirror_id = int(callback.data.split("_")[-1])
    
    # Получаем информацию о зеркале и платеже
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute('''
        SELECT m.invoice_id, m.mirror_name, m.directory_path, m.status, p.status as payment_status
        FROM mirrors m
        LEFT JOIN payments p ON m.id = p.mirror_id
        WHERE m.id = ?
    ''', (mirror_id,))
    result = cursor.fetchone()
    conn.close()
    
    if not result:
        await callback.message.answer("❌ Зеркало не найдено")
        return
    
    invoice_id, mirror_name, directory_path, mirror_status, payment_status = result
    
    if mirror_status == "active":
        await callback.message.answer("✅ Зеркало уже активировано!")
        return
    
    # Проверяем статус инвойса
    invoice_status = await check_invoice_status(int(invoice_id))
    
    if invoice_status == "paid":
        # Активируем зеркало
        await activate_mirror(mirror_id, mirror_name, directory_path, callback.from_user.id)
    elif invoice_status == "active":
        await callback.message.answer("⏳ Оплата еще не получена. Попробуйте позже.")
    else:
        await callback.message.answer("❌ Инвойс истек или отменен. Создайте новое зеркало.")

def create_web_client_entry(user_id: int, username: str, first_name: str, mirror_name: str, bot_username: str = None):
    """Создание записи клиента для веб-панели"""
    try:
        # Путь к БД веб-авторизации
        web_auth_db = os.path.join(MANAGER_DIR, "web_auth.db")
        
        # Инициализируем БД, если её нет
        conn = sqlite3.connect(web_auth_db)
        cursor = conn.cursor()
        
        # Создаем таблицы, если их нет
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS web_clients (
                user_id INTEGER PRIMARY KEY,
                username TEXT,
                first_name TEXT,
                web_url TEXT UNIQUE,
                bot_username TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        # Добавляем колонку bot_username, если её нет
        try:
            cursor.execute('ALTER TABLE web_clients ADD COLUMN bot_username TEXT')
        except sqlite3.OperationalError:
            pass  # Колонка уже существует
        
        # Создаем уникальный URL (username_userid или userid)
        if username and username != "без_username":
            web_url = f"{username}_{user_id}"
        else:
            web_url = str(user_id)
        
        # Проверяем, не существует ли уже такой URL
        cursor.execute('SELECT user_id FROM web_clients WHERE web_url = ?', (web_url,))
        if cursor.fetchone():
            # Если существует, добавляем суффикс
            counter = 1
            while True:
                test_url = f"{web_url}_{counter}"
                cursor.execute('SELECT user_id FROM web_clients WHERE web_url = ?', (test_url,))
                if not cursor.fetchone():
                    web_url = test_url
                    break
                counter += 1
        
        # Сохраняем клиента
        cursor.execute('''
            INSERT OR REPLACE INTO web_clients (user_id, username, first_name, web_url, bot_username)
            VALUES (?, ?, ?, ?, ?)
        ''', (user_id, username, first_name, web_url, bot_username))
        
        conn.commit()
        conn.close()
        
        return web_url
    except Exception as e:
        logging.error(f"Ошибка создания записи веб-клиента: {e}")
        return None

async def activate_mirror(mirror_id: int, mirror_name: str, directory_path: str, user_id: int, bot_username: str = None):
    """Активация зеркала после оплаты"""
    try:
        # Обновляем статус в БД
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        
        # Получаем информацию о пользователе и bot_username ДО закрытия соединения
        cursor.execute('SELECT username, first_name FROM users WHERE user_id = ?', (user_id,))
        user_data = cursor.fetchone()
        username = user_data[0] if user_data else "без_username"
        first_name = user_data[1] if user_data and user_data[1] else "Пользователь"
        
        # Если bot_username не передан, пытаемся получить из БД
        if not bot_username:
            try:
                cursor.execute('SELECT bot_username FROM mirrors WHERE id = ?', (mirror_id,))
                result = cursor.fetchone()
                if result and result[0]:
                    bot_username = result[0]
            except sqlite3.OperationalError:
                pass  # Колонка может не существовать
        
        cursor.execute('''
            UPDATE mirrors 
            SET status = 'active', activated_at = CURRENT_TIMESTAMP
            WHERE id = ?
        ''', (mirror_id,))
        cursor.execute('''
            UPDATE payments 
            SET status = 'paid', paid_at = CURRENT_TIMESTAMP
            WHERE mirror_id = ?
        ''', (mirror_id,))
        conn.commit()
        conn.close()
        
        # Создаем запись для веб-панели
        web_url = create_web_client_entry(user_id, username, first_name, mirror_name, bot_username)
        
        # Запускаем systemd сервис (только на Linux)
        if os.name != 'nt':  # Linux
            service_name = f"cloner-mirror-{mirror_name}"
            systemctl_paths = ["/usr/bin/systemctl", "/bin/systemctl", "systemctl"]
            systemctl_cmd = None
            
            for path in systemctl_paths:
                if os.path.exists(path) or path == "systemctl":
                    systemctl_cmd = path
                    break
            
            if systemctl_cmd:
                try:
                    os.system(f"{systemctl_cmd} enable {service_name}")
                    os.system(f"{systemctl_cmd} start {service_name}")
                    logging.info(f"Сервис {service_name} запущен через {systemctl_cmd}")
                except Exception as e:
                    logging.error(f"Ошибка при запуске сервиса: {e}")
            else:
                logging.error("systemctl не найден, сервис не запущен")
        else:  # Windows
            logging.info(f"Пропуск запуска systemd сервиса на Windows для {mirror_name}")
        
        # Формируем URL веб-панели
        if os.name == 'nt':  # Windows - localhost
            web_panel_url = f"http://localhost:5000/{web_url}" if web_url else "http://localhost:5000"
        else:  # Linux - продакшен домен
            web_panel_url = f"https://megateam.space/{web_url}" if web_url else "https://megateam.space"
        
        # Уведомляем пользователя
        await bot.send_message(
            user_id,
            f"🎉 <b>Зеркало активировано!</b>\n\n"
            f"✅ Ваше зеркало <code>{mirror_name}</code> запущено и готово к работе!\n\n"
            f"🌐 <b>Веб-панель:</b>\n"
            f"<a href=\"{web_panel_url}\">{web_panel_url}</a>\n\n"
            f"🔑 Для входа в веб-панель нажмите кнопку 'Отправить код' на сайте, "
            f"код будет отправлен в этом боте.\n\n"
            f"🤖 Теперь вы можете использовать все функции бота через ваше зеркало."
        )
        
        # Уведомление админу
        await send_to_admin(
            f"💰 <b>Зеркало активировано</b>\n"
            f"📁 Имя: {mirror_name}\n"
            f"✅ Статус: Активно\n"
            f"🚀 Сервис запущен"
        )
        
    except Exception as e:
        logging.error(f"Ошибка при активации зеркала: {e}")
        await bot.send_message(
            user_id,
            f"❌ Ошибка при активации зеркала. Обратитесь к администратору."
        )

# ============================================
# ПЕРИОДИЧЕСКАЯ ПРОВЕРКА ПЛАТЕЖЕЙ
# ============================================
async def check_pending_payments():
    """Периодическая проверка неоплаченных платежей"""
    while True:
        try:
            conn = sqlite3.connect(DB_PATH)
            cursor = conn.cursor()
            cursor.execute('''
                SELECT m.id, m.invoice_id, m.mirror_name, m.directory_path, m.user_id
                FROM mirrors m
                JOIN payments p ON m.id = p.mirror_id
                WHERE m.status = 'pending' AND p.status = 'active'
            ''')
            pending = cursor.fetchall()
            conn.close()
            
            for mirror_id, invoice_id, mirror_name, directory_path, user_id in pending:
                status = await check_invoice_status(int(invoice_id))
                if status == "paid":
                    # Получаем bot_username из БД
                    conn2 = sqlite3.connect(DB_PATH)
                    cursor2 = conn2.cursor()
                    bot_username = None
                    try:
                        cursor2.execute('SELECT bot_username FROM mirrors WHERE id = ?', (mirror_id,))
                        result = cursor2.fetchone()
                        if result and result[0]:
                            bot_username = result[0]
                    except sqlite3.OperationalError:
                        pass
                    conn2.close()
                    await activate_mirror(mirror_id, mirror_name, directory_path, user_id, bot_username)
            
            await asyncio.sleep(60)  # Проверка каждую минуту
            
        except Exception as e:
            logging.error(f"Ошибка при проверке платежей: {e}")
            await asyncio.sleep(60)

# ============================================
# АДМИН-КОМАНДЫ
# ============================================
@dp.message(Command("stats"))
async def cmd_stats(message: Message):
    """Статистика для админа"""
    if message.from_user.id != ADMIN_ID:
        return
    
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    # Статистика пользователей
    cursor.execute("SELECT COUNT(*) FROM users")
    total_users = cursor.fetchone()[0]
    
    # Статистика зеркал
    cursor.execute("SELECT COUNT(*) FROM mirrors")
    total_mirrors = cursor.fetchone()[0]
    
    cursor.execute("SELECT COUNT(*) FROM mirrors WHERE status = 'active'")
    active_mirrors = cursor.fetchone()[0]
    
    cursor.execute("SELECT COUNT(*) FROM mirrors WHERE status = 'pending'")
    pending_mirrors = cursor.fetchone()[0]
    
    # Статистика платежей
    cursor.execute("SELECT COUNT(*) FROM payments WHERE status = 'paid'")
    paid_payments = cursor.fetchone()[0]
    
    cursor.execute("SELECT SUM(amount) FROM payments WHERE status = 'paid'")
    total_revenue = cursor.fetchone()[0] or 0
    
    # Последние зеркала
    cursor.execute('''
        SELECT m.mirror_name, u.username, m.status, m.created_at
        FROM mirrors m
        LEFT JOIN users u ON m.user_id = u.user_id
        ORDER BY m.created_at DESC
        LIMIT 5
    ''')
    recent_mirrors = cursor.fetchall()
    
    conn.close()
    
    stats_text = f"""📊 <b>Статистика бота-менеджера</b>

👥 <b>Пользователи:</b>
• Всего: {total_users}

🪞 <b>Зеркала:</b>
• Всего: {total_mirrors}
• Активных: {active_mirrors}
• Ожидают оплаты: {pending_mirrors}

💰 <b>Платежи:</b>
• Оплачено: {paid_payments}
• Общий доход: ${total_revenue:.2f}

📋 <b>Последние зеркала:</b>
"""
    
    for mirror_name, username, status, created_at in recent_mirrors:
        status_emoji = "✅" if status == "active" else "⏳"
        stats_text += f"{status_emoji} <code>{mirror_name}</code> (@{username or 'N/A'})\n"
    
    await message.answer(stats_text)

@dp.message(Command("users"))
async def cmd_users(message: Message):
    """Список пользователей для админа"""
    if message.from_user.id != ADMIN_ID:
        return
    
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute('''
        SELECT user_id, username, first_name, mirrors_count, created_at
        FROM users
        ORDER BY created_at DESC
        LIMIT 20
    ''')
    users = cursor.fetchall()
    conn.close()
    
    users_text = "👥 <b>Последние пользователи:</b>\n\n"
    for user_id, username, first_name, mirrors_count, created_at in users:
        users_text += f"• @{username or 'N/A'} ({user_id})\n"
        users_text += f"  Имя: {first_name}\n"
        users_text += f"  Зеркал: {mirrors_count}\n\n"
    
    await message.answer(users_text)

@dp.message(Command("mirrors"))
async def cmd_mirrors(message: Message):
    """Список всех зеркал для админа"""
    if message.from_user.id != ADMIN_ID:
        return
    
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute('''
        SELECT m.mirror_name, u.username, m.status, m.directory_path, m.created_at
        FROM mirrors m
        LEFT JOIN users u ON m.user_id = u.user_id
        ORDER BY m.created_at DESC
    ''')
    mirrors = cursor.fetchall()
    conn.close()
    
    mirrors_text = f"🪞 <b>Все зеркала ({len(mirrors)}):</b>\n\n"
    for mirror_name, username, status, directory_path, created_at in mirrors:
        status_emoji = "✅" if status == "active" else "⏳"
        mirrors_text += f"{status_emoji} <code>{mirror_name}</code>\n"
        mirrors_text += f"   @{username or 'N/A'}\n"
        mirrors_text += f"   Путь: <code>{directory_path}</code>\n\n"
    
    await message.answer(mirrors_text)

# ============================================
# ЗАПУСК БОТА
# ============================================
async def main():
    """Главная функция"""
    # Инициализация БД
    init_manager_db()
    
    # Запуск периодической проверки платежей
    asyncio.create_task(check_pending_payments())
    
    logging.info("Бот-менеджер запущен")
    await send_to_admin("🚀 Бот-менеджер запущен и готов к работе!")
    await dp.start_polling(bot)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logging.info("Бот-менеджер остановлен")

