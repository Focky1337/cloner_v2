import os
import sqlite3
import asyncio
import logging
import sys
import json
import portalocker
import re
from datetime import datetime
from contextlib import contextmanager
from pathlib import Path
from dataclasses import dataclass
from typing import Optional, Dict, List, Any, Tuple
from aiogram import Bot, Dispatcher, types
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.filters import Command, CommandStart
from aiogram.exceptions import TelegramBadRequest
from aiogram.types import (
    Message,
    InlineKeyboardMarkup,
    InlineKeyboardButton,
    CallbackQuery,
    FSInputFile,
    ChatPermissions
)
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from pyrogram import Client, types
import pyrogram
from pyrogram import filters
from pyrogram.raw.functions.messages import CheckChatInvite
from pyrogram.errors import (
    ChannelPrivate, ChannelInvalid, BadRequest,
    SessionRevoked, AuthKeyUnregistered, FloodWait,
    PeerIdInvalid, ChatWriteForbidden, UsernameInvalid,
    PasswordHashInvalid, PhoneCodeInvalid, PhoneCodeExpired,
    PhoneNumberInvalid, PhoneNumberUnoccupied, PhoneNumberBanned
)
import random
import uuid
import time
import signal
import config as cfg

def force_account_rotation(accounts_list, last_used_account=None):
    if not accounts_list:
        return None
        
    if len(accounts_list) == 1:
        return accounts_list[0]
    
    if len(accounts_list) == 2 and last_used_account in accounts_list:
        new_account = [acc for acc in accounts_list if acc != last_used_account][0]
        logging.info(f"Строгое чередование 2х аккаунтов: исключен {last_used_account}, выбран {new_account}")
        return new_account
        
    if last_used_account in accounts_list and len(accounts_list) > 1:
        available_accounts = [acc for acc in accounts_list if acc != last_used_account]
        
        chosen_account = random.choice(available_accounts)
        logging.info(f"Случайное чередование: исключен {last_used_account}, выбран {chosen_account} из {len(available_accounts)} доступных")
        return chosen_account
    else:
        chosen_account = random.choice(accounts_list)
        logging.info(f"Первоначальный выбор аккаунта: выбран {chosen_account} из {len(accounts_list)} доступных")
        return chosen_account

def select_responder_account(accounts_list, sender_account=None, last_responder=None):
    available_accounts = accounts_list.copy()
    
    if sender_account and sender_account in available_accounts:
        available_accounts.remove(sender_account)
    
    if last_responder and last_responder in available_accounts:
        available_accounts.remove(last_responder)
    
    if available_accounts:
        return random.choice(available_accounts)
    elif accounts_list and sender_account in accounts_list:
        other_accounts = [acc for acc in accounts_list if acc != sender_account]
        if other_accounts:
            return random.choice(other_accounts)
    
    return None

SUBSCRIPTION_END = "2033-05-02"
if datetime.now() > datetime.strptime(SUBSCRIPTION_END, "%Y-%m-%d"):
    print("Подписка истекла! Продлите подписку.")
    sys.exit(0)

API_ID = cfg.API_ID
API_HASH = cfg.API_HASH
BOT_TOKEN = cfg.BOT_TOKEN
ADMIN_IDS = cfg.ADMIN_IDS
UPLOAD_DIR = cfg.UPLOAD_DIR
DB_PATH = cfg.DB_PATH
DELAY_SECONDS = cfg.DELAY_SECONDS
MAX_FILE_SIZE = cfg.MAX_FILE_SIZE
WEB_PANEL_URL = getattr(cfg, 'WEB_PANEL_URL', 'http://localhost:5000')
BOT_USERNAME = getattr(cfg, 'BOT_USERNAME', None)

if sys.platform == 'win32':
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
storage = MemoryStorage()
dp = Dispatcher(storage=storage)

config = {
    'last_messages_in_chats': {},
    'chat_id_cache': {},
    'target_chat_history': {},
    'group_account_map': {},
    'persistent_clients': {},
    'grouped_id_map': {},
    'delays': {
        'delay_between_messages': 7,
        'delay_between_accounts': 12,
        'flood_wait_multiplier': 1.5,
    }
}

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

logging.getLogger("ensure_joined_chat").setLevel(logging.WARNING)

logging.getLogger("pyrogram").setLevel(logging.WARNING)
logging.getLogger("pyrogram.session").setLevel(logging.WARNING)
logging.getLogger("pyrogram.connection").setLevel(logging.WARNING)
logging.getLogger("pyrogram.raw").setLevel(logging.ERROR)

class NoJsonFilter(logging.Filter):
    def filter(self, record):
        if record.getMessage().startswith("{") or record.getMessage().startswith("Sent: {") or record.getMessage().startswith("Received: {"):
            return False
        return True

logging.getLogger().addFilter(NoJsonFilter())

LOG_FILE = os.path.join(UPLOAD_DIR, 'bot_logs.txt')
file_handler = logging.FileHandler(LOG_FILE, encoding='utf-8')
file_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
logging.getLogger().addHandler(file_handler)

@dataclass
class Proxy:
    id: int
    host: str
    port: int
    scheme: str
    username: Optional[str] = None
    password: Optional[str] = None

class SessionDB:
    def __init__(self):
        try:
            os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
            self.conn = sqlite3.connect(DB_PATH, check_same_thread=False, timeout=10)
            self._initialize_db()
            logging.info("База данных успешно инициализирована")
        except sqlite3.Error as db_error:
            logging.error(f"Ошибка SQLite: {db_error}")
            raise RuntimeError("Не удалось подключиться к базе данных") from db_error
        except Exception as e:
            logging.error(f"Общая ошибка инициализации БД: {e}")
            raise RuntimeError("Ошибка инициализации базы данных") from e

    def _initialize_db(self):
        try:
            with self.conn:
                self.conn.execute('''
                    CREATE TABLE IF NOT EXISTS sessions (
                        phone TEXT PRIMARY KEY,
                        session TEXT,
                        source_chat TEXT,
                        dest_chats TEXT,
                        current_file TEXT,
                        copy_mode INTEGER,
                        last_message_id INTEGER,
                        last_sent_index INTEGER,
                        proxy_id INTEGER,
                        gender TEXT,
                        user_id INTEGER
                    )
                ''')
                self.conn.execute('''
                    CREATE TABLE IF NOT EXISTS message_files (
                        file_id TEXT PRIMARY KEY,
                        file_name TEXT,
                        messages TEXT
                    )
                ''')
                self.conn.execute('''
                    CREATE TABLE IF NOT EXISTS state (
                        id INTEGER PRIMARY KEY,
                        message_ptr INTEGER,
                        account_ptr INTEGER
                    )
                ''')
                self.conn.execute('''
                    CREATE TABLE IF NOT EXISTS proxies (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        host TEXT,
                        port INTEGER,
                        scheme TEXT,
                        username TEXT,
                        password TEXT
                    )
                ''')
                self.conn.execute('''
                    CREATE TABLE IF NOT EXISTS managed_groups (
                        group_id TEXT PRIMARY KEY,
                        title TEXT,
                        group_type INTEGER DEFAULT 0,
                        added_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                ''')

                # ✅ Гарантируем, что запись в таблице state существует
                self.conn.execute("INSERT OR IGNORE INTO state (id, message_ptr, account_ptr) VALUES (1, 0, 0)")

                # ✅ Проверяем, есть ли колонка user_id — добавляем, если надо
                cursor = self.conn.execute("PRAGMA table_info(sessions)")
                columns = [info[1] for info in cursor.fetchall()]
                if "user_id" not in columns:
                    self.conn.execute("ALTER TABLE sessions ADD COLUMN user_id INTEGER")

            return True
        except Exception as e:
            logging.error(f"Ошибка инициализации БД: {e}")
            return False

    def save_state(self, message_ptr: int, account_ptr: int):
        try:
            with self.conn:
                self.conn.execute(
                    "UPDATE state SET message_ptr = ?, account_ptr = ? WHERE id = 1",
                    (message_ptr, account_ptr))
            return True
        except Exception as e:
            logging.error(f"Ошибка сохранения состояния: {e}")
            return False

    def load_state(self):
        try:
            with self.conn:
                cursor = self.conn.execute(
                    "SELECT message_ptr, account_ptr FROM state WHERE id = 1"
                )
                result = cursor.fetchone()
                return result if result else (0, 0)
        except Exception as e:
            logging.error(f"Ошибка загрузки состояния: {e}")
            return (0, 0)
    
    def save_session(self, phone: str, session: str, source_chat: str = None, 
                   dest_chats: str = None, current_file: str = None, 
                   copy_mode: int = None, last_message_id: int = None, 
                   last_sent_index: int = None, proxy_id: int = None, gender: str = None, 
                   user_id: int = None):
        try:
            with self.conn:
                self.conn.execute('''
                    INSERT OR REPLACE INTO sessions
                    (phone, session, source_chat, dest_chats, current_file,
                     copy_mode, last_message_id, last_sent_index, proxy_id, gender, user_id)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (phone, session, source_chat, dest_chats, current_file,
                     copy_mode, last_message_id, last_sent_index, proxy_id, gender, user_id))
            return True
        except Exception as e:
            logging.error(f"Ошибка сохранения сессии: {e}")
            return False

    def load_sessions(self) -> Dict[str, Dict]:
        try:
            with self.conn:
                cursor = self.conn.execute('''
                    SELECT phone, session, source_chat, dest_chats, 
                           current_file, copy_mode, last_message_id, proxy_id, gender, user_id
                    FROM sessions
                ''')
                sessions_data = {}
                for row in cursor.fetchall():
                    sessions_data[row[0]] = {
                        "session": row[1],
                        "source_chat": row[2],
                        "dest_chats": row[3],
                        "current_file": row[4],
                        "copy_mode": row[5] if row[5] is not None else 0,
                        "last_message_id": row[6] if row[6] is not None else 0,
                        "proxy_id": row[7],
                        "gender": row[8] if row[8] else 'male',
                        "user_id": row[9]  # Добавляем поле user_id
                    }
                return sessions_data
        except Exception as e:
            logging.error(f"Ошибка загрузки сессий: {e}")
            return {}

    def delete_session(self, phone: str):
        try:
            with self.conn:
                self.conn.execute("DELETE FROM sessions WHERE phone = ?", (phone,))
            return True
        except Exception as e:
            logging.error(f"Ошибка удаления сессии: {e}")
            return False

    def save_message_file(self, file_id: str, file_name: str, messages: str):
        try:
            with self.conn:
                self.conn.execute('''
                    INSERT OR REPLACE INTO message_files 
                    (file_id, file_name, messages) 
                    VALUES (?, ?, ?)
                ''', (file_id, file_name, messages))
            return True
        except Exception as e:
            logging.error(f"Ошибка сохранения файла: {e}")
            return False

    def load_message_files(self) -> Dict[str, Dict]:
        try:
            with self.conn:
                cursor = self.conn.execute('''
                    SELECT file_id, file_name, messages FROM message_files
                ''')
                return {row[0]: {"name": row[1], "messages": row[2]} 
                       for row in cursor.fetchall()}
        except Exception as e:
            logging.error(f"Ошибка загрузки файлов: {e}")
            return {}

    def delete_message_file(self, file_id: str):
        try:
            logging.info(f"DB: Попытка удаления message_file с file_id = {file_id}")
            with self.conn:
                cursor = self.conn.execute("DELETE FROM message_files WHERE file_id = ?", 
                                (file_id,))
            if cursor.rowcount > 0:
                logging.info(f"DB: Успешно удалено {cursor.rowcount} строк для file_id = {file_id}")
                return True
            else:
                logging.warning(f"DB: Не найдено строк для удаления с file_id = {file_id}")
                return False # Важно вернуть False, если ничего не удалено
        except Exception as e:
            logging.error(f"DB Ошибка при удалении файла {file_id}: {e}")
            return False

    def set_copy_mode(self, phone: str, mode: int):
        try:
            with self.conn:
                self.conn.execute('''
                    UPDATE sessions SET copy_mode = ? WHERE phone = ?
                ''', (mode, phone))
            return True
        except Exception as e:
            logging.error(f"Ошибка установки режима копирования: {e}")
            return False

    def update_source_chat(self, phone: str, source_chat: str):
        try:
            with self.conn:
                self.conn.execute('''
                    UPDATE sessions SET source_chat = ? WHERE phone = ?
                ''', (source_chat, phone))
            return True
        except Exception as e:
            logging.error(f"Ошибка обновления источника: {e}")
            return False

    def update_dest_chats(self, phone: str, dest_chats: str):
        try:
            with self.conn:
                self.conn.execute('''
                    UPDATE sessions SET dest_chats = ? WHERE phone = ?
                ''', (dest_chats, phone))
            return True
        except Exception as e:
            logging.error(f"Ошибка обновления назначения: {e}")
            return False

    def update_last_message_id(self, phone: str, message_id: int):
        try:
            with self.conn:
                self.conn.execute('''
                    UPDATE sessions SET last_message_id = ? WHERE phone = ?
                ''', (message_id, phone))
            return True
        except Exception as e:
            logging.error(f"Ошибка обновления last_message_id: {e}")
            return False

    def add_proxy(self, proxy: Proxy):
        try:
            with self.conn:
                self.conn.execute('''
                    INSERT INTO proxies 
                    (host, port, scheme, username, password)
                    VALUES (?, ?, ?, ?, ?)
                ''', (proxy.host, proxy.port, proxy.scheme, 
                     proxy.username, proxy.password))
            return True
        except Exception as e:
            logging.error(f"Ошибка добавления прокси: {e}")
            return False

    def get_proxy(self, proxy_id: int) -> Optional[Proxy]:
        try:
            with self.conn:
                cursor = self.conn.execute('''
                    SELECT * FROM proxies WHERE id = ?
                ''', (proxy_id,))
                row = cursor.fetchone()
                return Proxy(*row) if row else None
        except Exception as e:
            logging.error(f"Ошибка получения прокси: {e}")
            return None

    def get_all_proxies(self) -> List[Proxy]:
        try:
            with self.conn:
                cursor = self.conn.execute('SELECT * FROM proxies')
                return [Proxy(*row) for row in cursor.fetchall()]
        except Exception as e:
            logging.error(f"Ошибка получения списка прокси: {e}")
            return []

    def delete_proxy(self, proxy_id: int):
        try:
            with self.conn:
                self.conn.execute('DELETE FROM proxies WHERE id = ?', (proxy_id,))
                self.conn.execute('''
                    UPDATE sessions SET proxy_id = NULL WHERE proxy_id = ?
                ''', (proxy_id,))
            return True
        except Exception as e:
            logging.error(f"Ошибка удаления прокси: {e}")
            return False

    def update_account_proxy(self, phone: str, proxy_id: Optional[int]):
        try:
            with self.conn:
                self.conn.execute('''
                    UPDATE sessions SET proxy_id = ? WHERE phone = ?
                ''', (proxy_id, phone))
            return True
        except Exception as e:
            logging.error(f"Ошибка обновления прокси аккаунта: {e}")
            return False
    
    def get_accounts_with_proxy(self) -> List[Tuple[str, int]]:
        try:
            with self.conn:
                cursor = self.conn.execute('''
                    SELECT phone, proxy_id FROM sessions WHERE proxy_id IS NOT NULL
                ''')
                return [(row[0], row[1]) for row in cursor.fetchall()]
        except Exception as e:
            logging.error(f"Ошибка получения аккаунтов с прокси: {e}")
            return []
            
    def update_account_gender(self, phone: str, gender: str):
        try:
            with self.conn:
                self.conn.execute('''
                    UPDATE sessions SET gender = ? WHERE phone = ?
                ''', (gender, phone))
            return True
        except Exception as e:
            logging.error(f"Ошибка обновления пола аккаунта: {e}")
            return False
            
    def get_account_gender(self, phone: str) -> Optional[str]:
        try:
            cursor = self.conn.execute('''
                SELECT gender FROM sessions WHERE phone = ?
            ''', (phone,))
            result = cursor.fetchone()
            
            # >>> НАЧАЛО ИЗМЕНЕНИЯ: Улучшенная логика и логирование <<<
            raw_gender = result[0] if result else None
            logging.info(f"[get_account_gender] Аккаунт: {phone}, Значение из БД: {repr(raw_gender)}")

            # Обрабатываем результат
            if result and raw_gender:
                # Убираем возможные пробелы и приводим к нижнему регистру для надежности
                cleaned_gender = str(raw_gender).strip().lower()
                if cleaned_gender == 'female':
                    logging.info(f"[get_account_gender] Аккаунт: {phone}, Возвращаем: 'female'")
                    return 'female'
                # Мы явно не проверяем 'male', так как это значение по умолчанию
                # Если значение не 'female', будем считать 'male'
                logging.info(f"[get_account_gender] Аккаунт: {phone}, Значение не 'female', Возвращаем: 'male'")
                return 'male' 
            else:
                # Строка не найдена или значение NULL
                logging.info(f"[get_account_gender] Аккаунт: {phone}, Строка не найдена или NULL, Возвращаем: 'male'")
                return 'male'
            # >>> КОНЕЦ ИЗМЕНЕНИЯ <<<

        except Exception as e:
            logging.error(f"Ошибка получения пола аккаунта: {e}")
            return 'male'  # по умолчанию мужской пол
    
    def is_problematic_account(self, phone: str) -> bool:
        """
        Проверяет, является ли аккаунт проблемным
        В данной реализации просто проверяем конкретный номер телефона
        В дальнейшем можно расширить для использования списка проблемных аккаунтов из базы данных
        """
        problematic_numbers = ['+79683091504']
        return phone in problematic_numbers
        
    def add_problematic_account(self, phone: str) -> None:
        """
        Отмечает аккаунт как проблемный
        """
        pass

    def close(self):
        try:
            self.conn.close()
        except Exception as e:
            logging.error(f"Ошибка закрытия БД: {e}")
            
    def add_managed_group(self, group_id: str, title: str, group_type: int = 0) -> bool:
        try:
            with self.conn:
                self.conn.execute('''
                    INSERT OR REPLACE INTO managed_groups
                    (group_id, title, group_type)
                    VALUES (?, ?, ?)
                ''', (group_id, title, group_type))
            return True
        except Exception as e:
            logging.error(f"Ошибка добавления группы: {e}")
            return False
            
    def get_managed_group(self, group_id: str) -> Optional[Dict]:
        try:
            with self.conn:
                cursor = self.conn.execute('''
                    SELECT group_id, title, group_type, added_at 
                    FROM managed_groups
                    WHERE group_id = ?
                ''', (group_id,))
                row = cursor.fetchone()
                if row:
                    return {
                        "group_id": row[0],
                        "title": row[1],
                        "group_type": row[2],
                        "added_at": row[3]
                    }
                return None
        except Exception as e:
            logging.error(f"Ошибка получения группы: {e}")
            return None
            
    def get_all_managed_groups(self) -> List[Dict]:
        try:
            with self.conn:
                cursor = self.conn.execute('''
                    SELECT group_id, title, group_type, added_at
                    FROM managed_groups
                    ORDER BY added_at DESC
                ''')
                groups = []
                for row in cursor.fetchall():
                    groups.append({
                        "group_id": row[0],
                        "title": row[1],
                        "group_type": row[2],
                        "added_at": row[3]
                    })
                return groups
        except Exception as e:
            logging.error(f"Ошибка получения списка групп: {e}")
            return []
            
    def update_group_type(self, group_id: str, group_type: int) -> bool:
        try:
            with self.conn:
                self.conn.execute('''
                    UPDATE managed_groups
                    SET group_type = ?
                    WHERE group_id = ?
                ''', (group_type, group_id))
            return True
        except Exception as e:
            logging.error(f"Ошибка обновления типа группы: {e}")
            return False
            
    def delete_managed_group(self, group_id: str) -> bool:
        try:
            with self.conn:
                self.conn.execute('''
                    DELETE FROM managed_groups
                    WHERE group_id = ?
                ''', (group_id,))
            return True
        except Exception as e:
            logging.error(f"Ошибка удаления группы: {e}")
            return False

class AuthStates(StatesGroup):
    WAITING_PHONE = State()
    WAITING_CODE = State()
    WAITING_2FA_PASSWORD = State()  # Добавленное состояние для проверки 2FA пароля

class MessageFileStates(StatesGroup):
    WAITING_FILE = State()

class SourceChatStates(StatesGroup):
    WAITING_SOURCE = State()

class DestChatStates(StatesGroup):
    WAITING_DEST = State()

class AccountEditStates(StatesGroup):
    SELECT_ACCOUNT = State()
    EDIT_SOURCE = State()
    EDIT_DEST = State()

class MassEditStates(StatesGroup):
    WAITING_SOURCE_ALL = State()
    WAITING_DEST_ALL = State()

class FileSelectionStates(StatesGroup):
    SELECTING_FILE = State()

class ProxyStates(StatesGroup):
    ADD_PROXY = State()
    SELECT_PROXY = State()
    DELETE_PROXY = State()
    ASSIGN_PROXY = State()

class CopyModeStates(StatesGroup):
    WAITING_COPY_MODE = State()
    WAITING_DELAY = State()  # Добавляем новое состояние

class GenderStates(StatesGroup):
    SELECTING_GENDER = State()

class GroupManagementStates(StatesGroup):
    WAITING_GROUP_ID = State()



GROUP_TYPE_CLOSED = 0
GROUP_TYPE_OPEN = 1

config = {
    'active_clients': {},  # Активные клиенты, ключ - телефон, значение - клиент
    'persistent_clients': {},  # Персистентные клиенты для копирования
    'group_account_map': {},  # Карта групп -> аккаунтов, последнее использование
    'message_account_map': {},  # Карта сообщение+группа -> аккаунт
    'copying_active': False,  # Флаг активного копирования
    'copying_mode': None,  # Режим копирования (file или chat)
    'processing_lock': False,  # Блокировка для операций
    'message_batch': [],  # Партия сообщений для отправки
    'pending_phones': {},  # Телефоны в процессе авторизации
    'delays': {
        'delay_between_messages': 7,  # Задержка между сообщениями в секундах (рекомендуется 7-10 для безопасности)
        'delay_between_accounts': 12,  # Задержка между аккаунтами в секундах (рекомендуется 10-15)
        'flood_wait_multiplier': 1.5,  # Множитель для FloodWait (1.5 = ждем на 50% дольше)
    },
    'last_external_sender_account': {},  # Карта для отслеживания последнего аккаунта, который отправлял сообщения от внешних отправителей
    'message_id_map': {},  # Карта для хранения соответствия ID сообщений
    'grouped_id_map': {},  # Карта для хранения grouped_id для веток сообщений
    'chat_id_cache': {},  # Кэш для хранения chat_id групп
    'target_chat_history': {},  # Хранилище истории сообщений в целевом чате
}

@contextmanager
def process_lock():
    lock_file = os.path.join(os.getenv('TEMP', os.getcwd()), 'bot.lock')
    try:
        with open(lock_file, 'w') as f:
            portalocker.lock(f, portalocker.LOCK_EX | portalocker.LOCK_NB)
            yield
    except portalocker.LockException:
        logging.error("Приложение уже запущено!")
        os._exit(1)
    except Exception as e:
        logging.error(f"Ошибка при создании lock-файла: {e}")
        os._exit(1)
    finally:
        try:
            if os.path.exists(lock_file):
                os.remove(lock_file)
        except Exception as e:
            logging.error(f"Ошибка при удалении lock-файла: {e}")

def main_menu_kb():
    keyboard = [
        [InlineKeyboardButton(text="📋 Список аккаунтов", callback_data="accounts_list"),
         InlineKeyboardButton(text="📱 Добавить аккаунт", callback_data="add_account")],
        [InlineKeyboardButton(text="🗂 Мои файлы", callback_data="my_files"),
         InlineKeyboardButton(text="⚙️ Прокси", callback_data="proxy_settings")],
        [InlineKeyboardButton(text="👥 Управление группами", callback_data="manage_groups"),
         InlineKeyboardButton(text="⏱ Изменить задержки", callback_data="change_delays")],
        [InlineKeyboardButton(text="🚀 Запустить копирование", callback_data="start_copying"),
         InlineKeyboardButton(text="🛑 Остановить", callback_data="stop_sending")],
    ]
    
    # Добавляем кнопку веб-панели только если URL валидный (не localhost)
    # Telegram не принимает localhost URLs в inline кнопках
    if WEB_PANEL_URL and not ('localhost' in WEB_PANEL_URL or '127.0.0.1' in WEB_PANEL_URL):
        keyboard.append([InlineKeyboardButton(text="🌐 Веб-панель", url=WEB_PANEL_URL),
                         InlineKeyboardButton(text="ℹ️ Помощь", callback_data="help")])
    else:
        keyboard.append([InlineKeyboardButton(text="ℹ️ Помощь", callback_data="help")])
    
    return InlineKeyboardMarkup(inline_keyboard=keyboard)

def status_text():
    try:
        sessions = db.load_sessions()
        files = db.load_message_files()
        proxies = db.get_all_proxies()
        active_accounts = sum(1 for acc in sessions.values() if acc.get('copy_mode', 0) == 1)
        delays = config['delays']
        
        # Проверяем какая структура задержек используется
        if 'delay_between_messages' in delays:
            # Новая структура
            msg_delay_str = f"{delays['delay_between_messages']}"
        else:
            # Старая структура
            msg_delay_str = f"{delays.get('delay_between_messages_min', 5)}-{delays.get('delay_between_messages_max', 10)}"
        
        return f"""⚙️ Текущие настройки:
        
Аккаунтов: {len(sessions)}
Активных: {active_accounts}
Прокси: {len(proxies)}
Файлов с сообщениями: {len(files)}
Задержки: {msg_delay_str}s/msg, {delays['delay_between_accounts']}s/acc
Копирование: {'активно' if config['copying_active'] else 'остановлено'}"""
    except Exception as e:
        logging.error(f"Ошибка получения статуса: {e}")
        return "⚙️ Не удалось получить текущий статус"

async def is_admin(user_id: int) -> bool:
    return user_id in ADMIN_IDS

async def send_log_to_admins(message: str):
    for admin_id in ADMIN_IDS:
        try:
            await bot.send_message(admin_id, message)
        except Exception as e:
            logging.error(f"Ошибка отправки лога админу {admin_id}: {e}")

@dp.message(CommandStart())
async def cmd_start(message: Message):
    try:
        if not await is_admin(message.from_user.id):
            return
        await message.answer(status_text(), reply_markup=main_menu_kb())
    except Exception as e:
        logging.error(f"Ошибка в cmd_start: {e}")
        await message.answer("❌ Произошла ошибка при запуске")

@dp.callback_query(lambda c: c.data == "change_delays")
async def change_delays_handler(callback: CallbackQuery):
    try:
        current_delays = config['delays']
        
        # Определяем правильный формат для отображения задержки между сообщениями
        if 'delay_between_messages' in current_delays:
            msg_delay_display = f"{current_delays['delay_between_messages']}"
        else:
            msg_delay_display = f"{current_delays.get('delay_between_messages_min', 5)}-{current_delays.get('delay_between_messages_max', 10)}"
        
        try:
            await callback.message.edit_text(
                f"⚙️ Текущие задержки:\n\n"
                f"• Между сообщениями: {msg_delay_display} сек.\n"
                f"• Между аккаунтами: {current_delays['delay_between_accounts']} сек.\n"
                f"• Множитель FloodWait: {current_delays['flood_wait_multiplier']}\n\n"
                "Выберите параметр для изменения:",
                reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                    [InlineKeyboardButton(text="🕒 Задержка между сообщениями", callback_data="set_msg_delay")],
                    [InlineKeyboardButton(text="⏳ Задержка между аккаунтами", callback_data="set_acc_delay")],
                    [InlineKeyboardButton(text="⚠️ Множитель FloodWait", callback_data="set_flood_multiplier")],
                    [InlineKeyboardButton(text="⬅️ Назад", callback_data="back_to_menu")]
                ])
            )
        except:
            # Если не удалось отредактировать, отправляем новое сообщение
            await callback.message.answer(
                f"⚙️ Текущие задержки:\n\n"
                f"• Между сообщениями: {msg_delay_display} сек.\n"
                f"• Между аккаунтами: {current_delays['delay_between_accounts']} сек.\n"
                f"• Множитель FloodWait: {current_delays['flood_wait_multiplier']}\n\n"
                "Выберите параметр для изменения:",
                reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                    [InlineKeyboardButton(text="🕒 Задержка между сообщениями", callback_data="set_msg_delay")],
                    [InlineKeyboardButton(text="⏳ Задержка между аккаунтами", callback_data="set_acc_delay")],
                    [InlineKeyboardButton(text="⚠️ Множитель FloodWait", callback_data="set_flood_multiplier")],
                    [InlineKeyboardButton(text="⬅️ Назад", callback_data="back_to_menu")]
                ])
            )
        await callback.answer()
    except Exception as e:
        logging.error(f"Ошибка в change_delays_handler: {e}")
        await callback.answer("❌ Произошла ошибка", show_alert=True)

@dp.callback_query(lambda c: c.data == "add_account")
async def add_account(callback: CallbackQuery, state: FSMContext):
    try:
        await state.set_state(AuthStates.WAITING_PHONE)
        await callback.message.answer(
            "Введите номер телефона в формате +79123456789:",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="❌ Отмена", callback_data="back_to_menu")]
            ])
        )
        await callback.answer()   
    except Exception as e:
        logging.error(f"Ошибка в add_account: {e}")
        await callback.answer("❌ Произошла ошибка", show_alert=True)

@dp.callback_query(lambda c: c.data == "set_msg_delay")
async def set_msg_delay_handler(callback: CallbackQuery, state: FSMContext):
    await state.set_state(CopyModeStates.WAITING_DELAY)
    await state.update_data(delay_type='message')
    await callback.message.edit_text(
        "Введите новую задержку между сообщениями в секундах:",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="❌ Отмена", callback_data="change_delays")]
        ])
    )
    await callback.answer()

@dp.callback_query(lambda c: c.data == "set_acc_delay")
async def set_acc_delay_handler(callback: CallbackQuery, state: FSMContext):
    await state.set_state(CopyModeStates.WAITING_DELAY)
    await state.update_data(delay_type='account')
    await callback.message.edit_text(
        "Введите новую задержку между аккаунтами (в секундах):",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="❌ Отмена", callback_data="change_delays")]
        ]))
    await callback.answer()

@dp.callback_query(lambda c: c.data == "set_flood_multiplier")
async def set_flood_multiplier_handler(callback: CallbackQuery, state: FSMContext):
    await state.set_state(CopyModeStates.WAITING_DELAY)
    await state.update_data(delay_type='flood')
    await callback.message.edit_text(
        "Введите новый множитель для FloodWait (например, 1.5):",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="❌ Отмена", callback_data="change_delays")]
        ]))
    await callback.answer()

@dp.message(CopyModeStates.WAITING_DELAY)
async def process_delay_value(message: Message, state: FSMContext):
    try:
        data = await state.get_data()
        delay_type = data.get('delay_type')
        input_text = message.text.strip()
        
        try:
            if delay_type == 'message':
                # Теперь просто сохраняем одно значение, а не диапазон
                value = int(input_text)
                if value <= 0:
                    raise ValueError("Значения должны быть больше нуля")
                display_value = value
            elif delay_type == 'flood':
                value = float(input_text)
                if value <= 0:
                    raise ValueError("Значение должно быть больше 0")
                display_value = value
            else: # account delay
                value = int(input_text)
                if value <= 0:
                    raise ValueError("Значение должно быть больше 0")
                display_value = value

        except ValueError as e:
            await message.answer(f"❌ Ошибка: {e}")
            return
        
        # Обновляем конфиг
        if delay_type == 'message':
            config['delays']['delay_between_messages'] = value
            delay_name = "между сообщениями"
        elif delay_type == 'account':
            config['delays']['delay_between_accounts'] = value
            delay_name = "между аккаунтами"
        elif delay_type == 'flood':
            config['delays']['flood_wait_multiplier'] = value
            delay_name = "множитель FloodWait"
        else:
             delay_name = "неизвестный параметр"
             display_value = "N/A"

        await message.answer(f"✅ Задержка ({delay_name}) успешно изменена на {display_value}")
        await state.clear()
        
        # Просто возвращаемся в главное меню
        await message.answer(status_text(), reply_markup=main_menu_kb())
        
    except Exception as e:
        logging.error(f"Ошибка в process_delay_value: {e}")
        await message.answer("❌ Произошла ошибка при изменении задержки")
        await state.clear()

@dp.message(AuthStates.WAITING_PHONE)
async def process_phone(message: Message, state: FSMContext):
    try:
        phone_number = message.text
        if not phone_number.startswith("+"):
            await message.answer("❌ Неверный формат! Номер должен начинаться с '+'")
            return
        
        # Очищаем номер от пробелов и других символов
        phone_number = "".join(c for c in phone_number if c.isdigit() or c == "+")
        logging.info(f"Начинаем процесс авторизации для номера {phone_number}")
        
        # Используем фиксированную сессию вместо случайного имени
        session_name = f"auth_{phone_number}"  # Убираем случайное число
        logging.info(f"Создаем клиент с фиксированным именем сессии: {session_name}")
        
        client = Client(
            name=session_name,
            api_id=API_ID,
            api_hash=API_HASH,
            workdir=UPLOAD_DIR
            # Убрано in_memory=True, чтобы код приходил на реальное устройство
        )
        
        try:
            logging.info("Пытаемся запустить клиент и начать авторизацию")
            
            # Метод 1: Используем raw API SendCode (самый надежный метод)
            try:
                await client.connect()
                logging.info(f"Клиент подключен, состояние: {client.is_connected}")
                
                from pyrogram.raw.functions.auth import SendCode
                from pyrogram.raw.types import CodeSettings
                
                logging.info("Отправляем код через raw API...")
                result = await client.invoke(
                    SendCode(
                        phone_number=phone_number,
                        api_id=API_ID,
                        api_hash=API_HASH,
                        settings=CodeSettings(
                            allow_flashcall=True,
                            current_number=True,
                            allow_app_hash=True
                        )
                    )
                )
                
                code_hash = result.phone_code_hash
                logging.info(f"Код отправлен через raw API, hash: {code_hash}")
                
                config['pending_phones'][message.from_user.id] = {
                    'phone': phone_number,
                    'code_hash': code_hash,
                    'client': client
                }
                logging.info(f"Телефон добавлен в pending_phones: {message.from_user.id} -> {phone_number}")
                
            except Exception as raw_error:
                logging.error(f"Ошибка при использовании raw API: {raw_error}")
                
                # Метод 2: Пробуем с помощью phone_code_callback
                try:
                    # Отключаем клиент если он подключен
                    if client.is_connected:
                        await client.disconnect()
                    
                    # Определяем асинхронную функцию обратного вызова для кода
                    code_hash_container = {"hash": None}
                    
                    async def phone_code_callback():
                        nonlocal code_hash_container
                        # Эта функция не будет ждать пользовательского ввода,
                        # а просто вернет пустой код, чтобы получить hash
                        return ""
                    
                    # Неполный запуск клиента с нашим обработчиком
                    logging.info("Пробуем запустить клиент с phone_code_callback")
                    try:
                        await client.connect()
                        sent = await client.send_code(phone_number)
                        code_hash = sent.phone_code_hash
                        logging.info(f"Получен code_hash: {code_hash}")
                        
                        config['pending_phones'][message.from_user.id] = {
                            'phone': phone_number,
                            'code_hash': code_hash,
                            'client': client
                        }
                        logging.info(f"Телефон добавлен в pending_phones: {message.from_user.id} -> {phone_number}")
                        
                    except Exception as e:
                        logging.error(f"Ошибка при запуске с phone_code_callback: {e}")
                        raise e
                        
                except Exception as callback_error:
                    logging.error(f"Ошибка при использовании phone_code_callback: {callback_error}")
                    raise callback_error
            
            await state.set_state(AuthStates.WAITING_CODE)
            await message.answer(
                "✅ Код отправлен! Введите код:",
                reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                    [InlineKeyboardButton(text="❌ Отмена", callback_data="back_to_menu")]
                ])
            )
        except PhoneNumberInvalid:
            logging.warning(f"Неверный номер телефона: {phone_number}")
            await message.answer("❌ Указан неверный номер телефона. Проверьте формат и попробуйте снова.")
            await state.clear()
            try:
                await client.disconnect()
            except:
                pass
        except PhoneNumberBanned:
            logging.warning(f"Номер телефона заблокирован: {phone_number}")
            await message.answer("❌ Данный номер заблокирован в Telegram. Используйте другой номер.")
            await state.clear()
            try:
                await client.disconnect()
            except:
                pass
        except PhoneNumberUnoccupied:
            logging.warning(f"Номер телефона не зарегистрирован: {phone_number}")
            await message.answer("❌ Данный номер не зарегистрирован в Telegram. Сначала зарегистрируйте аккаунт.")
            await state.clear()
            try:
                await client.disconnect()
            except:
                pass
        except FloodWait as e:
            wait_time = int(e.value * config['delays']['flood_wait_multiplier'])
            logging.warning(f"FloodWait при отправке кода: ждем {wait_time} сек.")
            await message.answer(f"⚠️ Telegram ограничил отправку кодов. Пожалуйста, подождите {wait_time} секунд и попробуйте снова.")
            await state.clear()
            try:
                await client.disconnect()
                logging.info("Клиент отключен после FloodWait")
            except Exception as disconnect_error:
                logging.error(f"Ошибка при отключении клиента: {str(disconnect_error)}")
        except Exception as e:
            logging.error(f"Детальная ошибка при отправке кода: {str(e)}, тип: {type(e)}")
            await message.answer(f"❌ Ошибка при отправке кода: {str(e)}")
            await state.clear()
            try:
                await client.disconnect()
                logging.info("Клиент отключен после ошибки")
            except Exception as disconnect_error:
                logging.error(f"Ошибка при отключении клиента: {str(disconnect_error)}")
    except Exception as e:
        logging.error(f"Детальная ошибка в process_phone: {e}, тип: {type(e)}")
        await message.answer("❌ Произошла ошибка при обработке номера")
        await state.clear()

@dp.message(AuthStates.WAITING_CODE)
async def process_code(message: Message, state: FSMContext):
    try:
        user_data = config['pending_phones'].get(message.from_user.id)
        if not user_data:
            logging.warning(f"Пользователь {message.from_user.id} пытается ввести код, но нет данных в pending_phones")
            await message.answer("❌ Время сессии истекло или данные аутентификации не найдены! Начните заново.")
            await state.clear()
            return

        client = user_data['client']
        phone = user_data['phone']
        
        logging.info(f"Обработка кода для номера {phone}")
        
        try:
            # Пробуем войти с кодом
            try:
                logging.info(f"Пытаемся авторизоваться с кодом для {phone}")
                await client.sign_in(
                    phone_number=phone,
                    phone_code_hash=user_data['code_hash'],
                    phone_code=message.text
                )
                
                # Если нет 2FA, сразу сохраняем сессию
                logging.info(f"Успешная авторизация для {phone}")
                session_string = await client.export_session_string()
                logging.info(f"Получена строка сессии для {phone}")
                
                if not db.save_session(phone, session_string):
                    logging.error(f"Ошибка сохранения сессии в БД для {phone}")
                    raise Exception("Ошибка сохранения сессии в БД")
                
                logging.info(f"Сессия сохранена в БД для {phone}")
                await message.answer(
                    f"✅ Аккаунт {phone} авторизован!",
                    reply_markup=main_menu_kb()
                )
            except pyrogram.errors.SessionPasswordNeeded:
                # Если требуется пароль двухфакторной аутентификации
                logging.info(f"Требуется 2FA для {phone}")
                await message.answer("🔐 Обнаружена двухфакторная аутентификация. Пожалуйста, введите пароль 2FA:")
                # Сохраняем данные для следующего шага
                await state.set_state(AuthStates.WAITING_2FA_PASSWORD)
                return  # Важно! Не отключаем клиент и не очищаем state
            except pyrogram.errors.PhoneCodeInvalid:
                logging.warning(f"Неверный код для {phone}")
                await message.answer("❌ Неверный код. Пожалуйста, проверьте и введите заново.")
                return  # Даем возможность ввести код снова
            except pyrogram.errors.PhoneCodeExpired:
                logging.warning(f"Код истек для {phone}")
                await message.answer("❌ Код истек. Пожалуйста, запросите новый код, начав процесс заново.")
                try:
                    await client.disconnect()
                except:
                    pass
                del config['pending_phones'][message.from_user.id]
                await state.clear()
                return
                
        except Exception as e:
            logging.error(f"Ошибка при авторизации для {phone}: {str(e)}, тип: {type(e)}")
            await message.answer(f"❌ Ошибка при авторизации: {str(e)}")
        finally:
            if await state.get_state() != AuthStates.WAITING_2FA_PASSWORD:
                try:
                    await client.disconnect()
                    logging.info(f"Клиент отключен для {phone}")
                except Exception as e:
                    logging.error(f"Ошибка при отключении клиента для {phone}: {str(e)}")
                    
                if message.from_user.id in config['pending_phones']:
                    del config['pending_phones'][message.from_user.id]
                    logging.info(f"Удален {phone} из pending_phones")
                    
                await state.clear()
    except Exception as e:
        logging.error(f"Общая ошибка в process_code: {e}, тип: {type(e)}")
        await message.answer("❌ Произошла ошибка при обработке кода")
        await state.clear()

@dp.message(AuthStates.WAITING_2FA_PASSWORD)
async def process_2fa_password(message: Message, state: FSMContext):
    try:
        user_data = config['pending_phones'].get(message.from_user.id)
        if not user_data:
            await message.answer("❌ Время сессии истекло!")
            await state.clear()
            return

        client = user_data['client']
        phone = user_data['phone']
        password = message.text.strip()
        
        try:
            # Пытаемся войти с паролем 2FA
            await client.check_password(password)
            
            # Если пароль правильный, сохраняем сессию
            session_string = await client.export_session_string()
            
            if not db.save_session(phone, session_string):
                raise Exception("Ошибка сохранения сессии в БД")
            
            await message.answer(
                f"✅ Аккаунт {phone} успешно авторизован с 2FA!",
                reply_markup=main_menu_kb()
            )
        except pyrogram.errors.PasswordHashInvalid:
            # Если пароль неверный
            await message.answer("❌ Неверный пароль 2FA. Пожалуйста, попробуйте снова:")
            return  # Не очищаем state, даём возможность ввести пароль снова
        except Exception as e:
            await message.answer(f"❌ Ошибка при проверке пароля: {str(e)}")
        finally:
            # Если не возвращаемся к вводу пароля, очищаем сессию
            if await state.get_state() != AuthStates.WAITING_2FA_PASSWORD:
                try:
                    await client.disconnect()
                except:
                    pass
                del config['pending_phones'][message.from_user.id]
                await state.clear()
    except Exception as e:
        logging.error(f"Ошибка в process_2fa_password: {e}")
        await message.answer("❌ Произошла ошибка при обработке пароля")
        await state.clear()

async def finalize_auth(message: Message, phone: str, client, state: FSMContext):
    # Получаем имя пользователя и сохраняем сессию
    me = await client.get_me()
    username = f"@{me.username}" if me.username else f"{me.first_name} {me.last_name}"
    
    # Сохраняем сессию с идентификатором пользователя
    session_path = f"sessions/{phone}.session"
    user_id = me.id  # Сохраняем ID пользователя Telegram
    db.save_session(phone, session_path, gender="unknown", user_id=user_id)
    
    await message.answer(f"Аккаунт {username} успешно добавлен!", reply_markup=main_menu_kb())
    await state.clear()
    
    # Отключаем временный клиент
    await client.disconnect()

@dp.callback_query(lambda c: c.data == "accounts_list")
async def show_accounts(callback: CallbackQuery):
    try:
        sessions = db.load_sessions()
        if not sessions:
            await callback.message.edit_text(
                "📋 Нет добавленных аккаунтов",
                reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                    [InlineKeyboardButton(text="⬅️ Назад", callback_data="back_to_menu")]
                ])
            )
            return
        
        kb = []
        for phone, data in sessions.items():
            kb.append([
                InlineKeyboardButton(
                    text=f"📱 {phone}",
                    callback_data=f"account_info_{phone}"
                )
            ])
        
        # Добавляем кнопки массового редактирования
        kb.append([
            InlineKeyboardButton(text="✏️ Указать источник для всех", callback_data="set_source_all"),
            InlineKeyboardButton(text="✏️ Указать назначение для всех", callback_data="set_dest_all")
        ])
        
        # Добавляем кнопки массового включения/выключения
        active_count = sum(1 for acc in sessions.values() if acc.get('copy_mode', 0) == 1)
        total_count = len(sessions)
        
        if active_count < total_count:
            kb.append([
                InlineKeyboardButton(text="✅ Включить все", callback_data="enable_all_accounts")
            ])
        if active_count > 0:
            kb.append([
                InlineKeyboardButton(text="❌ Выключить все", callback_data="disable_all_accounts")
            ])
        
        kb.append([InlineKeyboardButton(text="⬅️ Назад", callback_data="back_to_menu")])
        
        await callback.message.edit_text(
            "📋 Список аккаунтов:",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=kb))
    except Exception as e:
        logging.error(f"Ошибка в show_accounts: {e}")
        await callback.answer("❌ Произошла ошибка", show_alert=True)

@dp.callback_query(lambda c: c.data == "set_source_all")
async def set_source_all_handler(callback: CallbackQuery, state: FSMContext):
    await state.set_state(MassEditStates.WAITING_SOURCE_ALL)
    await callback.message.edit_text(
        "Введите ID или username источника (чата/канала) для ВСЕХ аккаунтов:",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="❌ Отмена", callback_data="accounts_list")]
        ])
    )
    await callback.answer()

@dp.callback_query(lambda c: c.data == "set_dest_all")
async def set_dest_all_handler(callback: CallbackQuery, state: FSMContext):
    await state.set_state(MassEditStates.WAITING_DEST_ALL)
    await callback.message.edit_text(
        "Введите ID или username назначения (чата/канала) для ВСЕХ аккаунтов:",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="❌ Отмена", callback_data="accounts_list")]
        ])
    )
    await callback.answer()

@dp.message(MassEditStates.WAITING_SOURCE_ALL)
async def process_source_all(message: Message, state: FSMContext):
    try:
        source_chat = message.text.strip()
        sessions = db.load_sessions()
        updated = 0
        
        for phone, data in sessions.items():
            if db.update_source_chat(phone, source_chat):
                updated += 1
        
        await message.answer(
            f"✅ Источник обновлен для {updated}/{len(sessions)} аккаунтов\n"
            f"Новый источник: {source_chat}"
        )
        await state.clear()
        await show_accounts_from_message(message)
        
    except Exception as e:
        logging.error(f"Ошибка в process_source_all: {e}")
        await message.answer("❌ Произошла ошибка при массовом обновлении источника")
        await state.clear()

@dp.message(MassEditStates.WAITING_DEST_ALL)
async def process_dest_all(message: Message, state: FSMContext):
    try:
        dest_chats = message.text.strip()
        # Парсим список групп, разделенных запятыми или новыми строками
        dest_chats_list = [chat.strip() for chat in re.split(r'[,\n]', dest_chats) if chat.strip()]
        
        if not dest_chats_list:
            await message.answer("❌ Не указаны группы назначения!")
            return
            
        # Сохраняем список в формате JSON
        dest_chats_json = json.dumps(dest_chats_list)
        
        sessions = db.load_sessions()
        updated = 0
        
        for phone, data in sessions.items():
            if db.update_dest_chats(phone, dest_chats_json):
                updated += 1
        
        await message.answer(
            f"✅ Назначение обновлено для {updated}/{len(sessions)} аккаунтов\n"
            f"Новые группы назначения ({len(dest_chats_list)}):\n" +
            "\n".join([f"- {chat}" for chat in dest_chats_list[:5]]) +
            (f"\n...и еще {len(dest_chats_list) - 5} групп" if len(dest_chats_list) > 5 else "")
        )
        await state.clear()
        await show_accounts_from_message(message)
        
    except Exception as e:
        logging.error(f"Ошибка в process_dest_all: {e}")
        await message.answer("❌ Произошла ошибка при массовом обновлении назначения")
        await state.clear()

@dp.callback_query(lambda c: c.data == "enable_all_accounts")
async def enable_all_accounts(callback: CallbackQuery):
    """Включает режим копирования для всех аккаунтов"""
    try:
        sessions = db.load_sessions()
        if not sessions:
            await callback.answer("❌ Нет аккаунтов!", show_alert=True)
            return
        
        enabled = 0
        for phone in sessions.keys():
            if db.set_copy_mode(phone, 1):
                enabled += 1
        
        await callback.answer(f"✅ Включено для {enabled}/{len(sessions)} аккаунтов", show_alert=True)
        await show_accounts(callback)
    except Exception as e:
        logging.error(f"Ошибка в enable_all_accounts: {e}")
        await callback.answer("❌ Произошла ошибка", show_alert=True)

@dp.callback_query(lambda c: c.data == "disable_all_accounts")
async def disable_all_accounts(callback: CallbackQuery):
    """Выключает режим копирования для всех аккаунтов"""
    try:
        sessions = db.load_sessions()
        if not sessions:
            await callback.answer("❌ Нет аккаунтов!", show_alert=True)
            return
        
        disabled = 0
        for phone in sessions.keys():
            if db.set_copy_mode(phone, 0):
                disabled += 1
        
        await callback.answer(f"✅ Выключено для {disabled}/{len(sessions)} аккаунтов", show_alert=True)
        await show_accounts(callback)
    except Exception as e:
        logging.error(f"Ошибка в disable_all_accounts: {e}")
        await callback.answer("❌ Произошла ошибка", show_alert=True)

async def show_accounts_from_message(message: Message):
    sessions = db.load_sessions()
    kb = []
    for phone, data in sessions.items():
        kb.append([
            InlineKeyboardButton(
                text=f"📱 {phone}",
                callback_data=f"account_info_{phone}"
            )
        ])
    
    kb.append([
        InlineKeyboardButton(text="✏️ Указать источник для всех", callback_data="set_source_all"),
        InlineKeyboardButton(text="✏️ Указать назначение для всех", callback_data="set_dest_all")
    ])
    kb.append([InlineKeyboardButton(text="⬅️ Назад", callback_data="back_to_menu")])
    
    await message.answer(
        "📋 Список аккаунтов:",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=kb)
    )

@dp.callback_query(lambda c: c.data.startswith("account_info_"))
async def account_info(callback: CallbackQuery):
    try:
        phone = callback.data.split("_")[2]
        sessions = db.load_sessions()
        if phone not in sessions:
            await callback.answer("❌ Аккаунт не найден!", show_alert=True)
            return
        
        account_data = sessions[phone]
        status = "✅ Включено" if account_data.get('copy_mode', 0) == 1 else "❌ Выключено"
        
        # Получаем список групп назначения
        dest_chats_text = "не указано"
        dest_chats = account_data.get('dest_chats')
        if dest_chats:
            try:
                dest_list = json.loads(dest_chats)
                if isinstance(dest_list, list) and dest_list:
                    count = len(dest_list)
                    if count == 1:
                        dest_chats_text = dest_list[0]
                    else:
                        dest_chats_text = f"{count} групп"
            except:
                # Если формат старый (строка), показываем как есть
                dest_chats_text = dest_chats
        
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text=f"Режим копирования: {status}", callback_data=f"toggle_copy_{phone}")],
            [InlineKeyboardButton(text="✏️ Изменить источник", callback_data=f"edit_source_{phone}")],
            [InlineKeyboardButton(text="✏️ Изменить назначение", callback_data=f"edit_dest_{phone}")],
            [InlineKeyboardButton(text="🔑 Настроить прокси", callback_data=f"set_proxy_{phone}")],
            [InlineKeyboardButton(text="👥 Указать пол", callback_data=f"set_gender_{phone}")],
            [InlineKeyboardButton(text="🗑 Удалить аккаунт", callback_data=f"delete_account_{phone}")],
            [InlineKeyboardButton(text="⬅️ Назад", callback_data="accounts_list")]
        ])
        
        # Формируем подробную и красивую информацию об аккаунте
        account_info = "<b>📱 Информация об аккаунте 📱</b>\n\n"
        account_info += f"<b>Номер телефона:</b> {phone}\n"
        account_info += f"<b>Режим копирования:</b> {status}\n"
        account_info += f"<b>Источник:</b> {account_data.get('source_chat') if account_data.get('source_chat') else 'Не задан'}\n"
        account_info += f"<b>Назначение:</b> {account_data.get('dest_chats') if account_data.get('dest_chats') else 'Не задано'}\n"
        account_info += f"<b>Пол:</b> {account_data.get('gender') if account_data.get('gender') else 'Не указан'}\n"
        account_info += f"<b>Прокси:</b> {account_data.get('proxy_id') if account_data.get('proxy_id') else 'Не указан'}\n"
        account_info += "\n<b>🔧 Дополнительные настройки 🔧</b>\n"
        account_info += f"<b>Задержка между сообщениями:</b> {config['delays']['delay_between_messages']} сек.\n"
        account_info += f"<b>Задержка смены аккаунта:</b> {config['delays']['delay_between_accounts']} сек.\n"
        account_info += f"<b>Множитель флуд-таймаута:</b> {config['delays']['flood_wait_multiplier']}x\n"

        await callback.message.edit_text(
            account_info,
            reply_markup=kb,
            parse_mode='HTML'
        )
        await callback.answer()
    except Exception as e:
        logging.error(f"Ошибка в account_info: {e}")
        await callback.answer("❌ Произошла ошибка", show_alert=True)

@dp.callback_query(lambda c: c.data.startswith("toggle_copy_"))
async def toggle_copy_mode(callback: CallbackQuery):
    try:
        phone = callback.data.split("_")[2]
        sessions = db.load_sessions()
        
        if phone not in sessions:
            await callback.answer("❌ Аккаунт не найден!", show_alert=True)
            return
        
        current_mode = sessions[phone].get('copy_mode', 0)
        new_mode = 1 if current_mode == 0 else 0
        
        if not db.set_copy_mode(phone, new_mode):
            raise Exception("Ошибка обновления режима копирования")
        
        await callback.answer(f"✅ Режим копирования {'включен' if new_mode == 1 else 'выключен'}", show_alert=True)
        await account_info(callback)
    except Exception as e:
        logging.error(f"Ошибка в toggle_copy_mode: {e}")
        await callback.answer("❌ Произошла ошибка", show_alert=True)

@dp.callback_query(lambda c: c.data.startswith("edit_source_"))
async def edit_source_start(callback: CallbackQuery, state: FSMContext):
    try:
        phone = callback.data.split("_")[2]
        config['editing_account'] = phone
        await state.set_state(SourceChatStates.WAITING_SOURCE)
        await callback.message.answer(
            "Введите ID или username источника (чата/канала):",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="❌ Отмена", callback_data=f"account_info_{phone}")]
            ])
        )
        await callback.answer()
    except Exception as e:
        logging.error(f"Ошибка в edit_source_start: {e}")
        await callback.answer("❌ Произошла ошибка", show_alert=True)

@dp.message(SourceChatStates.WAITING_SOURCE)
async def edit_source_finish(message: Message, state: FSMContext):
    try:
        phone = config['editing_account']
        if not phone:
            await message.answer("❌ Ошибка: аккаунт не выбран")
            await state.clear()
            return
        
        source_chat = message.text.strip()
        if not db.update_source_chat(phone, source_chat):
            raise Exception("Ошибка обновления источника")
        
        await message.answer(f"✅ Источник для {phone} обновлен на {source_chat}")
        await state.clear()
        config['editing_account'] = None
        
        # Возвращаемся к информации об аккаунте
        sessions = db.load_sessions()
        if phone in sessions:
            account_data = sessions[phone]
            status = "✅ Включено" if account_data.get('copy_mode', 0) == 1 else "❌ Выключено"
            
            # Формируем клавиатуру для информации об аккаунте
            kb = InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text=f"Режим копирования: {status}", callback_data=f"toggle_copy_{phone}")],
                [InlineKeyboardButton(text="✏️ Изменить источник", callback_data=f"edit_source_{phone}")],
                [InlineKeyboardButton(text="✏️ Изменить назначение", callback_data=f"edit_dest_{phone}")],
                [InlineKeyboardButton(text="🔑 Настроить прокси", callback_data=f"set_proxy_{phone}")],
                [InlineKeyboardButton(text="👥 Указать пол", callback_data=f"set_gender_{phone}")],
                [InlineKeyboardButton(text="🗑 Удалить аккаунт", callback_data=f"delete_account_{phone}")],
                [InlineKeyboardButton(text="⬅️ Назад", callback_data="accounts_list")]
            ])
            
            # Формируем информацию об аккаунте
            account_info_text = "<b>📱 Информация об аккаунте 📱</b>\n\n"
            account_info_text += f"<b>Номер телефона:</b> {phone}\n"
            account_info_text += f"<b>Режим копирования:</b> {status}\n"
            account_info_text += f"<b>Источник:</b> {account_data.get('source_chat') if account_data.get('source_chat') else 'Не задан'}\n"
            account_info_text += f"<b>Назначение:</b> {account_data.get('dest_chats') if account_data.get('dest_chats') else 'Не задано'}\n"
            account_info_text += f"<b>Пол:</b> {account_data.get('gender') if account_data.get('gender') else 'Не указан'}\n"
            account_info_text += f"<b>Прокси:</b> {account_data.get('proxy_id') if account_data.get('proxy_id') else 'Не указан'}\n"
            account_info_text += "\n<b>🔧 Дополнительные настройки 🔧</b>\n"
            account_info_text += f"<b>Задержка между сообщениями:</b> {config['delays']['delay_between_messages']} сек.\n"
            account_info_text += f"<b>Задержка смены аккаунта:</b> {config['delays']['delay_between_accounts']} сек.\n"
            account_info_text += f"<b>Множитель флуд-таймаута:</b> {config['delays']['flood_wait_multiplier']}x\n"
            
            await message.answer(
                account_info_text,
                reply_markup=kb,
                parse_mode='HTML'
            )
    except Exception as e:
        logging.error(f"Ошибка в edit_source_finish: {e}")
        await message.answer("❌ Произошла ошибка при обновлении источника")
        await state.clear()

@dp.callback_query(lambda c: c.data.startswith("edit_dest_"))
async def edit_dest_start(callback: CallbackQuery, state: FSMContext):
    try:
        phone = callback.data.split("_")[2]
        config['editing_account'] = phone
        await state.set_state(DestChatStates.WAITING_DEST)
        await callback.message.answer(
            "Введите ID или username назначения (чата/канала):",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="❌ Отмена", callback_data=f"account_info_{phone}")]
            ])
        )
        await callback.answer()
    except Exception as e:
        logging.error(f"Ошибка в edit_dest_start: {e}")
        await callback.answer("❌ Произошла ошибка", show_alert=True)

@dp.message(DestChatStates.WAITING_DEST)
async def edit_dest_finish(message: Message, state: FSMContext):
    try:
        phone = config['editing_account']
        if not phone:
            await message.answer("❌ Ошибка: аккаунт не выбран")
            await state.clear()
            return
        
        dest_chats = message.text.strip()
        # Парсим список групп, разделенных запятыми или новыми строками
        dest_chats_list = [chat.strip() for chat in re.split(r'[,\n]', dest_chats) if chat.strip()]
        
        if not dest_chats_list:
            await message.answer("❌ Не указаны группы назначения!")
            return
            
        # Сохраняем список в формате JSON
        dest_chats_json = json.dumps(dest_chats_list)
        
        if not db.update_dest_chats(phone, dest_chats_json):
            raise Exception("Ошибка обновления назначения")
        
        await message.answer(
            f"✅ Назначение для {phone} обновлено\n"
            f"Новые группы назначения ({len(dest_chats_list)}):\n" +
            "\n".join([f"- {chat}" for chat in dest_chats_list[:5]]) +
            (f"\n...и еще {len(dest_chats_list) - 5} групп" if len(dest_chats_list) > 5 else "")
        )
        await state.clear()
        config['editing_account'] = None
        
        # Возвращаемся к информации об аккаунте
        sessions = db.load_sessions()
        if phone in sessions:
            account_data = sessions[phone]
            status = "✅ Включено" if account_data.get('copy_mode', 0) == 1 else "❌ Выключено"
            
            # Формируем клавиатуру для информации об аккаунте
            kb = InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text=f"Режим копирования: {status}", callback_data=f"toggle_copy_{phone}")],
                [InlineKeyboardButton(text="✏️ Изменить источник", callback_data=f"edit_source_{phone}")],
                [InlineKeyboardButton(text="✏️ Изменить назначение", callback_data=f"edit_dest_{phone}")],
                [InlineKeyboardButton(text="🔑 Настроить прокси", callback_data=f"set_proxy_{phone}")],
                [InlineKeyboardButton(text="👥 Указать пол", callback_data=f"set_gender_{phone}")],
                [InlineKeyboardButton(text="🗑 Удалить аккаунт", callback_data=f"delete_account_{phone}")],
                [InlineKeyboardButton(text="⬅️ Назад", callback_data="accounts_list")]
            ])
            
            # Формируем информацию об аккаунте
            account_info_text = "<b>📱 Информация об аккаунте 📱</b>\n\n"
            account_info_text += f"<b>Номер телефона:</b> {phone}\n"
            account_info_text += f"<b>Режим копирования:</b> {status}\n"
            account_info_text += f"<b>Источник:</b> {account_data.get('source_chat') if account_data.get('source_chat') else 'Не задан'}\n"
            account_info_text += f"<b>Назначение:</b> {account_data.get('dest_chats') if account_data.get('dest_chats') else 'Не задано'}\n"
            account_info_text += f"<b>Пол:</b> {account_data.get('gender') if account_data.get('gender') else 'Не указан'}\n"
            account_info_text += f"<b>Прокси:</b> {account_data.get('proxy_id') if account_data.get('proxy_id') else 'Не указан'}\n"
            account_info_text += "\n<b>🔧 Дополнительные настройки 🔧</b>\n"
            account_info_text += f"<b>Задержка между сообщениями:</b> {config['delays']['delay_between_messages']} сек.\n"
            account_info_text += f"<b>Задержка смены аккаунта:</b> {config['delays']['delay_between_accounts']} сек.\n"
            account_info_text += f"<b>Множитель флуд-таймаута:</b> {config['delays']['flood_wait_multiplier']}x\n"
            
            await message.answer(
                account_info_text,
                reply_markup=kb,
                parse_mode='HTML'
            )
    except Exception as e:
        logging.error(f"Ошибка в edit_dest_finish: {e}")
        await message.answer("❌ Произошла ошибка при обновлении назначения")
        await state.clear()

@dp.callback_query(lambda c: c.data.startswith("set_proxy_"))
async def set_proxy_start(callback: CallbackQuery):
    try:
        phone = callback.data.split("_")[2]
        proxies = db.get_all_proxies()
        
        if not proxies:
            await callback.answer("❌ Нет доступных прокси!", show_alert=True)
            return
        
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(
                text=f"{p.host}:{p.port} ({p.scheme})", 
                callback_data=f"assign_proxy_{phone}_{p.id}"
            )] for p in proxies
        ])
        kb.inline_keyboard.append([
            InlineKeyboardButton(
                text="❌ Без прокси",
                callback_data=f"remove_proxy_{phone}"
            )
        ])
        kb.inline_keyboard.append([
            InlineKeyboardButton(text="⬅️ Назад", callback_data=f"account_info_{phone}")
        ])
        
        await callback.message.edit_text(
            "Выберите прокси для аккаунта:",
            reply_markup=kb
        )
        await callback.answer()
    except Exception as e:
        logging.error(f"Ошибка в set_proxy_start: {e}")
        await callback.answer("❌ Произошла ошибка", show_alert=True)

@dp.callback_query(lambda c: c.data.startswith("assign_proxy_"))
async def assign_proxy(callback: CallbackQuery):
    try:
        _, _, phone, proxy_id = callback.data.split("_")
        proxy_id = int(proxy_id)
        
        if not db.update_account_proxy(phone, proxy_id):
            raise Exception("Ошибка обновления прокси аккаунта")
        
        await callback.answer(f"✅ Прокси успешно назначен", show_alert=True)
        
        # Получаем данные аккаунта и обновляем информацию напрямую
        sessions = db.load_sessions()
        if phone not in sessions:
            await callback.answer("❌ Аккаунт не найден!", show_alert=True)
            return
        
        account_data = sessions[phone]
        status = "✅ Включено" if account_data.get('copy_mode', 0) == 1 else "❌ Выключено"
        
        # Формируем клавиатуру и текст как в функции account_info
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text=f"Режим копирования: {status}", callback_data=f"toggle_copy_{phone}")],
            [InlineKeyboardButton(text="✏️ Изменить источник", callback_data=f"edit_source_{phone}")],
            [InlineKeyboardButton(text="✏️ Изменить назначение", callback_data=f"edit_dest_{phone}")],
            [InlineKeyboardButton(text="🔑 Настроить прокси", callback_data=f"set_proxy_{phone}")],
            [InlineKeyboardButton(text="👥 Указать пол", callback_data=f"set_gender_{phone}")],
            [InlineKeyboardButton(text="🗑 Удалить аккаунт", callback_data=f"delete_account_{phone}")],
            [InlineKeyboardButton(text="⬅️ Назад", callback_data="accounts_list")]
        ])
        
        # Формируем информацию об аккаунте
        account_info = "<b>📱 Информация об аккаунте 📱</b>\n\n"
        account_info += f"<b>Номер телефона:</b> {phone}\n"
        account_info += f"<b>Режим копирования:</b> {status}\n"
        account_info += f"<b>Источник:</b> {account_data.get('source_chat') if account_data.get('source_chat') else 'Не задан'}\n"
        account_info += f"<b>Назначение:</b> {account_data.get('dest_chats') if account_data.get('dest_chats') else 'Не задано'}\n"
        account_info += f"<b>Пол:</b> {account_data.get('gender') if account_data.get('gender') else 'Не указан'}\n"
        account_info += f"<b>Прокси:</b> {account_data.get('proxy_id') if account_data.get('proxy_id') else 'Не указан'}\n"
        account_info += "\n<b>🔧 Дополнительные настройки 🔧</b>\n"
        account_info += f"<b>Задержка между сообщениями:</b> {config['delays']['delay_between_messages']} сек.\n"
        account_info += f"<b>Задержка смены аккаунта:</b> {config['delays']['delay_between_accounts']} сек.\n"
        account_info += f"<b>Множитель флуд-таймаута:</b> {config['delays']['flood_wait_multiplier']}x\n"

        await callback.message.edit_text(
            account_info,
            reply_markup=kb,
            parse_mode='HTML'
        )
    except Exception as e:
        logging.error(f"Ошибка в assign_proxy: {e}")
        await callback.answer("❌ Произошла ошибка", show_alert=True)

@dp.callback_query(lambda c: c.data.startswith("remove_proxy_"))
async def remove_proxy(callback: CallbackQuery):
    try:
        phone = callback.data.split("_")[2]
        
        if not db.update_account_proxy(phone, None):
            raise Exception("Ошибка удаления прокси аккаунта")
            
        await callback.answer("✅ Прокси удален", show_alert=True)
        
        # Получаем данные аккаунта и обновляем информацию напрямую
        sessions = db.load_sessions()
        if phone not in sessions:
            await callback.answer("❌ Аккаунт не найден!", show_alert=True)
            return
        
        account_data = sessions[phone]
        status = "✅ Включено" if account_data.get('copy_mode', 0) == 1 else "❌ Выключено"
        
        # Формируем клавиатуру и текст как в функции account_info
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text=f"Режим копирования: {status}", callback_data=f"toggle_copy_{phone}")],
            [InlineKeyboardButton(text="✏️ Изменить источник", callback_data=f"edit_source_{phone}")],
            [InlineKeyboardButton(text="✏️ Изменить назначение", callback_data=f"edit_dest_{phone}")],
            [InlineKeyboardButton(text="🔑 Настроить прокси", callback_data=f"set_proxy_{phone}")],
            [InlineKeyboardButton(text="👥 Указать пол", callback_data=f"set_gender_{phone}")],
            [InlineKeyboardButton(text="🗑 Удалить аккаунт", callback_data=f"delete_account_{phone}")],
            [InlineKeyboardButton(text="⬅️ Назад", callback_data="accounts_list")]
        ])
        
        # Формируем информацию об аккаунте
        account_info = "<b>📱 Информация об аккаунте 📱</b>\n\n"
        account_info += f"<b>Номер телефона:</b> {phone}\n"
        account_info += f"<b>Режим копирования:</b> {status}\n"
        account_info += f"<b>Источник:</b> {account_data.get('source_chat') if account_data.get('source_chat') else 'Не задан'}\n"
        account_info += f"<b>Назначение:</b> {account_data.get('dest_chats') if account_data.get('dest_chats') else 'Не задано'}\n"
        account_info += f"<b>Пол:</b> {account_data.get('gender') if account_data.get('gender') else 'Не указан'}\n"
        account_info += f"<b>Прокси:</b> {account_data.get('proxy_id') if account_data.get('proxy_id') else 'Не указан'}\n"
        account_info += "\n<b>🔧 Дополнительные настройки 🔧</b>\n"
        account_info += f"<b>Задержка между сообщениями:</b> {config['delays']['delay_between_messages']} сек.\n"
        account_info += f"<b>Задержка смены аккаунта:</b> {config['delays']['delay_between_accounts']} сек.\n"
        account_info += f"<b>Множитель флуд-таймаута:</b> {config['delays']['flood_wait_multiplier']}x\n"

        await callback.message.edit_text(
            account_info,
            reply_markup=kb,
            parse_mode='HTML'
        )
    except Exception as e:
        logging.error(f"Ошибка в remove_proxy: {e}")
        await callback.answer("❌ Произошла ошибка", show_alert=True)

@dp.callback_query(lambda c: c.data.startswith("set_gender_"))
async def set_gender(callback: CallbackQuery):
    try:
        phone = callback.data.split("_")[2]
        sessions = db.load_sessions()

        if phone not in sessions:
            await callback.answer("❌ Аккаунт не найден!", show_alert=True)
            return

        current_gender = sessions[phone].get('gender', 'male')
        gender_text = 'Женский' if current_gender == 'female' else 'Мужской'

        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="👨 Мужской", callback_data=f"change_gender_{phone}_male")],
            [InlineKeyboardButton(text="👩 Женский", callback_data=f"change_gender_{phone}_female")],
            [InlineKeyboardButton(text="⬅️ Назад", callback_data=f"account_info_{phone}")]
        ])

        await callback.message.edit_text(
            f"👤 Выберите пол для аккаунта {phone}\n\n"
            f"Текущий пол: {gender_text}\n\n"
            f"От пола зависит автоматическая корректировка сообщений с учетом рода глаголов и прилагательных.",
            reply_markup=kb
        )
        await callback.answer()
    except Exception as e:
        logging.error(f"Ошибка в set_gender: {e}")
        await callback.answer("❌ Произошла ошибка", show_alert=True)

@dp.callback_query(lambda c: c.data.startswith("change_gender_"))
async def change_gender(callback: CallbackQuery):
    try:
        parts = callback.data.split("_")
        phone = parts[2]
        gender = parts[3]

        if not db.update_account_gender(phone, gender):
            await callback.answer("❌ Ошибка при изменении пола", show_alert=True)
            return

        gender_text = 'Женский' if gender == 'female' else 'Мужской'
        await callback.answer(f"✅ Установлен пол: {gender_text}", show_alert=True)
        
        # Получаем данные аккаунта и обновляем информацию напрямую
        sessions = db.load_sessions()
        if phone not in sessions:
            await callback.answer("❌ Аккаунт не найден!", show_alert=True)
            return
        
        account_data = sessions[phone]
        status = "✅ Включено" if account_data.get('copy_mode', 0) == 1 else "❌ Выключено"
        
        # Формируем клавиатуру и текст как в функции account_info
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text=f"Режим копирования: {status}", callback_data=f"toggle_copy_{phone}")],
            [InlineKeyboardButton(text="✏️ Изменить источник", callback_data=f"edit_source_{phone}")],
            [InlineKeyboardButton(text="✏️ Изменить назначение", callback_data=f"edit_dest_{phone}")],
            [InlineKeyboardButton(text="🔑 Настроить прокси", callback_data=f"set_proxy_{phone}")],
            [InlineKeyboardButton(text="👥 Указать пол", callback_data=f"set_gender_{phone}")],
            [InlineKeyboardButton(text="🗑 Удалить аккаунт", callback_data=f"delete_account_{phone}")],
            [InlineKeyboardButton(text="⬅️ Назад", callback_data="accounts_list")]
        ])
        
        # Формируем информацию об аккаунте
        account_info = "<b>📱 Информация об аккаунте 📱</b>\n\n"
        account_info += f"<b>Номер телефона:</b> {phone}\n"
        account_info += f"<b>Режим копирования:</b> {status}\n"
        account_info += f"<b>Источник:</b> {account_data.get('source_chat') if account_data.get('source_chat') else 'Не задан'}\n"
        account_info += f"<b>Назначение:</b> {account_data.get('dest_chats') if account_data.get('dest_chats') else 'Не задано'}\n"
        account_info += f"<b>Пол:</b> {account_data.get('gender') if account_data.get('gender') else 'Не указан'}\n"
        account_info += f"<b>Прокси:</b> {account_data.get('proxy_id') if account_data.get('proxy_id') else 'Не указан'}\n"
        account_info += "\n<b>🔧 Дополнительные настройки 🔧</b>\n"
        account_info += f"<b>Задержка между сообщениями:</b> {config['delays']['delay_between_messages']} сек.\n"
        account_info += f"<b>Задержка смены аккаунта:</b> {config['delays']['delay_between_accounts']} сек.\n"
        account_info += f"<b>Множитель флуд-таймаута:</b> {config['delays']['flood_wait_multiplier']}x\n"

        await callback.message.edit_text(
            account_info,
            reply_markup=kb,
            parse_mode='HTML'
        )
    except Exception as e:
        logging.error(f"Ошибка в change_gender: {e}")
        await callback.answer("❌ Произошла ошибка", show_alert=True)

@dp.callback_query(lambda c: c.data.startswith("delete_account_"))
async def delete_account(callback: CallbackQuery):
    try:
        phone = callback.data.split("_")[2]
        
        if not db.delete_session(phone):
            raise Exception("Ошибка удаления аккаунта из БД")
        
        if phone in config['active_clients']:
            try:
                await config['active_clients'][phone].stop()
                del config['active_clients'][phone]
            except Exception as e:
                logging.error(f"Ошибка остановки клиента {phone}: {e}")
        
        await callback.answer(f"✅ Аккаунт {phone} удалён", show_alert=True)
        await show_accounts(callback)
    except Exception as e:
        logging.error(f"Ошибка в delete_account: {e}")
        await callback.answer("❌ Произошла ошибка", show_alert=True)

@dp.callback_query(lambda c: c.data == "my_files")
async def show_my_files(callback: CallbackQuery):
    try:
        await callback.answer()
        
        files = db.load_message_files()
        if not files:
            await callback.message.edit_text("🗂 У вас нет загруженных файлов", reply_markup=InlineKeyboardMarkup(
                inline_keyboard=[
                    [InlineKeyboardButton(text="⬅️ Назад", callback_data="back_to_menu")],
                    [InlineKeyboardButton(text="📤 Загрузить файл", callback_data="upload_file")]
                ]
            ))
            return
        
        # Формируем клавиатуру с файлами
        kb = []
        
        for file_id, file_data in files.items():
            kb.append([
                InlineKeyboardButton(
                    text=f"📄 {file_data['name']} ({len(file_data['messages'].split(chr(10)))} сообщ.)",
                    callback_data=f"select_file_{file_id}"
                ),
                InlineKeyboardButton(
                    text="❌ Удалить",
                    callback_data=f"delete_file_{file_id}"
                )
            ])
        
        # Добавляем кнопки управления
        kb.append([
            InlineKeyboardButton(text="📤 Загрузить файл", callback_data="upload_file"),
            InlineKeyboardButton(text="✅ Выбрать для всех", callback_data="select_all_files")
        ])
        kb.append([InlineKeyboardButton(text="⬅️ Назад", callback_data="back_to_menu")])
        
        await callback.message.edit_text(
            "🗂 Ваши файлы с сообщениями:",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=kb)
        )
    except Exception as e:
        logging.error(f"Ошибка в show_my_files: {e}")
        try:
            await callback.answer("❌ Произошла ошибка при показе файлов", show_alert=True)
        except Exception:
            logging.error("Не удалось отправить ответ на callback")

@dp.callback_query(lambda c: c.data == "upload_file")
async def upload_file_handler(callback: CallbackQuery, state: FSMContext):
    try:
        await state.set_state(MessageFileStates.WAITING_FILE)
        await callback.message.answer(
            "Отправьте файл .txt с сообщениями (каждое сообщение на новой строке):",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="❌ Отмена", callback_data="my_files")]
            ])
        )
        await callback.answer()
    except Exception as e:
        logging.error(f"Ошибка в upload_file_handler: {e}")
        await callback.answer("❌ Произошла ошибка", show_alert=True)

@dp.message(MessageFileStates.WAITING_FILE)
async def handle_message_file(message: Message, state: FSMContext):
    await process_message_file(message, state)

async def process_message_file(message: Message, state: FSMContext):
    try:
        # Получаем файл
        file_id = str(uuid.uuid4())
        file = message.document
        
        # Проверяем размер файла
        if file.file_size > MAX_FILE_SIZE:
            await message.reply(f"❌ Файл слишком большой. Максимальный размер: {MAX_FILE_SIZE/1024/1024:.1f} МБ")
            return
            
        # Проверяем тип файла
        if not file.file_name.endswith('.txt'):
            await message.reply("❌ Пожалуйста, загрузите текстовый файл (*.txt)")
            return
            
        # Загружаем файл
        await message.reply("⏳ Загрузка файла...")
        file_content_io = await bot.download(file.file_id)
        
        if not file_content_io:
            await message.reply("❌ Не удалось загрузить файл")
            return
            
        # Читаем байты из BytesIO объекта
        file_content = file_content_io.read()
            
        # Декодируем содержимое файла
        try:
            file_text = file_content.decode('utf-8')
        except UnicodeDecodeError:
            try:
                file_text = file_content.decode('cp1251')  # Пробуем Windows-1251 (для русских файлов)
            except UnicodeDecodeError:
                await message.reply("❌ Не удалось распознать кодировку файла. Используйте UTF-8 или Windows-1251.")
                return
                
        # Сохраняем файл в базу данных
        db.save_message_file(file_id, file.file_name, file_text)
        
        # Считаем количество сообщений
        message_count = len([msg for msg in file_text.split('\n') if msg.strip()])
        
        # Отправляем информацию о загруженном файле
        await message.reply(
            f"✅ Файл <b>{file.file_name}</b> успешно загружен!\n"
            f"📊 Количество сообщений: <b>{message_count}</b>\n\n"
            f"Теперь вы можете выбрать его для использования в рассылке."
        )
        
        # Очищаем состояние
        await state.clear()
        
        # Обновляем список файлов
        kb = []
        
        # Загружаем текущие файлы
        files = db.load_message_files()
        
        if not files:
            kb.append([InlineKeyboardButton(text="📤 Загрузить файл", callback_data="upload_file")])
            kb.append([InlineKeyboardButton(text="⬅️ Назад", callback_data="back_to_menu")])
            
            await message.answer(
                "🗂 У вас нет других загруженных файлов",
                reply_markup=InlineKeyboardMarkup(inline_keyboard=kb)
            )
            return
        
        # Формируем клавиатуру с файлами
        for file_id, file_data in files.items():
            kb.append([
                InlineKeyboardButton(
                    text=f"📄 {file_data['name']} ({len(file_data['messages'].split(chr(10)))} сообщ.)",
                    callback_data=f"select_file_{file_id}"
                ),
                InlineKeyboardButton(
                    text="❌ Удалить",
                    callback_data=f"delete_file_{file_id}"
                )
            ])
        
        kb.append([
            InlineKeyboardButton(text="📤 Загрузить файл", callback_data="upload_file"),
            InlineKeyboardButton(text="✅ Выбрать для всех", callback_data="select_all_files")
        ])
        kb.append([InlineKeyboardButton(text="⬅️ Назад", callback_data="back_to_menu")])
        
        await message.answer(
            "🗂 Ваши файлы с сообщениями:",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=kb)
        )
        
    except Exception as e:
        logging.error(f"Ошибка в process_message_file: {e}")
        await message.reply(f"❌ Произошла ошибка при обработке файла: {e}")
        await state.clear()

def process_chat_link(chat_link):
    """Обрабатывает ссылку на чат/канал и возвращает чистое имя или ID"""
    if not chat_link:
        return None

    processed = chat_link
    
    # Удаляем prefixes из ссылок
    if isinstance(processed, str):
        # Для публичных чатов и каналов
        if processed.startswith("https://t.me/"):
            processed = processed.replace("https://t.me/", "")
        elif processed.startswith("http://t.me/"):
            processed = processed.replace("http://t.me/", "")
        elif processed.startswith("t.me/"):
            processed = processed.replace("t.me/", "")
        elif processed.startswith("@"):
            processed = processed[1:]
        
        # Обработка приватных ссылок для дальнейшего использования
        # Сохраняем полную ссылку для приватных групп (с "+")
        if 'joinchat' in processed or processed.startswith('+'):
            # Если это полная ссылка с joinchat, сохраняем её
            if 'joinchat' in processed:
                return processed
            # Если это приватный хеш, который уже выделен (начинается с +), сохраняем его
            if processed.startswith('+'):
                return processed
            
        # Если после обработки остались '/', берем только последнюю часть
        # (кроме случая с joinchat - этот случай уже обработан выше)
        if '/' in processed and not 'joinchat' in processed:
            processed = processed.split('/')[-1]
    
    return processed

async def check_proxy_connection(proxy_dict, timeout=5):
    """Проверяет работоспособность прокси"""
    try:
        # Создаем соединение напрямую, без использования библиотек
        host = proxy_dict['hostname'] 
        port = proxy_dict['port']
        
        # Асинхронная проверка соединения с таймаутом
        try:
            future = asyncio.open_connection(host, port)
            reader, writer = await asyncio.wait_for(future, timeout=timeout)
            writer.close()
            await writer.wait_closed()
            logging.info(f"Прокси {proxy_dict['scheme']}://{host}:{port} работоспособен")
            return True
        except (asyncio.TimeoutError, ConnectionRefusedError) as e:
            logging.warning(f"Прокси {proxy_dict['scheme']}://{host}:{port} не отвечает: {e}")
            return False
    except Exception as e:
        logging.error(f"Ошибка при проверке прокси: {e}")
        return False

async def rotate_proxy_for_account(phone: str, current_proxy_id: Optional[int] = None):
    """
    Автоматически меняет прокси для аккаунта на следующий доступный.
    Если текущий прокси не работает, выбирает следующий из списка.
    """
    try:
        all_proxies = db.get_all_proxies()
        if not all_proxies:
            logging.warning(f"Нет доступных прокси для ротации аккаунта {phone}")
            await send_log_to_admins(f"⚠️ Нет доступных прокси для ротации аккаунта {phone}")
            return False
        
        # Если указан текущий прокси, ищем следующий после него
        if current_proxy_id:
            current_index = next((i for i, p in enumerate(all_proxies) if p.id == current_proxy_id), -1)
            if current_index >= 0:
                # Берем следующий прокси (с зацикливанием)
                next_index = (current_index + 1) % len(all_proxies)
                new_proxy = all_proxies[next_index]
            else:
                # Если текущий прокси не найден, берем первый
                new_proxy = all_proxies[0]
        else:
            # Если прокси не указан, берем первый доступный
            new_proxy = all_proxies[0]
        
        # Назначаем новый прокси
        if db.update_account_proxy(phone, new_proxy.id):
            logging.info(f"✅ Прокси для аккаунта {phone} изменен на {new_proxy.host}:{new_proxy.port}")
            await send_log_to_admins(
                f"🔄 Ротация прокси: аккаунт {phone}\n"
                f"Новый прокси: {new_proxy.host}:{new_proxy.port} ({new_proxy.scheme})"
            )
            return True
        else:
            logging.error(f"❌ Не удалось обновить прокси для аккаунта {phone}")
            return False
            
    except Exception as e:
        logging.error(f"Ошибка при ротации прокси для аккаунта {phone}: {e}")
        await send_log_to_admins(f"❌ Ошибка ротации прокси для {phone}: {e}")
        return False

async def get_or_create_client(phone, account_data):
    """Создает новый клиент Pyrogram для указанного аккаунта."""
    try:
        # Генерируем имя сессии
        session_name = f"{phone}_{random.randint(10000, 99999)}"
        
        # Проверяем наличие прокси
        proxy_dict = None
        mtproto_proxy = None
        if account_data.get('proxy_id'):
            proxy = db.get_proxy(account_data['proxy_id'])
            if proxy:
                # Убираем возможную точку в конце доменного имени
                host = proxy.host.rstrip('.')
                
                # Определяем тип прокси - MTProto или обычный (SOCKS/HTTP)
                if proxy.scheme.lower() == 'mtproto':
                    # MTProto прокси (Telegram)
                    mtproto_proxy = (host, proxy.port, proxy.password if proxy.password else '')
                    logging.info(f"Используем MTProto прокси {host}:{proxy.port} для {phone}")
                else:
                    # Обычный прокси (SOCKS/HTTP)
                    proxy_dict = {
                        'scheme': proxy.scheme,
                        'hostname': host,
                        'port': proxy.port,
                        'timeout': 30  # Увеличиваем таймаут до 30 секунд
                    }
                    if proxy.username and proxy.password:
                        proxy_dict['username'] = proxy.username
                        proxy_dict['password'] = proxy.password
                    logging.info(f"Используем {proxy.scheme} прокси {host}:{proxy.port} для {phone} (таймаут: 30 сек)")
                    
                    # Проверяем работоспособность обычного прокси перед использованием
                    proxy_working = await check_proxy_connection(proxy_dict)
                    if not proxy_working:
                        logging.warning(f"Прокси для {phone} не отвечает! Пробуем ротацию прокси...")
                        # Пробуем автоматически сменить прокси
                        await rotate_proxy_for_account(phone, account_data['proxy_id'])
                        proxy_dict = None  # Отключаем прокси, если ротация не помогла
        
        # Получаем строку сессии из базы данных, если она существует
        session_string = account_data.get('session')
        
        # Если есть строка сессии, используем её
        if session_string:
            try:
                client = Client(
                    name=session_name,
                    api_id=API_ID,
                    api_hash=API_HASH,
                    session_string=session_string,  # Используем сохранённую строку сессии
                    device_model="Linux Server", 
                    system_version="Linux",
                    app_version="Telegram Desktop 4.12.1",
                    ipv6=False,  # Явно указываем использовать только IPv4
                    no_updates=False,  # Включаем обновления для получения новых сообщений
                    # parse_mode убран, чтобы использовался режим по умолчанию
                    in_memory=True  # Для более эффективной работы
                )
                
                if proxy_dict:
                    client.proxy = proxy_dict
                elif mtproto_proxy:
                    client.mtproto_proxy = mtproto_proxy
                    
                await client.start()
                proxy_type = "MTProto прокси" if mtproto_proxy else "прокси" if proxy_dict else "без прокси"
                logging.info(f"Временный клиент {phone} ({session_name}) успешно запущен с session_string ({proxy_type})")
                return client
            except Exception as session_err:
                logging.warning(f"Не удалось запустить клиент с session_string: {session_err}")
                # Если не получилось с session_string, продолжаем обычным способом
        
        # Обычный способ создания клиента
        client = Client(
            name=session_name,
            api_id=API_ID,
            api_hash=API_HASH,
            workdir=UPLOAD_DIR,
            device_model="Linux Server", 
            system_version="Linux",
            app_version="Telegram Desktop 4.12.1",
            ipv6=False,  # Явно указываем использовать только IPv4
            no_updates=False,  # Включаем обновления для получения новых сообщений
            # parse_mode убран, чтобы использовался режим по умолчанию
            in_memory=True  # Для более эффективной работы
        )
        
        if proxy_dict:
            client.proxy = proxy_dict
        elif mtproto_proxy:
            client.mtproto_proxy = mtproto_proxy
            
        await client.start()
        proxy_type = "MTProto прокси" if mtproto_proxy else "прокси" if proxy_dict else "без прокси"
        logging.info(f"Временный клиент {phone} ({session_name}) успешно запущен ({proxy_type})")
        return client
            
    except (SessionRevoked, AuthKeyUnregistered) as e:
        logging.error(f"Ошибка авторизации при создании клиента {phone}: {e} - Аккаунт будет исключен.")
        if phone in config['copying_accounts']:
            config['copying_accounts'].remove(phone)
            await send_log_to_admins(f"⚠️ Аккаунт {phone} исключен из копирования из-за ошибки авторизации: {e}")
        return None
    except FloodWait as fw:
        logging.error(f"FloodWait при создании клиента {phone}: {fw}")
        await send_log_to_admins(f"⚠️ FloodWait для аккаунта {phone}: ждите {fw.value} секунд")
        return None
    except Exception as e:
        logging.error(f"Ошибка создания клиента {phone}: {e}")
        return None

async def send_message_for_account(phone: str, account_data: dict, msg_text: str, account_groups_list: list, msg_log_index: int):
    """Асинхронная задача для отправки сообщения одним аккаунтом."""
    client = None
    sent_groups_count = 0
    sent_to_groups = []
    try:
        # Создаем клиент
        client = await get_or_create_client(phone, account_data)
        if not client:
            logging.error(f"[Task {phone}] Не удалось создать клиент")
            return phone, -1, [] # -1 как индикатор критической ошибки

        # Корректируем пол
        account_gender = db.get_account_gender(phone)
        corrected_msg_text = fix_gender_specific_text(msg_text, account_gender)

        # Отправляем в группы (последовательно для этого аккаунта)
        sent_to_groups = []
        for group in account_groups_list:
            try:
                # Обрабатываем формат ID чата без присоединения
                processed_chat_id = process_chat_link(group)
                if not processed_chat_id:
                    logging.error(f"[Task {phone}] Некорректный ID чата: {group}")
                    continue

                # Показываем индикатор "печатает" перед отправкой
                try:
                    await client.send_chat_action(processed_chat_id, "typing")
                    # Небольшая задержка для визуального эффекта
                    await asyncio.sleep(0.5)
                except Exception as typing_error:
                    logging.warning(f"[Task {phone}] Не удалось отправить typing indicator: {typing_error}")

                # Отправляем сообщение
                await client.send_message(processed_chat_id, corrected_msg_text)
                logging.info(f"[Task {phone}] Отправил сообщение #{msg_log_index} в группу {group}")
                sent_groups_count += 1
                sent_to_groups.append(group)

                # Задержка между сообщениями внутри задачи аккаунта
                if 'delay_between_messages' in config['delays']:
                    # Новая структура
                    actual_delay = config['delays']['delay_between_messages']
                else:
                    # Старая структура для обратной совместимости
                    min_delay = config['delays'].get('delay_between_messages_min', 5)
                    max_delay = config['delays'].get('delay_between_messages_max', 10)
                    if max_delay < min_delay: max_delay = min_delay
                    actual_delay = random.randint(min_delay, max_delay)
                    
                # Убрали лог паузы чтобы не спамить
                # logging.info(f"[Task {phone}] Пауза {actual_delay}s перед след. группой")
                await asyncio.sleep(actual_delay)

            except FloodWait as e:
                 wait_time = int(e.value * config['delays']['flood_wait_multiplier'])
                 logging.warning(f"[Task {phone}] FloodWait для группы {group}: ждем {wait_time} сек.")
                 # Не отправляем админу лог о FloodWait прямо отсюда, т.к. их может быть много
                 await asyncio.sleep(wait_time)
                 continue
            except ChatWriteForbidden as e:
                 logging.error(f"[Task {phone}] ChatWriteForbidden в группу {group}: {e}. Возможно бан.")
                 # Отправляем уведомление админам о возможном бане
                 await send_log_to_admins(
                     f"🚫 ВНИМАНИЕ: Аккаунт {phone} не может писать в группу {group}\n"
                     f"Возможные причины: бан, отсутствие прав, группа удалена\n"
                     f"Аккаунт исключен из отправки в эту группу"
                 )
                 continue
            except Exception as e:
                logging.error(f"[Task {phone}] Ошибка при отправке в группу {group}: {e}")
                continue

        logging.info(f"[Task {phone}] Завершено, отправлено в {sent_groups_count} групп.")
        return phone, sent_groups_count, sent_to_groups

    except Exception as account_error:
         # Логируем ошибку уровня аккаунта (например, ошибка авторизации при создании клиента)
         logging.error(f"[Task {phone}] КРИТИЧЕСКАЯ ОШИБКА: {account_error}")
         # В случае критической ошибки возвращаем кортеж, но с флагом ошибки
         return phone, -1, [] # -1 как индикатор критической ошибки
    finally:
        if client:
            try:
                await client.stop()
                # logging.info(f"[Task {phone}] Клиент остановлен") # Убрали лог остановки
            except Exception as stop_error:
                logging.error(f"[Task {phone}] Ошибка остановки клиента: {stop_error}")

async def handle_file_one_message(active_accounts, sessions, account_groups, all_groups):
    try:
        # 1. Get the current file ID
        file_id = None
        for phone_check in active_accounts:
            if sessions[phone_check].get('current_file'):
                file_id = sessions[phone_check].get('current_file')
                break
        
        if not file_id:
            logging.error("Не найден файл для отправки")
            return
        
        # 2. Load messages
        files = db.load_message_files()
        if file_id not in files:
            logging.error(f"Файл {file_id} не найден в базе данных")
            return
                        
        # Получаем сообщения и фильтруем пустые строки
        messages = [msg for msg in files[file_id]['messages'].split('\n') if msg.strip()]
                        
        if not messages:
            logging.error(f"Файл {file_id} не содержит валидных сообщений")
            await send_log_to_admins(f"⚠️ Файл {files[file_id]['name']} не содержит сообщений")
            return
        
        # 3. Get current message index and account index
        message_ptr, account_ptr = db.load_state()
                        
        # 4. Check and correct pointers
        if message_ptr >= len(messages):
            message_ptr = 0  # Cycle messages
            logging.info(f"Достигнут конец файла {file_id}, начинаем отправку с начала")
            await send_log_to_admins(f"🔄 Достигнут конец файла {files[file_id]['name']}, начинаем заново")
        
        # Filter active_accounts to those with groups for accurate cycling
        # Важное изменение: используем только аккаунты с запущенными клиентами
        eligible_accounts = [acc for acc in active_accounts 
                            if acc in config['persistent_clients'] 
                            and acc in account_groups 
                            and account_groups[acc]]
                            
        if not eligible_accounts:
             logging.warning(f"Нет аккаунтов с группами и активными клиентами для файла {file_id}")
             # Even if no accounts, cycle the message pointer
             next_message_ptr = (message_ptr + 1) % len(messages)
             db.save_state(next_message_ptr, 0) 
             return

        if account_ptr >= len(eligible_accounts):
            account_ptr = 0 # Cycle accounts
            logging.info("Достигнут конец списка аккаунтов (файл), начинаем заново")
        
        # 5. Select current message and account
        msg_text = messages[message_ptr]
        selected_phone = eligible_accounts[account_ptr]
        
        # 6. Get groups for the selected account
        groups = account_groups.get(selected_phone, []) # Should always exist due to filtering above
        logging.info(f"Файл: Аккаунт {selected_phone} ({account_ptr+1}/{len(eligible_accounts)}) отправляет сообщение #{message_ptr+1}/{len(messages)} в {len(groups)} групп.")
        
        # 7. Используем существующий персистентный клиент
        client = config['persistent_clients'].get(selected_phone)
        sent_groups = []
        
        if not client or not client.is_connected:
            logging.error(f"Персистентный клиент для аккаунта {selected_phone} не найден или не подключен")
            # Skip this account for this cycle, advance state
            next_account_ptr = (account_ptr + 1) % len(eligible_accounts)
            db.save_state(message_ptr, next_account_ptr) 
            return
        
        try:
            # Get gender and fix text
            account_gender = db.get_account_gender(selected_phone)
            corrected_msg_text = fix_gender_specific_text(msg_text, account_gender)
            
            # Send to all groups of this account
            for group in groups:
                try:
                    # Обрабатываем формат ID чата без присоединения
                    processed_chat_id = process_chat_link(group)
                    if not processed_chat_id:
                        logging.error(f"Аккаунт {selected_phone}: некорректный ID группы {group} (файл)")
                        continue
                    
                    # Если это приватная группа, проверяем, присоединены ли мы
                    if processed_chat_id.startswith('+') or 'joinchat' in processed_chat_id:
                        joined, actual_chat_id = await ensure_joined_chat(client, processed_chat_id)
                        if joined:
                            processed_chat_id = actual_chat_id
                        else:
                            logging.error(f"Не удалось присоединиться к {group}")
                            continue
                    
                    # Показываем индикатор "печатает" перед отправкой
                    try:
                        await client.send_chat_action(processed_chat_id, "typing")
                        # Небольшая задержка для визуального эффекта
                        await asyncio.sleep(0.5)
                    except Exception as typing_error:
                        logging.warning(f"Аккаунт {selected_phone}: не удалось отправить typing indicator: {typing_error}")
                    
                    await client.send_message(processed_chat_id, corrected_msg_text)
                    logging.info(f"Аккаунт {selected_phone} отправил сообщение #{message_ptr+1} в группу {group}")
                    sent_groups.append(group)
                    
                    # Delay between messages
                    if 'delay_between_messages' in config['delays']:
                        # Новая структура
                        actual_delay = config['delays']['delay_between_messages']
                    else:
                        # Старая структура для обратной совместимости
                        min_delay = config['delays'].get('delay_between_messages_min', 5)
                        max_delay = config['delays'].get('delay_between_messages_max', 10)
                        if max_delay < min_delay: max_delay = min_delay
                        actual_delay = random.randint(min_delay, max_delay)
                        
                    logging.info(f"Аккаунт {selected_phone}: пауза {actual_delay}s перед след. группой (файл)")
                    await asyncio.sleep(actual_delay)
                
                except FloodWait as e:
                    wait_time = int(e.value * config['delays']['flood_wait_multiplier'])
                    logging.warning(f"FloodWait {selected_phone} для группы {group} (файл): ждем {wait_time} сек.")
                    await send_log_to_admins(f"⏳ FloodWait для {selected_phone} -> {group} (файл): {wait_time} сек.")
                    await asyncio.sleep(wait_time)
                    continue # Skip this group after waiting
                except ChatWriteForbidden as e:
                     logging.error(f"Ошибка ChatWriteForbidden аккаунт {selected_phone} в группу {group} (файл): {e}. Возможно, бан.")
                     await send_log_to_admins(
                         f"🚫 ВНИМАНИЕ: Аккаунт {selected_phone} не может писать в группу {group} (режим файла)\n"
                         f"Возможные причины: бан, отсутствие прав, группа удалена\n"
                         f"Проверьте статус аккаунта и группы"
                     )
                     continue # Skip this group
                except Exception as e:
                    logging.error(f"Ошибка при отправке сообщения аккаунтом {selected_phone} в группу {group} (файл): {e}")
                    continue # Continue with other groups
            
            # Log success for this account
            if sent_groups:
                log_msg = (
                    f"📤 Файл: Сообщение #{message_ptr+1}/{len(messages)}\n"
                    f"📝 Текст: {msg_text[:100]}{'...' if len(msg_text) > 100 else ''}\n"
                    f"👤 Аккаунт: {selected_phone}\n"
                    f"👥 Отправлено в {len(sent_groups)}/{len(groups)} групп"
                )
                await send_log_to_admins(log_msg)
            else:
                logging.warning(f"Аккаунт {selected_phone} не отправил сообщение #{message_ptr+1} ни в одну из своих групп.")
            
            # 8. Update state: Advance to the next account, keep the same message
            next_account_ptr = (account_ptr + 1) % len(eligible_accounts)
            # Проверяем, нужно ли перейти к следующему сообщению
            if next_account_ptr == 0:  # Если закончился цикл аккаунтов
                next_message_ptr = (message_ptr + 1) % len(messages)  # Переходим к следующему сообщению
                db.save_state(next_message_ptr, 0)
                logging.info(f"Состояние обновлено: msg={next_message_ptr}, acc=0 (переход к новому сообщению)")
            else:
                db.save_state(message_ptr, next_account_ptr) 
                logging.info(f"Состояние обновлено: msg={message_ptr}, acc={next_account_ptr} (файл)")
            
        except Exception as e:
            logging.exception(f"Критическая ошибка при обработке аккаунта {selected_phone} (файл): {e}")
            # If an error occurred during account processing, advance to the next account anyway
            next_account_ptr = (account_ptr + 1) % len(eligible_accounts)
            db.save_state(message_ptr, next_account_ptr)
            await send_log_to_admins(f"❌ Крит. ошибка с аккаунтом {selected_phone} (файл), переход к след.: {e}")
                        
    except Exception as e:
        logging.exception(f"Критическая ошибка в handle_file_one_message: {e}")
        await send_log_to_admins(f"❌ Критическая ошибка в обработке файла: {e}")

# >>> НАЧАЛО: Вспомогательная функция для параллельной отправки в чат-режиме <<<
async def send_chat_message_for_account(phone: str, client, msg_text: str, target_group: str, source_msg_id: int, source_message=None, message_id_map=None, media_type: str = None, media_content: dict = None, active_accounts=None, try_buffer=True, reply_to_id=None, grouped_id=None):
    # Глобально отключаем цитирование по запросу пользователя
    DISABLE_QUOTING = True
    
    # Переопределяем функцию для форматирования текста без цитат
    def format_text_without_quotes(text, quote_text=None):
        return text
    try:
        # Минимальное логирование основных параметров
        logging.info(f"Отправка сообщения от {phone} в {target_group}")
        # Весь код функции будет здесь

        # Если grouped_id не передан и это не ответ, генерируем новый
        if not grouped_id and not reply_to_id:
            grouped_id = int(str(uuid.uuid4().int)[:9])  # Уникальный ID (9 цифр для Telegram)
            logging.info(f"Сгенерирован новый grouped_id для ветки: {grouped_id}")

        # Инициализируем словарь chat_id_cache, если он еще не существует
        if 'chat_id_cache' not in config:
            config['chat_id_cache'] = {}
            
        # Получаем или вычисляем chat_id
        chat_id = config['chat_id_cache'].get(target_group)
        
        if not chat_id:
            try:
                chat = await client.get_chat(target_group)
                chat_id = chat.id
                # Сохраняем правильный chat_id в кеш
                config['chat_id_cache'][target_group] = chat_id
                logging.info(f"Кеширован chat_id {chat_id} для группы {target_group}")
            except Exception as e:
                logging.error(f"Ошибка получения chat_id для {target_group}: {e}")
                return False, f"Ошибка получения chat_id: {str(e)}"
        else:
            logging.info(f"Использован кешированный chat_id {chat_id} для группы {target_group}")

        # Проверяем буфер ответов и наличие цитаты
        reply_to_message_id = reply_to_id
        add_quote = False
        original_text = None
        sender_phone = None
        source_chat_id = None
        
        # Проверяем, является ли исходное сообщение ответом
        is_source_reply = source_message and hasattr(source_message, 'reply_to_message') and source_message.reply_to_message is not None
        if is_source_reply:
            logging.info(f"Исходное сообщение является ответом на другое сообщение в источнике")
        else:
            logging.info(f"Исходное сообщение не является ответом в источнике")
        
        # Инициализируем словарь last_messages_in_chats, если он еще не существует
        if 'last_messages_in_chats' not in config:
            config['last_messages_in_chats'] = {}
            logging.info("Инициализирован словарь last_messages_in_chats")
            
        # Если сообщение из источника имеет reply_to, обрабатываем его
        if try_buffer and source_message and source_message.reply_to_message:
            source_chat_id = getattr(source_message, 'chat', {}).id if hasattr(source_message, 'chat') else None
            if source_chat_id:
                buffer_reply_id, reply_sender_phone = await process_reply_buffer(
                    client, source_message, source_chat_id, chat_id, message_id_map, phone
                )
                
                if buffer_reply_id:
                    reply_to_message_id = buffer_reply_id
                    sender_phone = reply_sender_phone
                    
                    # Если сообщения отправлены разными аккаунтами
                    if sender_phone and sender_phone != phone:
                        try:
                            # Пытаемся получить текст оригинального сообщения
                            original_message = await client.get_messages(chat_id, message_ids=[buffer_reply_id])
                            # Проверяем, что сообщение существует и имеет текст или подпись
                            if original_message and original_message[0] and (hasattr(original_message[0], 'text') or hasattr(original_message[0], 'caption')):
                                original_text = original_message[0].text or original_message[0].caption or ""
                            else:
                                # Сбрасываем buffer_reply_id, так как сообщение пустое или не существует
                                buffer_reply_id = None
                                original_text = None
                        except Exception as e:
                            logging.warning(f"Не удалось получить текст оригинального сообщения: {e}")
                            buffer_reply_id = None
                            original_text = None
                else:
                    # Если не нашли ID для ответа, получаем текст для цитаты из исходного сообщения
                    original_text = getattr(source_message.reply_to_message, 'text', None) or getattr(source_message.reply_to_message, 'caption', None) or ""
                # Проверка на null reply_to_message_id и валидация
            else:
                logging.warning("Не удалось определить source_chat_id для буфера ответов")
        # Убираем автоматический ответ на последнее сообщение в чате - 
        # отправляем сообщение как ответ только если оно было ответом в источнике
        # Дополнительная логика автоответа отключена по запросу пользователя
        pass

                # Цитирование отключено по запросу пользователя
        add_quote = False
        

        
        # Дополнительное логирование для отладки ответов
        if reply_to_message_id:
            
            try:
                # Проверим, что reply_to_message_id валиден
                test_msg = await client.get_messages(chat_id, message_ids=[reply_to_message_id])
                if test_msg and test_msg[0]:
                    reply_sender = test_msg[0].from_user
                    pass
                else:
                    logging.warning(f"Сообщение {reply_to_message_id} не найдено перед отправкой ответа")
            except Exception as e:
                logging.error(f"Ошибка проверки сообщения {reply_to_message_id}: {e}")

        # Отправляем сообщение в зависимости от типа контента
        sent_message = None
        source_msg_key = None
        if source_chat_id and source_msg_id:
            source_msg_key = f"{source_chat_id}:{source_msg_id}"
            
        if media_type == "text" or not media_type:
            if reply_to_message_id:
                # Цитирование отключено по запросу пользователя
                use_quote = False
                # Цитирование отключено по запросу пользователя
                formatted_msg = msg_text
                
                # Обеспечиваем правильную отправку ответа с минимальными параметрами
                try:
                    # Дополнительная проверка перед отправкой
                    if reply_to_message_id:
                        try:
                            original_msg = await client.get_messages(chat_id, message_ids=[reply_to_message_id])
                            if not original_msg or not original_msg[0] or not hasattr(original_msg[0], 'text'):
                                logging.warning(f"Сообщение {reply_to_message_id} не найдено или пустое, отправляем без reply_to")
                                # Сбрасываем reply_to_message_id, чтобы избежать ошибок
                                reply_to_message_id = None
                        except Exception as e:
                            logging.error(f"Ошибка при проверке сообщения {reply_to_message_id}: {e}")
                    
                    # Показываем индикатор "печатает" перед отправкой
                    try:
                        await client.send_chat_action(chat_id, "typing")
                        await asyncio.sleep(0.5)
                    except Exception as typing_error:
                        logging.warning(f"Не удалось отправить typing indicator для {phone}: {typing_error}")
                    
                    # Отправляем сообщение с минимальными параметрами для корректной работы ответов
                    sent_message = await client.send_message(
                        chat_id=chat_id,
                        text=formatted_msg,
                        reply_to_message_id=reply_to_message_id
                    )
                except Exception as e:
                    logging.error(f"Ошибка при отправке сообщения с reply_to: {e}")
                    # Если не удалось отправить с reply_to_message_id, пробуем без него
                    sent_message = await client.send_message(
                        chat_id=chat_id,
                        text=formatted_msg
                    )
                logging.info(f"✅ Сообщение {phone} → Ответ на #{reply_to_message_id}")
                
                # Сохраняем это сообщение как последнее в данном чате
                if sent_message:
                    config['last_messages_in_chats'][str(chat_id)] = (sent_message.id, phone)
                    logging.info(f"Обновлено последнее сообщение в чате {chat_id}: id={sent_message.id}, phone={phone}")
                    
                    # Регистрируем отправленное сообщение для будущих ответов
                    if message_id_map is not None and source_msg_key:
                        register_message_id(sent_message, source_msg_key, chat_id, f"[{phone}]", phone, message_id_map, grouped_id)
            else:
                sent_message = await client.send_message(
                    chat_id=chat_id,
                    text=msg_text
                )
                logging.info(f"✅ Сообщение {phone} → Отправлено")
                
                # Сохраняем это сообщение как последнее в данном чате
                if sent_message:
                    config['last_messages_in_chats'][str(chat_id)] = (sent_message.id, phone)
                    logging.info(f"Обновлено последнее сообщение в чате {chat_id}: id={sent_message.id}, phone={phone}")
                    
                    # Регистрируем отправленное сообщение для будущих ответов
                    if message_id_map is not None and source_msg_key:
                        register_message_id(sent_message, source_msg_key, chat_id, f"[{phone}]", phone, message_id_map, grouped_id)
        elif media_type == "photo" and media_content:
            file_id = media_content.get('file_id')
            file_sizes = media_content.get('file_sizes', [file_id])
            file_unique_id = media_content.get('file_unique_id')
            
            # Получаем подпись из медиа-контента (безопасный способ)
            # Сначала смотрим caption в media_content, потом в msg_text как запасной вариант
            caption = media_content.get('caption', '') or msg_text or None
            
            if file_id:
                logging.info(f"Пытаемся отправить фото с file_id: {file_id}, подпись: '{caption}'")
                
                if reply_to_message_id:
                    # Проверяем, нужно ли делать цитирование (quote=True)
                    use_quote = sender_phone and sender_phone != phone
                    # Если отвечаем на сообщение другого аккаунта, можно добавить цитату в текст
                    if use_quote and original_text:
                        # Добавляем цитату в начало сообщения
                        quote_text = original_text[:100] + ("..." if len(original_text) > 100 else "")
                        formatted_quote = "\n".join([f"> {line}" for line in quote_text.split('\n')])
                        formatted_msg = f"{formatted_quote}\n\n{caption}" if caption else formatted_quote
                    else:
                        formatted_msg = caption
                    
                    # Пробуем отправить фото с несколькими попытками используя разные методы
                    photo_sent = False
                    
                    # Показываем индикатор "загружает фото" перед отправкой
                    try:
                        await client.send_chat_action(chat_id, "upload_photo")
                        await asyncio.sleep(0.5)
                    except Exception as typing_error:
                        logging.warning(f"Не удалось отправить typing indicator для фото {phone}: {typing_error}")
                    
                    # Попытка 1: Стандартная отправка с file_id
                    try:
                        sent_message = await client.send_photo(
                            chat_id=chat_id,
                            photo=file_id,
                            caption=formatted_msg,
                            reply_to_message_id=reply_to_message_id
                        )
                        logging.info(f"✅ Фото с reply_to {phone} → Отправлено")
                        photo_sent = True
                    except Exception as e:
                        logging.error(f"❌ Ошибка отправки фото с reply_to: {str(e)}")
                        
                        # Попытка 2: Попробуем другие размеры фото
                        if not photo_sent and len(file_sizes) > 1:
                            for alt_file_id in file_sizes:
                                if alt_file_id != file_id:  # Пробуем другой размер
                                    try:
                                        sent_message = await client.send_photo(
                                            chat_id=chat_id,
                                            photo=alt_file_id,
                                            caption=formatted_msg,
                                            reply_to_message_id=reply_to_message_id
                                        )
                                        logging.info(f"✅ Фото (альтернативный размер) с reply_to {phone} → Отправлено")
                                        photo_sent = True
                                        break
                                    except Exception as e2:
                                        logging.error(f"❌ Ошибка отправки фото (альт. размер): {str(e2)}")
                        
                        # Попытка 3: Отправить без reply_to
                        if not photo_sent:
                            try:
                                sent_message = await client.send_photo(
                                    chat_id=chat_id,
                                    photo=file_id,
                                    caption=formatted_msg
                                )
                                logging.info(f"✅ Фото (без reply) {phone} → Отправлено")
                                photo_sent = True
                            except Exception as e3:
                                logging.error(f"❌ Ошибка отправки фото без reply: {str(e3)}")
                                
                        # Попытка 4: Если всё не получилось, отправляем как текст
                        if not photo_sent:
                            sent_message = await client.send_message(
                                chat_id=chat_id,
                                text=f"[Фото не может быть отправлено]\n{formatted_msg}",
                                reply_to_message_id=reply_to_message_id
                            )
                            logging.info(f"✅ Сообщение о фото {phone} → Отправлено")
                else:
                    # Аналогичная логика для отправки без reply_to
                    photo_sent = False
                    
                    # Получаем подпись из media_content
                    caption_to_use = caption or msg_text
                    
                    # Попытка 1: Стандартная отправка
                    try:
                        sent_message = await client.send_photo(
                            chat_id=chat_id,
                            photo=file_id,
                            caption=caption_to_use
                        )
                        logging.info(f"✅ Фото {phone} → Отправлено")
                        photo_sent = True
                    except Exception as e:
                        logging.error(f"❌ Ошибка отправки фото: {str(e)}")
                        
                        # Попытка 2: Попробуем другие размеры фото
                        if not photo_sent and len(file_sizes) > 1:
                            for alt_file_id in file_sizes:
                                if alt_file_id != file_id:  # Пробуем другой размер
                                    try:
                                        sent_message = await client.send_photo(
                                            chat_id=chat_id,
                                            photo=alt_file_id,
                                            caption=caption_to_use
                                        )
                                        logging.info(f"✅ Фото (альтернативный размер) {phone} → Отправлено")
                                        photo_sent = True
                                        break
                                    except Exception as e2:
                                        logging.error(f"❌ Ошибка отправки фото (альт. размер): {str(e2)}")
                        
                        # Попытка 3: Если фото выглядит как URL, пробуем отправить как URL
                        if not photo_sent and (file_id.startswith(('http://', 'https://', '/')) or 
                                             any(s.startswith(('http://', 'https://', '/')) for s in file_sizes)):
                            try:
                                url = next((s for s in file_sizes if s.startswith(('http://', 'https://', '/'))), file_id)
                                sent_message = await client.send_photo(
                                    chat_id=chat_id,
                                    photo=url,
                                    caption=caption_to_use
                                )
                                logging.info(f"✅ Фото (из URL) {phone} → Отправлено")
                                photo_sent = True
                            except Exception as e3:
                                logging.error(f"❌ Ошибка отправки фото из URL: {str(e3)}")
                                
                        # Попытка 4: Если всё не получилось, отправляем как текст
                        if not photo_sent:
                            sent_message = await client.send_message(
                                chat_id=chat_id,
                                text=f"[Фото не может быть отправлено]\n{caption_to_use if caption_to_use else ''}"
                            )
                            logging.info(f"✅ Сообщение о фото {phone} → Отправлено")
        elif media_type == "video" and media_content:
            file_id = media_content.get('file_id')
            if file_id:
                # Получаем подпись из медиа-контента
                caption = media_content.get('caption', '') or msg_text or None
                logging.info(f"Пытаемся отправить видео с file_id: {file_id}, подпись: '{caption}'")
                
                if reply_to_message_id:
                    # Проверяем, нужно ли делать цитирование (quote=True)
                    use_quote = sender_phone and sender_phone != phone
                    # Если отвечаем на сообщение другого аккаунта, можно добавить цитату в текст
                    if use_quote and original_text:
                        # Добавляем цитату в начало сообщения
                        quote_text = original_text[:100] + ("..." if len(original_text) > 100 else "")
                        formatted_quote = "\n".join([f"> {line}" for line in quote_text.split('\n')])
                        formatted_msg = f"{formatted_quote}\n\n{caption}" if caption else formatted_quote
                    else:
                        formatted_msg = caption
                    
                    # Показываем индикатор "загружает видео" перед отправкой
                    try:
                        await client.send_chat_action(chat_id, "upload_video")
                        await asyncio.sleep(0.5)
                    except Exception as typing_error:
                        logging.warning(f"Не удалось отправить typing indicator для видео {phone}: {typing_error}")
                    
                    try:    
                        sent_message = await client.send_video(
                            chat_id=chat_id,
                            video=file_id,
                            caption=formatted_msg,
                            reply_to_message_id=reply_to_message_id
                        )
                        logging.info(f"✅ Видео с reply_to {phone} → Отправлено")
                    except Exception as e:
                        logging.error(f"❌ Ошибка отправки видео с reply_to: {str(e)}")
                        try:
                            # Пробуем отправить без reply_to_message_id в случае ошибки
                            sent_message = await client.send_video(
                                chat_id=chat_id,
                                video=file_id,
                                caption=formatted_msg
                            )
                            logging.info(f"✅ Видео (без reply) {phone} → Отправлено")
                        except Exception as e2:
                            logging.error(f"❌ Повторная ошибка отправки видео: {str(e2)}")
                            raise e2
                else:
                    try:
                        sent_message = await client.send_video(
                            chat_id=chat_id,
                            video=file_id,
                            caption=msg_text if msg_text else ""
                        )
                        logging.info(f"✅ Видео {phone} → Отправлено")
                    except Exception as e:
                        logging.error(f"❌ Ошибка отправки видео: {str(e)}")
                        # Повторная попытка, но с загрузкой из URL или файла, если есть
                        if file_id.startswith(('http://', 'https://', '/')):
                            sent_message = await client.send_video(
                                chat_id=chat_id,
                                video=file_id,
                                caption=msg_text
                            )
                            logging.info(f"✅ Видео (из URL/файла) {phone} → Отправлено")
                        else:
                            raise e
        elif media_type == "sticker" and media_content:
            file_id = media_content.get('file_id')
            if file_id:
                logging.info(f"Пытаемся отправить стикер с file_id: {file_id}")
                # Стикеры не поддерживают группировку в Telegram API
                try:
                    if reply_to_message_id:
                        # Проверяем, нужно ли делать цитирование (quote=True)
                        use_quote = sender_phone and sender_phone != phone
                        # Для стикеров не можем добавить цитату в текст, т.к. у них нет текста
                        sent_message = await client.send_sticker(
                            chat_id=chat_id,
                            sticker=file_id,
                            reply_to_message_id=reply_to_message_id
                        )
                        logging.info(f"✅ Стикер с reply_to {phone} → Отправлен")
                    else:
                        sent_message = await client.send_sticker(
                            chat_id=chat_id,
                            sticker=file_id
                        )
                        logging.info(f"✅ Стикер {phone} → Отправлен")
                except Exception as e:
                    logging.error(f"❌ Ошибка отправки стикера: {str(e)}")
                    # Если в группе запрещены стикеры, отправляем как обычное сообщение
                    if "CHAT_SEND_STICKERS_FORBIDDEN" in str(e):
                        logging.info(f"В группе запрещены стикеры, отправляем сообщение о стикере")
                        sent_message = await client.send_message(
                            chat_id=chat_id,
                            text="[Стикер не может быть отправлен в эту группу]",
                            reply_to_message_id=reply_to_message_id if reply_to_message_id else None
                        )
                        logging.info(f"✅ Сообщение о стикере {phone} → Отправлено")
                    else:
                        raise e
        elif media_type == "voice" and media_content:
            file_id = media_content.get('file_id')
            if file_id:
                logging.info(f"Пытаемся отправить голосовое с file_id: {file_id}")
                # Голосовые сообщения не поддерживают группировку
                try:
                    if reply_to_message_id:
                        # Проверяем, нужно ли делать цитирование (quote=True)
                        use_quote = sender_phone and sender_phone != phone
                        # Если отвечаем на сообщение другого аккаунта, можно добавить цитату в текст
                        if use_quote and original_text:
                            # Добавляем цитату в начало сообщения
                            quote_text = original_text[:100] + ("..." if len(original_text) > 100 else "")
                            formatted_quote = "\n".join([f"> {line}" for line in quote_text.split('\n')])
                            formatted_msg = f"{formatted_quote}\n\n{msg_text}"
                        else:
                            formatted_msg = msg_text

                        # Показываем индикатор "записывает голосовое" перед отправкой
                        try:
                            await client.send_chat_action(chat_id, "record_voice")
                            await asyncio.sleep(0.5)
                        except Exception as typing_error:
                            logging.warning(f"Не удалось отправить typing indicator для голосового {phone}: {typing_error}")

                        sent_message = await client.send_voice(
                            chat_id=chat_id,
                            voice=file_id,
                            caption=formatted_msg,
                            reply_to_message_id=reply_to_message_id
                        )
                        logging.info(f"✅ Голосовое с reply_to {phone} → Отправлено")
                    else:
                        # Показываем индикатор "записывает голосовое" перед отправкой
                        try:
                            await client.send_chat_action(chat_id, "record_voice")
                            await asyncio.sleep(0.5)
                        except Exception as typing_error:
                            logging.warning(f"Не удалось отправить typing indicator для голосового {phone}: {typing_error}")
                        
                        sent_message = await client.send_voice(
                            chat_id=chat_id,
                            voice=file_id,
                            caption=msg_text if msg_text else ""
                        )
                        logging.info(f"✅ Голосовое {phone} → Отправлено")
                except Exception as e:
                    logging.error(f"❌ Ошибка отправки голосового: {str(e)}")
                    try:
                        # Пробуем отправить без reply_to
                        if reply_to_message_id:
                            sent_message = await client.send_voice(
                                chat_id=chat_id,
                                voice=file_id,
                                caption=msg_text
                            )
                            logging.info(f"✅ Голосовое (без reply) {phone} → Отправлено")
                        else:
                            # Если не помогло, пробуем отправить как текстовое сообщение
                            sent_message = await client.send_message(
                                chat_id=chat_id,
                                text=f"[Голосовое сообщение не может быть отправлено]\n{msg_text if msg_text else ''}"
                            )
                            logging.info(f"✅ Сообщение о голосовом {phone} → Отправлено")
                    except Exception as e2:
                        logging.error(f"❌ Повторная ошибка отправки голосового: {str(e2)}")
                        raise e2
        elif media_type == "animation" and media_content:
            file_id = media_content.get('file_id')
            if file_id:
                logging.info(f"Пытаемся отправить анимацию/GIF с file_id: {file_id}")
                
                try:
                    if reply_to_message_id:
                        # Проверяем, нужно ли делать цитирование (quote=True)
                        use_quote = sender_phone and sender_phone != phone
                        # Если отвечаем на сообщение другого аккаунта, можно добавить цитату в текст
                        if use_quote and original_text:
                            # Добавляем цитату в начало сообщения
                            quote_text = original_text[:100] + ("..." if len(original_text) > 100 else "")
                            formatted_quote = "\n".join([f"> {line}" for line in quote_text.split('\n')])
                            formatted_msg = f"{formatted_quote}\n\n{msg_text}"
                        else:
                            formatted_msg = msg_text
                        
                        sent_message = await client.send_animation(
                            chat_id=chat_id,
                            animation=file_id,
                            caption=formatted_msg,
                            reply_to_message_id=reply_to_message_id
                        )
                        logging.info(f"✅ Анимация с reply_to {phone} → Отправлена")
                    else:
                        sent_message = await client.send_animation(
                            chat_id=chat_id,
                            animation=file_id,
                            caption=msg_text if msg_text else ""
                        )
                        logging.info(f"✅ Анимация {phone} → Отправлена")
                except Exception as e:
                    logging.error(f"❌ Ошибка отправки анимации: {str(e)}")
                    try:
                        # Попробуем скачать файл и отправить
                        logging.info("Пробуем скачать и отправить анимацию как видео")
                        # Если сообщение пришло из источника, пробуем получить оригинальное сообщение
                        try:
                            if source_message and hasattr(source_message, 'animation'):
                                # Сначала получим свежую версию сообщения для обновления file_reference
                                try:
                                    fresh_message = await client.get_messages(
                                        chat_id=source_message.chat.id,
                                        message_ids=source_message.id
                                    )
                                    if fresh_message and hasattr(fresh_message, 'animation'):
                                        logging.info(f"Получено свежее сообщение с анимацией")
                                        # Скачаем файл из исходного сообщения
                                        file_path = f"temp_animation_{int(time.time())}.mp4"
                                        await client.download_media(fresh_message, file_path)
                                        logging.info(f"Анимация успешно скачана в {file_path}")
                                        
                                        # Проверка существования файла
                                        if not os.path.exists(file_path) or os.path.getsize(file_path) == 0:
                                            raise Exception(f"Файл {file_path} не существует или пуст")
                                            
                                        sent_message = await client.send_video(
                                            chat_id=chat_id,
                                            video=file_path,
                                            caption=msg_text if msg_text else "",
                                            reply_to_message_id=reply_to_message_id if reply_to_message_id else None
                                        )
                                        logging.info(f"✅ Анимация как видео {phone} → Отправлена")
                                    else:
                                        raise Exception("Не удалось получить свежее сообщение с анимацией")
                                except Exception as e_fresh:
                                    logging.error(f"Ошибка при получении свежего сообщения: {str(e_fresh)}")
                                    raise e_fresh
                            else:
                                # Если не удалось получить оригинальное сообщение, отправляем текст
                                sent_message = await client.send_message(
                                    chat_id=chat_id,
                                    text=f"[Анимация не может быть отправлена]\n{msg_text if msg_text else ''}",
                                    reply_to_message_id=reply_to_message_id if reply_to_message_id else None
                                )
                                logging.info(f"✅ Сообщение об анимации {phone} → Отправлено")
                        except Exception as e2:
                            logging.error(f"❌ Ошибка при скачивании/отправке анимации: {str(e2)}")
                            sent_message = await client.send_message(
                                chat_id=chat_id,
                                text=f"[Анимация не может быть отправлена]\n{msg_text if msg_text else ''}",
                                reply_to_message_id=reply_to_message_id if reply_to_message_id else None
                            )
                            logging.info(f"✅ Сообщение об анимации {phone} → Отправлено")
                    except Exception as e_outer:
                        logging.error(f"❌ Ошибка при обработке анимации: {str(e_outer)}")
                        # Отправляем как текст в крайнем случае
                        sent_message = await client.send_message(
                            chat_id=chat_id,
                            text=f"[Анимация не может быть отправлена]\n{msg_text if msg_text else ''}",
                            reply_to_message_id=reply_to_message_id if reply_to_message_id else None
                        )
                        logging.info(f"✅ Сообщение об анимации {phone} → Отправлено")
                    finally:
                        # Удаляем временный файл
                        try:
                            # Проверяем, есть ли файл для удаления в текущей области видимости
                            if 'file_path' in locals():
                                if os.path.exists(file_path):
                                    os.remove(file_path)
                                    logging.info(f"Удален временный файл {file_path}")
                                else:
                                    logging.info(f"Файл {file_path} не существует, не требует удаления")
                        except Exception as e3:
                            logging.error(f"Ошибка при удалении временного файла: {str(e3)}")
        else:
            logging.warning(f"Неподдерживаемый тип медиа: {media_type}")
            # Отправляем как обычный текст
            # Проверяем, нужно ли делать цитирование
            use_quote = sender_phone and sender_phone != phone and reply_to_message_id is not None
            
            # Если отвечаем на сообщение другого аккаунта, можно добавить цитату в текст
            if use_quote and original_text:
                # Добавляем цитату в начало сообщения
                quote_text = original_text[:100] + ("..." if len(original_text) > 100 else "")
                formatted_quote = "\n".join([f"> {line}" for line in quote_text.split('\n')])
                formatted_msg = f"{formatted_quote}\n\n{msg_text}"
            else:
                formatted_msg = msg_text
            
            # Показываем индикатор "печатает" перед отправкой текстового сообщения
            try:
                await client.send_chat_action(chat_id, "typing")
                # Небольшая задержка для визуального эффекта
                await asyncio.sleep(0.5)
            except Exception as typing_error:
                logging.warning(f"Не удалось отправить typing indicator для {phone}: {typing_error}")
                
            sent_message = await client.send_message(
                chat_id=chat_id,
                text=formatted_msg,
                reply_to_message_id=reply_to_message_id
            )
            logging.info(f"✅ Сообщение {phone} → Отправлено (неизвестный медиатип)")
        
        # Регистрируем соответствие сообщений с grouped_id
        if sent_message:
            # Сохраняем это сообщение как последнее в данном чате для ответов
            config['last_messages_in_chats'][str(chat_id)] = (sent_message.id, phone)
            logging.info(f"Обновлено последнее сообщение в чате {chat_id}: id={sent_message.id}, phone={phone}")
            
            # Регистрируем сообщение в message_id_map, если есть исходное сообщение
            if message_id_map is not None and source_message and hasattr(source_message, 'chat') and hasattr(source_message, 'id'):
                message_id_map = register_message_id(sent_message, f"{source_message.chat.id}:{source_message.id}", chat_id, f"[{phone}]", phone, message_id_map, grouped_id)
            elif message_id_map is not None and source_msg_key:
                register_message_id(sent_message, source_msg_key, chat_id, f"[{phone}]", phone, message_id_map, grouped_id)
                
            # Инициализируем словарь target_chat_history, если он еще не существует
            if 'target_chat_history' not in config:
                config['target_chat_history'] = {}
                
            # Добавляем сообщение в историю целевого чата
            if chat_id not in config['target_chat_history']:
                config['target_chat_history'][chat_id] = []
                
            # Получаем текст отправленного сообщения
            sent_text = ""
            if hasattr(sent_message, 'text') and sent_message.text:
                sent_text = sent_message.text
            elif hasattr(sent_message, 'caption') and sent_message.caption:
                sent_text = sent_message.caption
                
            config['target_chat_history'][chat_id].append({
                'message_id': sent_message.id,
                'text': sent_text,
                'sender_phone': phone,
                'reply_to_message_id': reply_to_message_id
            })
            logging.info(f"Добавлено сообщение {sent_message.id} в историю чата {chat_id}")

        # Возвращаем результат отправки
        return True, ""

    except FloodWait as e:
        wait_time = int(e.value * config['delays']['flood_wait_multiplier'])
        logging.warning(f"FloodWait для {phone}: ждем {wait_time} сек")
        await asyncio.sleep(wait_time)
        return False, f"FloodWait: {wait_time} сек"
    except ChatWriteForbidden:
        logging.error(f"Нет прав на отправку в {target_group} для {phone}")
        # Отправляем уведомление админам о проблеме
        await send_log_to_admins(
            f"🚫 ВНИМАНИЕ: Аккаунт {phone} не может писать в группу {target_group} (чат-режим)\n"
            f"Возможные причины: бан, отсутствие прав, группа удалена\n"
            f"Проверьте статус аккаунта и группы"
        )
        return False, "Нет прав на отправку"
    except Exception as e:
        logging.error(f"Ошибка отправки сообщения в {target_group} от {phone}: {e}")
        return False, str(e)

async def handle_chat_mode(active_accounts, sessions, account_groups, all_groups):
    try:
        # Создаем маппинг user_id -> phone для быстрого поиска
        user_id_to_phone = {}
        for phone, data in sessions.items():
            if 'user_id' in data and data['user_id']:
                user_id_to_phone[data['user_id']] = phone

        # >>> ИСПОЛЬЗУЕМ ПЕРСИСТЕНТНЫЕ КЛИЕНТЫ ДЛЯ GET_ME <<<
        active_account_info = {} # Информация об аккаунтах с активным клиентом
        logging.info(f"Получение информации для {len(config['persistent_clients'])} активных персистентных клиентов...")
        phones_with_active_clients = list(config['persistent_clients'].keys()) # Копируем ключи
        
        # Проверяем и используем активные клиенты
        valid_active_accounts = [] # Список аккаунтов, у которых клиент точно работает
        for phone in phones_with_active_clients:
            client = config['persistent_clients'].get(phone)
            if client and client.is_connected:
                try:
                    me = await client.get_me()
                    active_account_info[phone] = {
                        'id': me.id,
                        'username': me.username,
                        'first_name': me.first_name,
                        'last_name': getattr(me, 'last_name', None)
                    }
                    # Обновляем user_id в базе данных, если нужно
                    if sessions.get(phone) and sessions[phone].get('user_id') != me.id:
                         logging.info(f"Обновление user_id для {phone} на {me.id}")
                         db.save_session(
                             phone=phone,
                             session=sessions[phone].get('session'),
                             source_chat=sessions[phone].get('source_chat'),
                             dest_chats=sessions[phone].get('dest_chats'),
                             current_file=sessions[phone].get('current_file'),
                             copy_mode=sessions[phone].get('copy_mode'),
                             last_message_id=sessions[phone].get('last_message_id'),
                             last_sent_index=sessions[phone].get('last_sent_index'),
                             proxy_id=sessions[phone].get('proxy_id'),
                             gender=sessions[phone].get('gender'),
                             user_id=me.id
                         )
                         user_id_to_phone[me.id] = phone
                    valid_active_accounts.append(phone) # Добавляем в список валидных
                except Exception as e:
                    logging.error(f"Ошибка get_me для {phone} (персистентный клиент): {e}")
                    # Если ошибка, считаем аккаунт невалидным для этого цикла
            else:
                logging.warning(f"Персистентный клиент для {phone} отсутствует или не подключен.")
        
        # Используем только валидные аккаунты дальше
        active_accounts = valid_active_accounts
        account_groups = {p: g for p, g in account_groups.items() if p in active_accounts}
        all_groups = set(g for groups in account_groups.values() for g in groups)

        if not active_accounts:
             logging.warning("В handle_chat_mode не осталось активных аккаунтов после проверки клиентов.")
             return
             
        logging.info(f"Проверка клиентов завершена. Активных аккаунтов: {len(active_accounts)}. Групп: {len(all_groups)}")

        # Собираем новые сообщения от всех аккаунтов с уникальными источниками
        unique_sources = set(process_chat_link(acc_data.get('source_chat'))
                           for phone, acc_data in sessions.items()
                           if phone in active_accounts and acc_data and acc_data.get('source_chat'))

        # >>> ИЗМЕНЕНИЕ: Выбираем один основной аккаунт для каждого источника <<<
        source_to_account = {}
        for source_id in unique_sources:
            if not source_id: continue
            
            # Выбираем один аккаунт для данного источника
            for phone in active_accounts:
                if process_chat_link(sessions[phone].get('source_chat')) == source_id:
                    potential_client = config['persistent_clients'].get(phone)
                    if potential_client and potential_client.is_connected:
                        source_to_account[source_id] = phone
                        break
            
            if source_id not in source_to_account:
                logging.warning(f"Не найден активный аккаунт для источника {source_id}")
        
        logging.info(f"Назначено {len(source_to_account)} основных аккаунтов для чтения источников")
        # Добавляем дополнительное логирование для проверки инициализации
        for source_id, phone in source_to_account.items():
            logging.info(f"Проверка инициализации для {phone}: источник {source_id}")
        # >>> КОНЕЦ ИЗМЕНЕНИЯ <<<

        all_new_messages = []
        # >>> ИСПОЛЬЗУЕМ ПЕРСИСТЕНТНЫЕ КЛИЕНТЫ ДЛЯ GET_NEW_MESSAGES <<<
        for source_id in unique_sources:
             if not source_id: continue
             
             # >>> ИЗМЕНЕНИЕ: Используем только один аккаунт для каждого источника <<<
             fetch_phone = source_to_account.get(source_id)
             if not fetch_phone:
                 logging.warning(f"Пропуск источника {source_id} - нет назначенного аккаунта")
                 continue
                 
             client_to_fetch = config['persistent_clients'].get(fetch_phone)
             if not client_to_fetch or not client_to_fetch.is_connected:
                 logging.warning(f"Аккаунт {fetch_phone} для источника {source_id} не имеет активного клиента")
                 continue
             # >>> КОНЕЦ ИЗМЕНЕНИЯ <<<

             last_message_id = sessions[fetch_phone].get('last_message_id', 0)

             try:
                 source_joined, processed_source_id = await ensure_joined_chat(client_to_fetch, source_id)
                 if not source_joined:
                     logging.error(f"Аккаунт {fetch_phone} (персистентный) не смог присоединиться к источнику {source_id}")
                     continue

                 messages, new_last_id = await get_new_messages(client_to_fetch, processed_source_id, last_message_id)

                 if messages:
                     for msg_id, msg_txt, sender_id, sender_info in messages:
                          all_new_messages.append((msg_id, msg_txt, source_id, sender_id, sender_info))
                          logging.info(f"Добавлено новое сообщение #{msg_id} из {source_id} (через {fetch_phone})")

                     if new_last_id > last_message_id:
                         for p, ad in sessions.items():
                             if p in active_accounts and process_chat_link(ad.get('source_chat')) == source_id:
                                 if not db.update_last_message_id(p, new_last_id):
                                      logging.error(f"Не удалось обновить last_message_id для {p}")

             except Exception as e:
                 logging.error(f"Ошибка получения сообщений из {source_id} аккаунтом {fetch_phone} (персистентным): {e}")

        if not all_new_messages:
            return

        # >>> Обработка каждого нового сообщения (остальная логика без изменений) <<<
        for message_id, message_text, source_id, sender_id, sender_info in all_new_messages:

            tasks = []
            accounts_chosen_for_groups = {} # group -> phone
            phones_to_use = set() # Уникальные телефоны, для которых нужны клиенты
            task_params = [] # Список кортежей (phone, group)

            logging.info(f"----- Начало ПАРАЛЛЕЛЬНОЙ обработки сообщения чата #{message_id} из {source_id} ----- ")

            # 1. Определяем отправителя
            sender_phone = None
            if sender_id in user_id_to_phone:
                sender_phone = user_id_to_phone[sender_id]
            if not sender_phone and sender_info:
                if 'username' in sender_info and sender_info['username']:
                    for phone, info in active_account_info.items():
                        if info.get('username') == sender_info['username']: sender_phone = phone; break
                if not sender_phone and 'first_name' in sender_info:
                    for phone, info in active_account_info.items():
                        last_name_match = True
                        if info.get('last_name') or sender_info.get('last_name'):
                             last_name_match = (info.get('last_name') == sender_info.get('last_name'))
                        if info.get('first_name') == sender_info['first_name'] and last_name_match:
                             sender_phone = phone; break

            if sender_phone: logging.info(f"Определен отправитель сообщения чата #{message_id}: {sender_phone}")

            # 2. Определяем пары (аккаунт, группа) и собираем уникальные телефоны
            if sender_phone and sender_phone in active_accounts:
                # Сценарий 1: Отправитель - наш активный аккаунт
                if sender_phone in config['persistent_clients']:
                    sender_groups = account_groups.get(sender_phone, [])
                    if sender_groups:
                        logging.info(f"Сообщение #{message_id} от нашего аккаунта {sender_phone}. Целевые группы: {sender_groups}")
                        phones_to_use.add(sender_phone)
                        for group in sender_groups:
                            task_params.append((sender_phone, group))
                            accounts_chosen_for_groups[group] = sender_phone
                    else:
                        logging.warning(f"Аккаунт-отправитель {sender_phone} не имеет групп. Сообщение #{message_id} не переслано.")
                        continue
                else:
                    logging.warning(f"Клиент аккаунта-отправителя {sender_phone} неактивен. Сообщение #{message_id} не переслано.")
                    continue
            else:
                # Сценарий 2: Внешний отправитель или не удалось определить
                log_sender_type = f"от внешнего источника ({sender_info})" if sender_info else "от неопределенного источника"
                logging.info(f"Сообщение #{message_id} {log_sender_type}. Ротация аккаунтов для групп: {list(all_groups)}")
                for group in all_groups:
                    chosen_phone = None
                    eligible_accounts_for_group = [
                        p for p in active_accounts
                        if p in account_groups and group in account_groups[p] and p in config['persistent_clients']
                    ]
                    if not eligible_accounts_for_group:
                        logging.warning(f"Нет активных аккаунтов для группы {group}")
                        continue

                    last_account = config['group_account_map'].get(group)
                    temp_eligible = eligible_accounts_for_group.copy()
                    if last_account in temp_eligible and len(temp_eligible) > 1:
                        temp_eligible.remove(last_account)

                    chosen_phone = random.choice(temp_eligible)
                    if chosen_phone:
                        config['group_account_map'][group] = chosen_phone
                        phones_to_use.add(chosen_phone)
                        task_params.append((chosen_phone, group))
                        accounts_chosen_for_groups[group] = chosen_phone
                    else:
                        logging.error(f"Не удалось выбрать аккаунт для группы {group}")

            if not task_params:
                logging.warning(f"Не определены пары (аккаунт, группа) для сообщения #{message_id}")
                continue

            # 4. Создаем ЗАДАЧИ, используя персистентные клиенты
            for phone, group in task_params:
                client_to_pass = config['persistent_clients'].get(phone)
                if client_to_pass and client_to_pass.is_connected:
                    tasks.append(send_chat_message_for_account(phone, client_to_pass, message_text, group, message_id))
                else:
                    logging.warning(f"Пропуск задачи {phone} -> {group}, клиент неактивен.")

            if not tasks:
                logging.warning(f"Не создано задач для сообщения #{message_id}." )
                continue

            # 5. Запускаем задачи параллельно
            results = []
            try:
                logging.info(f"Запуск {len(tasks)} задач для сообщения #{message_id}")
                # results = await asyncio.gather(*tasks, return_exceptions=False) # Remove parallel execution
                # logging.info(f"Задачи для сообщения #{message_id} завершены.")

                # Modified to send sequentially
                for task in tasks:
                    try:
                        result = await task
                        results.append(result)
                        logging.info(f"Последовательно выполнена задача для сообщения #{message_id}")
                        # Добавляем задержку между отправками для последовательного выполнения
                        await asyncio.sleep(config['delays'].get('delay_between_messages', 3))
                    except Exception as single_task_err:
                        logging.error(f"Ошибка при выполнении задачи: {single_task_err}", exc_info=True)

            except Exception as gather_err:
                 logging.error(f"Ошибка в asyncio.gather: {gather_err}", exc_info=True)

            # 7. Обрабатываем результаты
            total_sent_count_this_message = 0
            successful_sends = {} # group -> phone
            failed_sends = {} # group -> phone
            critical_errors_accounts = set()
            
            for result in results:
                if isinstance(result, tuple) and len(result) >= 2:
                    success, error_msg = result[0], result[1]
                    if success:  # Проверяем успешность отправки
                        total_sent_count_this_message += 1  # Увеличиваем счетчик успешных отправок
                        # Добавляем информацию об успешной отправке в соответствующую группу и аккаунт
                        for phone, group in task_params:
                            group_key = group if group else "UNKNOWN_GROUP"
                            successful_sends[group_key] = phone
                    else:
                        # Добавляем информацию о неудачной отправке
                        for phone, group in task_params:
                            group_key = group if group else "UNKNOWN_GROUP"
                            failed_sends[group_key] = phone
                else:
                    logging.error(f"Неожиданный результат задачи чата: {result}")

            # 8. Логируем итог
            success_groups = list(successful_sends.keys())
            failed_groups = list(failed_sends.keys())
            log_summary = (
                f"📤 Чат: Сообщение #{message_id} из {source_id} обработано\\n"
                f"📝 Текст: {message_text[:70]}{'...' if len(message_text) > 70 else ''}\\n"
                f"✅ Успешно ({len(success_groups)}): {', '.join(success_groups) if success_groups else 'Нет'}\\n"
                f"⚠️ Не отп ({len(failed_groups)}): {', '.join(failed_groups) if failed_groups else 'Нет'}\\n"
                f"❌ Ошибки ({len(critical_errors_accounts)}): {', '.join(critical_errors_accounts) if critical_errors_accounts else 'Нет'}\\n"
                f"📊 Всего отправок: {total_sent_count_this_message}"
            )
            await send_log_to_admins(log_summary)
            logging.info(f"Итог сообщения #{message_id}: Успешно={len(success_groups)}, Не отпр={len(failed_groups)}, Ошибки={len(critical_errors_accounts)}, Всего={total_sent_count_this_message}")
            logging.info(f"----- Завершение обработки сообщения #{message_id} ----- ")

            # 9. Пауза после обработки сообщения
            msg_processing_delay = config['delays']['delay_between_accounts']
            await asyncio.sleep(msg_processing_delay)

    except Exception as e:
        logging.exception(f"Критическая ошибка в handle_chat_mode: {e}")
        await send_log_to_admins(f"❌ Критическая ошибка в handle_chat_mode: {e}")

@dp.callback_query(lambda c: c.data.startswith("select_file_"))
async def select_file_handler(callback: CallbackQuery):
    logging.info(f"Вызван select_file_handler для data: {callback.data}")
    try:
        # Извлекаем file_id из callback.data
        file_id = callback.data.replace("select_file_", "")
        
        # Проверяем наличие файла
        files = db.load_message_files()
        if file_id not in files:
            logging.warning(f"Файл {file_id} не найден в БД при попытке выбора.")
            await callback.answer("❌ Файл не найден", show_alert=True)
            return
            
        # Получаем данные о файле
        file_data = files[file_id]
        file_name = file_data['name']
        
        # Загружаем сессии
        sessions = db.load_sessions()
        if not sessions:
            await callback.answer("❌ У вас нет аккаунтов для выбора файла", show_alert=True)
            return
            
        # Формируем клавиатуру с аккаунтами
        kb = []
        
        for phone, session_data in sessions.items():
            kb.append([
                InlineKeyboardButton(
                    text=f"📱 {phone}",
                    callback_data=f"use_file_{file_id}_{phone}"
                )
            ])
            
        kb.append([
            InlineKeyboardButton(text="✅ Для всех аккаунтов", callback_data=f"use_file_all_{file_id}")
        ])
        kb.append([
            InlineKeyboardButton(text="⬅️ Назад", callback_data="my_files")
        ])
        
        await callback.message.edit_text(
            f"📄 <b>Выберите аккаунт</b> для файла <b>{file_name}</b>:",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=kb)
        )
        
    except Exception as e:
        logging.error(f"Ошибка в select_file_handler: {e}")
        await callback.answer("❌ Произошла ошибка при выборе файла", show_alert=True)

@dp.callback_query(lambda c: c.data.startswith("delete_file_"))
async def delete_file_handler(callback: CallbackQuery):
    try:
        # Извлекаем file_id из callback.data
        file_id = callback.data.replace("delete_file_", "")
        
        # Проверяем, существует ли файл
        files = db.load_message_files()
        if file_id not in files:
            await callback.answer("❌ Файл не найден", show_alert=True)
            return

        # Получаем имя файла для лога
        file_name = files[file_id]["name"]
        
        # Удаляем файл из базы данных
        db.delete_message_file(file_id)
        
        # Удаляем связи с этим файлом во всех сессиях
        sessions = db.load_sessions()
        for phone, session_data in sessions.items():
            if session_data.get('current_file') == file_id:
                db.save_session(
                    phone=phone,
                    session=session_data.get('session', ''),
                    source_chat=session_data.get('source_chat'),
                    dest_chats=session_data.get('dest_chats'),
                    current_file=None,  # Очищаем ссылку на удаленный файл
                    copy_mode=session_data.get('copy_mode'),
                    last_message_id=session_data.get('last_message_id'),
                    last_sent_index=session_data.get('last_sent_index'),
                    proxy_id=session_data.get('proxy_id')
                )
        
        logging.info(f"Файл {file_name} (ID: {file_id}) удален пользователем {callback.from_user.id}")
        
        # Показываем сообщение об успешном удалении
        await callback.answer(f"✅ Файл {file_name} успешно удален", show_alert=True)
        
        # Обновляем список файлов
        await show_my_files(callback)
        
    except Exception as e:
        logging.error(f"Ошибка в delete_file_handler: {e}")
        await callback.answer("❌ Произошла ошибка при удалении файла", show_alert=True)

async def get_new_messages(client, processed_source_chat_id, last_message_id):
    """Получает новые сообщения из указанного источника."""
    try:
        # Проверка формата ID источника
        if not processed_source_chat_id:
            logging.error(f"Пустой ID источника. Убедитесь, что источник указан корректно.")
            await send_log_to_admins(f"⚠️ Пустой ID источника. Убедитесь, что источник указан корректно в настройках аккаунта.")
            return [], last_message_id
            
        # "Прогреваем" клиент, чтобы убедиться, что чаты доступны
        try:
            logging.info("Подготовка к получению сообщений...")
            dialogs_count = 0
            # Загружаем диалоги сразу, чтобы инициализировать все чаты
            async for dialog in client.get_dialogs(limit=50):
                dialogs_count += 1
            logging.info(f"Подготовлено {dialogs_count} диалогов")
        except Exception as e:
            logging.warning(f"Ошибка при подготовке диалогов: {e}")
            await asyncio.sleep(2)  # Задержка в случае ошибки
        
        # Очищаем ID для универсального сравнения
        clean_source_id = str(processed_source_chat_id).lstrip('-').replace('100', '', 1).replace('+', '')
        if 'joinchat' in clean_source_id:
            clean_source_id = clean_source_id.split('joinchat/')[-1]
        
        # Загружаем диалоги и находим нужный чат по имени/юзернейму/ссылке
        logging.info(f"Поиск чата в диалогах по ID: {processed_source_chat_id} (очищенный: {clean_source_id})")
        
        # Получаем все диалоги
        all_dialogs = []
        matching_dialog = None
        
        try:
            async for dialog in client.get_dialogs(limit=100):
                all_dialogs.append(dialog)
                
                if dialog.chat:
                    chat_id = str(dialog.chat.id) if hasattr(dialog.chat, 'id') else ''
                    
                    # Проверка на точное совпадение ID
                    if chat_id == processed_source_chat_id:
                        matching_dialog = dialog
                        logging.info(f"Найден точный диалог по ID: {chat_id}")
                        break
                    
                    # Сравниваем очищенные версии ID
                    chat_clean_id = chat_id.lstrip('-').replace('100', '', 1)
                    if chat_clean_id == clean_source_id:
                        matching_dialog = dialog
                        logging.info(f"Найден диалог по очищенному ID: {chat_id}")
                        break
                    
                    # Проверка по юзернейму/ссылке
                    if hasattr(dialog.chat, 'username') and dialog.chat.username:
                        username = dialog.chat.username.lower()
                        source_username = processed_source_chat_id.replace('@', '').lower()
                        
                        if username == source_username or f"t.me/{username}" in processed_source_chat_id.lower():
                            matching_dialog = dialog
                            logging.info(f"Найден диалог по имени пользователя: {username}")
                            break
            
        except Exception as e:
            logging.error(f"Ошибка при получении диалогов: {e}")
            await asyncio.sleep(2)  # Добавляем задержку при ошибке
        
        # Если не нашли точное соответствие, проверяем ещё раз всю информацию о чатах
        if not matching_dialog and all_dialogs:
            logging.info("Поиск соответствия по хэшу инвайта...")
            for dialog in all_dialogs:
                if dialog.chat:
                    try:
                        # Получаем полную информацию о чате
                        chat_info = await client.get_chat(dialog.chat.id)
                        
                        # Проверяем все поля на соответствие
                        if hasattr(chat_info, 'invite_link') and chat_info.invite_link:
                            invite_hash = chat_info.invite_link.split('/')[-1]
                            if clean_source_id == invite_hash or clean_source_id in invite_hash:
                                matching_dialog = dialog
                                logging.info(f"Найден диалог по инвайт-ссылке: {dialog.chat.id}")
                                break
                    except Exception:
                        continue
        
        # Если не нашли соответствия, берём первый диалог (последний активный)
        if not matching_dialog and all_dialogs:
            # Возможно, нужный нам чат - один из первых в списке диалогов (недавно присоединились)
            for dialog in all_dialogs[:5]:  # Проверяем первые 5 диалогов
                if dialog.chat and hasattr(dialog.chat, 'type') and dialog.chat.type in ('group', 'supergroup', 'channel'):
                    matching_dialog = dialog
                    logging.info(f"Использую первый подходящий групповой чат: {dialog.chat.id}")
                    break
        
        # Если всё ещё нет соответствия, выходим
        if not matching_dialog:
            logging.error(f"Не удалось найти соответствующий диалог для {processed_source_chat_id}")
            return [], last_message_id

        # Дополнительная проверка, что чат доступен
        try:
            # Пробуем получить немного сообщений для проверки доступа
            test_count = 0
            async for _ in client.get_chat_history(matching_dialog.chat.id, limit=1):
                test_count += 1
            if test_count == 0:
                # Добавляем задержку и повторяем
                logging.info(f"Чат {matching_dialog.chat.id} еще не инициализирован, ждем 3 секунды...")
                await asyncio.sleep(3)
        except Exception:
            # Если ошибка, добавляем задержку
            logging.warning(f"Ошибка при тестировании доступа к чату, ждем 3 секунды...")
            await asyncio.sleep(3)
        
        # Получаем историю из найденного диалога
        messages_reversed = []
        max_message_id_processed = last_message_id
        
        try:
            logging.info(f"Получение истории из диалога с ID: {matching_dialog.chat.id}")
            async for message in client.get_chat_history(matching_dialog.chat.id, limit=50):
                if message.id <= last_message_id:
                    break
                
                if message.text:  # Берем только текстовые сообщения
                    # Получаем отправителя сообщения
                    sender_id = None
                    sender_info = {}
                    
                    if hasattr(message, 'from_user') and message.from_user:
                        sender_id = message.from_user.id
                        
                        # Сохраняем дополнительную информацию для идентификации
                        if hasattr(message.from_user, 'username') and message.from_user.username:
                            sender_info['username'] = message.from_user.username
                        
                        if hasattr(message.from_user, 'first_name'):
                            sender_info['first_name'] = message.from_user.first_name
                        
                        if hasattr(message.from_user, 'last_name') and message.from_user.last_name:
                            sender_info['last_name'] = message.from_user.last_name
                    
                    messages_reversed.append((message.id, message.text, sender_id, sender_info))
                    max_message_id_processed = max(message.id, max_message_id_processed)
            
            logging.info(f"Получено {len(messages_reversed)} новых сообщений")
            
        except Exception as e:
            logging.error(f"Ошибка при получении истории чата {matching_dialog.chat.id}: {e}")
            return [], last_message_id
        
        # Переворачиваем список, чтобы сообщения были в хронологическом порядке (старые -> новые)
        messages_chronological = messages_reversed[::-1]
        
        return messages_chronological, max_message_id_processed
    
    except Exception as e:
        logging.error(f"Непредвиденная ошибка получения сообщений из {processed_source_chat_id}: {e}")
        return [], last_message_id

# Глобальный словарь для кеширования чатов, чтобы избежать повторных присоединений
# Формат: {оригинальный_идентификатор: (присоединен_успешно, обработанный_идентификатор)}
CHAT_ID_CACHE = {}

async def ensure_joined_chat(client, chat_id):
    """Проверяет, присоединен ли клиент к чату, и при необходимости присоединяется.
    Возвращает кортеж (bool, str): статус и обработанный идентификатор чата"""
    try:
        client_id = client.session_name if hasattr(client, 'session_name') else str(id(client))
        cache_key = f"{client_id}:{chat_id}"

        if cache_key in CHAT_ID_CACHE:
            is_joined, processed_id = CHAT_ID_CACHE[cache_key]
            if is_joined:
                logging.info(f"Используем кешированный ID для {chat_id}: {processed_id}")
                return is_joined, processed_id

        # Если chat_id уже является числом, просто преобразуем в строку и вернем
        if isinstance(chat_id, int):
            chat_id_str = str(chat_id)
            CHAT_ID_CACHE[cache_key] = (True, chat_id_str)
            return True, chat_id_str

        processed_chat_id = process_chat_link(chat_id)
        if not processed_chat_id:
            logging.error(f"Пустая ссылка на чат после обработки: {chat_id}")
            return False, chat_id

        if not hasattr(client, 'get_chat'):
            logging.error(f"Клиент не поддерживает метод get_chat! Версия Pyrogram: {pyrogram.__version__}")
            return False, chat_id

        if processed_chat_id.startswith('+') or 'joinchat' in processed_chat_id:
            logging.info(f"Обнаружена ссылка на приватный чат: {processed_chat_id}")

            if processed_chat_id.startswith('+') and 'joinchat' not in processed_chat_id:
                full_invite_link = f"https://t.me/+{processed_chat_id[1:]}"
                invite_hash = processed_chat_id[1:]
            elif 'joinchat' in processed_chat_id:
                full_invite_link = processed_chat_id if processed_chat_id.startswith('http') else f"https://{processed_chat_id}"
                invite_hash = full_invite_link.split('/joinchat/')[-1] if '/joinchat/' in full_invite_link else None
            else:
                full_invite_link = processed_chat_id
                invite_hash = None

            try:
                logging.info(f"Проверяем, является ли пользователь уже участником чата: {full_invite_link}")
                found_in_dialogs = False
                chat_id_from_dialogs = None

                # Счетчик для ограничения вывода логов
                dialogs_checked = 0
                logging.debug("Начинаем проверку диалогов...")
                
                async for dialog in client.get_dialogs(limit=100):
                    if dialog.chat:
                        # Удаляем избыточный лог, логируем только при обнаружении совпадения
                        dialogs_checked += 1
                        
                        if invite_hash and hasattr(dialog.chat, 'invite_link') and dialog.chat.invite_link:
                            dialog_invite_hash = dialog.chat.invite_link.split('/')[-1]
                            if invite_hash in dialog_invite_hash:
                                chat_id_from_dialogs = str(dialog.chat.id)
                                chat_title = dialog.chat.title if hasattr(dialog.chat, 'title') else 'None'
                                logging.info(f"Найден чат по инвайт-хэшу в диалогах: {chat_title} ({chat_id_from_dialogs})")
                                found_in_dialogs = True
                                break
                
                logging.debug(f"Проверено {dialogs_checked} диалогов")

                if found_in_dialogs:
                    CHAT_ID_CACHE[cache_key] = (True, chat_id_from_dialogs)
                    return True, chat_id_from_dialogs

                logging.info(f"Не нашли чат в диалогах, пробуем присоединиться: {full_invite_link}")

                try:
                    chat = await client.join_chat(full_invite_link)
                    if chat and hasattr(chat, 'id'):
                        chat_id_str = str(chat.id)
                        logging.info(f"Успешно присоединились к чату, получен ID: {chat_id_str}")
                        CHAT_ID_CACHE[cache_key] = (True, chat_id_str)
                        await asyncio.sleep(5)
                        return True, chat_id_str
                    else:
                        logging.error(f"Не удалось получить ID чата после присоединения: {full_invite_link}")
                        CHAT_ID_CACHE[cache_key] = (False, processed_chat_id)
                        return False, processed_chat_id

                except FloodWait as fw:
                    logging.warning(f"FloodWait при присоединении к чату {full_invite_link}: {fw.value} секунд")
                    logging.info(f"Получили FloodWait, выполняем расширенный поиск в диалогах")

                    # Создаем список потенциальных чатов
                    potential_chats = []
                    dialogs_checked = 0
                    
                    async for dialog in client.get_dialogs(limit=150):
                        dialogs_checked += 1
                        if dialog.chat and hasattr(dialog.chat, 'id'):
                            chat_id_str = str(dialog.chat.id)
                            # Проверяем, что это не служебный ID Telegram
                            if chat_id_str == "777000" or chat_id_str == "333000":
                                continue
                                
                            chat_title = dialog.chat.title if hasattr(dialog.chat, 'title') else None
                            
                            # Ищем чат с названием Источник/Source
                            if chat_title:
                                title_lower = chat_title.lower()
                                priority = 1
                                if "источник" in title_lower or "источ" in title_lower or "source" in title_lower:
                                    priority = 10
                                    logging.info(f"Найден потенциальный чат-источник по имени: {chat_title} ({chat_id_str})")
                                
                                # Если это групповой чат, добавляем его в список
                                if chat_id_str.startswith("-"):
                                    potential_chats.append((chat_id_str, chat_title, priority))
                    
                    logging.debug(f"Проверено {dialogs_checked} диалогов при поиске потенциальных чатов")
                    
                    # Выбираем лучший кандидат из потенциальных чатов
                    if potential_chats:
                        # Сортируем по приоритету (от высокого к низкому)
                        potential_chats.sort(key=lambda x: x[2], reverse=True)
                        
                        # Проверяем, есть ли чат с ID, из которого приходят сообщения
                        for chat_id, chat_title, priority in potential_chats:
                            if chat_id == str(processed_chat_id):
                                logging.info(f"Найден чат с ID источника: {chat_title} ({chat_id})")
                                CHAT_ID_CACHE[cache_key] = (True, chat_id)
                                return True, chat_id
                        
                        # Если не нашли чат с ID источника, берем чат с наивысшим приоритетом
                        best_chat_id = potential_chats[0][0]
                        best_chat_name = potential_chats[0][1]
                        logging.info(f"Выбран оптимальный чат: {best_chat_name} ({best_chat_id})")
                        CHAT_ID_CACHE[cache_key] = (True, best_chat_id)
                        return True, best_chat_id

                    logging.error(f"Не найдено подходящих чатов для {full_invite_link}")
                    CHAT_ID_CACHE[cache_key] = (False, processed_chat_id)
                    return False, processed_chat_id

                except Exception as join_err:
                    if 'USER_ALREADY_PARTICIPANT' in str(join_err):
                        logging.info(f"Пользователь уже участник чата, пробуем получить ID через get_chat")
                        try:
                            if invite_hash:
                                try:
                                    chat = await client.get_chat(full_invite_link)
                                    if chat and hasattr(chat, 'id'):
                                        chat_id_str = str(chat.id)
                                        logging.info(f"Получен ID чата через get_chat: {chat_id_str}")
                                        CHAT_ID_CACHE[cache_key] = (True, chat_id_str)
                                        return True, chat_id_str
                                except Exception as e:
                                    logging.warning(f"Не удалось получить чат через get_chat: {e}")

                            # Используем приоритетный поиск чатов
                            potential_chats = []
                            
                            async for dialog in client.get_dialogs(limit=150):
                                if dialog.chat and hasattr(dialog.chat, 'id'):
                                    chat_id_str = str(dialog.chat.id)
                                    # Пропускаем служебные ID
                                    if chat_id_str == "777000" or chat_id_str == "333000":
                                        continue
                                        
                                    dialog_title = dialog.chat.title if hasattr(dialog.chat, 'title') else 'None'
                                    
                                    # Приоритизируем групповые чаты
                                    if chat_id_str.startswith("-"):
                                        priority = 1
                                        # Если в названии есть ключевые слова - повышаем приоритет
                                        if dialog_title and ("источ" in dialog_title.lower() or "source" in dialog_title.lower()):
                                            priority = 10
                                        potential_chats.append((chat_id_str, dialog_title, priority))

                            # Выбираем чат с наивысшим приоритетом
                            if potential_chats:
                                potential_chats.sort(key=lambda x: x[2], reverse=True)
                                best_chat_id = potential_chats[0][0]
                                best_chat_name = potential_chats[0][1]
                                logging.info(f"Выбран чат с наивысшим приоритетом: {best_chat_name} ({best_chat_id})")
                                CHAT_ID_CACHE[cache_key] = (True, best_chat_id)
                                return True, best_chat_id

                            logging.error(f"Не удалось найти чат в диалогах для {full_invite_link}")
                            CHAT_ID_CACHE[cache_key] = (False, processed_chat_id)
                            return False, processed_chat_id
                        except Exception as e:
                            logging.error(f"Ошибка при получении ID чата: {e}")
                            CHAT_ID_CACHE[cache_key] = (False, processed_chat_id)
                            return False, processed_chat_id
                    else:
                        logging.error(f"Ошибка при присоединении к приватному чату {processed_chat_id}: {join_err}")
                        CHAT_ID_CACHE[cache_key] = (False, processed_chat_id)
                        return False, processed_chat_id

            except Exception as e:
                logging.error(f"Ошибка при проверке диалогов: {e}")
                CHAT_ID_CACHE[cache_key] = (False, processed_chat_id)
                return False, processed_chat_id

        else:
            logging.info(f"Обрабатываем публичный чат или ID: {processed_chat_id}")
            try:
                try:
                    chat = await client.get_chat(int(processed_chat_id))
                except (ValueError, TypeError):
                    chat = await client.get_chat(processed_chat_id)

                if chat and hasattr(chat, 'id'):
                    chat_id_str = str(chat.id)
                    logging.info(f"Чат найден: {chat_id_str}")
                    CHAT_ID_CACHE[cache_key] = (True, chat_id_str)
                    return True, chat_id_str

                logging.error(f"Чат {processed_chat_id} не найден")
                CHAT_ID_CACHE[cache_key] = (False, processed_chat_id)
                return False, processed_chat_id

            except Exception as e:
                logging.error(f"Ошибка при получении чата {processed_chat_id}: {e}")
                try:
                    chat = await client.join_chat(processed_chat_id)
                    if chat and hasattr(chat, 'id'):
                        chat_id_str = str(chat.id)
                        logging.info(f"Присоединился к публичной группе: {chat_id_str}")
                        CHAT_ID_CACHE[cache_key] = (True, chat_id_str)
                        await asyncio.sleep(3)
                        return True, chat_id_str
                    else:
                        logging.error(f"Не удалось присоединиться к чату {processed_chat_id}")
                        CHAT_ID_CACHE[cache_key] = (False, processed_chat_id)
                        return False, processed_chat_id

                except FloodWait as fw:
                    logging.warning(f"FloodWait при присоединении к публичному чату {processed_chat_id}: {fw.value} секунд")
                    try:
                        # Используем приоритетный поиск чатов
                        potential_chats = []
                        
                        async for dialog in client.get_dialogs(limit=150):
                            if dialog.chat and hasattr(dialog.chat, 'id'):
                                chat_id_str = str(dialog.chat.id)
                                # Пропускаем служебные ID
                                if chat_id_str == "777000" or chat_id_str == "333000":
                                    continue
                                    
                                dialog_username = getattr(dialog.chat, 'username', None)
                                dialog_title = getattr(dialog.chat, 'title', None)
                                processed_chat_lower = processed_chat_id.lower().strip('@') if isinstance(processed_chat_id, str) else ""
                                
                                priority = 0
                                # Ищем чат по username
                                if dialog_username and processed_chat_lower and dialog_username.lower() == processed_chat_lower:
                                    logging.info(f"Найден чат по username в диалогах: {chat_id_str} ({dialog_title})")
                                    CHAT_ID_CACHE[cache_key] = (True, chat_id_str)
                                    return True, chat_id_str
                                
                                # Ищем чаты с ключевыми словами
                                if dialog_title:
                                    if "источ" in dialog_title.lower() or "source" in dialog_title.lower():
                                        priority = 10
                                    elif "назнач" in dialog_title.lower() or "dest" in dialog_title.lower():
                                        priority = 5
                                
                                # Сохраняем только групповые чаты
                                if chat_id_str.startswith("-"):
                                    potential_chats.append((chat_id_str, dialog_title, priority))

                        # Выбираем лучший кандидат
                        if potential_chats:
                            potential_chats.sort(key=lambda x: x[2], reverse=True)
                            best_chat_id = potential_chats[0][0]
                            best_chat_name = potential_chats[0][1] or "Без имени"
                            logging.info(f"Выбран чат с наивысшим приоритетом: {best_chat_name} ({best_chat_id})")
                            CHAT_ID_CACHE[cache_key] = (True, best_chat_id)
                            return True, best_chat_id

                        CHAT_ID_CACHE[cache_key] = (False, processed_chat_id)
                        return False, processed_chat_id

                    except Exception as e:
                        logging.error(f"Ошибка при поиске в диалогах: {e}")
                        CHAT_ID_CACHE[cache_key] = (False, processed_chat_id)
                        return False, processed_chat_id

                except Exception as e:
                    logging.error(f"Не удалось присоединиться к публичной группе {processed_chat_id}: {e}")
                    CHAT_ID_CACHE[cache_key] = (False, processed_chat_id)
                    return False, processed_chat_id

    except Exception as e:
        logging.error(f"Непредвиденная ошибка при присоединении к чату {chat_id}: {e}")
        return False, chat_id

async def check_bot_in_group(group_id: str) -> Tuple[bool, Optional[str]]:
    """Проверяет, является ли бот участником группы
    
    Возвращает: кортеж (успех, название_группы)
    """
    try:
        # Пытаемся получить информацию о чате
        chat = await bot.get_chat(group_id)
        
        # Проверяем, является ли полученный объект группой/супергруппой
        if chat.type in ['group', 'supergroup']:
            # Пытаемся получить информацию о себе в группе
            bot_member = await bot.get_chat_member(chat_id=group_id, user_id=bot.id)
            
            # Проверяем, действительно ли мы участник группы
            if bot_member.status not in ['left', 'kicked']:
                logging.info(f"Бот является участником группы {group_id}")
                return True, chat.title
        
        logging.warning(f"Бот не является участником группы {group_id}")
        return False, None
    except Exception as e:
        logging.error(f"Ошибка при проверке бота в группе {group_id}: {e}")
        return False, None

async def copying_task():
    """Основная задача копирования сообщений между чатами"""
    message_handlers = []  # Инициализируем список обработчиков
    client_handlers = {}  # Словарь для хранения обработчиков по телефону
    last_message_per_group = {}  # {group_id: {'message_id': id, 'sender': phone}}
    last_responder_per_group = {}
    try:
        # Предотвращаем запуск нескольких экземпляров задачи
        if hasattr(copying_task, 'is_running') and copying_task.is_running:
            logging.error("Задача копирования уже запущена!")
            await send_log_to_admins("⚠️ Задача копирования уже запущена!")
            return
            
        copying_task.is_running = True
        logging.info("Запуск задачи копирования сообщений...")
        
        # Инициализируем или сбрасываем структуры данных для сопоставления сообщений
        # Это помогает избежать проблем с неверным отображением сообщений после перезапуска бота
        config['message_map'] = {}  # Карта соответствий сообщений (source_id:msg_id -> target_id:msg_id)
        config['message_account_map'] = {}  # Карта аккаунтов для сообщений
        
        # Сбрасываем историю целевых чатов при новом запуске копирования
        config['target_chat_history'] = {}  # Очищаем историю сообщений
        
        # Добавляем новые структуры данных для решения проблемы с ротацией аккаунтов
        config['last_message_per_group'] = {}  # Словарь для хранения последних сообщений в каждой группе и отправившего аккаунта
        config['last_responder_per_group'] = {}  # Словарь для хранения последнего аккаунта, который отвечал в группе
        
        # Проверяем наличие клиентов в глобальном пуле
        if not config['persistent_clients']:
            logging.warning("Нет подключённых клиентов для копирования")
            copying_task.is_running = False
            return
            
        # Берем активные все аккаунты с запущенными клиентами
        sessions = db.load_sessions()
        active_accounts = {phone: sessions.get(phone, {}) for phone in sessions}
        
        logging.info(f"Найдено {len(active_accounts)} активных аккаунтов")
        
        # Создаем карту соответствий групп и аккаунтов
        # Карта: group -> список телефонов аккаунтов, которые работают с этой группой
        account_groups = {}
        
        # Также создаем карту групп источников и соответствующих им аккаунтов
        # Карта: source_chat -> список телефонов аккаунтов для этого источника
        source_accounts = {}
        
        # Список всех уникальных групп
        all_groups = set()
        
        # Список всех целевых чатов для отслеживания истории
        target_chats = set()
        
        # Создадим карту реальных ID для кеширования
        id_cache = {}
        
        # Перебираем все аккаунты и строим карту groups -> accounts
        for phone, data in active_accounts.items():
            # Пропускаем аккаунты без клиентов
            if phone not in config['persistent_clients']:
                continue
                
            # Получаем клиент для этого аккаунта
            client = config['persistent_clients'].get(phone)
            if not client or not client.is_connected:
                continue
                
            # Получаем информацию об источнике
            source_chat = data.get('source_chat')
            if source_chat:
                # Преобразуем исходный идентификатор источника в актуальный chat_id
                if source_chat not in id_cache:
                    try:
                        joined, processed_source = await ensure_joined_chat(client, source_chat)
                        if joined:
                            id_cache[source_chat] = processed_source
                            # Добавляем аккаунт в список для этого источника
                            if processed_source not in source_accounts:
                                source_accounts[processed_source] = []
                            
                            source_accounts[processed_source].append(phone)
                            logging.info(f"Аккаунт {phone} добавлен к источнику {processed_source}")
                    except Exception as e:
                        logging.error(f"Ошибка при обработке источника {source_chat} для {phone}: {e}")
                else:
                    # Используем кешированный ID
                    processed_source = id_cache[source_chat]
                    if processed_source not in source_accounts:
                        source_accounts[processed_source] = []
                    source_accounts[processed_source].append(phone)
            
            # Получаем информацию о группах назначения
            dest_chats = data.get('dest_chats', '')
            if dest_chats:
                try:
                    # Парсим список групп назначения
                    dest_list = json.loads(dest_chats) if dest_chats.startswith('[') else dest_chats.split(',')
                    dest_list = [chat.strip('[]"\n') for chat in dest_list if chat.strip('[]"\n')]
                    
                    # Добавляем аккаунт в соответствующие группы назначения
                    for group in dest_list:
                        # Преобразуем идентификатор группы в актуальный chat_id
                        if group not in id_cache:
                            try:
                                joined, processed_group = await ensure_joined_chat(client, group)
                                if joined:
                                    id_cache[group] = processed_group
                                    
                                    # Добавляем группу в общий список и карту
                                    all_groups.add(processed_group)
                                    target_chats.add(processed_group)  # Добавляем в целевые чаты для отслеживания
                                    
                                    # Инициализируем историю для целевого чата, если её еще нет
                                    if processed_group not in config['target_chat_history']:
                                        config['target_chat_history'][processed_group] = []
                                        
                                    if processed_group not in account_groups:
                                        account_groups[processed_group] = []
                                    account_groups[processed_group].append(phone)
                                    
                                    logging.info(f"Аккаунт {phone} добавлен к группе {processed_group} (из {group})")
                                else:
                                    logging.warning(f"Не удалось присоединиться к {group} аккаунтом {phone}")
                            except Exception as e:
                                logging.error(f"Ошибка при обработке группы {group} для {phone}: {e}")
                        else:
                            # Используем кешированный ID
                            processed_group = id_cache[group]
                            all_groups.add(processed_group)
                            target_chats.add(processed_group)  # Добавляем в целевые чаты для отслеживания
                            
                            # Инициализируем историю для целевого чата, если её еще нет
                            if processed_group not in config['target_chat_history']:
                                config['target_chat_history'][processed_group] = []
                                
                            if processed_group not in account_groups:
                                account_groups[processed_group] = []
                            account_groups[processed_group].append(phone)
                            logging.info(f"Аккаунт {phone} добавлен к кешированной группе {processed_group}")
                except json.JSONDecodeError as e:
                    logging.error(f"Ошибка парсинга dest_chats для {phone}: {e}")
                    continue
        
        # Используем заданный режим копирования из глобальной конфигурации
        copy_mode = config.get('copying_mode', 2)  # По умолчанию чат-режим
        mode_str = "file" if copy_mode == 1 else "chat"
        logging.info(f"Режим копирования: {mode_str}")
        
        # Если режим файла - запускаем отдельный обработчик
        if copy_mode == 1:
            # Вызываем обработчик для файлового режима (без изменений)
            if config['copying_active']:
                await handle_file_one_message(active_accounts, sessions, account_groups, all_groups)
            copying_task.is_running = False
            return
        
        # РЕЖИМ ЧАТА - устанавливаем обработчики на источники сообщений
        # >>> НАЧАЛО: Настройка обработчиков сообщений для всех активных аккаунтов <<<
        
        # Определяем аккаунты, которые будут использоваться для каждого источника
        source_to_account = {}
        main_accounts = []
        
        logging.info(f"Настройка обработчиков для {len(config['persistent_clients'])} активных клиентов...")
        
        # Для каждого источника выбираем один аккаунт, который будет следить за сообщениями
        for source_id, accounts in source_accounts.items():
            if not source_id: 
                continue
                
            # Выбираем аккаунты с активным клиентом
            accounts_with_clients = [a for a in accounts if a in config['persistent_clients']]
            
            if not accounts_with_clients:
                logging.warning(f"Нет активных аккаунтов для источника {source_id}")
                continue
                
            # Выбираем один аккаунт для источника
            fetching_account = accounts_with_clients[0]
            source_to_account[source_id] = fetching_account
            main_accounts.append(fetching_account)
            
            logging.info(f"Для источника {source_id} выбран аккаунт {fetching_account}")
        
        logging.info(f"Назначено {len(main_accounts)} основных аккаунтов для чтения источников")
        
        # >>> Настраиваем обработчики для основных аккаунтов <<<
        # Создаем словарь для сопоставления аккаунтов и источников сообщений
        source_assignments = {}
        for processed_source, phone in source_to_account.items():
            source_assignments[phone] = processed_source
            logging.info(f"source_assignments: телефону {phone} назначен источник {processed_source}")
            
        logging.info(f"Итоговый source_assignments: {source_assignments}")
            
        # Для каждого активного клиента, загрузим начальную историю целевых чатов
        # Выбираем первый доступный клиент для загрузки истории
        if config['persistent_clients'] and target_chats:
            history_loading_client = next(iter(config['persistent_clients'].values()))
            
            for chat_id in target_chats:
                try:
                    chat_history_loaded = 0
                    logging.info(f"Загрузка истории для целевого чата {chat_id}...")
                    
                    async for message in history_loading_client.get_chat_history(chat_id, limit=100):
                        if chat_id not in config['target_chat_history']:
                            config['target_chat_history'][chat_id] = []
                            
                        # Получаем информацию об отправителе
                        sender_phone = None
                        if hasattr(message, 'from_user') and message.from_user:
                            # Если сообщение от пользователя, пытаемся найти соответствующий аккаунт
                            user_id = message.from_user.id
                            # Проходим по всем аккаунтам и ищем совпадение по user_id
                            for phone, data in sessions.items():
                                if data.get('user_id') == user_id:
                                    sender_phone = phone
                                    break
                        
                        # Добавляем сообщение в историю
                        config['target_chat_history'][chat_id].append({
                            'message_id': message.id, 
                            'text': message.text or message.caption or '',
                            'sender_phone': sender_phone,
                            'reply_to_message_id': message.reply_to_message.id if message.reply_to_message else None
                        })
                        chat_history_loaded += 1
                        
                    logging.info(f"Загружено {chat_history_loaded} сообщений в историю чата {chat_id}")
                except Exception as e:
                    logging.error(f"Ошибка загрузки истории для {chat_id}: {e}")
            
        for phone in main_accounts:
            if phone not in config['persistent_clients']:
                logging.warning(f"Пропуск настройки обработчика для {phone} - клиент не активен")
                continue
                
            client = config['persistent_clients'][phone]
            source_id = sessions[phone].get('source_chat')
            
            if not source_id:
                logging.warning(f"Пропуск настройки обработчика для {phone} - нет источника")
                continue
            
            # Обрабатываем ссылку на чат
            processed_source_id = process_chat_link(source_id)
            logging.info(f"Обнаружена ссылка на приватный чат: {processed_source_id}")
            
            try:
                # Пробуем присоединиться к чату источника
                source_joined, processed_source = await ensure_joined_chat(client, processed_source_id)
                if not source_joined:
                    logging.error(f"Аккаунт {phone} не смог присоединиться к источнику {processed_source_id}")
                    continue
                
                # Получаем сущность чата для использования в фильтре
                try:
                    source_entity = await client.get_chat(processed_source)
                    if not source_entity:
                        logging.error(f"Не удалось получить чат для источника: {processed_source}")
                        continue
                except Exception as e:
                    logging.error(f"Ошибка при получении чата для источника {processed_source}: {e}")
                    continue
                
                logging.info(f"Успешно получен чат для источника {processed_source}")
                
                # Устанавливаем обработчик для новых сообщений
                # Убираем filters.incoming, чтобы перехватывать все сообщения, включая свои
                
                # Сохраняем замыкание для телефона
                current_phone = phone
                
                @client.on_message()  # Убираем фильтр по чату для обработки всех сообщений
                async def message_handler(client, message):
                    logging.info(f"Получено сообщение: phone={current_phone}, chat_id={message.chat.id}, copying_active={config['copying_active']}")
                    
                    # Проверяем, является ли чат целевым, и обновляем его историю
                    if message.chat.id in target_chats:
                        chat_id = message.chat.id
                        if chat_id not in config['target_chat_history']:
                            config['target_chat_history'][chat_id] = []
                            
                        # Получаем информацию о отправителе
                        sender_phone = current_phone  # По умолчанию используем текущий телефон
                        
                        # Если сообщение от пользователя, пытаемся найти соответствующий аккаунт
                        if hasattr(message, 'from_user') and message.from_user:
                            user_id = message.from_user.id
                            # Проходим по всем аккаунтам и ищем совпадение по user_id
                            for acc_phone, data in sessions.items():
                                if data.get('user_id') == user_id:
                                    sender_phone = acc_phone
                                    break
                        
                        # Добавляем сообщение в историю
                        config['target_chat_history'][chat_id].append({
                            'message_id': message.id,
                            'text': message.text or message.caption or '',
                            'sender_phone': sender_phone,
                            'reply_to_message_id': message.reply_to_message.id if message.reply_to_message else None
                        })
                        logging.info(f"Обновлена история для {chat_id}: добавлено сообщение {message.id} от {sender_phone}")
                    
                    if not config['copying_active']:
                        logging.info("Копирование не активно, пропускаем сообщение")
                        return
                        
                    expected_source_id = source_assignments.get(phone)
                    logging.info(f"Сравниваем chat_id={message.chat.id} с expected_source_id={expected_source_id}")
                    if str(message.chat.id) != str(expected_source_id):
                        logging.info(f"chat_id {message.chat.id} не соответствует ожидаемому источнику {expected_source_id}, пропускаем")
                        return
                        
                    logging.info(f"Получено новое сообщение в источнике {message.chat.id} для {phone}")
                    
                    # Получаем текст сообщения или подпись для медиа-сообщений
                    msg_text = ""
                    media_type = None
                    media_content = None

                    if message.photo:
                        logging.info("Обнаружено фото")
                        try:
                            if isinstance(message.photo, list):
                                # Берем фото с наилучшим качеством (последнее в списке)
                                file_id = message.photo[-1].file_id
                                # Сохраняем все размеры для дальнейшего использования
                                file_sizes = [p.file_id for p in message.photo]
                                logging.info(f"Обнаружено {len(file_sizes)} размеров фото")
                            else:
                                file_id = message.photo.file_id
                                file_sizes = [file_id]
                            
                            caption = message.caption if message.caption else ""
                            logging.info(f"Фото с подписью: '{caption}'")
                            
                            media_type = 'photo'
                            media_content = {
                                'file_id': file_id,
                                'file_sizes': file_sizes,
                                'file_unique_id': message.photo[-1].file_unique_id if isinstance(message.photo, list) else message.photo.file_unique_id,
                                'caption': caption
                            }
                            msg_text = caption  # Сохраняем подпись в msg_text для совместимости
                            logging.info(f"Фото file_id: {file_id}, unique_id: {media_content['file_unique_id']}")
                        except Exception as e:
                            logging.error(f"Ошибка при обработке фото: {str(e)}")
                            media_type = 'text'  # В случае ошибки отправим как текст
                            media_content = None
                            msg_text = "[Ошибка обработки фото]"
                            if message.caption:
                                msg_text += "\n" + message.caption
                    elif message.video:
                        logging.info("Обнаружено видео")
                        try:
                            caption = message.caption if message.caption else ""
                            logging.info(f"Видео с подписью: '{caption}'")
                            
                            media_type = 'video'
                            media_content = {
                                'file_id': message.video.file_id, 
                                'file_unique_id': message.video.file_unique_id,
                                'duration': message.video.duration if hasattr(message.video, 'duration') else 0,
                                'caption': caption
                            }
                            msg_text = caption  # Сохраняем подпись в msg_text для совместимости
                            logging.info(f"Видео file_id: {message.video.file_id}, unique_id: {message.video.file_unique_id}")
                        except Exception as e:
                            logging.error(f"Ошибка при обработке видео: {str(e)}")
                            media_type = 'text'
                            media_content = None
                            msg_text = "[Ошибка обработки видео]"
                            if message.caption:
                                msg_text += "\n" + message.caption
                    elif message.animation:
                        logging.info("Обнаружен GIF/анимация")
                        try:
                            caption = message.caption if message.caption else ""
                            logging.info(f"Анимация с подписью: '{caption}'")
                            
                            media_type = 'animation'
                            media_content = {
                                'file_id': message.animation.file_id,
                                'file_unique_id': message.animation.file_unique_id,
                                'duration': message.animation.duration if hasattr(message.animation, 'duration') else 0,
                                'width': message.animation.width if hasattr(message.animation, 'width') else 0,
                                'height': message.animation.height if hasattr(message.animation, 'height') else 0,
                                'caption': caption
                            }
                            msg_text = caption  # Сохраняем подпись в msg_text для совместимости
                            logging.info(f"Анимация file_id: {message.animation.file_id}, unique_id: {message.animation.file_unique_id}")
                        except Exception as e:
                            logging.error(f"Ошибка при обработке анимации: {str(e)}")
                            media_type = 'text'
                            media_content = None
                            msg_text = "[Ошибка обработки анимации]"
                            if message.caption:
                                msg_text += "\n" + message.caption
                    elif message.sticker:
                        logging.info("Обнаружен стикер")
                        try:
                            media_type = 'sticker'
                            media_content = {
                                'file_id': message.sticker.file_id,
                                'file_unique_id': message.sticker.file_unique_id,
                                'emoji': message.sticker.emoji if hasattr(message.sticker, 'emoji') else "🔄"
                            }
                            msg_text = message.sticker.emoji if hasattr(message.sticker, 'emoji') else "..."
                            logging.info(f"Стикер file_id: {message.sticker.file_id}, unique_id: {message.sticker.file_unique_id}, emoji: {msg_text}")
                        except Exception as e:
                            logging.error(f"Ошибка при обработке стикера: {str(e)}")
                            media_type = 'text'
                            media_content = None
                            msg_text = "[Стикер]"
                    elif message.voice:
                        logging.info("Обнаружено голосовое сообщение")
                        try:
                            caption = message.caption if message.caption else ""
                            logging.info(f"Голосовое с подписью: '{caption}'")
                            
                            media_type = 'voice'
                            media_content = {
                                'file_id': message.voice.file_id,
                                'file_unique_id': message.voice.file_unique_id,
                                'duration': message.voice.duration if hasattr(message.voice, 'duration') else 0,
                                'caption': caption
                            }
                            msg_text = caption  # Сохраняем подпись в msg_text для совместимости
                            logging.info(f"Голосовое file_id: {message.voice.file_id}, unique_id: {message.voice.file_unique_id}")
                        except Exception as e:
                            logging.error(f"Ошибка при обработке голосового сообщения: {str(e)}")
                            media_type = 'text'
                            media_content = None
                            msg_text = "[Голосовое сообщение]"
                            if message.caption:
                                msg_text += "\n" + message.caption
                    elif message.text:
                        msg_text = message.text
                        media_type = None
                        media_content = None
                    else:
                        logging.warning("Неизвестный тип сообщения")
                        return
                    
                    # Логируем получение нового сообщения
                    logging.info(f"Получено новое сообщение в источнике {message.chat.id} для {phone}")
                    if msg_text:
                        shortened_text = (msg_text[:50] + "...") if len(msg_text) > 50 else msg_text
                        logging.info(f"Текстовое сообщение: {shortened_text}")
                    
                    # Проверяем наличие map для id сообщений
                    if 'message_id_map' not in config:
                        config['message_id_map'] = {}
                    
                    # Выводим текущие данные в message_id_map для отладки
                    message_id_map_keys = list(config['message_id_map'].keys())
                    logging.info(f"Текущие ключи в message_id_map: {message_id_map_keys[:10]} (показаны первые 10)")

                    # Обрабатываем данные отправителя
                    sender_name = ""
                    sender_id = None
                    is_outgoing = False
                    
                    if message.from_user:
                        sender_name = message.from_user.username or (f"{message.from_user.first_name} {message.from_user.last_name}").strip()
                        sender_id = message.from_user.id
                        is_outgoing = message.outgoing
                        
                    logging.info(f"Обработка сообщения от {sender_name}, ID: {sender_id}, исходящее: {is_outgoing}")
                    
                    # Получаем все группы назначения для этого источника на основе ключа телефона
                    target_groups = []
                    for account, data in active_accounts.items():
                        if account == phone:  # Находим данные только для текущего аккаунта
                            if "dest_chats" in data and data["dest_chats"]:
                                try:
                                    # Правильно обрабатываем данные в формате JSON и обычных строк
                                    if data["dest_chats"].startswith('['):
                                        dest_list = json.loads(data["dest_chats"])
                                    else:
                                        dest_list = data["dest_chats"].split(',')
                                        
                                    # Очищаем каждый элемент от кавычек и других символов
                                    for dest in dest_list:
                                        dest = dest.strip('[]"\' \n') # Исправлено: одинарный слеш
                                        if dest:  # Только непустые назначения
                                            target_groups.append(dest)
                                except json.JSONDecodeError as e:
                                    logging.error(f"Ошибка парсинга dest_chats для {phone}: {e}")
                                    # Пробуем использовать как строку в случае ошибки
                                    dest = data["dest_chats"].strip()
                                    if dest:
                                        target_groups.append(dest)
                            break
                    
                    # Если нет групп назначения, ничего не делаем
                    if not target_groups:
                        logging.warning(f"Для аккаунта {phone} не найдены группы назначения.")
                        return
                    
                    logging.info(f"Обработка сообщения для {len(target_groups)} групп назначения: {target_groups}")
                    
                    # Для каждой группы назначения выбираем и используем аккаунт для отправки
                    for group in target_groups:
                        # Очищаем URL группы от любых форматирований
                        clean_group = group.strip('[]"\' \n') # Исправлено: одинарный слеш
                        logging.info(f"Обработка группы назначения: {clean_group}")
                        
                        # Общий метод выбора аккаунта для всех типов сообщений
                        chosen_phone = None
                        group_key = f"{message.chat.id}:{message.id}:{clean_group}"
                        
                        try:
                            # Правило 1: сначала проверяем наличие предварительно выбранного аккаунта
                            if group_key in config['message_account_map']:
                                # Используем уже выбранный аккаунт для этого сообщения
                                chosen_phone = config['message_account_map'][group_key]
                                logging.info(f"Используем предварительно выбранный аккаунт {chosen_phone} для сообщения в {clean_group}")
                            else:
                                # Пробуем найти группу сначала как есть, затем ищем в обработанных ID
                                accounts_for_group = []
                                
                                # Проверяем, есть ли группа в account_groups напрямую
                                if clean_group in account_groups:
                                    accounts_for_group = account_groups[clean_group]
                                    logging.info(f"Найдена группа: {clean_group} с {len(accounts_for_group)} аккаунтами")
                                else:
                                    # Проверяем, есть ли эта группа в кешированных ID
                                    for cached_group, processed_id in id_cache.items():
                                        if str(processed_id) == str(clean_group) or cached_group == clean_group:
                                            if processed_id in account_groups:
                                                accounts_for_group = account_groups[processed_id]
                                                logging.info(f"Найдена кешированная группа: {processed_id} с {len(accounts_for_group)} аккаунтами")
                                                break
                        except Exception as e:
                            logging.error(f"Ошибка при обработке группы {clean_group}: {e}")
                            accounts_for_group = []
                        
                        if not accounts_for_group:
                            logging.warning(f"Нет доступных аккаунтов для группы {clean_group}")
                            continue
                        
                        # Проверяем, не отправил ли отправитель сообщение сам себе
                        safe_accounts = accounts_for_group.copy()
                        
                        try:
                            # По умолчанию считаем отправителя внешним
                            is_external_sender = True
                                        
                            # Если отправитель - один из наших аккаунтов
                            if sender_id and is_external_sender:
                                for acc_phone, acc_client in config['persistent_clients'].items():
                                    if acc_client and hasattr(acc_client, 'get_me'):
                                        try:
                                            me = await acc_client.get_me()
                                            if me and me.id == sender_id:
                                                is_external_sender = False
                                                chosen_phone = acc_phone
                                                # Если это наш аккаунт, используем его же для пересылки
                                                logging.info(f"Сообщение от нашего аккаунта {acc_phone}, используем его же")
                                                break
                                        except Exception as e:
                                            logging.warning(f"Не удалось получить информацию о пользователе для {acc_phone}: {str(e)}")
                            
                            # Если отправитель внешний, используем принудительное чередование
                            if is_external_sender:
                                # Определим тип контента для более точной ротации
                                content_type = "text"
                                if message:  # Используем переменную message вместо source_message
                                    if hasattr(message, 'sticker') and message.sticker:
                                        content_type = "sticker"
                                    elif hasattr(message, 'animation') and message.animation:
                                        content_type = "animation"
                                    elif hasattr(message, 'photo') and message.photo:
                                        content_type = "photo"
                                    elif hasattr(message, 'voice') and message.voice:
                                        content_type = "voice"
                                    elif hasattr(message, 'video') and message.video:
                                        content_type = "video"
                                # Если chosen_phone еще не установлен, выбираем его с ротацией
                                if not chosen_phone:
                                    last_used_account = config['last_used_account_per_content'].get(content_type)
                                    chosen_phone = force_account_rotation(accounts_for_group, last_used_account)
                                    if chosen_phone:
                                        config['last_used_account_per_content'][content_type] = chosen_phone
                                        logging.info(f"Для группы {clean_group} принудительно выбран аккаунт {chosen_phone} вместо {last_used_account} (тип контента: {content_type})")
                                    else:
                                        logging.warning(f"Не удалось выбрать аккаунт для группы {clean_group} (тип контента: {content_type}).")
                                        continue # Пропускаем группу, если не удалось выбрать аккаунт
                        except Exception as e:
                            logging.error(f"Ошибка при выборе аккаунта для группы {clean_group}: {e}")
                            continue # Пропускаем текущую группу
                        
                        # Сохраняем выбранный аккаунт для этого сообщения и группы
                        config['message_account_map'][group_key] = chosen_phone
                        logging.info(f"Выбран аккаунт {chosen_phone} для сообщения в {clean_group}")
                        
                        # Вызываем функцию send_chat_message_for_account
                        client_to_pass = config['persistent_clients'].get(chosen_phone)
                        if not client_to_pass or not client_to_pass.is_connected:
                            logging.error(f"Клиент для аккаунта {chosen_phone} не найден или не подключен. Пропускаем отправку.")
                            continue

                        # Определяем, является ли это сообщением-ответом и находим grouped_id
                        reply_to_message_id = None
                        current_grouped_id = None

                        if message.reply_to_message:
                            logging.info(f"Сообщение является ответом на сообщение с ID: {message.reply_to_message.id}")
                            # Пытаемся найти ID оригинального сообщения в целевом чате
                            reply_target_id, reply_sender_phone = await process_reply_buffer(
                                client, message, message.chat.id, clean_group, config['message_id_map'], chosen_phone
                            )
                            if reply_target_id:
                                reply_to_message_id = reply_target_id
                                # Если найдено соответствие, пытаемся получить grouped_id для этой ветки
                                source_key_for_grouped_id = f"{message.chat.id}:{message.reply_to_message.id}"
                                if source_key_for_grouped_id in config['grouped_id_map']:
                                    current_grouped_id = config['grouped_id_map'][source_key_for_grouped_id]
                                    logging.info(f"Найден grouped_id {current_grouped_id} для ответа на сообщение {source_key_for_grouped_id}")
                                else:
                                    # Если grouped_id не найден для этой ветки, генерируем новый
                                    current_grouped_id = int(str(uuid.uuid4().int)[:9])
                                    logging.warning(f"Не найден grouped_id для ответа на сообщение {source_key_for_grouped_id}, сгенерирован новый: {current_grouped_id}")
                            else:
                                logging.warning(f"Предварительная проверка: не найдено соответствие для ответа на сообщение {message.reply_to_message.id}")
                                # Если не найдено соответствие, но это ответ, генерируем новый grouped_id
                                current_grouped_id = int(str(uuid.uuid4().int)[:9]) # Генерируем новый, если нет
                                logging.info(f"Сгенерирован новый grouped_id для сообщения-ответа без прямого соответствия: {current_grouped_id}")
                        
                        # Если сообщение не является ответом и grouped_id еще не установлен, генерируем новый
                        if not current_grouped_id and not message.reply_to_message:
                            current_grouped_id = int(str(uuid.uuid4().int)[:9]) # Генерируем новый, если нет
                            logging.info(f"Сгенерирован новый grouped_id для нового сообщения: {current_grouped_id}")

                        result, sent_message = await send_chat_message_for_account(
                            chosen_phone,
                            client_to_pass,
                            msg_text,
                            clean_group,
                            message.id,
                            source_message=message,
                            message_id_map=config['message_id_map'],
                            media_type=media_type,
                            media_content=media_content,
                            active_accounts=active_accounts,
                            try_buffer=True,
                            reply_to_id=reply_to_message_id,  # Передаем найденный ID для ответа
                            grouped_id=current_grouped_id # Передаем найденный или сгенерированный grouped_id
                        )
                        if result:
                            logging.info(f"✅ Сообщение отправлено в {clean_group} аккаунтом {chosen_phone}")
                            # Регистрируем соответствие ID после успешной отправки
                            source_key = f"{message.chat.id}:{message.id}"
                            # Для target_chat_id: если sent_message строка, используем словарь chat_id_cache и clean_group, иначе - sent_message.chat.id
                            target_chat_id = None
                            if isinstance(sent_message, str):
                                # Используем кешированный chat_id, если доступен
                                target_chat_id = config['chat_id_cache'].get(clean_group)
                            else:
                                # Иначе используем chat_id из объекта сообщения
                                target_chat_id = sent_message.chat.id if hasattr(sent_message, 'chat') else None
                                
                            if target_chat_id:
                                message_id_map = register_message_id(sent_message, source_key, target_chat_id, 
                                    f"[{chosen_phone}]", chosen_phone, config['message_id_map'], current_grouped_id)
                            else:
                                logging.warning(f"Не удалось определить target_chat_id для сообщения")
                                
                            # Добавляем сообщение в историю целевого чата
                            if not isinstance(sent_message, str) and hasattr(sent_message, 'chat') and hasattr(sent_message, 'id'):
                                chat_id_for_history = sent_message.chat.id
                                
                                if chat_id_for_history not in config['target_chat_history']:
                                    config['target_chat_history'][chat_id_for_history] = []
                                    
                                config['target_chat_history'][chat_id_for_history].append({
                                    'message_id': sent_message.id,
                                    'text': getattr(sent_message, 'text', '') or getattr(sent_message, 'caption', '') or '',
                                    'sender_phone': chosen_phone,
                                    'reply_to_message_id': reply_to_message_id
                                })
                            
                        else:  # Этот блок должен соответствовать if success:
                            logging.warning(f"❌ Не удалось отправить сообщение в {clean_group} аккаунтом {chosen_phone}: {result}")

                message_handlers.append(message_handler)
                if phone not in client_handlers:
                    client_handlers[phone] = []
                client_handlers[phone].append(message_handler)
                logging.info(f"Установлен обработчик для {phone}")
                
            except Exception as e:
                logging.error(f"Ошибка при настройке обработчика для {phone}: {e}", exc_info=True)
        
        # Инициализируем message_id_map в конфигурации, если его там нет
        if 'message_id_map' not in config:
            config['message_id_map'] = {}
        
        # Инициализируем grouped_id_map в конфигурации, если его там нет
        if 'grouped_id_map' not in config: # Исправлено: было 'not not in'
            config['grouped_id_map'] = {}

        # Инициализируем last_used_account_per_content в конфигурации, если его там нет
        if 'last_used_account_per_content' not in config:
            config['last_used_account_per_content'] = {}

        # Ждем остановки копирования
        logging.info(f"Все обработчики ({len(message_handlers)}) настроены, ожидаем сообщений...")
        config['copying_active'] = True
        while config['copying_active']:
            await asyncio.sleep(1)
            
    except Exception as e:
        logging.error(f"Ошибка в задаче копирования: {e}", exc_info=True)
        config['copying_active'] = False
        await send_log_to_admins(f"⚠️ Копирование остановлено: {e}")
    finally:
        copying_task.is_running = False  # Сбрасываем флаг

async def stop_persistent_clients():
    """Останавливает все клиенты в пуле persistent_clients."""
    logging.info(f"Начало остановки {len(config['persistent_clients'])} персистентных клиентов...")
    clients_to_stop = list(config['persistent_clients'].items()) # Копируем элементы для безопасной итерации
    config['persistent_clients'].clear() # Очищаем пул сразу
    
    for phone, client in clients_to_stop:
        try:
            if client and client.is_connected:
                await client.stop()
                logging.info(f"Персистентный клиент {phone} остановлен.")
        except Exception as e:
            logging.error(f"Ошибка при остановке персистентного клиента {phone}: {e}")
    logging.info("Завершение остановки персистентных клиентов.")

@dp.callback_query(lambda c: c.data == "stop_sending")
async def stop_sending(callback: CallbackQuery):
    """Остановка копирования"""
    try:
        config['copying_active'] = False
        config['copying_mode'] = None # Сбрасываем режим
        
        # Важно! Сбрасываем флаг запуска задачи
        if hasattr(copying_task, 'is_running'):
            copying_task.is_running = False
            
        await stop_persistent_clients() # Останавливаем персистентные клиенты
        await callback.answer("🛑 Копирование остановлено", show_alert=True)
        
        # Обновляем сообщение только если оно изменилось
        new_text = status_text()
        new_markup = main_menu_kb()
        try:
            await callback.message.edit_text(new_text, reply_markup=new_markup)
        except Exception as edit_error:
            # Если сообщение не изменилось, просто игнорируем ошибку
            if "message is not modified" not in str(edit_error).lower():
                raise edit_error
    except Exception as e:
        logging.error(f"Ошибка в stop_sending: {e}")
        await callback.answer("❌ Произошла ошибка", show_alert=True)

@dp.callback_query(lambda c: c.data == "proxy_settings")
async def proxy_settings_handler(callback: CallbackQuery, state: FSMContext):
    """Обработчик для кнопки управления прокси"""
    try:
        await callback.answer()
        proxies = db.get_all_proxies()
        
        kb = InlineKeyboardMarkup(inline_keyboard=[])
        
        # Добавляем список прокси
        for p in proxies:
            kb.inline_keyboard.append([
                InlineKeyboardButton(
                    text=f"{p.host}:{p.port} ({p.scheme})",
                    callback_data=f"proxy_info_{p.id}"
                ),
                InlineKeyboardButton(
                    text="❌ Удалить",
                    callback_data=f"proxy_delete_{p.id}"
                )
            ])
        
        # Добавляем кнопку "Добавить прокси"
        kb.inline_keyboard.append([
            InlineKeyboardButton(text="➕ Добавить прокси", callback_data="add_proxy")
        ])
        
        # Кнопка возврата в меню
        kb.inline_keyboard.append([
            InlineKeyboardButton(text="⬅️ Назад", callback_data="back_to_menu")
        ])
        
        await callback.message.edit_text(
            f"📊 Управление прокси\n\n"
            f"Всего прокси: {len(proxies)}\n\n"
            f"Выберите прокси для управления или добавьте новый:",
            reply_markup=kb
        )
    except Exception as e:
        logging.error(f"Ошибка в proxy_settings_handler: {e}")
        await callback.answer("❌ Произошла ошибка", show_alert=True)

@dp.callback_query(lambda c: c.data.startswith("proxy_delete_"))
async def delete_proxy_handler(callback: CallbackQuery):
    """Обработчик для удаления прокси"""
    try:
        proxy_id = int(callback.data.split("_")[2])
        
        # Получаем информацию о прокси перед удалением для сообщения
        proxy = db.get_proxy(proxy_id)
        if not proxy:
            await callback.answer("❌ Прокси не найден", show_alert=True)
            return
            
        # Удаляем прокси
        if db.delete_proxy(proxy_id):
            await callback.answer(f"✅ Прокси {proxy.host}:{proxy.port} удален", show_alert=True)
        else:
            await callback.answer("❌ Ошибка при удалении прокси", show_alert=True)
            
        # Обновляем список прокси
        callback.data = "proxy_settings"
        await proxy_settings_handler(callback, None)
    except Exception as e:
        logging.error(f"Ошибка в delete_proxy_handler: {e}")
        await callback.answer("❌ Произошла ошибка", show_alert=True)

@dp.callback_query(lambda c: c.data == "add_proxy")
async def add_proxy_handler(callback: CallbackQuery, state: FSMContext):
    """Обработчик для добавления прокси"""
    try:
        await state.set_state(ProxyStates.ADD_PROXY)
        
        await callback.message.edit_text(
            "📤 Отправьте ссылку на прокси в формате:\n\n"
            "https://t.me/proxy?server=88.99.149.121&port=8423&secret=DDBighLLvXrFGRMCBVJdFQRueWVrdGFuZXQuY29t\n\n"
            "Или введите данные вручную в формате:\n"
            "scheme://username:password@host:port\n"
            "Например: http://user:pass@1.2.3.4:8080",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="❌ Отмена", callback_data="proxy_settings")]
            ])
        )
        await callback.answer()
    except Exception as e:
        logging.error(f"Ошибка в add_proxy_handler: {e}")
        await callback.answer("❌ Произошла ошибка", show_alert=True)

@dp.message(ProxyStates.ADD_PROXY)
async def process_proxy(message: Message, state: FSMContext):
    """Обработчик для получения и обработки прокси от пользователя"""
    try:
        text = message.text.strip()
        
        # Пробуем обработать как ссылку Telegram
        if text.startswith("https://t.me/proxy"):
            proxy_data = parse_telegram_proxy_url(text)
            if not proxy_data:
                await message.answer("❌ Не удалось распознать ссылку на прокси. Проверьте формат и попробуйте снова.")
                return
            
            # Определяем тип прокси
            scheme = proxy_data.get("scheme", "socks5")
            host = proxy_data.get("server")
            port = int(proxy_data.get("port", 1080))
            username = None
            password = None
            secret = proxy_data.get("secret")
            
            # В случае с MTProto secret используется как пароль
            if secret:
                password = secret
                
        # Пробуем обработать как строку прокси
        else:
            try:
                # Парсим строку формата scheme://username:password@host:port
                parts = text.split("://")
                if len(parts) != 2:
                    await message.answer("❌ Неверный формат прокси. Используйте формат scheme://username:password@host:port")
                    return
                    
                scheme = parts[0].lower()
                
                # Разделяем остальную часть
                auth_host_port = parts[1].split("@")
                
                if len(auth_host_port) == 2:
                    # Есть аутентификация
                    auth = auth_host_port[0].split(":")
                    if len(auth) == 2:
                        username, password = auth
                    else:
                        username = auth[0]
                        password = None
                        
                    host_port = auth_host_port[1].split(":")
                else:
                    # Нет аутентификации
                    username = None
                    password = None
                    host_port = auth_host_port[0].split(":")
                    
                if len(host_port) != 2:
                    await message.answer("❌ Неверный формат хоста и порта. Используйте формат host:port")
                    return
                    
                host = host_port[0]
                port = int(host_port[1])
            except Exception as e:
                await message.answer(f"❌ Ошибка при парсинге прокси: {e}")
                return
                
        # Создаем и добавляем прокси
        proxy = Proxy(id=0, host=host, port=port, scheme=scheme, 
                     username=username, password=password)
                     
        if db.add_proxy(proxy):
            await message.answer("✅ Прокси успешно добавлен!")
        else:
            await message.answer("❌ Ошибка при добавлении прокси в базу данных")
            
        # Сбрасываем состояние и показываем список прокси
        await state.clear()
        
        # Отправляем новое сообщение со списком прокси
        await show_proxy_list(message)
        
    except Exception as e:
        logging.error(f"Ошибка в process_proxy: {e}")
        await message.answer(f"❌ Произошла ошибка: {e}")
        await state.clear()

def parse_telegram_proxy_url(url: str) -> Dict[str, str]:
    """Парсит ссылку на прокси Telegram и возвращает словарь с параметрами"""
    try:
        # Разделяем URL на части и извлекаем параметры
        from urllib.parse import urlparse, parse_qs
        
        parsed_url = urlparse(url)
        query_params = parse_qs(parsed_url.query)
        
        # Преобразуем параметры из списков в строки
        params = {k: v[0] for k, v in query_params.items()}
        
        # Проверяем наличие обязательных параметров
        if 'server' not in params or 'port' not in params:
            return None
        
        # Определяем тип прокси - MTProto или SOCKS5
        # Если есть secret параметр - это MTProto прокси
        if 'secret' in params:
            params['scheme'] = 'mtproto'
        else:
            params['scheme'] = 'socks5'
            
        return params
    except Exception as e:
        logging.error(f"Ошибка при парсинге ссылки прокси: {e}")
        return None

async def show_proxy_list(message):
    """Показывает список прокси"""
    proxies = db.get_all_proxies()
    
    kb = InlineKeyboardMarkup(inline_keyboard=[])
    
    # Добавляем список прокси
    for p in proxies:
        kb.inline_keyboard.append([
            InlineKeyboardButton(
                text=f"{p.host}:{p.port} ({p.scheme})",
                callback_data=f"proxy_info_{p.id}"
            ),
            InlineKeyboardButton(
                text="❌ Удалить",
                callback_data=f"proxy_delete_{p.id}"
            )
        ])
    
    # Добавляем кнопку "Добавить прокси"
    kb.inline_keyboard.append([
        InlineKeyboardButton(text="➕ Добавить прокси", callback_data="add_proxy")
    ])
    
    # Кнопка возврата в меню
    kb.inline_keyboard.append([
        InlineKeyboardButton(text="⬅️ Назад", callback_data="back_to_menu")
    ])
    
    await message.answer(
        f"📊 Управление прокси\n\n"
        f"Всего прокси: {len(proxies)}\n\n"
        f"Выберите прокси для управления или добавьте новый:",
        reply_markup=kb
    )

@dp.callback_query(lambda c: c.data == "back_to_menu")
async def back_to_menu(callback: CallbackQuery):
    try:
        await callback.message.edit_text(status_text(), reply_markup=main_menu_kb())
        await callback.answer()
    except Exception as e:
        logging.error(f"Ошибка в back_to_menu: {e}")
        await callback.answer("❌ Произошла ошибка", show_alert=True)

@dp.callback_query(lambda c: c.data == "proxy_settings")
async def proxy_settings_handler(callback: CallbackQuery, state: FSMContext):
    try:
        await callback.answer()
        proxies = db.get_all_proxies()
        
        kb = InlineKeyboardMarkup(inline_keyboard=[])
        
        # Добавляем список прокси
        for p in proxies:
            kb.inline_keyboard.append([
                InlineKeyboardButton(
                    text=f"{p.host}:{p.port} ({p.scheme})",
                    callback_data=f"proxy_info_{p.id}"
                ),
                InlineKeyboardButton(
                    text="❌ Удалить",
                    callback_data=f"proxy_delete_{p.id}"
                )
            ])
        
        # Добавляем кнопку "Добавить прокси"
        kb.inline_keyboard.append([
            InlineKeyboardButton(text="➕ Добавить прокси", callback_data="add_proxy")
        ])
        
        # Кнопка возврата в меню
        kb.inline_keyboard.append([
            InlineKeyboardButton(text="⬅️ Назад", callback_data="back_to_menu")
        ])
        
        await callback.message.edit_text(
            f"📊 Управление прокси\n\n"
            f"Всего прокси: {len(proxies)}\n\n"
            f"Выберите прокси для управления или добавьте новый:",
            reply_markup=kb
        )
    except Exception as e:
        logging.error(f"Ошибка в proxy_settings_handler: {e}")
        await callback.answer("❌ Произошла ошибка", show_alert=True)

@dp.callback_query(lambda c: c.data.startswith("proxy_delete_"))
async def delete_proxy_handler(callback: CallbackQuery):
    try:
        proxy_id = int(callback.data.split("_")[2])
        
        # Получаем информацию о прокси перед удалением для сообщения
        proxy = db.get_proxy(proxy_id)
        if not proxy:
            await callback.answer("❌ Прокси не найден", show_alert=True)
            return
            
        # Удаляем прокси
        if db.delete_proxy(proxy_id):
            await callback.answer(f"✅ Прокси {proxy.host}:{proxy.port} удален", show_alert=True)
        else:
            await callback.answer("❌ Ошибка при удалении прокси", show_alert=True)
            
        # Обновляем список прокси
        callback.data = "proxy_settings"
        await proxy_settings_handler(callback, None)
    except Exception as e:
        logging.error(f"Ошибка в delete_proxy_handler: {e}")
        await callback.answer("❌ Произошла ошибка", show_alert=True)

@dp.callback_query(lambda c: c.data == "add_proxy")
async def add_proxy_handler(callback: CallbackQuery, state: FSMContext):
    """Обработчик для добавления прокси"""
    try:
        await state.set_state(ProxyStates.ADD_PROXY)
        
        await callback.message.edit_text(
            "📤 Отправьте ссылку на прокси в формате:\n\n"
            "https://t.me/proxy?server=88.99.149.121&port=8423&secret=DDBighLLvXrFGRMCBVJdFQRueWVrdGFuZXQuY29t\n\n"
            "Или введите данные вручную в формате:\n"
            "scheme://username:password@host:port\n"
            "Например: http://user:pass@1.2.3.4:8080",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="❌ Отмена", callback_data="proxy_settings")]
            ])
        )
        await callback.answer()
    except Exception as e:
        logging.error(f"Ошибка в add_proxy_handler: {e}")
        await callback.answer("❌ Произошла ошибка", show_alert=True)

@dp.callback_query(lambda c: c.data == "help")
async def show_help(callback: CallbackQuery):
    try:
        await callback.message.answer(
            "📌 Помощь по боту:\n\n"
            "1. Добавьте аккаунты через меню '📱 Добавить аккаунт'\n"
            "2. Для каждого аккаунта укажите источник и назначение\n"
            "3. Настройте прокси при необходимости\n"
            "4. Загрузите файлы с сообщениями (если нужно)\n"
            "5. Включите режим копирования для нужных аккаунтов\n"
            "6. Запустите процесс копирования\n\n"
            "Бот поддерживает:\n"
            "- Копирование новых сообщений из чатов/каналов\n"
            "- Отправку сообщений из текстовых файлов\n"
            "- Работу через прокси\n"
            "- Автоматическую проверку работоспособности аккаунтов\n\n"
            "Техподдержка: @imfocky2000",
            reply_markup=main_menu_kb()
        )
        await callback.answer()
    except Exception as e:
        logging.error(f"Ошибка в show_help: {e}")
        await callback.answer("❌ Произошла ошибка", show_alert=True)

@dp.callback_query(lambda c: c.data == "manage_groups")
async def manage_groups(callback: CallbackQuery):
    """Обработчик для управления группами"""
    try:
        # Получаем список групп из базы данных
        groups = db.get_all_managed_groups()
        
        # Формируем клавиатуру с кнопками для каждой группы
        keyboard = []
        for group in groups:
            title = group['title']
            group_id = group['group_id']
            # Ограничиваем длину названия группы
            if len(title) > 30:
                title = title[:27] + "..."
            keyboard.append([InlineKeyboardButton(text=f"👥 {title}", callback_data=f"group_info_{group_id}")])
        
        # Добавляем кнопки "Добавить группу" и "Назад"
        keyboard.append([InlineKeyboardButton(text="➕ Добавить группу", callback_data="add_group")])
        keyboard.append([InlineKeyboardButton(text="⬅️ Назад", callback_data="back_to_menu")])
        
        text = "👥 Управление группами\n\n"
        if groups:
            text += f"Всего групп: {len(groups)}\n"
            text += "Выберите группу для управления или добавьте новую."
        else:
            text += "У вас пока нет добавленных групп.\nНажмите 'Добавить группу' для добавления новой группы."
        
        await callback.message.edit_text(
            text,
            reply_markup=InlineKeyboardMarkup(inline_keyboard=keyboard)
        )
        await callback.answer()
    except Exception as e:
        logging.error(f"Ошибка в manage_groups: {e}")
        await callback.answer("❌ Произошла ошибка", show_alert=True)

@dp.callback_query(lambda c: c.data == "add_group")
async def add_group_start(callback: CallbackQuery, state: FSMContext):
    """Начинаем процесс добавления группы"""
    try:
        await state.set_state(GroupManagementStates.WAITING_GROUP_ID)
        await callback.message.edit_text(
            "Введите ID группы или ссылку на группу, которую хотите добавить:\n\n"
            "Примеры:\n"
            "-100123456789\n"
            "@group_name\n"
            "t.me/group_name",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="❌ Отмена", callback_data="manage_groups")]
            ])
        )
        await callback.answer()
    except Exception as e:
        logging.error(f"Ошибка в add_group_start: {e}")
        await callback.answer("❌ Произошла ошибка", show_alert=True)

@dp.message(GroupManagementStates.WAITING_GROUP_ID)
async def process_group_id(message: Message, state: FSMContext):
    """Обрабатываем введенный ID группы"""
    try:
        await state.clear()
        group_id = message.text.strip()
        
        # Обрабатываем ссылку или ID
        if group_id.startswith("@"):
            group_id = group_id[1:]  # Убираем @ из начала
        elif group_id.startswith("t.me/"):
            group_id = group_id[5:]  # Убираем t.me/ из начала
        elif group_id.startswith("https://t.me/"):
            group_id = group_id[13:]  # Убираем https://t.me/ из начала
        
        # Проверяем, состоит ли бот в этой группе
        is_member, title = await check_bot_in_group(group_id)
        
        if not is_member:
            await message.answer(
                "❌ Бот не является участником указанной группы или группа не существует.\n"
                "Добавьте бота в группу и попробуйте снова.",
                reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                    [InlineKeyboardButton(text="↩️ К управлению группами", callback_data="manage_groups")]
                ])
            )
            return
        
        # Добавляем группу в базу данных
        success = db.add_managed_group(group_id, title)
        
        if success:
            await message.answer(
                f"✅ Группа \"{title}\" успешно добавлена!\n\n"
                "По умолчанию группа закрыта для сообщений. Вы можете изменить это в настройках группы.",
                reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                    [InlineKeyboardButton(text="⚙️ Настройки группы", callback_data=f"group_info_{group_id}")],
                    [InlineKeyboardButton(text="↩️ К управлению группами", callback_data="manage_groups")]
                ])
            )
        else:
            await message.answer(
                "❌ Ошибка при добавлении группы. Пожалуйста, попробуйте еще раз.",
                reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                    [InlineKeyboardButton(text="↩️ К управлению группами", callback_data="manage_groups")]
                ])
            )
    except Exception as e:
        logging.error(f"Ошибка в process_group_id: {e}")
        await message.answer(
            "❌ Произошла ошибка при обработке ID группы.",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="↩️ К управлению группами", callback_data="manage_groups")]
            ])
        )

@dp.callback_query(lambda c: c.data.startswith("group_info_"))
async def group_info(callback: CallbackQuery):
    """Показываем информацию о группе"""
    try:
        group_id = callback.data.split("group_info_")[1]
        group = db.get_managed_group(group_id)
        
        if not group:
            await callback.answer("Группа не найдена", show_alert=True)
            await manage_groups(callback)
            return
        
        # Проверяем актуальный статус группы
        is_member, _ = await check_bot_in_group(group_id)
        if not is_member:
            status_text = "⚠️ Бот больше не состоит в этой группе!"
        else:
            status_text = "✅ Бот состоит в группе"
        
        # Получаем тип группы: 0 = закрыта, 1 = открыта
        group_type = group['group_type']
        type_text = "🔒 Закрыта" if group_type == 0 else "🔓 Открыта"
        action_text = "Открыть" if group_type == 0 else "Закрыть"
        action_data = f"toggle_group_{group_id}_{1 if group_type == 0 else 0}"
        
        await callback.message.edit_text(
            f"👥 Информация о группе\n\n"
            f"Название: {group['title']}\n"
            f"ID: {group['group_id']}\n"
            f"Тип: {type_text}\n"
            f"Добавлена: {group['added_at']}\n"
            f"Статус: {status_text}",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text=f"{action_text} группу", callback_data=action_data)],
                [InlineKeyboardButton(text="❌ Удалить группу", callback_data=f"delete_group_{group_id}")],
                [InlineKeyboardButton(text="↩️ К списку групп", callback_data="manage_groups")]
            ])
        )
        await callback.answer()
    except Exception as e:
        logging.error(f"Ошибка в group_info: {e}")
        await callback.answer("❌ Произошла ошибка", show_alert=True)

@dp.callback_query(lambda c: c.data.startswith("toggle_group_"))
async def toggle_group(callback: CallbackQuery):
    """Изменяем тип группы (открыта/закрыта) и фактические разрешения в группе"""
    try:
        # Правильно разбираем данные callback
        # Формат: toggle_group_ИД-ГРУППЫ_НОВЫЙ-ТИП
        # где НОВЫЙ-ТИП: 0=закрыта, 1=открыта
        parts = callback.data.split('_')
        group_id = "group"  # По умолчанию используем "group"
        new_type = 1  # По умолчанию открываем группу
        
        if len(parts) >= 3:
            # Правильно определяем, какая часть - id группы, а какая - тип
            if parts[1] == "group" or (parts[1].startswith("-") and parts[1][1:].isdigit()):
                group_id = parts[1]  # Это ID группы
                if len(parts) >= 4 and parts[3].isdigit():
                    new_type = int(parts[3])
                elif len(parts) >= 3 and parts[2].isdigit():
                    new_type = int(parts[2])
            elif parts[2].isdigit():
                # Для совместимости со старым форматом
                group_id = parts[1]
                new_type = int(parts[2]) 
                
                # Проверяем тип на корректность (должен быть 0 или 1)
        if new_type != 0 and new_type != 1:
            logging.error(f"Недопустимый тип группы в запросе: {new_type}, исправляем")
            # Инвертируем текущий тип группы
            group = db.get_managed_group(group_id)
            if group and 'group_type' in group:
                current_type = group['group_type']
                new_type = 1 if current_type == 0 else 0
    
                
        logging.info(f"Вызов toggle_group с параметрами: group_id={group_id}, new_type={new_type}")
        
        try:
            # Определяем уровень разрешений на основе типа группы
            can_send_messages = new_type == 1  # Если new_type = 1 (открыта), то разрешаем отправку
            
            # Создаем словарь с разрешениями для aiogram
            permissions = {
                "can_send_messages": can_send_messages,
                "can_send_media_messages": can_send_messages,
                "can_add_web_page_previews": can_send_messages,
                "can_send_polls": can_send_messages,
                "can_invite_users": True,
                "can_pin_messages": False,
                "can_change_info": False,
                "can_send_other_messages": can_send_messages,  # Разрешение на стикеры, GIF и т.д.
            }
            
            # Преобразуем идентификатор группы, если нужно
            if group_id.lower() == 'group':
                # В этом случае нам нужно получить фактический ID группы из базы
                all_groups = db.get_all_managed_groups()
                if all_groups:
                    actual_group = all_groups[0]  # Предполагаем, что это первая группа в списке
                    group_id = actual_group["group_id"]
                    # НЕ изменяем new_type! Сохраняем оригинальное значение
                    logging.info(f"Получили фактический ID группы из БД: {group_id}")
                else:
                    raise ValueError("Не удалось найти группу в базе данных")
            
            # Telegram API требует числовой идентификатор, часто начинающийся с -100 для супергрупп
            if group_id.startswith('-100'):
                numeric_group_id = int(group_id)
            # Если это просто число, пытаемся добавить префикс -100
            elif group_id.lstrip('-').isdigit():
                # Убедимся, что не добавляем -100 к идентификатору, который уже начинается с -
                if group_id.startswith('-'):
                    numeric_group_id = int(group_id)
                else:
                    numeric_group_id = int(f"-100{group_id}")
            else:
                # Если это не числовой ID, просто используем как есть
                numeric_group_id = group_id
            
            logging.info(f"Попытка изменить разрешения в группе {group_id}, числовой ID: {numeric_group_id}")
            logging.info(f"Устанавливаем статус группы: {'открыта' if can_send_messages else 'закрыта'} (new_type={new_type})")
            
            # Изменяем разрешения в группе
            await bot.set_chat_permissions(
                chat_id=numeric_group_id,
                permissions=permissions
            )
            logging.info(f"Разрешения в группе {group_id} успешно изменены на {permissions}")
            
            # Теперь обновляем статус в базе данных
            success = db.update_group_type(group_id, new_type)
            
            if success:
                action_word = "открыта" if new_type == 1 else "закрыта"
                await callback.answer(f"✅ Группа {action_word} для сообщений", show_alert=True)
                
                # Получаем обновленную информацию о группе
                group = db.get_managed_group(group_id)
                
                if group:
                    # Проверяем актуальный статус группы
                    is_member, _ = await check_bot_in_group(group_id)
                    if not is_member:
                        status_text = "⚠️ Бот больше не состоит в этой группе!"
                    else:
                        status_text = "✅ Бот состоит в группе"
                    
                    # Получаем тип группы и проверяем его корректность
                    group_type = group['group_type']
                    # Исправляем возможный некорректный тип группы
                    if group_type != 0 and group_type != 1:
                        logging.warning(f"Обнаружен некорректный тип группы: {group_type}, исправляем на 0")
                        group_type = 0  # По умолчанию считаем группу закрытой
                        # Исправляем в базе данных
                        db.update_group_type(group_id, 0)
                        
                    # Определяем отображаемый текст и данные для действий
                    # ВАЖНО: type=0 значит ЗАКРЫТА, type=1 значит ОТКРЫТА
                    type_text = "🔒 Закрыта" if group_type == 0 else "🔓 Открыта"
                    action_text = "Открыть" if group_type == 0 else "Закрыть"
                    # При type=0 кнопка должна открывать (т.е. next_type=1)
                    next_type = 1 if group_type == 0 else 0
                    # Упрощаем данные callback для избежания ошибок
                    action_data = f"toggle_group_{group_id}_{next_type}"
                    logging.info(f"Установка action_data на {action_data}: текущий тип = {group_type}, следующий тип = {next_type}")
                    
                    # Обновляем сообщение напрямую
                    await callback.message.edit_text(
                        f"👥 Информация о группе\n\n"
                        f"Название: {group['title']}\n"
                        f"ID: {group['group_id']}\n"
                        f"Тип: {type_text}\n"
                        f"Добавлена: {group['added_at']}\n"
                        f"Статус: {status_text}",
                        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                            [InlineKeyboardButton(text=f"{action_text} группу", callback_data=action_data)],
                            [InlineKeyboardButton(text="❌ Удалить группу", callback_data=f"delete_group_{group_id}")],
                            [InlineKeyboardButton(text="↩️ К списку групп", callback_data="manage_groups")]
                        ])
                    )
                else:
                    await callback.answer("❌ Группа не найдена", show_alert=True)
                    await manage_groups(callback)
            else:
                await callback.answer("❌ Ошибка при изменении типа группы", show_alert=True)
                await manage_groups(callback)
                
        except Exception as chat_error:
            logging.error(f"Ошибка изменения разрешений в группе {group_id}: {chat_error}")
            await callback.answer(f"❌ Ошибка: {str(chat_error)[:50]}", show_alert=True)
            await manage_groups(callback)
    except Exception as e:
        logging.error(f"Ошибка в toggle_group: {e}")
        await callback.answer("❌ Произошла ошибка", show_alert=True)
        # В случае ошибки возвращаемся к списку групп12 
        await manage_groups(callback) 

@dp.callback_query(lambda c: c.data.startswith("delete_group_"))
async def delete_group(callback: CallbackQuery):
    """Удаляем группу"""
    try:
        group_id = callback.data.split("delete_group_")[1]
        group = db.get_managed_group(group_id)
        
        if not group:
            await callback.answer("Группа не найдена", show_alert=True)
            await manage_groups(callback)
            return
        
        # Спрашиваем подтверждение перед удалением
        await callback.message.edit_text(
            f"❓ Вы действительно хотите удалить группу \"{group['title']}\"?",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="✅ Да, удалить", callback_data=f"confirm_delete_group_{group_id}")],
                [InlineKeyboardButton(text="❌ Отмена", callback_data=f"group_info_{group_id}")]
            ])
        )
        await callback.answer()
    except Exception as e:
        logging.error(f"Ошибка в delete_group: {e}")
        await callback.answer("❌ Произошла ошибка", show_alert=True)

@dp.callback_query(lambda c: c.data.startswith("confirm_delete_group_"))
async def confirm_delete_group(callback: CallbackQuery):
    """Подтверждаем удаление группы"""
    try:
        group_id = callback.data.split("confirm_delete_group_")[1]
        
        success = db.delete_managed_group(group_id)
        
        if success:
            await callback.answer("✅ Группа удалена", show_alert=True)
        else:
            await callback.answer("❌ Ошибка при удалении группы", show_alert=True)
        
        # Возвращаемся к списку групп
        await manage_groups(callback)
    except Exception as e:
        logging.error(f"Ошибка в confirm_delete_group: {e}")
        await callback.answer("❌ Произошла ошибка", show_alert=True)

async def get_admin_chats(client):
    """Получение списка чатов, где бот является администратором"""
    try:
        admin_chats = []
        dialogs = []
        
        # Получаем список всех диалогов
        async for dialog in client.get_dialogs():
            dialogs.append(dialog)
            
        logging.info(f"Всего получено диалогов: {len(dialogs)}")
            
        # Проверяем, является ли клиент админом в группах
        for dialog in dialogs:
            if dialog.chat and dialog.chat.type in ["group", "supergroup", "channel"]:
                try:
                    chat_id = dialog.chat.id
                    chat_title = dialog.chat.title
                    
                    logging.info(f"Проверка чата: {chat_title} (ID: {chat_id}, тип: {dialog.chat.type})")
                    
                    # Получаем информацию о себе в чате
                    try:
                        member = await client.get_chat_member(chat_id, "me")
                        
                        logging.info(f"Статус в чате {chat_title}: {member.status}")
                        
                        # Проверяем, есть ли права администратора
                        if member.status in ["administrator", "creator"]:
                            # Получаем дополнительную информацию о чате
                            chat = await client.get_chat(chat_id)
                            
                            admin_chats.append({
                                "id": str(chat_id),
                                "title": chat_title,
                                "type": dialog.chat.type,
                                "members_count": getattr(chat, "members_count", 0),
                                "permissions": chat.permissions if hasattr(chat, "permissions") else None,
                                "status": member.status
                            })
                            
                            logging.info(f"Добавлен чат с правами админа: {chat_title}")
                    except Exception as e:
                        logging.warning(f"Ошибка при получении статуса в чате {chat_title}: {e}")
                        
                except Exception as e:
                    chat_name = getattr(dialog.chat, "title", "Неизвестный чат")
                    logging.warning(f"Ошибка при получении информации о чате {chat_name}: {e}")
        
        logging.info(f"Всего найдено чатов с правами администратора: {len(admin_chats)}")
        return admin_chats
    except Exception as e:
        logging.error(f"Ошибка при получении чатов с правами администратора: {e}")
        return []












async def shutdown_clients():
    # Эта функция, возможно, больше не нужна или должна быть объединена
    # с остановкой персистентных клиентов, если active_clients больше не используется.
    # Пока оставим ее и добавим остановку персистентных.
    await stop_persistent_clients() 
    
    # Старый код для active_clients (если еще где-то используется)
    try:
        logging.info("Отключение старых active_clients (если есть)...")
        if 'active_clients' in config: # Проверяем наличие ключа
             clients_to_stop_old = list(config['active_clients'].items())
             config['active_clients'].clear()
             for phone, client in clients_to_stop_old:
                 try:
                     if client and client.is_connected:
                         await client.disconnect() # или stop?
                         logging.info(f"Старый клиент {phone} отключён")
                 except Exception as e:
                     logging.error(f"Ошибка отключения старого клиента {phone}: {e}")
    except Exception as e:
        logging.error(f"Ошибка в shutdown_clients (старый код): {e}")

async def update_proxy_type_from_url(message: Message = None):
    """Функция для обновления типа прокси на MTProto, если он соответствует формату"""
    try:
        # Получаем все прокси
        proxies = db.get_all_proxies()
        
        updated = 0
        for proxy in proxies:
            # Проверяем наличие secretа в password - это признак MTProto прокси
            if proxy.password and (proxy.password.startswith('ee') or 
                                  len(proxy.password) > 20 or 
                                  'secret=' in proxy.password):
                # Обновляем схему на mtproto в БД
                # Для этого удаляем и пересоздаем прокси
                db.delete_proxy(proxy.id)
                new_proxy = Proxy(
                    id=0,  # ID будет назначен при добавлении
                    host=proxy.host,
                    port=proxy.port,
                    scheme='mtproto',
                    username=proxy.username,
                    password=proxy.password
                )
                if db.add_proxy(new_proxy):
                    updated += 1
                    logging.info(f"Прокси {proxy.host}:{proxy.port} обновлен с {proxy.scheme} на mtproto")
        
        if message and updated > 0:
            await message.answer(f"✅ Обновлено {updated} прокси до типа MTProto")
        elif message:
            await message.answer("❕ Не найдено прокси для обновления")
            
        return updated
    except Exception as e:
        logging.error(f"Ошибка при обновлении типов прокси: {e}")
        if message:
            await message.answer(f"❌ Ошибка при обновлении прокси: {e}")
        return 0

@dp.message(Command("update_proxies"))
async def cmd_update_proxies(message: Message):
    """Обработчик команды /update_proxies для обновления типов прокси"""
    try:
        if not await is_admin(message.from_user.id):
            return
        
        await message.answer("🔄 Обновляю типы прокси...")
        updated = await update_proxy_type_from_url(message)
        
    except Exception as e:
        logging.error(f"Ошибка в cmd_update_proxies: {e}")
        await message.answer(f"❌ Произошла ошибка: {e}")

async def main():
    # Объявляем глобальные переменные в начале функции
    global db, bot, BOT_USERNAME
    
    # Сбрасываем флаг запуска для копирования
    if hasattr(copying_task, 'is_running'):
        copying_task.is_running = False
    
    # Получаем username бота при запуске
    try:
        bot_info = await bot.get_me()
        if bot_info.username:
            BOT_USERNAME = bot_info.username
            logging.info(f"Бот запущен: @{BOT_USERNAME}")
            # Сохраняем в config.py для веб-панели
            try:
                import config as cfg_module
                cfg_module.BOT_USERNAME = BOT_USERNAME
            except:
                pass
    except Exception as e:
        logging.warning(f"Не удалось получить username бота: {e}")
    
    # Инициализация базы данных
    db = SessionDB()
    
    # Автоматически обновляем типы прокси при запуске
    try:
        updated = await update_proxy_type_from_url()
        if updated > 0:
            logging.info(f"При запуске автоматически обновлено {updated} прокси до типа MTProto")
    except Exception as e:
        logging.error(f"Ошибка при автоматическом обновлении прокси: {e}")
    
    # Инициализируем глобальную карту соответствий между ID сообщений
    if 'message_id_map' not in config:
        config['message_id_map'] = {}
    
    # Инициализируем другие переменные
    if 'chat_id_cache' not in config:
        config['chat_id_cache'] = {}
    
    if 'persistent_clients' not in config:
        config['persistent_clients'] = {}
    
    if 'copying_task' not in config:
        config['copying_task'] = None
    
    # Здесь выполняется инициализация bot, она уже есть
    
    # Запускаем проверку работоспособности аккаунтов
    # asyncio.create_task(check_accounts_health())
    
    with process_lock():
        try:
            logging.info("Запуск бота...")
            print("=" * 50)
            print("Бот запущен и готов к работе!")
            print("Нажмите Ctrl+C для остановки")
            print("=" * 50)
            await dp.start_polling(bot, allowed_updates=dp.resolve_used_update_types())
        except KeyboardInterrupt:
            logging.info("Получен сигнал остановки (Ctrl+C)")
            print("\nОстановка бота...")
        except Exception as e:
            logging.error(f"Критическая ошибка при работе бота: {e}", exc_info=True)
            print(f"\n❌ КРИТИЧЕСКАЯ ОШИБКА: {e}")
            import traceback
            traceback.print_exc()
        finally:
            logging.info("Завершение работы бота...")
            try:
                await shutdown_clients()
            except Exception as e:
                logging.error(f"Ошибка при закрытии клиентов: {e}")
            try:
                db.close()
            except Exception as e:
                logging.error(f"Ошибка при закрытии БД: {e}")

@dp.callback_query(lambda c: c.data == "start_copying")
async def start_copying(callback: CallbackQuery):
    try:
        sessions = db.load_sessions()
        active_accounts = [phone for phone, data in sessions.items() if data.get('copy_mode', 0) == 1]
        
        if not active_accounts:
            await callback.answer("❌ Нет активных аккаунтов для копирования!", show_alert=True)
            return
                
        # Показываем кнопки выбора режима
        mode_kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="📁 Копировать из файла", callback_data="start_mode_file")],
            [InlineKeyboardButton(text="💬 Копировать из чата", callback_data="start_mode_chat")],
            [InlineKeyboardButton(text="⬅️ Назад", callback_data="back_to_menu")]
        ])

        await callback.message.edit_text(
            "Выберите режим копирования:",
            reply_markup=mode_kb
        )
        await callback.answer()
        
    except Exception as e:
        logging.error(f"Ошибка в start_copying: {e}")
        await callback.answer("❌ Произошла ошибка", show_alert=True)

@dp.callback_query(lambda c: c.data == "start_mode_file")
async def start_copying_file(callback: CallbackQuery):
    try:
        # ---> НАЧАЛО: Остановка и очистка предыдущих персистентных клиентов < ---
        if config['persistent_clients']:
            logging.info("Остановка предыдущих персистентных клиентов перед запуском нового режима...")
            await stop_persistent_clients()
        # ---> КОНЕЦ: Остановка и очистка < ---
        
        sessions = db.load_sessions()
        active_accounts = [phone for phone, data in sessions.items() if data.get('copy_mode', 0) == 1]

        if not active_accounts:
            await callback.answer("❌ Нет активных аккаунтов для копирования!", show_alert=True)
            return

        # >>> НАЧАЛО ИЗМЕНЕНИЯ: Ранний ответ на callback и сообщение о загрузке <<<
        await callback.answer() # Отвечаем сразу, чтобы избежать таймаута
        await callback.message.edit_text(
            "⏳ Инициализация аккаунтов для файлового режима...",
            reply_markup=None # Убираем кнопки на время загрузки
        )
        # >>> КОНЕЦ ИЗМЕНЕНИЯ <<<

        # Проверяем, выбран ли файл хотя бы у одного активного аккаунта
        has_file = False
        files = db.load_message_files()
        
        # Собираем аккаунты с файлами и группами
        accounts_with_file_and_groups = []
        for phone in active_accounts:
            account_data = sessions[phone]
            if account_data.get('current_file') and account_data.get('dest_chats'):
                # Проверяем существование файла в базе
                if account_data.get('current_file') in files:
                    has_file = True
                    accounts_with_file_and_groups.append(phone)
        
        if not has_file:
            await callback.message.edit_text(
                "❌ Нет активных аккаунтов с настроенным файлом И группами назначения!", 
                reply_markup=main_menu_kb()
            )
            return
            
        # ---> НАЧАЛО: Предварительный запуск клиентов для файлового режима < ---
        logging.info(f"Предварительный запуск клиентов для {len(accounts_with_file_and_groups)} аккаунтов файлового режима...")
        failed_to_start = []
        successfully_started_clients = {} # Временный словарь для хранения успешно запущенных
        
        # Словарь для хранения статистики присоединения
        join_stats = {}
        
        for phone in accounts_with_file_and_groups:
            if phone not in sessions: continue # На всякий случай
            try:
                logging.info(f"Попытка запуска персистентного клиента для {phone}...")
                client = await get_or_create_client(phone, sessions[phone]) # Используем существующую функцию для создания
                if client and client.is_connected:
                    successfully_started_clients[phone] = client
                    logging.info(f"Персистентный клиент для {phone} успешно запущен.")
                    
                    # НОВЫЙ КОД: Присоединение к группам
                    dest_chats = sessions[phone].get('dest_chats')
                    if dest_chats:
                        try:
                            # Пробуем загрузить как JSON
                            dest_list = json.loads(dest_chats) if dest_chats.startswith('[') else dest_chats.split(',')
                            dest_list = [chat.strip('[]"\n') for chat in dest_list if chat.strip('[]"\n')]
                            if dest_list:
                                logging.info(f"Присоединение аккаунта {phone} к {len(dest_list)} группам...")
                                joined_groups, failed_groups = await join_account_groups(phone, client, dest_list)
                                join_stats[phone] = {
                                    "success": joined_groups,
                                    "failed": failed_groups
                                }
                                logging.info(f"Аккаунт {phone} присоединен к {len(joined_groups)} из {len(dest_list)} групп")
                        except Exception as join_error:
                            logging.error(f"Ошибка при присоединении {phone} к группам: {join_error}")
                else:
                    failed_to_start.append(phone)
                    logging.error(f"Не удалось запустить персистентный клиент для {phone}.")
            except Exception as start_err:
                failed_to_start.append(phone)
                logging.error(f"Ошибка при запуске персистентного клиента для {phone}: {start_err}")
        
        # Сохраняем только успешно запущенные клиенты в глобальный конфиг
        config['persistent_clients'] = successfully_started_clients
        
        # Отправляем статистику присоединения к группам администратору
        if join_stats:
            total_success = sum(len(stats["success"]) for stats in join_stats.values())
            total_failed = sum(len(stats["failed"]) for stats in join_stats.values())
            join_stats_message = f"📊 Статистика присоединения к группам (файловый режим):\n"
            join_stats_message += f"✅ Успешно: {total_success}\n"
            join_stats_message += f"❌ Неудачно: {total_failed}\n\n"
            for phone, stats in join_stats.items():
                if stats["failed"]:  # Показываем только аккаунты с проблемами
                    failed_groups_str = ", ".join(stats["failed"][:3])
                    if len(stats["failed"]) > 3:
                        failed_groups_str += f" и еще {len(stats['failed']) - 3}"
                    join_stats_message += f"📱 {phone}: проблемы с {len(stats['failed'])} группами ({failed_groups_str})\n"
            
            await send_log_to_admins(join_stats_message)
            
        if failed_to_start:
             log_msg_fail = f"⚠️ Не удалось запустить персистентные клиенты для {len(failed_to_start)} аккаунтов: {failed_to_start}"
             await send_log_to_admins(log_msg_fail)
             logging.warning(log_msg_fail)
        # ---> КОНЕЦ: Предварительный запуск клиентов < ---
        
        # Проверяем, остались ли клиенты после инициализации
        if not config['persistent_clients']:
            await callback.message.edit_text(
                "❌ Не удалось запустить ни одного клиента для файлового режима!", 
                reply_markup=main_menu_kb()
            )
            return

        config['copying_active'] = True
        config['copying_mode'] = 1  # Числовое значение для файлового режима
        config['copying_accounts'] = list(config['persistent_clients'].keys())  # Используем только успешно запущенные
        config['last_used_account'] = None
        config['group_account_map'] = {} # Сбрасываем карту аккаунтов
        db.save_state(0, 0) # Сбрасываем оба указателя: сообщения и аккаунта
        
        await callback.message.edit_text(status_text(), reply_markup=main_menu_kb())
        
        await send_log_to_admins(
            f"🚀 Копирование из файла запущено\n"
            f"👥 Активных аккаунтов: {len(config['persistent_clients'])}"
        )
        
        asyncio.create_task(copying_task())
    except Exception as e:
        logging.error(f"Ошибка в start_copying_file: {e}")
        await callback.answer("❌ Произошла ошибка", show_alert=True)
        await callback.message.edit_text(
            f"❌ Ошибка при запуске файлового режима: {e}",
            reply_markup=main_menu_kb()
        )

@dp.callback_query(lambda c: c.data == "start_mode_chat")
async def start_copying_chat(callback: CallbackQuery):
    try:
        # ---> НАЧАЛО: Остановка и очистка предыдущих персистентных клиентов < ---
        if config['persistent_clients']:
            logging.info("Остановка предыдущих персистентных клиентов перед запуском нового режима...")
            await stop_persistent_clients()
        # ---> КОНЕЦ: Остановка и очистка < ---

        sessions = db.load_sessions()
        active_accounts = [phone for phone, data in sessions.items() if data.get('copy_mode', 0) == 1]
        
        if not active_accounts:
            await callback.answer("❌ Нет активных аккаунтов для копирования!", show_alert=True)
            return
                    
        # >>> НАЧАЛО ИЗМЕНЕНИЯ: Ранний ответ на callback и сообщение о загрузке <<<
        await callback.answer() # Отвечаем сразу, чтобы избежать таймаута
        await callback.message.edit_text(
            "⏳ Инициализация источников и запуск клиентов...",
            reply_markup=None # Убираем кнопки на время загрузки
        )
        # >>> КОНЕЦ ИЗМЕНЕНИЯ <<<
            
        # Формируем уникальные источники и проверяем аккаунты с источниками и группами
        unique_sources = set()
        accounts_with_source = []
        
        for phone in active_accounts:
            account_data = sessions.get(phone)
            if not account_data: continue
            
            # Нужен и источник, и целевые группы
            has_source = account_data.get('source_chat')
            has_dest = account_data.get('dest_chats')
            
            if has_source and has_dest:
                # Получаем идентификатор источника (обрабатываем ссылку единообразно)
                source_id = process_chat_link(has_source)
                if source_id:
                    unique_sources.add(source_id)
                    accounts_with_source.append(phone)
        
        if not accounts_with_source:
            # >>> ИЗМЕНЕНИЕ СООБЩЕНИЯ ОБ ОШИБКЕ <<<
            # >>> НАЧАЛО ИЗМЕНЕНИЯ: Редактируем сообщение вместо ответа на callback <<<
            await callback.message.edit_text(
                "❌ Нет активных аккаунтов с настроенным источником И группами назначения!",
                reply_markup=main_menu_kb() # Возвращаем кнопки
            )
            # >>> КОНЕЦ ИЗМЕНЕНИЯ <<<
            return
            
        if not unique_sources:
            # >>> НАЧАЛО ИЗМЕНЕНИЯ: Редактируем сообщение вместо ответа на callback <<<
            await callback.message.edit_text(
                "❌ Не найдено валидных источников для копирования!",
                reply_markup=main_menu_kb() # Возвращаем кнопки
            )
            # >>> КОНЕЦ ИЗМЕНЕНИЯ <<<
            return
            
        # Обновляем сообщение о статусе
        await callback.message.edit_text(
            "🔄 Присоединение к группам и инициализация источников...",
            reply_markup=None
        )
        
        # ---> НАЧАЛО: Предварительный запуск клиентов для чат-режима < ---
        logging.info(f"Предварительный запуск клиентов для {len(accounts_with_source)} аккаунтов чат-режима...")
        failed_to_start = []
        successfully_started_clients = {} # Временный словарь для хранения успешно запущенных
        
        # Используем accounts_with_source, так как только они будут реально участвовать
        unique_phones_to_start = set(accounts_with_source)
        
        # Словарь для хранения статистики присоединения
        join_stats = {}
        
        for phone in unique_phones_to_start:
            if phone not in sessions: continue # На всякий случай
            try:
                logging.info(f"Попытка запуска персистентного клиента для {phone}...")
                client = await get_or_create_client(phone, sessions[phone]) # Используем существующую функцию для создания
                if client and client.is_connected:
                    successfully_started_clients[phone] = client
                    logging.info(f"Персистентный клиент для {phone} успешно запущен.")
                    
                    # НОВЫЙ КОД: Присоединение к группам
                    dest_chats = sessions[phone].get('dest_chats')
                    if dest_chats:
                        try:
                            # Пробуем загрузить как JSON
                            dest_list = json.loads(dest_chats) if dest_chats.startswith('[') else dest_chats.split(',')
                            dest_list = [chat.strip('[]"\n') for chat in dest_list if chat.strip('[]"\n')]
                            if dest_list:
                                logging.info(f"Присоединение аккаунта {phone} к {len(dest_list)} группам...")
                                joined_groups, failed_groups = await join_account_groups(phone, client, dest_list)
                                join_stats[phone] = {
                                    "success": joined_groups,
                                    "failed": failed_groups
                                }
                                logging.info(f"Аккаунт {phone} присоединен к {len(joined_groups)} из {len(dest_list)} групп")
                        except Exception as join_error:
                            logging.error(f"Ошибка при присоединении {phone} к группам: {join_error}")
                else:
                    failed_to_start.append(phone)
                    logging.error(f"Не удалось запустить персистентный клиент для {phone}.")
            except Exception as start_err:
                failed_to_start.append(phone)
                logging.error(f"Ошибка при запуске персистентного клиента для {phone}: {start_err}")
        
        # Сохраняем только успешно запущенные клиенты в глобальный конфиг
        config['persistent_clients'] = successfully_started_clients
        
        # Обновляем список активных аккаунтов, исключая те, для которых не удалось запустить клиент
        original_count = len(accounts_with_source)
        accounts_with_source = [acc for acc in accounts_with_source if acc in config['persistent_clients']]
        
        # Отправляем статистику присоединения к группам администратору
        if join_stats:
            total_success = sum(len(stats["success"]) for stats in join_stats.values())
            total_failed = sum(len(stats["failed"]) for stats in join_stats.values())
            join_stats_message = f"📊 Статистика присоединения к группам:\n"
            join_stats_message += f"✅ Успешно: {total_success}\n"
            join_stats_message += f"❌ Неудачно: {total_failed}\n\n"
            for phone, stats in join_stats.items():
                if stats["failed"]:  # Показываем только аккаунты с проблемами
                    failed_groups_str = ", ".join(stats["failed"][:3])
                    if len(stats["failed"]) > 3:
                        failed_groups_str += f" и еще {len(stats['failed']) - 3}"
                    join_stats_message += f"📱 {phone}: проблемы с {len(stats['failed'])} группами ({failed_groups_str})\n"
            
            await send_log_to_admins(join_stats_message)
            
        if failed_to_start:
             log_msg_fail = f"⚠️ Не удалось запустить персистентные клиенты для {len(failed_to_start)} аккаунтов: {failed_to_start}"
             await send_log_to_admins(log_msg_fail)
             logging.warning(log_msg_fail)
        
        if not accounts_with_source:
            # >>> НАЧАЛО ИЗМЕНЕНИЯ: Редактируем сообщение вместо ответа на callback <<<
            await callback.message.edit_text(
                "❌ Не удалось запустить ни одного клиента для активных аккаунтов! Копирование не начнется.",
                reply_markup=main_menu_kb() # Возвращаем кнопки
            )
            # >>> КОНЕЦ ИЗМЕНЕНИЯ <<<
            await stop_persistent_clients() # Убедимся, что все остановлено, если что-то запустилось
            return
        # ---> КОНЕЦ: Предварительный запуск клиентов < ---
            
        # Инициализация источников (получение последних message_id)
        initialization_errors = []
        
        for source_id in unique_sources:
            client = None
            try:
                # Находим первый попавшийся активный аккаунт для этого источника
                initializer_phone = None
                for phone in accounts_with_source:
                    if process_chat_link(sessions[phone].get('source_chat')) == source_id:
                        initializer_phone = phone
                        break
                
                if not initializer_phone:
                    error_msg = f"Не найден аккаунт для инициализации источника {source_id}"
                    logging.error(error_msg)
                    initialization_errors.append(error_msg)
                    continue
                
                # Создаем временный клиент для инициализации
                client = await get_or_create_client(initializer_phone, sessions[initializer_phone])
                if not client:
                    error_msg = f"Не удалось создать клиент для {initializer_phone} (источник {source_id})"
                    logging.error(error_msg)
                    initialization_errors.append(error_msg)
                    continue
                
                # Обрабатываем ID чата без присоединения
                processed_chat_id = process_chat_link(source_id)
                if not processed_chat_id:
                    error_msg = f"Некорректный ID источника {source_id}"
                    logging.error(error_msg)
                    initialization_errors.append(error_msg)
                    continue
                    
                # Проверка на невалидный формат удалена, так как символ '+' допустим в частных группах
                
                # Получаем последнее сообщение (чтобы не копировать старые)
                latest_message_id = 0
                
                try:
                    # Если это приватная группа, сначала присоединяемся к ней
                    if processed_chat_id.startswith('+') or 'joinchat' in processed_chat_id:
                        joined, actual_chat_id = await ensure_joined_chat(client, processed_chat_id)
                        if joined:
                            processed_chat_id = actual_chat_id
                        else:
                            error_msg = f"Не удалось присоединиться к приватному чату: {processed_chat_id}"
                            logging.error(error_msg)
                            initialization_errors.append(error_msg)
                            continue
                    
                    # Пробуем получить историю чата
                    async for message in client.get_chat_history(processed_chat_id, limit=1):
                        latest_message_id = message.id
                        break # Нужна только одна самая последняя запись
                except Exception as chat_error:
                    error_msg = f"Не удалось получить историю источника {source_id}: {chat_error}"
                    logging.error(error_msg)
                    initialization_errors.append(error_msg)
                    continue
                
                if latest_message_id > 0:
                    # Обновляем ID для всех аккаунтов с этим источником
                    updated_count = 0
                    for phone in accounts_with_source:
                        if process_chat_link(sessions[phone].get('source_chat')) == source_id:
                            if db.update_last_message_id(phone, latest_message_id):
                                updated_count += 1
                    logging.info(f"Источник {source_id} инициализирован latest_message_id={latest_message_id} для {updated_count} аккаунтов.")
                else:
                    logging.warning(f"Не удалось получить последнее сообщение для источника {source_id}")
                    # Не добавляем в ошибки, просто начинаем с 0
            
            except Exception as e:
                error_msg = f"Ошибка инициализации источника {source_id}: {e}"
                logging.error(error_msg)
                initialization_errors.append(error_msg)
            finally:
                if client:
                    try: 
                        await client.stop() 
                    except: 
                        pass
        
        # Если были ошибки инициализации, сообщаем админу
        if initialization_errors:
            errors_text = "\n".join([f"- {err}" for err in initialization_errors])
            await send_log_to_admins(f"⚠️ Произошли ошибки при инициализации источников:\n{errors_text}")
            # Можно добавить await callback.answer(...) с сообщением об ошибках, если нужно

        # Запускаем копирование (используем обновленный список accounts_with_source)
        config['copying_active'] = True
        config['copying_mode'] = 2  # Числовое значение для чат-режима
        # Используем только те аккаунты, для которых есть валидный источник и назначение
        config['copying_accounts'] = accounts_with_source 
        config['last_used_account'] = None
        config['group_account_map'] = {} # Сбрасываем карту аккаунтов
        
        # >>> НАЧАЛО ИЗМЕНЕНИЯ: Редактируем сообщение вместо ответа на callback <<<
        # Убираем callback.answer
        # await callback.answer("✅ Копирование из чата запущено", show_alert=True)
        await callback.message.edit_text(
            f"✅ Копирование из чата запущено!\n{status_text()}", # Показываем статус после запуска
            reply_markup=main_menu_kb()
        )
        # >>> КОНЕЦ ИЗМЕНЕНИЯ <<<
        
        await send_log_to_admins(
            f"🚀 Копирование из чата запущено\n"
            f"👥 Активных аккаунтов: {len(accounts_with_source)}\n"
            f"Источников для мониторинга: {len(unique_sources)}"
        )
        
        asyncio.create_task(copying_task())
    except Exception as e:
        logging.error(f"Ошибка в start_copying_chat: {e}")
        # >>> НАЧАЛО ИЗМЕНЕНИЯ: Редактируем сообщение вместо ответа на callback <<<
        try:
            await callback.message.edit_text(
                f"❌ Произошла ошибка при запуске копирования чата:\n\n{e}\n\n{status_text()}",
                reply_markup=main_menu_kb()
            )
        except Exception as inner_e: # Если редактирование тоже не удалось
            logging.error(f"Не удалось отредактировать сообщение об ошибке: {inner_e}")
            # Попытка отправить новое сообщение, если редактирование не удалось
            try:
                await bot.send_message(
                    chat_id=callback.message.chat.id,
                    text=f"❌ Произошла ошибка при запуске копирования чата:\n\n{e}\n\n{status_text()}",
                    reply_markup=main_menu_kb()
                )
            except Exception as send_err:
                 logging.error(f"Не удалось отправить сообщение об ошибке: {send_err}")
        # >>> КОНЕЦ ИЗМЕНЕНИЯ <<<

@dp.callback_query(lambda c: c.data == "select_all_files")
async def select_all_files_handler(callback: CallbackQuery):
    try:
        files = db.load_message_files()
        if not files:
            await callback.answer("❌ Нет доступных файлов", show_alert=True)
            return
                    
        # Берём первый файл из списка
        first_file_id = list(files.keys())[0]
        first_file_data = files[first_file_id]
        
        # Получаем все сессии
        sessions = db.load_sessions()
        if not sessions:
            await callback.answer("❌ Нет доступных аккаунтов", show_alert=True)
            return
            
        # Устанавливаем файл для всех аккаунтов
        for phone in sessions:
            db.save_session(
                phone=phone,
                session=sessions[phone].get('session', ''),
                source_chat=sessions[phone].get('source_chat'),
                dest_chats=sessions[phone].get('dest_chats'),
                current_file=first_file_id,
                copy_mode=sessions[phone].get('copy_mode'),
                last_message_id=sessions[phone].get('last_message_id'),
                last_sent_index=sessions[phone].get('last_sent_index'),
                proxy_id=sessions[phone].get('proxy_id')
            )
        
        logging.info(f"Пользователь {callback.from_user.id} выбрал файл {first_file_data['name']} для всех аккаунтов")
        
        await callback.answer(f"✅ Файл {first_file_data['name']} выбран для всех аккаунтов", show_alert=True)
        
        # Возвращаемся в меню
        await callback.message.edit_text("🔄 Главное меню:", reply_markup=main_menu_kb())
        
    except Exception as e:
        logging.error(f"Ошибка в select_all_files_handler: {e}")
        await callback.answer("❌ Произошла ошибка при выборе файла для всех аккаунтов", show_alert=True)

@dp.callback_query(lambda c: c.data.startswith("use_file_"))
async def use_file_handler(callback: CallbackQuery):
    try:
        await callback.answer()
        
        # Проверяем, для всех аккаунтов или для одного
        if callback.data.startswith("use_file_all_"):
            # Для всех аккаунтов
            file_id = callback.data.replace("use_file_all_", "")
            
            # Проверяем наличие файла
            files = db.load_message_files()
            if file_id not in files:
                await callback.answer("❌ Файл не найден", show_alert=True)
                return
                
            file_name = files[file_id]['name']
            
            # Загружаем сессии
            sessions = db.load_sessions()
            if not sessions:
                await callback.answer("❌ Нет доступных аккаунтов", show_alert=True)
                return
                
            # Устанавливаем файл для всех аккаунтов
            count = 0
            for phone, session_data in sessions.items():
                db.save_session(
                    phone=phone,
                    session=session_data.get('session', ''),
                    source_chat=session_data.get('source_chat'),
                    dest_chats=session_data.get('dest_chats'),
                    current_file=file_id,
                    copy_mode=session_data.get('copy_mode'),
                    last_message_id=session_data.get('last_message_id'),
                    last_sent_index=session_data.get('last_sent_index'),
                    proxy_id=session_data.get('proxy_id')
                )
                count += 1
                
            await callback.answer(f"✅ Файл {file_name} выбран для {count} аккаунтов", show_alert=True)
            
        else:
            # Для одного аккаунта
            parts = callback.data.replace("use_file_", "").split("_")
            if len(parts) != 2:
                await callback.answer("❌ Некорректный формат данных", show_alert=True)
                return
                
            file_id, phone = parts
            
            # Проверяем наличие файла
            files = db.load_message_files()
            if file_id not in files:
                await callback.answer("❌ Файл не найден", show_alert=True)
                return
                
            file_name = files[file_id]['name']
            
            # Загружаем сессии
            sessions = db.load_sessions()
            if phone not in sessions:
                await callback.answer("❌ Аккаунт не найден", show_alert=True)
                return
                
            # Устанавливаем файл для выбранного аккаунта
            session_data = sessions[phone]
            db.save_session(
                phone=phone,
                session=session_data.get('session', ''),
                source_chat=session_data.get('source_chat'),
                dest_chats=session_data.get('dest_chats'),
                current_file=file_id,
                copy_mode=session_data.get('copy_mode'),
                last_message_id=session_data.get('last_message_id'),
                last_sent_index=session_data.get('last_sent_index'),
                proxy_id=session_data.get('proxy_id')
            )
            
            await callback.answer(f"✅ Файл {file_name} выбран для аккаунта {phone}", show_alert=True)
        
        # Возвращаемся в меню
        await callback.message.edit_text("🔄 Главное меню:", reply_markup=main_menu_kb())
        
    except Exception as e:
        logging.error(f"Ошибка в use_file_handler: {e}")
        await callback.answer("❌ Произошла ошибка при применении файла", show_alert=True)

def fix_gender_specific_text(text: str, gender: str) -> str:
    # Защита от None значений
    if text is None:
        text = ""
        
    logging.info(f"[fix_gender_specific_text] Вызвана с gender='{gender}', text='{text[:50] if text else ''}...'")
    
    # Если гендер не указан или 'male', используем текст как есть
    if gender is None or gender.lower() == 'male':
        logging.info(f"[fix_gender_specific_text] Выбран мужской блок исправлений.")
        # Исправление фраз женского рода на мужской
        replacements = {
            # Глаголы прошедшего времени (Ж -> М)
            r'\bрада\b': 'рад',
            r'\bготова\b': 'готов',
            r'\bуверена\b': 'уверен',
            r'\bдовольна\b': 'доволен',
            r'\bсогласна\b': 'согласен',
            r'\bпришла\b': 'пришел',
            r'\bзашла\b': 'зашел',
            r'\bвышла\b': 'вышел',
            r'\bушла\b': 'ушел',
            r'\bпошла\b': 'пошел',
            r'\bнашла\b': 'нашел',
            r'\bувидела\b': 'увидел',
            r'\bсказала\b': 'сказал',
            r'\bнаписала\b': 'написал',
            r'\bпрочитала\b': 'прочитал',
            r'\bответила\b': 'ответил',
            r'\bспросила\b': 'спросил',
            r'\bподумала\b': 'подумал',
            r'\bрешила\b': 'решил',
            r'\bпоняла\b': 'понял',
            r'\bзнала\b': 'знал',
            r'\bхотела\b': 'хотел',
            r'\bмогла\b': 'мог',
            r'\bдолжна\b': 'должен',
            r'\bсмогла\b': 'смог',
            r'\bуспела\b': 'успел',
            r'\bбыла\b': 'был',
            r'\bстала\b': 'стал',
            r'\bсделала\b': 'сделал',
            r'\bполучила\b': 'получил',
            r'\bотправила\b': 'отправил',
            r'\bприняла\b': 'принял',
            r'\bпозвала\b': 'позвал',
            r'\bждала\b': 'ждал',
            r'\bискала\b': 'искал',
            r'\bначала\b': 'начал',
            r'\bзакончила\b': 'закончил',
            r'\bзабыла\b': 'забыл',
            r'\bвспомнила\b': 'вспомнил',
            r'\bпозвонила\b': 'позвонил',
            
            # Краткие прилагательные/причастия (Ж -> М)
            r'\bзанята\b': 'занят',
            r'\bсвободна\b': 'свободен',
            r'\bудивлена\b': 'удивлен',
            r'\bогорчена\b': 'огорчен',
            r'\bрасстроена\b': 'расстроен',
            r'\bуставшая\b': 'уставший',
            r'\bбольная\b': 'больной',
            r'\bздоровая\b': 'здоровый',
        }
    else:  # female
        logging.info("[fix_gender_specific_text] Выбран женский блок исправлений.")
        # Исправление фраз мужского рода на женский
        replacements = {
            # Глаголы прошедшего времени (М -> Ж)
            r'\bрад\b': 'рада',
            r'\bготов\b': 'готова',
            r'\bуверен\b': 'уверена',
            r'\bдоволен\b': 'довольна',
            r'\bсогласен\b': 'согласна',
            r'\bпришел\b': 'пришла',
            r'\bзашел\b': 'зашла',
            r'\bвышел\b': 'вышла',
            r'\bушел\b': 'ушла',
            r'\bпошел\b': 'пошла',
            r'\bнашел\b': 'нашла',
            r'\bувидела\b': 'увидела',
            r'\bсказал\b': 'сказала',
            r'\bнаписал\b': 'написала',
            r'\bпрочитал\b': 'прочитала',
            r'\bответил\b': 'ответила',
            r'\bспросил\b': 'спросила',
            r'\bподумал\b': 'подумала',
            r'\bрешил\b': 'решила',
            r'\bпонял\b': 'поняла',
            r'\bзнал\b': 'знала',
            r'\bхотел\b': 'хотела',
            r'\bмог\b': 'могла',
            r'\bдолжен\b': 'должна',
            r'\bсмог\b': 'смогла',
            r'\bуспел\b': 'успела',
            r'\bбыл\b': 'была',
            r'\bстал\b': 'стала',
            r'\bсделал\b': 'сделала',
            r'\bполучил\b': 'получила',
            r'\bотправил\b': 'отправила',
            r'\bпринял\b': 'приняла',
            r'\bпозвал\b': 'позвала',
            r'\bждал\b': 'ждала',
            r'\bискал\b': 'искала',
            r'\bначал\b': 'начала',
            r'\bзакончил\b': 'закончила',
            r'\bзабыл\b': 'забыла',
            r'\bвспомнил\b': 'вспомнила',
            r'\bпозвонил\b': 'позвонила',

            # Краткие прилагательные/причастия (М -> Ж)
            r'\bзанят\b': 'занята',
            r'\bсвободен\b': 'свободна',
            r'\bудивлен\b': 'удивлена',
            r'\bогорчен\b': 'огорчена',
            r'\bрасстроен\b': 'расстроена',
            r'\bуставший\b': 'уставшая',
            r'\bбольной\b': 'больная',
            r'\bздоровый\b': 'здоровая',
        }
        
    original_text = text # Сохраняем для сравнения
    for pattern, replacement in replacements.items():
        text = re.sub(pattern, replacement, text)

    if text != original_text:
        logging.info(f"[fix_gender_specific_text] Текст изменен: '{text[:50]}...'")
    else:
        logging.info("[fix_gender_specific_text] Текст не изменен.")
    return text

async def join_account_groups(phone, client, groups):
    """Присоединяет аккаунт ко всем указанным группам.
    Возвращает кортеж (удачные_группы, неудачные_группы)"""
    joined_successful = []
    joined_failed = []
    
    logging.info(f"Присоединение аккаунта {phone} к {len(groups)} группам...")
    
    for group in groups:
        try:
            # Используем ensure_joined_chat для проверки и присоединения к группе
            joined, processed_id = await ensure_joined_chat(client, group)
            if joined:
                joined_successful.append(processed_id)
                logging.info(f"Аккаунт {phone} успешно присоединен к группе {group} (ID: {processed_id})")
            else:
                joined_failed.append(group)
                logging.error(f"Аккаунт {phone} не смог присоединиться к группе {group}")
        except Exception as e:
            joined_failed.append(group)
            logging.error(f"Ошибка при присоединении {phone} к группе {group}: {e}")
    
    # Информационное сообщение о результатах
    if joined_successful:
        logging.info(f"Аккаунт {phone} присоединен к {len(joined_successful)} из {len(groups)} групп")
    if joined_failed:
        logging.warning(f"Аккаунт {phone} не смог присоединиться к {len(joined_failed)} группам")
    
    return joined_successful, joined_failed

@dp.message(Command("logs"))
async def cmd_logs(message: Message):
    try:
        # Проверка прав администратора
        if not await is_admin(message.from_user.id):
            await message.answer("❌ У вас нет доступа к этой команде.")
            return
        
        # Создаем временный файл для отправки
        temp_log_file = f"{UPLOAD_DIR}/temp_logs_{message.from_user.id}.txt"
        
        # Копируем содержимое лог-файла во временный файл
        with open(LOG_FILE, 'r', encoding='utf-8') as src, open(temp_log_file, 'w', encoding='utf-8') as dst:
            dst.write(src.read())
        
        # Отправляем файл
        await message.answer("📋 Отправляю файл логов...")
        log_document = FSInputFile(temp_log_file, filename=f"logs_{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.txt")
        await message.answer_document(log_document)
        
        # Удаляем временный файл
        if os.path.exists(temp_log_file):
            os.remove(temp_log_file)
            logging.info(f"Временный файл логов {temp_log_file} удален")
        
        await message.answer("✅ Файл логов отправлен и удален с сервера.")
    except Exception as e:
        logging.error(f"Ошибка при отправке логов: {e}")
        await message.answer(f"❌ Произошла ошибка при отправке логов: {str(e)}")

def register_message_id(sent_msg, source_key, processed_chat_id, log_prefix, sender_phone=None, message_id_map=None, grouped_id=None):
    """
    Регистрирует соответствие между ID исходного и отправленного сообщения.
    
    Args:
        sent_msg: Объект отправленного сообщения или его ID
        source_key: Ключ исходного сообщения (формат "source_chat_id:message_id")
        processed_chat_id: ID целевого чата
        log_prefix: Префикс для логов
        sender_phone: Номер телефона отправителя
        message_id_map: Карта соответствий ID сообщений
        grouped_id: ID группы сообщений (для цепочек ответов)
    """
    # Инициализируем словарь grouped_id_map, если он еще не существует
    if 'grouped_id_map' not in config:
        config['grouped_id_map'] = {}
        
    # Проверка на None или пустое значение для sent_msg
    if sent_msg is None:
        logging.warning(f"{log_prefix} Невозможно зарегистрировать сообщение, т.к. sent_msg=None")
        return message_id_map or {}
        
    # Проверка, что message_id_map не None
    if message_id_map is not None:
        # Получаем ID сообщения, проверяя объект на наличие атрибута id
        if hasattr(sent_msg, 'id') and sent_msg.id:
            target_id = str(sent_msg.id)
        elif isinstance(sent_msg, (int, str)) and sent_msg:
            target_id = str(sent_msg)
        else:
            logging.warning(f"{log_prefix} Невозможно получить ID сообщения из sent_msg: {sent_msg}")
            return message_id_map
            
        # Проверка, что target_id не пусто
        if not target_id:
            logging.warning(f"{log_prefix} Пустой ID сообщения в sent_msg: {sent_msg}")
            return message_id_map
            
        sender_id = sender_phone or 'unknown'
        
        # Формируем ключ и значение в новом формате
        target_value = f"{processed_chat_id}:{sender_id}:{target_id}"
        
        # Проверяем, существует ли уже такой ключ в карте
        if source_key in message_id_map:
            existing_value = message_id_map[source_key]
            if existing_value == target_value:
                # Если значение уже существует и такое же, не дублируем запись
                return message_id_map
            else:
                logging.info(f"{log_prefix} Обновление существующего соответствия для {source_key}: {existing_value} -> {target_value}")
        
        # Сохраняем в карту соответствий как строку
        message_id_map[source_key] = target_value
        
        # Сохраняем grouped_id для этой ветки, если он предоставлен
        if grouped_id:
            config['grouped_id_map'][source_key] = grouped_id
            logging.info(f"{log_prefix} Сохранен grouped_id {grouped_id} для ветки {source_key}")
        
        # Информационное логирование
        logging.info(
            f"{log_prefix} Зарегистрировано соответствие ID: source={source_key}, "
            f"target={target_value}, sender={sender_id}, grouped_id={grouped_id}"
        )
    
    return message_id_map

async def process_reply_buffer(client, message, source_chat_id, target_chat_id_or_link, message_id_map, current_phone=None):    
    if not message.reply_to_message:
        return None, None
    
    # Инициализируем словарь chat_id_cache, если он еще не существует
    if 'chat_id_cache' not in config:
        config['chat_id_cache'] = {}
        
    # Разрешаем target_chat_id в его числовом ID
    resolved_target_chat_id = config['chat_id_cache'].get(target_chat_id_or_link)
    if not resolved_target_chat_id:
        try:
            # Получаем chat_id для ссылки
            chat = await client.get_chat(target_chat_id_or_link)
            resolved_target_chat_id = chat.id
            config['chat_id_cache'][target_chat_id_or_link] = resolved_target_chat_id
        except Exception as e:
            logging.error(f"Ошибка получения chat_id: {e}")
            return None, None # Не можем продолжить без действительного ID чата

    original_msg_id = message.reply_to_message.id
    source_key = f"{source_chat_id}:{original_msg_id}"
    
    # Сначала проверяем message_id_map
    if message_id_map and source_key in message_id_map:
        # В новой структуре значение хранится напрямую как строка "chat_id:phone:msg_id"
        target_entry = message_id_map[source_key]
        
        if isinstance(target_entry, str) and ":" in target_entry:
            try:
                # Парсим запись - ожидаемый формат "chat_id:phone:msg_id"
                parts = target_entry.split(":")
                
                # Проверяем наличие всех необходимых частей
                if len(parts) >= 3:
                    target_chat_id = parts[0]
                    sender_phone = parts[1]
                    
                    # Проверяем последнюю часть (msg_id) на пустоту
                    if parts[2].strip():  
                        target_msg_id = int(parts[2])
                    
                        # Проверяем, что целевой чат совпадает с запрошенным
                        if str(target_chat_id) == str(resolved_target_chat_id):
                            # Проверяем видимость сообщения для текущего аккаунта
                            try:
                                message_visible = await client.get_messages(resolved_target_chat_id, message_ids=[target_msg_id])
                                if message_visible and message_visible[0] and hasattr(message_visible[0], 'text'):
                                    # Сообщение существует и имеет текстовое содержимое
                                    return target_msg_id, sender_phone
                                else:
                                    logging.warning(f"Сообщение {target_msg_id} не найдено в чате {resolved_target_chat_id}")
                                    # Возвращаем None вместо пустого сообщения
                                    return None, None
                            except Exception as e:
                                logging.error(f"Ошибка при проверке сообщения {target_msg_id}: {e}")
                        else:
                            logging.warning(f"Целевой чат не совпадает: {target_chat_id} != {resolved_target_chat_id}")
                    else:
                        logging.warning(f"Пустой ID сообщения в записи: {target_entry}")
                else:
                    logging.warning(f"Недостаточно частей в записи: {target_entry}, найдено {len(parts)}")
            except (ValueError, IndexError) as e:
                logging.error(f"Ошибка парсинга записи: {e}")
        else:
            logging.warning(f"Неверный формат записи")
    
    # Если не нашли соответствие в message_id_map, ищем в истории сообщений чата
    
    # Получаем исходный текст сообщения для поиска
    source_text = ""
    if hasattr(message.reply_to_message, 'text') and message.reply_to_message.text:
        source_text = message.reply_to_message.text
    elif hasattr(message.reply_to_message, 'caption') and message.reply_to_message.caption:
        source_text = message.reply_to_message.caption
        
    if not source_text:
        return None, None
    
    # Инициализируем словарь target_chat_history, если он еще не существует
    if 'target_chat_history' not in config:
        config['target_chat_history'] = {}
    
    # Проверяем наличие истории для целевого чата
    if resolved_target_chat_id in config['target_chat_history']:
        history = config['target_chat_history'][resolved_target_chat_id]
        
        # Сначала ищем точное совпадение по тексту
        for msg in history:
            if msg['text'] == source_text:
                target_msg_id = msg['message_id']
                sender_phone = msg['sender_phone']
                return target_msg_id, sender_phone
                
        # Если точного совпадения нет, ищем частичное
        if len(source_text) > 20:
            for msg in history:
                if source_text[:20] in msg['text'] or msg['text'][:20] in source_text:
                    target_msg_id = msg['message_id']
                    sender_phone = msg['sender_phone']
                    return target_msg_id, sender_phone
    return None, None
        
if __name__ == '__main__':
    try:
        print("=" * 50)
        print("Запуск Telegram Cloner Bot...")
        print("=" * 50)
        logging.info("=" * 50)
        logging.info("Запуск Telegram Cloner Bot...")
        logging.info("=" * 50)
        
        # Проверка конфигурации перед запуском
        print(f"Токен бота: {BOT_TOKEN[:20]}...")
        print(f"API ID: {API_ID}")
        print(f"База данных: {DB_PATH}")
        print(f"Директория: {UPLOAD_DIR}")
        logging.info(f"Токен бота: {BOT_TOKEN[:20]}...")
        logging.info(f"API ID: {API_ID}")
        logging.info(f"База данных: {DB_PATH}")
        logging.info(f"Директория: {UPLOAD_DIR}")
        
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        print("\nБот остановлен пользователем")
        logging.info("Бот остановлен")
    except Exception as e:
        print(f"\n❌ КРИТИЧЕСКАЯ ОШИБКА: {e}")
        logging.error(f"Критическая ошибка при запуске: {e}", exc_info=True)
        import traceback
        traceback.print_exc()
        input("\nНажмите Enter для выхода...")
    finally:
        # Закрываем соединение с базой данных
        try:
            if 'db' in globals():
                db.close()
                logging.info("Соединение с базой данных закрыто")
            # Освобождаем другие ресурсы если необходимо
        except Exception as e:
            logging.error(f"Ошибка при закрытии ресурсов: {e}")    