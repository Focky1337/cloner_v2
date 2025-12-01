import os

API_ID = 24670035
API_HASH = "f5f000a0f88b93ee5abea430945a94c8"

BOT_TOKEN = "8081550105:AAF6Qmg4hVqjdA5CIDDQBgRUL9Mxuwn6D8A"

ADMIN_IDS = [6995119648, 7238926883]

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
UPLOAD_DIR = os.path.join(BASE_DIR, "DataBase")
os.makedirs(UPLOAD_DIR, exist_ok=True)

DB_PATH = os.path.join(UPLOAD_DIR, 'sessions.db')

DELAY_SECONDS = 3

MAX_FILE_SIZE = 10 * 1024 * 1024

WEB_PANEL_URL = "http://localhost:5000"

BOT_USERNAME = None

