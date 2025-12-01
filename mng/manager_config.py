import os

MANAGER_BOT_TOKEN = "8497411395:AAGviCXYoizTvb9eSOjDa7T9Fn5LiGTeb64"

ADMIN_ID = 6995119648

CRYPTOBOT_TOKEN = "480256:AAR2xctdYFhiWPC5UhcJTWqqkJDvvgZi5Pu"

PAYMENT_AMOUNT = 0.0

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

if os.name == 'nt':
    CLONER_DIR = BASE_DIR
else:
    CLONER_DIR = "/root/cloner"

if os.name == 'nt':
    MIRROR_BASE_DIR = BASE_DIR
else:
    MIRROR_BASE_DIR = "/root/cloner"

if os.name == 'nt':
    MANAGER_DIR = os.path.join(BASE_DIR, "mng")
else:
    MANAGER_DIR = "/root/cloner/mng"

