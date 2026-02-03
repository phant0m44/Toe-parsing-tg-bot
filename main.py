import telebot
from telebot import types
import requests
import sqlite3
import time
import threading
import schedule
import logging
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo
from urllib.parse import quote

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

BOT_TOKEN = "Here your token"
DB_FILE = "bot_users.db"

API_BASE = "https://api-poweron.toe.com.ua/api/a_gpv_g"

BASE_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:147.0) Gecko/20100101 Firefox/147.0",
    "Accept": "application/json, text/plain, */*",
    "Referer": "https://poweron.toe.com.ua/",
    "Origin": "https://poweron.toe.com.ua"
}

GROUP_CREDS = {
    "1.1": {"time": "91989283", "key": "OTE5Lzg5MjgvMw=="},
    "1.2": {"time": "1032101411", "key": "MTAzMi8xMDE0MS8x"},
    "2.1": {"time": "13615462", "key": "MTM2LzE1NDYvMg=="},
    "2.2": {"time": "38337572", "key": "MzgzLzM3NTcvMg=="},
    "3.1": {"time": "212953484219", "key": "MjEyOTUvMzQ4NDIvMTk="},
    "3.2": {"time": "113", "key": "MS8xLzM="},
    "4.1": {"time": "10321005821", "key": "MTAzMi8xMDA1OC8yMQ=="},
    "4.2": {"time": "211294218552", "key": "MjExMjkvNDIxODUvNTI="},
    "5.1": {"time": "22064405571", "key": "MjIwNjQvNDA1NTcvMQ=="},
    "5.2": {"time": "22039462178", "key": "MjIwMzkvNDYyMTcvOA=="},
    "6.1": {"time": "227315422", "key": "MjI3LzMxNTQyLzI="},
    "6.2": {"time": "21525365701", "key": "MjE1MjUvMzY1NzAvMQ=="}
}

try:
    KYIV_TZ = ZoneInfo("Europe/Kyiv")
except Exception:
    KYIV_TZ = timezone.utc

bot = telebot.TeleBot(BOT_TOKEN)

schedules_cache = {}
last_sent_alerts = {}

def init_db():
    try:
        with sqlite3.connect(DB_FILE) as conn:
            cursor = conn.cursor()
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS users (
                    user_id INTEGER PRIMARY KEY,
                    group_id TEXT,
                    notifications INTEGER DEFAULT 1
                )
            ''')
            conn.commit()
        logger.info("Database initialized.")
    except Exception as e:
        logger.critical(f"Database error: {e}")

def db_set_group(user_id, group_id):
    try:
        with sqlite3.connect(DB_FILE) as conn:
            cursor = conn.cursor()
            cursor.execute("INSERT OR REPLACE INTO users (user_id, group_id, notifications) VALUES (?, ?, COALESCE((SELECT notifications FROM users WHERE user_id = ?), 1))", (user_id, group_id, user_id))
            conn.commit()
    except Exception as e:
        logger.error(f"Error setting group: {e}")

def db_get_user(user_id):
    try:
        with sqlite3.connect(DB_FILE) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT group_id, notifications FROM users WHERE user_id = ?", (user_id,))
            return cursor.fetchone()
    except Exception:
        return None

def db_toggle_notification(user_id):
    try:
        with sqlite3.connect(DB_FILE) as conn:
            cursor = conn.cursor()
            res = cursor.execute("SELECT notifications FROM users WHERE user_id = ?", (user_id,)).fetchone()
            new_status = 0 if res and res[0] == 1 else 1
            cursor.execute("UPDATE users SET notifications = ? WHERE user_id = ?", (new_status, user_id))
            conn.commit()
            return new_status
    except Exception:
        return 1

def db_get_all_users_with_groups():
    try:
        with sqlite3.connect(DB_FILE) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT user_id, group_id FROM users WHERE notifications = 1")
            return cursor.fetchall()
    except Exception:
        return []

def fetch_full_schedule_data(group_id):
    """
    {
        'today': {times},
        'today_date': 'YYYY-MM-DD',
        'tomorrow': {times} або None,
        'tomorrow_date': 'YYYY-MM-DD'
    }
    """
    utc_now = datetime.now(timezone.utc)
    date_before = (utc_now + timedelta(days=2)).strftime("%Y-%m-%dT00:00:00+00:00")
    date_after = (utc_now - timedelta(days=1)).strftime("%Y-%m-%dT00:00:00+00:00")

    creds = GROUP_CREDS.get(group_id)
    if not creds:
        creds = GROUP_CREDS["3.1"]

    safe_before = quote(date_before)
    safe_after = quote(date_after)
    full_url = f"{API_BASE}?before={safe_before}&after={safe_after}&group[]={group_id}&time={creds['time']}"

    headers = BASE_HEADERS.copy()
    headers["X-debug-key"] = creds["key"]

    try:
        response = requests.get(full_url, headers=headers, timeout=20)
        if response.status_code == 404:
            return None
        response.raise_for_status()
        data = response.json()
        
        items = data.get('hydra:member', [])
        if not items:
            return None

        today_str = datetime.now(KYIV_TZ).strftime("%Y-%m-%d")
        tomorrow_str = (datetime.now(KYIV_TZ) + timedelta(days=1)).strftime("%Y-%m-%d")

        result = {
            'today': None,
            'today_date': today_str,
            'tomorrow': None,
            'tomorrow_date': tomorrow_str
        }

        for item in items:
            date_graph = item.get('dateGraph', '').split('T')[0]

            times = None
            if group_id in item['dataJson']:
                times = item['dataJson'][group_id]['times']
            elif item['dataJson']:
                first_key = list(item['dataJson'].keys())[0]
                times = item['dataJson'][first_key]['times']

            if date_graph == today_str:
                result['today'] = times
            elif date_graph == tomorrow_str:
                result['tomorrow'] = times


        return result

    except Exception as e:
        logger.error(f"Request error for {group_id}: {e}")
        return None

def notify_users(group_id, all_users, schedule_data, is_tomorrow=False):
    target_users = [u_id for u_id, g_id in all_users if g_id == group_id]
    if not target_users: return

    date_label = schedule_data['tomorrow_date'] if is_tomorrow else schedule_data['today_date']
    times = schedule_data['tomorrow'] if is_tomorrow else schedule_data['today']
    
    if not times: return

    formatted_schedule = format_schedule_list(times)
    
    if is_tomorrow:
        header = f"⚡️ <b>З'явився графік на ЗАВТРА ({date_label})!</b>"
    else:
        header = f"⚠️ <b>Увага! Графік на СЬОГОДНІ ({date_label}) змінено!</b>"

    msg_text = f"{header}\n\n{formatted_schedule}"

    logger.info(f"Sending notification to group {group_id} (Tomorrow: {is_tomorrow})")

    for user_id in target_users:
        try:
            bot.send_message(user_id, msg_text, parse_mode="HTML")
        except Exception:
            pass

def update_all_schedules():
    logger.info("Updating schedules...")
    groups = [f"{i}.{j}" for i in range(1, 7) for j in range(1, 3)]
    all_users = db_get_all_users_with_groups()
    
    for gr in groups:
        new_data = fetch_full_schedule_data(gr)
        if not new_data:
            continue
        
        old_data = schedules_cache.get(gr)
        schedules_cache[gr] = new_data

        if old_data is None:
            continue

        if new_data['today'] and new_data['today'] != old_data.get('today'):
            notify_users(gr, all_users, new_data, is_tomorrow=False)

        if new_data['tomorrow']:
            if old_data.get('tomorrow') is None or new_data['tomorrow'] != old_data.get('tomorrow'):
                notify_users(gr, all_users, new_data, is_tomorrow=True)

STATUS_MAP = {
    "0": ("🟢", "Є світло"),
    "1": ("🔴", "НЕМАЄ світла"),
    "10": ("🟡", "Можливе вимкнення")
}

def get_status_text_full(code):
    icon, text = STATUS_MAP.get(code, ("❓", "Невідомо"))
    return f"{icon} — {text}"

def format_schedule_list(schedule):
    lines = []
    for h in range(12):
        for m in (0, 30):
            t1 = f"{h:02}:{m:02}"
            t2 = f"{h+12:02}:{m:02}"
            
            c1 = schedule.get(t1, "?")
            c2 = schedule.get(t2, "?")
            
            i1 = STATUS_MAP.get(c1, ("❓",))[0]
            i2 = STATUS_MAP.get(c2, ("❓",))[0]
            
            lines.append(f"{t1} {i1}    {t2} {i2}")
            
    legend = "\n\n🟢 — Є світло\n🟡 — Можливе вимкнення\n🔴 — Немає світла"
    return "\n".join(lines) + legend

def get_current_status_message(schedule):
    now = datetime.now(KYIV_TZ)
    minute = 30 if now.minute >= 30 else 0
    current_slot = f"{now.hour:02}:{minute:02}"
    
    current_code = schedule.get(current_slot, "0")
    
    times = sorted(list(schedule.keys()))
    try:
        start_index = times.index(current_slot)
    except ValueError:
        return "⚠️ Не вдалося визначити поточний час у графіку."

    end_slot = "24:00"
    for i in range(start_index + 1, len(times)):
        next_time = times[i]
        if schedule[next_time] != current_code:
            end_slot = next_time
            break

    icon, text = STATUS_MAP.get(current_code, ("❓", "Невідомо"))
    
    msg = f"Зараз ({current_slot}): {icon} <b>{text}</b>\n"
    if current_code == "0":
        msg += f"Світло БУДЕ з {current_slot} по {end_slot}"
    elif current_code == "1":
        msg += f"Світла НЕ БУДЕ з {current_slot} по {end_slot}"
    else:
        msg += f"МОЖЛИВЕ відключення з {current_slot} по {end_slot}"
    return msg

def main_menu_kb():
    markup = types.ReplyKeyboardMarkup(resize_keyboard=True)
    markup.row("📅 Отримати графік", "💡 Стан")
    markup.row("⚙️ Налаштування")
    return markup

def groups_kb():
    markup = types.InlineKeyboardMarkup()
    buttons = []
    for i in range(1, 7):
        row = []
        for j in range(1, 3):
            group_code = f"{i}.{j}"
            row.append(types.InlineKeyboardButton(group_code, callback_data=f"set_group_{group_code}"))
        buttons.append(row)
    for row in buttons:
        markup.row(*row)
    return markup

@bot.message_handler(commands=['start'])
def send_welcome(message):
    init_db()
    try:
        user_data = db_get_user(message.chat.id)
        if user_data:
            bot.send_message(message.chat.id, f"Вітаю! Твоя група: {user_data[0]}", reply_markup=main_menu_kb())
        else:
            bot.send_message(message.chat.id, "Оберіть свою чергу (групу):", reply_markup=groups_kb())
    except Exception as e:
        logger.error(f"Start error: {e}")

@bot.callback_query_handler(func=lambda call: call.data.startswith('set_group_'))
def callback_set_group(call):
    try:
        group_id = call.data.split('_')[2]
        db_set_group(call.message.chat.id, group_id)
        bot.answer_callback_query(call.id, "Групу збережено!")
        bot.send_message(call.message.chat.id, f"✅ Ви обрали групу {group_id}.", reply_markup=main_menu_kb())

        if group_id not in schedules_cache:
            data = fetch_full_schedule_data(group_id)
            if data: schedules_cache[group_id] = data
    except Exception as e:
        logger.error(f"Set group error: {e}")

@bot.callback_query_handler(func=lambda call: call.data.startswith('show_tomorrow_'))
def callback_show_tomorrow(call):
    try:
        user_data = db_get_user(call.message.chat.id)
        if not user_data: return
        
        group_id = user_data[0]
        cached = schedules_cache.get(group_id)
        
        if not cached:
            cached = fetch_full_schedule_data(group_id)
            
        if cached and cached.get('tomorrow'):
            text = format_schedule_list(cached['tomorrow'])
            bot.send_message(call.message.chat.id, f"📅 <b>Графік на ЗАВТРА ({cached['tomorrow_date']}):</b>\n\n{text}", parse_mode="HTML")
            bot.answer_callback_query(call.id)
        else:
            bot.answer_callback_query(call.id, "Графік на завтра ще не доступний")
    except Exception:
        pass

@bot.message_handler(func=lambda message: message.text == "📅 Отримати графік")
def send_schedule(message):
    try:
        user_data = db_get_user(message.chat.id)
        if not user_data:
            return bot.send_message(message.chat.id, "Спочатку натисніть /start")
        
        group_id = user_data[0]
        cached = schedules_cache.get(group_id)
        
        if not cached:
            cached = fetch_full_schedule_data(group_id)
            if cached: schedules_cache[group_id] = cached
        
        if cached and cached.get('today'):
            text = format_schedule_list(cached['today'])

            markup = None
            if cached.get('tomorrow'):
                markup = types.InlineKeyboardMarkup()
                markup.add(types.InlineKeyboardButton(f"➡️ Графік на Завтра ({cached['tomorrow_date']})", callback_data=f"show_tomorrow_{group_id}"))
            
            bot.send_message(message.chat.id, f"📅 <b>Графік на СЬОГОДНІ ({cached['today_date']}):</b>\n\n{text}", parse_mode="HTML", reply_markup=markup)
        else:
            bot.send_message(message.chat.id, "❌ Графік на сьогодні зараз недоступний або не знайдений.")
    except Exception as e:
        logger.error(f"Send schedule error: {e}")

@bot.message_handler(func=lambda message: message.text == "💡 Стан")
def send_status(message):
    try:
        user_data = db_get_user(message.chat.id)
        if not user_data: 
            return bot.send_message(message.chat.id, "Спочатку виберіть групу через /start")
        
        group_id = user_data[0]
        cached = schedules_cache.get(group_id)
        
        if not cached:
            cached = fetch_full_schedule_data(group_id)
            if cached: schedules_cache[group_id] = cached
        
        if cached and cached.get('today'):
            text = get_current_status_message(cached['today'])
            bot.send_message(message.chat.id, text, parse_mode="HTML")
        else:
            bot.send_message(message.chat.id, "Дані відсутні.")
    except Exception:
        pass

@bot.message_handler(func=lambda message: message.text == "⚙️ Налаштування")
def settings(message):
    try:
        user_data = db_get_user(message.chat.id)
        if not user_data: return bot.send_message(message.chat.id, "/start")

        notif_status = "Увімкнено ✅" if user_data[1] else "Вимкнено 🔕"
        markup = types.InlineKeyboardMarkup()
        markup.add(types.InlineKeyboardButton(f"Сповіщення: {notif_status}", callback_data="toggle_notif"))
        markup.add(types.InlineKeyboardButton("Змінити групу", callback_data="change_group"))
        
        bot.send_message(message.chat.id, f"Ваша група: {user_data[0]}\nНалаштування:", reply_markup=markup)
    except Exception:
        pass

@bot.callback_query_handler(func=lambda call: call.data == "change_group")
def change_group_btn(call):
    bot.send_message(call.message.chat.id, "Виберіть нову групу:", reply_markup=groups_kb())

@bot.callback_query_handler(func=lambda call: call.data == "toggle_notif")
def toggle_notifications(call):
    new_status = db_toggle_notification(call.message.chat.id)
    text = "Увімкнено ✅" if new_status else "Вимкнено 🔕"
    markup = types.InlineKeyboardMarkup()
    markup.add(types.InlineKeyboardButton(f"Сповіщення: {text}", callback_data="toggle_notif"))
    markup.add(types.InlineKeyboardButton("Змінити групу", callback_data="change_group"))
    bot.edit_message_reply_markup(call.message.chat.id, call.message.message_id, reply_markup=markup)

def check_upcoming_changes():
    try:
        now = datetime.now(KYIV_TZ)
        if not (28 <= now.minute <= 32 or 58 <= now.minute <= 59 or 0 <= now.minute <= 2):
            return

        future_time = now + timedelta(minutes=30)
        minute_str = "30" if future_time.minute >= 30 else "00"
        check_slot = f"{future_time.hour:02}:{minute_str}"
        current_slot = f"{now.hour:02}:{minute_str}"

        users = db_get_all_users_with_groups()
        for user_id, group_id in users:
            if last_sent_alerts.get(user_id) == check_slot: continue
            
            cached = schedules_cache.get(group_id)
            if not cached or not cached.get('today'): continue
            
            schedule_data = cached['today']
            current_status = schedule_data.get(current_slot)
            next_status = schedule_data.get(check_slot)
            
            if current_status is None or next_status is None or current_status == next_status:
                continue
                
            msg = ""
            if next_status == "0": msg = f"🟢 Через 30 хв ({check_slot}) Буде світло!"
            elif next_status == "1": msg = f"🔴 Через 30 хв ({check_slot}) вимкнення світла!"
            elif next_status == "10": msg = f"🟡 Через 30 хв ({check_slot}) можливе вимкнення."
            
            try:
                bot.send_message(user_id, msg)
                last_sent_alerts[user_id] = check_slot
            except Exception:
                pass
    except Exception as e:
        logger.error(f"Check alerts error: {e}")

def scheduler_loop():
    schedule.every(15).minutes.do(update_all_schedules)
    schedule.every(1).minutes.do(check_upcoming_changes)
    update_all_schedules()
    while True:
        try:
            schedule.run_pending()
            time.sleep(1)
        except Exception as e:
            logger.error(f"Scheduler error: {e}")
            time.sleep(5)

if __name__ == "__main__":
    init_db()
    t = threading.Thread(target=scheduler_loop)
    t.start()
    logger.info("Bot started!")
    while True:
        try:
            bot.infinity_polling(timeout=10, long_polling_timeout=5)
        except Exception as e:
            logger.critical(f"Bot crash: {e}. Restarting...")
            time.sleep(5)