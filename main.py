import telebot
from telebot import types
import requests
import sqlite3
import time
import threading
import schedule
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

BOT_TOKEN = "Your telegram bot token"
DB_FILE = "bot_users.db"

API_URL = "https://api-toe-poweron.inneti.net/api/a_gpv_g"
BASE_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:147.0) Gecko/20100101 Firefox/147.0",
    "Referer": "https://toe-poweron.inneti.net/"
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

KYIV_TZ = ZoneInfo("Europe/Kyiv")

bot = telebot.TeleBot(BOT_TOKEN)

schedules_cache = {}
last_sent_alerts = {}

def init_db():
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

def db_set_group(user_id, group_id):
    with sqlite3.connect(DB_FILE) as conn:
        cursor = conn.cursor()
        cursor.execute("INSERT OR REPLACE INTO users (user_id, group_id, notifications) VALUES (?, ?, COALESCE((SELECT notifications FROM users WHERE user_id = ?), 1))", (user_id, group_id, user_id))
        conn.commit()

def db_get_user(user_id):
    with sqlite3.connect(DB_FILE) as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT group_id, notifications FROM users WHERE user_id = ?", (user_id,))
        return cursor.fetchone()

def db_toggle_notification(user_id):
    with sqlite3.connect(DB_FILE) as conn:
        cursor = conn.cursor()
        res = cursor.execute("SELECT notifications FROM users WHERE user_id = ?", (user_id,)).fetchone()
        new_status = 0 if res and res[0] == 1 else 1
        cursor.execute("UPDATE users SET notifications = ? WHERE user_id = ?", (new_status, user_id))
        conn.commit()
        return new_status

def db_get_all_users_with_groups():
    with sqlite3.connect(DB_FILE) as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT user_id, group_id FROM users WHERE notifications = 1")
        return cursor.fetchall()

def fetch_schedule_for_group(group_id):
    utc_now = datetime.now(timezone.utc)
    date_before = (utc_now + timedelta(days=1)).strftime("%Y-%m-%dT00:00:00+00:00")
    date_after = (utc_now - timedelta(days=1)).strftime("%Y-%m-%dT00:00:00+00:00")

    creds = GROUP_CREDS.get(group_id)
    if not creds:
        return None

    params = {
        "before": date_before,
        "after": date_after,
        "group[]": group_id, 
        "time": creds["time"]
    }

    headers = BASE_HEADERS.copy()
    headers["X-debug-key"] = creds["key"]

    try:
        response = requests.get(API_URL, params=params, headers=headers, timeout=10)
        response.raise_for_status()
        data = response.json()
        
        if not data.get('hydra:member'):
            return None

        schedule_block = data['hydra:member'][0]
        
        if group_id in schedule_block['dataJson']:
            return schedule_block['dataJson'][group_id]['times']
        else:
            return schedule_block['dataJson'].get(group_id, {}).get('times')

    except Exception:
        return None

def notify_users_about_change(group_id, all_users):
    target_users = [u_id for u_id, g_id in all_users if g_id == group_id]
    
    if not target_users:
        return

    markup = types.InlineKeyboardMarkup()
    markup.add(types.InlineKeyboardButton("📊 Показати графік", callback_data="show_current_schedule"))

    for user_id in target_users:
        try:
            bot.send_message(
                user_id, 
                f"⚠️ <b>Увага! Графік для групи {group_id} було змінено!</b>", 
                parse_mode="HTML", 
                reply_markup=markup
            )
        except Exception:
            pass

def update_all_schedules():
    groups = [f"{i}.{j}" for i in range(1, 7) for j in range(1, 3)]
    all_users = db_get_all_users_with_groups()
    
    for gr in groups:
        new_data = fetch_schedule_for_group(gr)
        if not new_data:
            continue
            
        old_data = schedules_cache.get(gr)
        schedules_cache[gr] = new_data

        if old_data is not None and old_data != new_data:
            notify_users_about_change(gr, all_users)

STATUS_MAP = {
    "0": ("🟢", "Є світло"),
    "1": ("🔴", "НЕМАЄ світла"),
    "10": ("🟡", "Можливе вимкнення")
}

def get_status_text_full(code):
    icon, text = STATUS_MAP.get(code, ("❓", "Невідомо"))
    if code == "10": return f"{icon} — Можливе вимкнення"
    return f"{icon} — {text}"

def format_schedule_list(schedule):
    lines = []
    for time_slot in sorted(schedule.keys()):
        code = schedule[time_slot]
        lines.append(f"{get_status_text_full(code).split(' — ')[0]} {time_slot} — {get_status_text_full(code).split(' — ')[1]}")
    return "\n".join(lines)

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
    user_data = db_get_user(message.chat.id)
    if user_data:
        bot.send_message(message.chat.id, f"Вітаю! Твоя група: {user_data[0]}", reply_markup=main_menu_kb())
    else:
        bot.send_message(message.chat.id, "Оберіть свою чергу (групу):", reply_markup=groups_kb())

@bot.callback_query_handler(func=lambda call: call.data.startswith('set_group_'))
def callback_set_group(call):
    group_id = call.data.split('_')[2]
    db_set_group(call.message.chat.id, group_id)
    bot.answer_callback_query(call.id, "Групу збережено!")
    bot.send_message(call.message.chat.id, f"✅ Ви обрали групу {group_id}.", reply_markup=main_menu_kb())
    
    if group_id not in schedules_cache:
        data = fetch_schedule_for_group(group_id)
        if data: schedules_cache[group_id] = data

@bot.callback_query_handler(func=lambda call: call.data == "show_current_schedule")
def callback_show_schedule(call):
    user_data = db_get_user(call.message.chat.id)
    if not user_data:
        return bot.answer_callback_query(call.id, "Група не обрана")
    
    group_id = user_data[0]
    schedule_data = schedules_cache.get(group_id)
    
    if not schedule_data:
        schedule_data = fetch_schedule_for_group(group_id)
    
    if schedule_data:
        text = format_schedule_list(schedule_data)
        bot.send_message(call.message.chat.id, f"📅 <b>Оновлений графік ({group_id}):</b>\n\n{text}", parse_mode="HTML")
        bot.answer_callback_query(call.id)
    else:
        bot.answer_callback_query(call.id, "Графік недоступний")

@bot.message_handler(func=lambda message: message.text == "📅 Отримати графік")
def send_schedule(message):
    user_data = db_get_user(message.chat.id)
    if not user_data:
        return bot.send_message(message.chat.id, "Спочатку виберіть групу через /start")
    
    group_id = user_data[0]
    schedule_data = schedules_cache.get(group_id)
    
    if not schedule_data:
        schedule_data = fetch_schedule_for_group(group_id)
        if schedule_data: schedules_cache[group_id] = schedule_data
    
    if schedule_data:
        text = format_schedule_list(schedule_data)
        bot.send_message(message.chat.id, f"📅 <b>Графік для групи {group_id}:</b>\n\n{text}", parse_mode="HTML")
    else:
        bot.send_message(message.chat.id, f"❌ На жаль, графік для групи {group_id} зараз недоступний.")

@bot.message_handler(func=lambda message: message.text == "💡 Стан")
def send_status(message):
    user_data = db_get_user(message.chat.id)
    if not user_data: 
        return bot.send_message(message.chat.id, "Спочатку виберіть групу через /start")
    
    group_id = user_data[0]
    schedule_data = schedules_cache.get(group_id) or fetch_schedule_for_group(group_id)
    
    if schedule_data:
        text = get_current_status_message(schedule_data)
        bot.send_message(message.chat.id, text, parse_mode="HTML")
    else:
        bot.send_message(message.chat.id, "Дані відсутні або графік недоступний.")

@bot.message_handler(func=lambda message: message.text == "⚙️ Налаштування")
def settings(message):
    user_data = db_get_user(message.chat.id)
    
    if not user_data:
        bot.send_message(message.chat.id, "❌ Помилка: користувача не знайдено. Будь ласка, натисніть /start")
        return

    notif_status = "Увімкнено ✅" if user_data[1] else "Вимкнено 🔕"
    
    markup = types.InlineKeyboardMarkup()
    markup.add(types.InlineKeyboardButton(f"Сповіщення: {notif_status}", callback_data="toggle_notif"))
    markup.add(types.InlineKeyboardButton("Змінити групу", callback_data="change_group"))
    
    bot.send_message(message.chat.id, f"Ваша група: {user_data[0]}\nНалаштування:", reply_markup=markup)

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
    now = datetime.now(KYIV_TZ)
    
    if not (28 <= now.minute <= 32 or 58 <= now.minute <= 59 or 0 <= now.minute <= 2):
        return

    future_time = now + timedelta(minutes=30)
    minute_str = "30" if future_time.minute >= 30 else "00"
    check_slot = f"{future_time.hour:02}:{minute_str}"
    
    prev_minute_str = "30" if now.minute >= 30 else "00"
    current_slot = f"{now.hour:02}:{prev_minute_str}"

    users = db_get_all_users_with_groups()
    
    for user_id, group_id in users:
        if last_sent_alerts.get(user_id) == check_slot:
            continue
            
        schedule_data = schedules_cache.get(group_id)
        if not schedule_data: continue
        
        current_status = schedule_data.get(current_slot)
        next_status = schedule_data.get(check_slot)
        
        if current_status is None or next_status is None or current_status == next_status:
            continue
            
        msg = ""
        if next_status == "0":
            msg = f"🟢 Через 30 хв ({check_slot}) Буде світло!"
        elif next_status == "1":
            msg = f"🔴 Через 30 хв ({check_slot}) вимкнення світла!"
        elif next_status == "10":
            msg = f"🟡 Через 30 хв ({check_slot}) можливе вимкнення світла."
            
        try:
            bot.send_message(user_id, msg)
            last_sent_alerts[user_id] = check_slot
        except Exception:
            pass

def scheduler_loop():
    schedule.every(15).minutes.do(update_all_schedules)
    schedule.every(1).minutes.do(check_upcoming_changes)
    
    update_all_schedules()
    
    while True:
        schedule.run_pending()
        time.sleep(1)

if __name__ == "__main__":
    init_db()
    t = threading.Thread(target=scheduler_loop)
    t.start()
    bot.infinity_polling()