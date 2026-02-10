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
from bs4 import BeautifulSoup
import re

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

BOT_TOKEN = "Your token here"
DB_FILE = "bot_users.db"
ADMIN_ID = 5292087312

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

ODESA_URL = "https://alerts.org.ua/odeska-oblast/"
ODESA_IMAGE_BASE = "https://alerts.org.ua/app/8/_cache/_graph/{date}/_8.jpg"
ODESA_GROUPS = [f"{i}.{j}" for i in range(1, 7) for j in range(1, 3)]

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
                    region TEXT DEFAULT 'ternopil',
                    group_id TEXT,
                    notifications INTEGER DEFAULT 1
                )
            ''')
            try:
                cursor.execute("ALTER TABLE users ADD COLUMN region TEXT DEFAULT 'ternopil'")
                cursor.execute("UPDATE users SET region = 'ternopil' WHERE region IS NULL")
                conn.commit()
                logger.info("Migrated: added region column, all existing users → ternopil")
            except sqlite3.OperationalError:
                pass
            conn.commit()
        logger.info("Database initialized.")
    except Exception as e:
        logger.critical(f"Database error: {e}")

def db_set_region(user_id, region):
    try:
        with sqlite3.connect(DB_FILE) as conn:
            cursor = conn.cursor()
            cursor.execute(
                "INSERT OR IGNORE INTO users (user_id, region, notifications) VALUES (?, ?, 1)",
                (user_id, region)
            )
            cursor.execute(
                "UPDATE users SET region = ?, group_id = NULL WHERE user_id = ?",
                (region, user_id)
            )
            conn.commit()
    except Exception as e:
        logger.error(f"Error setting region: {e}")

def db_set_group(user_id, group_id):
    try:
        with sqlite3.connect(DB_FILE) as conn:
            cursor = conn.cursor()
            cursor.execute(
                "INSERT OR REPLACE INTO users (user_id, region, group_id, notifications) "
                "VALUES (?, COALESCE((SELECT region FROM users WHERE user_id=?),'ternopil'), ?, "
                "COALESCE((SELECT notifications FROM users WHERE user_id=?), 1))",
                (user_id, user_id, group_id, user_id)
            )
            conn.commit()
    except Exception as e:
        logger.error(f"Error setting group: {e}")

def db_get_user(user_id):
    try:
        with sqlite3.connect(DB_FILE) as conn:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT region, group_id, notifications FROM users WHERE user_id = ?",
                (user_id,)
            )
            return cursor.fetchone()
    except Exception:
        return None

def db_toggle_notification(user_id):
    try:
        with sqlite3.connect(DB_FILE) as conn:
            cursor = conn.cursor()
            res = cursor.execute(
                "SELECT notifications FROM users WHERE user_id = ?", (user_id,)
            ).fetchone()
            new_status = 0 if res and res[0] == 1 else 1
            cursor.execute(
                "UPDATE users SET notifications = ? WHERE user_id = ?",
                (new_status, user_id)
            )
            conn.commit()
            return new_status
    except Exception:
        return 1

def db_get_all_users_with_groups():
    try:
        with sqlite3.connect(DB_FILE) as conn:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT user_id, region, group_id FROM users "
                "WHERE notifications = 1 AND group_id IS NOT NULL"
            )
            return cursor.fetchall()
    except Exception:
        return []

def fetch_ternopil_schedule(group_id):
    utc_now = datetime.now(timezone.utc)
    date_before = (utc_now + timedelta(days=2)).strftime("%Y-%m-%dT00:00:00+00:00")
    date_after  = (utc_now - timedelta(days=1)).strftime("%Y-%m-%dT00:00:00+00:00")
    creds = GROUP_CREDS.get(group_id, GROUP_CREDS["3.1"])
    safe_before = quote(date_before)
    safe_after  = quote(date_after)
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
        today_str    = datetime.now(KYIV_TZ).strftime("%Y-%m-%d")
        tomorrow_str = (datetime.now(KYIV_TZ) + timedelta(days=1)).strftime("%Y-%m-%d")
        result = {
            'today': None, 'today_date': today_str,
            'tomorrow': None, 'tomorrow_date': tomorrow_str
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
        logger.error(f"Ternopil request error for {group_id}: {e}")
        return None

WEB_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:147.0) Gecko/20100101 Firefox/147.0",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "uk,en;q=0.5",
}

def parse_time_to_minutes(t: str) -> int:
    t = t.strip()
    h, m = t.split(":")
    return int(h) * 60 + int(m)

def fetch_odesa_schedule(group_id):
    today_str    = datetime.now(KYIV_TZ).strftime("%Y-%m-%d")
    tomorrow_str = (datetime.now(KYIV_TZ) + timedelta(days=1)).strftime("%Y-%m-%d")
    result = {
        'today': None, 'today_date': today_str,
        'tomorrow': None, 'tomorrow_date': tomorrow_str,
        'image_url': None
    }
    def _parse_page(url):
        try:
            resp = requests.get(url, headers=WEB_HEADERS, timeout=15)
            resp.raise_for_status()
            return BeautifulSoup(resp.text, "html.parser")
        except Exception as e:
            logger.error(f"Odesa fetch error {url}: {e}")
            return None
    def _extract_times_for_group(soup, group_id):
        major, minor = group_id.split(".")
        attr = f"r8g{major}-{minor}"
        group_div = soup.find(attrs={"data-group-id": attr})
        if not group_div:
            for b_tag in soup.find_all("b", class_="group-name"):
                if b_tag.get_text(strip=True) == f"Група {group_id}":
                    group_div = b_tag.find_parent(class_="js-group") or b_tag.find_parent()
                    break
        if not group_div:
            return None
        period_div = group_div.find(class_="period")
        if not period_div:
            return None
        slots = {}
        for slot_div in period_div.find_all("div", recursive=False):
            start_raw = slot_div.get("data-start", "").strip()
            end_raw   = slot_div.get("data-end", "").strip()
            if not start_raw or not end_raw:
                continue
            b_tag = slot_div.find("b")
            if not b_tag:
                continue
            status_text = b_tag.get_text(strip=True).upper()
            code = "0" if status_text == "ON" else "1"
            try:
                start_m = parse_time_to_minutes(start_raw)
                end_m   = parse_time_to_minutes(end_raw) if end_raw != "24:00" else 24 * 60
            except Exception:
                continue
            cur = start_m
            while cur < end_m:
                h = cur // 60
                m = cur % 60
                slot_key = f"{h:02}:{m:02}"
                slots[slot_key] = code
                cur += 30
        return slots if slots else None
    soup_today = _parse_page(ODESA_URL)
    if soup_today:
        result['today'] = _extract_times_for_group(soup_today, group_id)
        img_tag = soup_today.find("img", src=re.compile(r"_cache/_graph"))
        if img_tag:
            src = img_tag.get("src", "")
            result['image_url'] = src if src.startswith("http") else f"https://alerts.org.ua{src}"
    tomorrow_url = f"https://alerts.org.ua/odeska-oblast/{tomorrow_str}.html"
    soup_tomorrow = _parse_page(tomorrow_url)
    if soup_tomorrow:
        result['tomorrow'] = _extract_times_for_group(soup_tomorrow, group_id)
    return result

def get_odesa_image_url(date_str=None):
    if not date_str:
        date_str = datetime.now(KYIV_TZ).strftime("%Y-%m-%d")
    return ODESA_IMAGE_BASE.format(date=date_str)

def get_cache_key(region, group_id):
    return f"{region}_{group_id}"

def fetch_schedule(region, group_id):
    if region == "ternopil":
        return fetch_ternopil_schedule(group_id)
    else:
        return fetch_odesa_schedule(group_id)

def get_cached_schedule(region, group_id):
    key = get_cache_key(region, group_id)
    cached = schedules_cache.get(key)
    if not cached:
        cached = fetch_schedule(region, group_id)
        if cached:
            schedules_cache[key] = cached
    return cached

STATUS_MAP = {
    "0":  ("🟢", "Є світло"),
    "1":  ("🔴", "НЕМАЄ світла"),
    "10": ("🟡", "Можливе вимкнення")
}

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
    
    # 1. Заповнюємо можливі пропуски в графіку, щоб була повна доба
    full_schedule = {}
    for h in range(24):
        for m in (0, 30):
            k = f"{h:02}:{m:02}"
            full_schedule[k] = schedule.get(k, "10") # 10 (сірий) якщо даних немає

    times = sorted(list(full_schedule.keys()))
    current_code = full_schedule.get(current_slot, "10")
    
    # Визначаємо поточний стан: Світло є (0, 10) або Немає (1)
    is_light_now = current_code in ["0", "10"]

    try:
        curr_idx = times.index(current_slot)
    except ValueError:
        return "⚠️ Не вдалося визначити час."

    # 2. Шукаємо ПОЧАТОК цього стану (йдемо назад)
    start_idx = curr_idx
    while start_idx > 0:
        prev_slot = times[start_idx - 1]
        prev_code = full_schedule[prev_slot]
        prev_is_light = prev_code in ["0", "10"]
        
        if prev_is_light != is_light_now:
            break # Знайшли зміну статусу
        start_idx -= 1
    
    start_slot = times[start_idx]

    # 3. Шукаємо КІНЕЦЬ цього стану (йдемо вперед)
    end_idx = curr_idx
    end_slot = "24:00"
    
    while end_idx < len(times) - 1:
        next_slot = times[end_idx + 1]
        next_code = full_schedule[next_slot]
        next_is_light = next_code in ["0", "10"]
        
        if next_is_light != is_light_now:
            end_slot = next_slot # Це початок іншого статусу = кінець нашого
            break
        end_idx += 1
        
    # 4. Рахуємо загальну тривалість (Кінець - Початок)
    def time_to_minutes(t_str):
        if t_str == "24:00": return 24 * 60
        h, m = map(int, t_str.split(':'))
        return h * 60 + m

    total_minutes = time_to_minutes(end_slot) - time_to_minutes(start_slot)
    
    hours = total_minutes // 60
    mins = total_minutes % 60
    
    duration_str = f"{hours} год"
    if mins > 0:
        duration_str += f" {mins} хв"

    # 5. Формуємо повідомлення
    icon = STATUS_MAP.get(current_code, "❓")[0] # Беремо тільки іконку
    text_status = "Невідомо"
    
    if current_code == "0": text_status = "Є світло"
    elif current_code == "1": text_status = "НЕМАЄ світла"
    elif current_code == "10": text_status = "Можливе відключення"

    msg = f"Зараз ({current_slot}): {icon} <b>{text_status}</b>\n"
    
    if is_light_now:
        msg += f"Світло БУДЕ з {start_slot} по {end_slot}\n"
    else:
        msg += f"Світла НЕ БУДЕ з {start_slot} по {end_slot}\n"
        
    msg += f"⏱ Всього: <b>{duration_str}</b>"
    
    return msg

REGION_NAMES = {
    "ternopil": "🏔 Тернопільська",
    "odesa":    "🌊 Одеська"
}

def main_menu_kb():
    markup = types.ReplyKeyboardMarkup(resize_keyboard=True)
    markup.row("📅 Отримати графік", "💡 Стан")
    markup.row("⚙️ Налаштування")
    return markup

def region_kb():
    markup = types.InlineKeyboardMarkup()
    markup.row(
        types.InlineKeyboardButton("🏔 Тернопільська область", callback_data="set_region_ternopil")
    )
    markup.row(
        types.InlineKeyboardButton("🌊 Одеська область", callback_data="set_region_odesa")
    )
    return markup

def groups_kb(region="ternopil"):
    markup = types.InlineKeyboardMarkup()
    for i in range(1, 7):
        row = []
        for j in range(1, 3):
            code = f"{i}.{j}"
            row.append(
                types.InlineKeyboardButton(
                    f"Група {code}",
                    callback_data=f"set_group_{code}"
                )
            )
        markup.row(*row)
    return markup

@bot.message_handler(commands=['start'])
def send_welcome(message):
    init_db()
    try:
        user_data = db_get_user(message.chat.id)
        if user_data and user_data[1]:
            region, group_id, _ = user_data
            rname = REGION_NAMES.get(region, region)
            bot.send_message(
                message.chat.id,
                f"Вітаю! 👋\n"
                f"Ваша область: <b>{rname}</b>\n"
                f"Ваша група: <b>{group_id}</b>\n\n"
                f"Для зміни — скористайтесь <b>⚙️ Налаштування</b>",
                reply_markup=main_menu_kb(),
                parse_mode="HTML"
            )
        else:
            bot.send_message(
                message.chat.id,
                "👋 Вітаю!\n\nСпочатку оберіть вашу <b>область</b>:",
                reply_markup=region_kb(),
                parse_mode="HTML"
            )
    except Exception as e:
        logger.error(f"Start error: {e}")

@bot.callback_query_handler(func=lambda call: call.data.startswith('set_region_'))
def callback_set_region(call):
    try:
        region = call.data.replace('set_region_', '')
        db_set_region(call.message.chat.id, region)
        rname = REGION_NAMES.get(region, region)
        bot.answer_callback_query(call.id, f"Обрано: {rname}")
        bot.send_message(
            call.message.chat.id,
            f"✅ Область: <b>{rname}</b>\n\nТепер оберіть вашу <b>групу</b>:",
            reply_markup=groups_kb(region),
            parse_mode="HTML"
        )
    except Exception as e:
        logger.error(f"Set region error: {e}")

@bot.callback_query_handler(func=lambda call: call.data.startswith('set_group_'))
def callback_set_group(call):
    try:
        group_id = call.data.replace('set_group_', '')
        db_set_group(call.message.chat.id, group_id)
        user_data = db_get_user(call.message.chat.id)
        region = user_data[0] if user_data else "ternopil"
        rname = REGION_NAMES.get(region, region)
        bot.answer_callback_query(call.id, "Групу збережено!")
        bot.send_message(
            call.message.chat.id,
            f"✅ Область: <b>{rname}</b>\nГрупа: <b>{group_id}</b>",
            reply_markup=main_menu_kb(),
            parse_mode="HTML"
        )
        key = get_cache_key(region, group_id)
        if key not in schedules_cache:
            data = fetch_schedule(region, group_id)
            if data:
                schedules_cache[key] = data
    except Exception as e:
        logger.error(f"Set group error: {e}")

@bot.callback_query_handler(func=lambda call: call.data.startswith('show_tomorrow_'))
def callback_show_tomorrow(call):
    try:
        user_data = db_get_user(call.message.chat.id)
        if not user_data:
            return
        region, group_id, _ = user_data
        cached = get_cached_schedule(region, group_id)
        if cached and cached.get('tomorrow'):
            text = format_schedule_list(cached['tomorrow'])
            bot.send_message(
                call.message.chat.id,
                f"📅 <b>Графік на ЗАВТРА ({cached['tomorrow_date']}):</b>\n\n{text}",
                parse_mode="HTML"
            )
            if region == "odesa":
                img_url = get_odesa_image_url(cached['tomorrow_date'])
                try:
                    bot.send_photo(call.message.chat.id, img_url,
                                   caption=f"📊 Загальний графік Одеської обл. на {cached['tomorrow_date']}")
                except Exception:
                    pass
            bot.answer_callback_query(call.id)
        else:
            bot.answer_callback_query(call.id, "Графік на завтра ще не доступний")
    except Exception as e:
        logger.error(f"Show tomorrow error: {e}")

@bot.message_handler(func=lambda message: message.text == "📅 Отримати графік")
def send_schedule(message):
    try:
        user_data = db_get_user(message.chat.id)
        if not user_data or not user_data[1]:
            return bot.send_message(message.chat.id, "Спочатку натисніть /start")
        region, group_id, _ = user_data
        if not group_id:
            return bot.send_message(message.chat.id, "Спочатку оберіть групу через /start")
        cached = get_cached_schedule(region, group_id)
        if cached and cached.get('today'):
            text = format_schedule_list(cached['today'])
            markup = None
            if cached.get('tomorrow'):
                markup = types.InlineKeyboardMarkup()
                markup.add(types.InlineKeyboardButton(
                    f"➡️ Графік на Завтра ({cached['tomorrow_date']})",
                    callback_data=f"show_tomorrow_{group_id}"
                ))
            rname = REGION_NAMES.get(region, region)
            bot.send_message(
                message.chat.id,
                f"📅 <b>Графік на СЬОГОДНІ ({cached['today_date']}):</b>\n"
                f"<i>{rname}, Група {group_id}</i>\n\n{text}",
                parse_mode="HTML",
                reply_markup=markup
            )
            if region == "odesa":
                img_url = cached.get('image_url') or get_odesa_image_url(cached['today_date'])
                try:
                    bot.send_photo(
                        message.chat.id,
                        img_url,
                        caption=f"📊 Загальний графік Одеської обл. на {cached['today_date']}"
                    )
                except Exception as e:
                    logger.warning(f"Could not send Odesa image: {e}")
        else:
            bot.send_message(message.chat.id, "❌ Графік на сьогодні зараз недоступний або не знайдений.")
    except Exception as e:
        logger.error(f"Send schedule error: {e}")

@bot.message_handler(func=lambda message: message.text == "💡 Стан")
def send_status(message):
    try:
        user_data = db_get_user(message.chat.id)
        if not user_data or not user_data[1]:
            return bot.send_message(message.chat.id, "Спочатку виберіть групу через /start")
        region, group_id, _ = user_data
        if not group_id:
            return bot.send_message(message.chat.id, "Спочатку оберіть групу через /start")
        cached = get_cached_schedule(region, group_id)
        if cached and cached.get('today'):
            text = get_current_status_message(cached['today'])
            rname = REGION_NAMES.get(region, region)
            bot.send_message(
                message.chat.id,
                f"<i>{rname}, Група {group_id}</i>\n\n{text}",
                parse_mode="HTML"
            )
        else:
            bot.send_message(message.chat.id, "Дані відсутні.")
    except Exception as e:
        logger.error(f"Send status error: {e}")

@bot.message_handler(func=lambda message: message.text == "⚙️ Налаштування")
def settings(message):
    try:
        user_data = db_get_user(message.chat.id)
        if not user_data:
            return bot.send_message(message.chat.id, "Натисніть /start")
        region, group_id, notifications = user_data
        rname = REGION_NAMES.get(region, region)
        notif_status = "Увімкнено ✅" if notifications else "Вимкнено 🔕"
        group_str = group_id if group_id else "не обрано"
        markup = types.InlineKeyboardMarkup()
        markup.add(types.InlineKeyboardButton(
            f"Сповіщення: {notif_status}", callback_data="toggle_notif"
        ))
        markup.add(types.InlineKeyboardButton(
            "🔄 Змінити групу", callback_data="change_group"
        ))
        markup.add(types.InlineKeyboardButton(
            "🗺 Змінити область", callback_data="change_region"
        ))
        bot.send_message(
            message.chat.id,
            f"⚙️ <b>Налаштування</b>\n\n"
            f"Область: <b>{rname}</b>\n"
            f"Група: <b>{group_str}</b>\n\n"
            f"Сповіщення про зміни у графіку:",
            reply_markup=markup,
            parse_mode="HTML"
        )
    except Exception as e:
        logger.error(f"Settings error: {e}")

@bot.callback_query_handler(func=lambda call: call.data == "change_region")
def change_region_btn(call):
    bot.send_message(
        call.message.chat.id,
        "🗺 Оберіть нову область:",
        reply_markup=region_kb()
    )
    bot.answer_callback_query(call.id)

@bot.callback_query_handler(func=lambda call: call.data == "change_group")
def change_group_btn(call):
    user_data = db_get_user(call.message.chat.id)
    region = user_data[0] if user_data else "ternopil"
    bot.send_message(
        call.message.chat.id,
        "Виберіть нову групу:",
        reply_markup=groups_kb(region)
    )
    bot.answer_callback_query(call.id)

@bot.callback_query_handler(func=lambda call: call.data == "toggle_notif")
def toggle_notifications(call):
    new_status = db_toggle_notification(call.message.chat.id)
    text = "Увімкнено ✅" if new_status else "Вимкнено 🔕"
    markup = types.InlineKeyboardMarkup()
    markup.add(types.InlineKeyboardButton(
        f"Сповіщення: {text}", callback_data="toggle_notif"
    ))
    markup.add(types.InlineKeyboardButton("🔄 Змінити групу", callback_data="change_group"))
    markup.add(types.InlineKeyboardButton("🗺 Змінити область", callback_data="change_region"))
    try:
        bot.edit_message_reply_markup(
            call.message.chat.id, call.message.message_id, reply_markup=markup
        )
    except Exception:
        pass

@bot.message_handler(commands=['msg_id'])
def admin_send_private(message):
    if message.from_user.id != ADMIN_ID:
        return
    try:
        parts = message.text.split(maxsplit=2)
        if len(parts) < 3:
            bot.reply_to(message, "⚠️ Формат: `/msg_id ID_ЮЗЕРА Текст`", parse_mode="Markdown")
            return
        target_id = int(parts[1])
        msg_text = parts[2]
        bot.send_message(target_id, msg_text, parse_mode="HTML")
        bot.reply_to(message, f"✅ Відправлено `{target_id}`", parse_mode="Markdown")
    except Exception as e:
        bot.reply_to(message, f"❌ Помилка: {e}")

@bot.message_handler(commands=['msg_all'])
def admin_send_broadcast(message):
    if message.from_user.id != ADMIN_ID:
        return
    try:
        parts = message.text.split(maxsplit=1)
        if len(parts) < 2:
            bot.reply_to(message, "⚠️ Формат: `/msg_all Текст`", parse_mode="Markdown")
            return
        msg_text = parts[1]
        users = db_get_all_users_with_groups()
        bot.reply_to(message, f"⏳ Розсилка на {len(users)} користувачів...")
        success = blocked = 0
        for user in users:
            try:
                bot.send_message(user[0], msg_text, parse_mode="HTML")
                success += 1
                time.sleep(1.05)
            except Exception:
                blocked += 1
        bot.send_message(
            message.chat.id,
            f"🏁 Розсилку завершено!\n✅ Отримали: {success}\n💀 Заблокували: {blocked}"
        )
    except Exception as e:
        bot.reply_to(message, f"❌ Помилка: {e}")

@bot.message_handler(commands=['stats'])
def admin_stats(message):
    if message.from_user.id != ADMIN_ID:
        return
    try:
        with sqlite3.connect(DB_FILE) as conn:
            cursor = conn.cursor()
            total = cursor.execute("SELECT COUNT(*) FROM users").fetchone()[0]
            ternopil = cursor.execute(
                "SELECT COUNT(*) FROM users WHERE region='ternopil'"
            ).fetchone()[0]
            odesa = cursor.execute(
                "SELECT COUNT(*) FROM users WHERE region='odesa'"
            ).fetchone()[0]
            no_group = cursor.execute(
                "SELECT COUNT(*) FROM users WHERE group_id IS NULL"
            ).fetchone()[0]
        bot.reply_to(
            message,
            f"📊 <b>Статистика</b>\n\n"
            f"Всього користувачів: {total}\n"
            f"🏔 Тернопільська: {ternopil}\n"
            f"🌊 Одеська: {odesa}\n"
            f"❓ Без групи: {no_group}",
            parse_mode="HTML"
        )
    except Exception as e:
        bot.reply_to(message, f"❌ {e}")

def update_all_schedules():
    logger.info("Updating schedules...")
    groups = [f"{i}.{j}" for i in range(1, 7) for j in range(1, 3)]
    regions = {"ternopil", "odesa"}
    
    for region in regions:
        for gr in groups:
            key = get_cache_key(region, gr)
            new_data = fetch_schedule(region, gr)
            if not new_data:
                continue
            schedules_cache[key] = new_data

def check_upcoming_changes():
    try:
        now = datetime.now(KYIV_TZ)

        if now.minute not in [0, 30]:
            return

        future_time = now + timedelta(minutes=30)
        current_slot = f"{now.hour:02}:{30 if now.minute >= 30 else 0:02}"
        future_slot = f"{future_time.hour:02}:{30 if future_time.minute >= 30 else 0:02}"
        
        users = db_get_all_users_with_groups()
        
        for user_id, region, group_id in users:
            if not group_id:
                continue
            
            alert_key = f"{user_id}_{future_slot}"
            if last_sent_alerts.get(alert_key):
                continue
            
            cached = schedules_cache.get(get_cache_key(region, group_id))
            if not cached or not cached.get('today'):
                continue
            
            schedule_data = cached['today']
            current_status = schedule_data.get(current_slot)
            future_status = schedule_data.get(future_slot)
            
            if current_status is None or future_status is None:
                continue

            current_has_light = current_status in ["0", "10"]
            future_has_light = future_status in ["0", "10"]

            if current_has_light == future_has_light:
                continue

            times = sorted(list(schedule_data.keys()))
            try:
                future_index = times.index(future_slot)
            except ValueError:
                continue
            
            duration_minutes = 0
            for i in range(future_index, len(times)):
                slot_code = schedule_data[times[i]]
                slot_has_light = slot_code in ["0", "10"]
                if slot_has_light != future_has_light:
                    break
                duration_minutes += 30
            
            hours = duration_minutes // 60
            minutes = duration_minutes % 60
            duration_text = f"{hours} год {minutes} хв" if minutes > 0 else f"{hours} год"

            if future_has_light and not current_has_light:
                msg = f"🟢 Через 30 хв ({future_slot}) буде світло!\n⏱ Всього: <b>{duration_text}</b>"
            elif not future_has_light and current_has_light:
                msg = f"🔴 Через 30 хв ({future_slot}) вимкнення світла!\n⏱ Всього: <b>{duration_text}</b>"
            else:
                continue
            
            try:
                bot.send_message(user_id, msg, parse_mode="HTML")
                last_sent_alerts[alert_key] = True
                logger.info(f"Sent alert to {user_id} for {future_slot}: {future_status}")
            except Exception as e:
                logger.error(f"Failed to send alert to {user_id}: {e}")
    
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
    t = threading.Thread(target=scheduler_loop, daemon=True)
    t.start()
    logger.info("Bot started!")
    while True:
        try:
            bot.infinity_polling(timeout=10, long_polling_timeout=5)
        except Exception as e:
            logger.critical(f"Bot crash: {e}. Restarting...")
            time.sleep(5)