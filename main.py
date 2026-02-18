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

BOT_TOKEN = "TOKEN HERE"
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

WEB_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:147.0) Gecko/20100101 Firefox/147.0",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "uk,en;q=0.5",
}

REGIONS_CONFIG = {
    "vinnytsia": {"name": "🍇 Вінницька", "url": "https://alerts.org.ua/vinnytcka-oblast/", "region_id": 2},
    "volyn": {"name": "🌲 Волинська", "url": "https://alerts.org.ua/volynska-oblast/", "region_id": 3},
    "dnipro": {"name": "⚙️ Дніпропетровська", "url": "https://alerts.org.ua/dnipropetrovska-oblast/", "region_id": 4},
    "zhytomyr": {"name": "🌳 Житомирська", "url": "https://alerts.org.ua/zhytomyrska-oblast/", "region_id": 6},
    "zakarpattia": {"name": "🏔️ Закарпатська", "url": "https://alerts.org.ua/zakarpatska-oblast/", "region_id": 7},
    "zaporizhzhia": {"name": "🏭 Запорізька", "url": "https://alerts.org.ua/zaporizka-oblast/", "region_id": 8},
    "ivano-frankivsk": {"name": "⛰️ Івано-Франківська", "url": "https://alerts.org.ua/ivano-frankivska-oblast/", "region_id": 9},
    "kyiv": {"name": "🏛️ Київська", "url": "https://alerts.org.ua/kyivska-oblast/", "region_id": 10},
    "kirovohrad": {"name": "🌾 Кіровоградська", "url": "https://alerts.org.ua/kirovogradska-oblast/", "region_id": 11},
    "lviv": {"name": "🦁 Львівська", "url": "https://alerts.org.ua/lvivska-oblast/", "region_id": 13},
    "mykolaiv": {"name": "⚓ Миколаївська", "url": "https://alerts.org.ua/mykolaivska-oblast/", "region_id": 14},
    "odesa": {"name": "🌊 Одеська", "url": "https://alerts.org.ua/odeska-oblast/", "region_id": 15},
    "poltava": {"name": "🌻 Полтавська", "url": "https://alerts.org.ua/poltavska-oblast/", "region_id": 16},
    "rivne": {"name": "🌲 Рівненська", "url": "https://alerts.org.ua/rivnenska-oblast/", "region_id": 17},
    "sumy": {"name": "🌾 Сумська", "url": "https://alerts.org.ua/sumska-oblast/", "region_id": 18},
    "ternopil": {"name": "🏔 Тернопільська", "url": None, "region_id": 19},  # API
    "kharkiv": {"name": "🎓 Харківська", "url": "https://alerts.org.ua/harkivska-oblast/", "region_id": 20},
    "kherson": {"name": "🍉 Херсонська", "url": "https://alerts.org.ua/hersonska-oblast/", "region_id": 21},
    "khmelnytskyi": {"name": "🏰 Хмельницька", "url": "https://alerts.org.ua/hmelnitcka-oblast/", "region_id": 22},
    "cherkasy": {"name": "🌊 Черкаська", "url": "https://alerts.org.ua/cherkaska-oblast/", "region_id": 23},
    "chernivtsi": {"name": "🏔️ Чернівецька", "url": "https://alerts.org.ua/chernivetcka-oblast/", "region_id": 24},
    "chernihiv": {"name": "🌲 Чернігівська", "url": "https://alerts.org.ua/chernigivska-oblast/", "region_id": 25},
    "kyiv-city": {"name": "🏙️ м. Київ", "url": "https://alerts.org.ua/kyiv/", "region_id": 26},
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
                    region TEXT DEFAULT 'ternopil',
                    group_id TEXT,
                    notifications INTEGER DEFAULT 1,
                    username TEXT,
                    is_active INTEGER DEFAULT 1,
                    last_activity TEXT
                )
            ''')
            try:
                cursor.execute("ALTER TABLE users ADD COLUMN region TEXT DEFAULT 'ternopil'")
                cursor.execute("UPDATE users SET region = 'ternopil' WHERE region IS NULL")
                conn.commit()
                logger.info("Migrated: added region column")
            except sqlite3.OperationalError:
                pass
            
            try:
                cursor.execute("ALTER TABLE users ADD COLUMN username TEXT")
                logger.info("Migrated: added username column")
            except sqlite3.OperationalError:
                pass
            
            try:
                cursor.execute("ALTER TABLE users ADD COLUMN is_active INTEGER DEFAULT 1")
                cursor.execute("UPDATE users SET is_active = 1 WHERE is_active IS NULL")
                logger.info("Migrated: added is_active column")
            except sqlite3.OperationalError:
                pass
            
            try:
                cursor.execute("ALTER TABLE users ADD COLUMN last_activity TEXT")
                logger.info("Migrated: added last_activity column")
            except sqlite3.OperationalError:
                pass
            
            conn.commit()
        logger.info("Database initialized.")
    except Exception as e:
        logger.critical(f"Database error: {e}")

def db_update_user_activity(user_id, username=None):
    try:
        with sqlite3.connect(DB_FILE) as conn:
            cursor = conn.cursor()
            now = datetime.now(KYIV_TZ).isoformat()

            cursor.execute("SELECT user_id, username FROM users WHERE user_id = ?", (user_id,))
            existing = cursor.fetchone()
            
            if existing:
                if username and (not existing[1] or existing[1] != username):
                    cursor.execute(
                        "UPDATE users SET username = ?, is_active = 1, last_activity = ? WHERE user_id = ?",
                        (username, now, user_id)
                    )
                else:
                    cursor.execute(
                        "UPDATE users SET is_active = 1, last_activity = ? WHERE user_id = ?",
                        (now, user_id)
                    )
            else:
                cursor.execute(
                    "INSERT INTO users (user_id, username, is_active, last_activity, notifications) VALUES (?, ?, 1, ?, 1)",
                    (user_id, username, now)
                )
            
            conn.commit()
    except Exception as e:
        logger.error(f"Error updating user activity: {e}")

def db_set_region(user_id, region, username=None):
    try:
        with sqlite3.connect(DB_FILE) as conn:
            cursor = conn.cursor()
            now = datetime.now(KYIV_TZ).isoformat()
            cursor.execute(
                "INSERT OR IGNORE INTO users (user_id, region, notifications, username, is_active, last_activity) VALUES (?, ?, 1, ?, 1, ?)",
                (user_id, region, username, now)
            )
            
            if username:
                cursor.execute(
                    "UPDATE users SET region = ?, group_id = NULL, username = ?, is_active = 1, last_activity = ? WHERE user_id = ?",
                    (region, username, now, user_id)
                )
            else:
                cursor.execute(
                    "UPDATE users SET region = ?, group_id = NULL, is_active = 1, last_activity = ? WHERE user_id = ?",
                    (region, now, user_id)
                )
            conn.commit()
    except Exception as e:
        logger.error(f"Error setting region: {e}")

def db_set_group(user_id, group_id, username=None):
    try:
        with sqlite3.connect(DB_FILE) as conn:
            cursor = conn.cursor()
            now = datetime.now(KYIV_TZ).isoformat()
            
            if username:
                cursor.execute(
                    "INSERT OR REPLACE INTO users (user_id, region, group_id, notifications, username, is_active, last_activity) "
                    "VALUES (?, COALESCE((SELECT region FROM users WHERE user_id=?),'ternopil'), ?, "
                    "COALESCE((SELECT notifications FROM users WHERE user_id=?), 1), ?, 1, ?)",
                    (user_id, user_id, group_id, user_id, username, now)
                )
            else:
                cursor.execute(
                    "INSERT OR REPLACE INTO users (user_id, region, group_id, notifications, is_active, last_activity) "
                    "VALUES (?, COALESCE((SELECT region FROM users WHERE user_id=?),'ternopil'), ?, "
                    "COALESCE((SELECT notifications FROM users WHERE user_id=?), 1), 1, ?)",
                    (user_id, user_id, group_id, user_id, now)
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

def fetch_general_image_url(region):
    config = REGIONS_CONFIG.get(region)
    if not config:
        return None

    if region == "ternopil":
        url = "https://alerts.org.ua/ternopilska-oblast/"
    else:
        url = config.get("url")
    
    if not url:
        return None
    
    try:
        resp = requests.get(url, headers=WEB_HEADERS, timeout=15)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "html.parser")

        img_tag = soup.find("img", src=re.compile(r"_cache/_graph"))
        if img_tag:
            src = img_tag.get("src", "")
            if src.startswith("http"):
                return src
            else:
                return f"https://alerts.org.ua{src}"
        return None
    except Exception as e:
        logger.error(f"Error fetching general image for {region}: {e}")
        return None

def parse_time_to_minutes(t: str) -> int:
    t = t.strip()
    h, m = t.split(":")
    return int(h) * 60 + int(m)

def fetch_alerts_schedule(region, group_id):
    config = REGIONS_CONFIG.get(region)
    if not config or not config["url"]:
        return None
    
    today_str = datetime.now(KYIV_TZ).strftime("%Y-%m-%d")
    tomorrow_str = (datetime.now(KYIV_TZ) + timedelta(days=1)).strftime("%Y-%m-%d")
    result = {
        'today': None, 'today_date': today_str,
        'tomorrow': None, 'tomorrow_date': tomorrow_str
    }
    
    def _parse_page(url):
        try:
            resp = requests.get(url, headers=WEB_HEADERS, timeout=15)
            resp.raise_for_status()
            return BeautifulSoup(resp.text, "html.parser")
        except Exception as e:
            logger.error(f"{region} fetch error {url}: {e}")
            return None
    
    def _extract_times_for_group(soup, group_id):
        major, minor = group_id.split(".")
        attr = f"r{config['region_id']}g{major}-{minor}"
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
    
    soup_today = _parse_page(config["url"])
    if soup_today:
        result['today'] = _extract_times_for_group(soup_today, group_id)
    
    tomorrow_url = f"{config['url'].rstrip('/')}/{tomorrow_str}.html"
    soup_tomorrow = _parse_page(tomorrow_url)
    if soup_tomorrow:
        result['tomorrow'] = _extract_times_for_group(soup_tomorrow, group_id)
    
    return result

def get_cache_key(region, group_id):
    return f"{region}_{group_id}"

def fetch_schedule(region, group_id):
    if region == "ternopil":
        return fetch_ternopil_schedule(group_id)
    else:
        return fetch_alerts_schedule(region, group_id)

def get_cached_schedule(region, group_id):
    key = get_cache_key(region, group_id)
    cached = schedules_cache.get(key)
    if not cached:
        cached = fetch_schedule(region, group_id)
        if cached:
            schedules_cache[key] = cached
    return cached

def format_schedule_list(schedule, region):
    show_maybe = (region == "ternopil")
    
    STATUS_MAP = {
        "0":  ("🟢", "Є світло"),
        "1":  ("🔴", "НЕМАЄ світла"),
        "10": ("🟡", "Можливе вимкнення") if show_maybe else ("🟢", "Є світло")
    }
    
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
    
    if show_maybe:
        legend = "\n\n🟢 — Є світло\n🟡 — Можливе вимкнення\n🔴 — Немає світла"
    else:
        legend = "\n\n🟢 — Є світло\n🔴 — Немає світла"
    
    return "\n".join(lines) + legend

def get_current_status_message(schedule, region):
    now = datetime.now(KYIV_TZ)
    minute = 30 if now.minute >= 30 else 0
    current_slot = f"{now.hour:02}:{minute:02}"
    show_maybe = (region == "ternopil")
    
    full_schedule = {}
    for h in range(24):
        for m in (0, 30):
            k = f"{h:02}:{m:02}"
            code = schedule.get(k, "10")
            if not show_maybe and code == "10":
                code = "0"
            full_schedule[k] = code

    times = sorted(list(full_schedule.keys()))
    current_code = full_schedule.get(current_slot, "10" if show_maybe else "0")
    if show_maybe:
        is_light_now = current_code in ["0", "10"]
    else:
        is_light_now = current_code == "0"

    try:
        curr_idx = times.index(current_slot)
    except ValueError:
        return "⚠️ Не вдалося визначити час."

    start_idx = curr_idx
    while start_idx > 0:
        prev_slot = times[start_idx - 1]
        prev_code = full_schedule[prev_slot]
        if show_maybe:
            prev_is_light = prev_code in ["0", "10"]
        else:
            prev_is_light = prev_code == "0"
        
        if prev_is_light != is_light_now:
            break
        start_idx -= 1
    
    start_slot = times[start_idx]

    end_idx = curr_idx
    end_slot = "24:00"
    
    while end_idx < len(times) - 1:
        next_slot = times[end_idx + 1]
        next_code = full_schedule[next_slot]
        if show_maybe:
            next_is_light = next_code in ["0", "10"]
        else:
            next_is_light = next_code == "0"
        
        if next_is_light != is_light_now:
            end_slot = next_slot
            break
        end_idx += 1

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

    # Статус з урахуванням регіону
    if show_maybe:
        STATUS_MAP = {
            "0":  ("🟢", "Є світло"),
            "1":  ("🔴", "НЕМАЄ світла"),
            "10": ("🟡", "Можливе відключення")
        }
    else:
        STATUS_MAP = {
            "0":  ("🟢", "Є світло"),
            "1":  ("🔴", "НЕМАЄ світла"),
            "10": ("🟢", "Є світло")
        }
    
    icon = STATUS_MAP.get(current_code, ("❓", "Невідомо"))[0]
    text_status = STATUS_MAP.get(current_code, ("❓", "Невідомо"))[1]

    msg = f"Зараз ({current_slot}): {icon} <b>{text_status}</b>\n"
    
    if is_light_now:
        msg += f"Світло БУДЕ з {start_slot} по {end_slot}\n"
    else:
        msg += f"Світла НЕ БУДЕ з {start_slot} по {end_slot}\n"
        
    msg += f"⏱ Всього: <b>{duration_str}</b>"
    
    return msg

def main_menu_kb():
    markup = types.ReplyKeyboardMarkup(resize_keyboard=True)
    markup.row("📅 Отримати графік", "💡 Стан")
    markup.row("📊 Загальний графік")
    markup.row("⚙️ Налаштування")
    return markup

def region_kb():
    markup = types.InlineKeyboardMarkup(row_width=2)
    buttons = []
    for region_key, config in sorted(REGIONS_CONFIG.items(), key=lambda x: x[1]['name']):
        buttons.append(types.InlineKeyboardButton(
            config['name'],
            callback_data=f"set_region_{region_key}"
        ))
    for i in range(0, len(buttons), 2):
        if i + 1 < len(buttons):
            markup.row(buttons[i], buttons[i+1])
        else:
            markup.row(buttons[i])
    return markup

def groups_kb():
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
        username = message.from_user.username
        user_id = message.chat.id

        db_update_user_activity(user_id, username)
        
        user_data = db_get_user(user_id)
        if user_data and user_data[1]:
            region, group_id, _ = user_data
            rname = REGIONS_CONFIG.get(region, {}).get('name', region)
            bot.send_message(
                user_id,
                f"Вітаю! 👋\n"
                f"Ваша область: <b>{rname}</b>\n"
                f"Ваша група: <b>{group_id}</b>\n\n"
                f"Для зміни — скористайтесь <b>⚙️ Налаштування</b>",
                reply_markup=main_menu_kb(),
                parse_mode="HTML"
            )
        else:
            bot.send_message(
                user_id,
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
        username = call.from_user.username
        user_id = call.message.chat.id
        
        db_set_region(user_id, region, username)
        db_update_user_activity(user_id, username)
        
        rname = REGIONS_CONFIG.get(region, {}).get('name', region)
        bot.answer_callback_query(call.id, f"Обрано: {rname}")
        bot.send_message(
            user_id,
            f"✅ Область: <b>{rname}</b>\n\nТепер оберіть вашу <b>групу</b>:",
            reply_markup=groups_kb(),
            parse_mode="HTML"
        )
    except Exception as e:
        logger.error(f"Set region error: {e}")

@bot.callback_query_handler(func=lambda call: call.data.startswith('set_group_'))
def callback_set_group(call):
    try:
        group_id = call.data.replace('set_group_', '')
        username = call.from_user.username
        user_id = call.message.chat.id
        
        db_set_group(user_id, group_id, username)
        db_update_user_activity(user_id, username)
        
        user_data = db_get_user(user_id)
        region = user_data[0] if user_data else "ternopil"
        rname = REGIONS_CONFIG.get(region, {}).get('name', region)
        bot.answer_callback_query(call.id, "Групу збережено!")
        bot.send_message(
            user_id,
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
            text = format_schedule_list(cached['tomorrow'], region)
            bot.send_message(
                call.message.chat.id,
                f"📅 <b>Графік на ЗАВТРА ({cached['tomorrow_date']}):</b>\n\n{text}",
                parse_mode="HTML"
            )
            bot.answer_callback_query(call.id)
        else:
            bot.answer_callback_query(call.id, "Графік на завтра ще не доступний")
    except Exception as e:
        logger.error(f"Show tomorrow error: {e}")

@bot.message_handler(func=lambda message: message.text == "📅 Отримати графік")
def send_schedule(message):
    try:
        username = message.from_user.username
        user_id = message.chat.id
        db_update_user_activity(user_id, username)
        
        user_data = db_get_user(user_id)
        if not user_data or not user_data[1]:
            return bot.send_message(user_id, "Спочатку натисніть /start")
        region, group_id, _ = user_data
        if not group_id:
            return bot.send_message(user_id, "Спочатку оберіть групу через /start")
        cached = get_cached_schedule(region, group_id)
        if cached and cached.get('today'):
            text = format_schedule_list(cached['today'], region)
            markup = None
            if cached.get('tomorrow'):
                markup = types.InlineKeyboardMarkup()
                markup.add(types.InlineKeyboardButton(
                    f"➡️ Графік на Завтра ({cached['tomorrow_date']})",
                    callback_data=f"show_tomorrow_{group_id}"
                ))
            rname = REGIONS_CONFIG.get(region, {}).get('name', region)
            bot.send_message(
                user_id,
                f"📅 <b>Графік на СЬОГОДНІ ({cached['today_date']}):</b>\n"
                f"<i>{rname}, Група {group_id}</i>\n\n{text}",
                parse_mode="HTML",
                reply_markup=markup
            )
        else:
            bot.send_message(user_id, "❌ Графік на сьогодні зараз недоступний або не знайдений.")
    except Exception as e:
        logger.error(f"Send schedule error: {e}")

@bot.message_handler(func=lambda message: message.text == "💡 Стан")
def send_status(message):
    try:
        username = message.from_user.username
        user_id = message.chat.id
        db_update_user_activity(user_id, username)
        
        user_data = db_get_user(user_id)
        if not user_data or not user_data[1]:
            return bot.send_message(user_id, "Спочатку виберіть групу через /start")
        region, group_id, _ = user_data
        if not group_id:
            return bot.send_message(user_id, "Спочатку оберіть групу через /start")
        cached = get_cached_schedule(region, group_id)
        if cached and cached.get('today'):
            text = get_current_status_message(cached['today'], region)
            rname = REGIONS_CONFIG.get(region, {}).get('name', region)
            bot.send_message(
                user_id,
                f"<i>{rname}, Група {group_id}</i>\n\n{text}",
                parse_mode="HTML"
            )
        else:
            bot.send_message(user_id, "Дані відсутні.")
    except Exception as e:
        logger.error(f"Send status error: {e}")

@bot.message_handler(func=lambda message: message.text == "📊 Загальний графік")
def send_general_schedule(message):
    try:
        username = message.from_user.username
        user_id = message.chat.id
        db_update_user_activity(user_id, username)
        
        user_data = db_get_user(user_id)
        if not user_data:
            return bot.send_message(user_id, "Спочатку натисніть /start")
        
        region, group_id, _ = user_data
        if not region:
            return bot.send_message(user_id, "Спочатку оберіть область через /start")
        
        rname = REGIONS_CONFIG.get(region, {}).get('name', region)
        image_url = fetch_general_image_url(region)
        
        if image_url:
            today_str = datetime.now(KYIV_TZ).strftime("%Y-%m-%d")
            try:
                bot.send_photo(
                    user_id,
                    image_url,
                    caption=f"📊 <b>Загальний графік</b>\n{rname}\n{today_str}",
                    parse_mode="HTML"
                )
            except Exception as e:
                logger.error(f"Error sending general image: {e}")
                bot.send_message(
                    user_id,
                    f"❌ Не вдалося завантажити зображення загального графіка.\n\n"
                    f"Ви можете переглянути його на сайті:\n"
                    f"{REGIONS_CONFIG.get(region, {}).get('url', 'https://alerts.org.ua')}"
                )
        else:
            bot.send_message(
                user_id,
                f"❌ Загальний графік для {rname} наразі недоступний."
            )
    except Exception as e:
        logger.error(f"Send general schedule error: {e}")

@bot.message_handler(func=lambda message: message.text == "⚙️ Налаштування")
def settings(message):
    try:
        username = message.from_user.username
        user_id = message.chat.id
        db_update_user_activity(user_id, username)
        
        user_data = db_get_user(user_id)
        if not user_data:
            return bot.send_message(user_id, "Натисніть /start")
        region, group_id, notifications = user_data
        rname = REGIONS_CONFIG.get(region, {}).get('name', region)
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
            user_id,
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
    bot.send_message(
        call.message.chat.id,
        "Виберіть нову групу:",
        reply_markup=groups_kb()
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

@bot.message_handler(commands=['help'])
def admin_help(message):
    if message.from_user.id != ADMIN_ID:
        return
    
    help_text = """🤖 <b>Команди Адміністратора</b>

📊 <b>СТАТИСТИКА:</b>

/stats - Базова статистика
├ Кількість користувачів загалом
├ Розподіл по областях
└ Користувачі без групи

/analytics - Детальна аналітика
├ Активні/заблоковані користувачі
├ Користувачі з/без username
├ Активність за 24 год / 7 днів / 30 днів
├ Топ-5 областей за тиждень
├ Топ-5 груп по популярності
└ Останні 10 користувачів з часом

📤 <b>ЕКСПОРТ ДАНИХ:</b>

/export_no_username - Список без username
└ До 50 користувачів без username

/export_blocked - Список заблокованих
└ Користувачі, які заблокували бота

💬 <b>РОЗСИЛКА:</b>

/msg_id [ID] [текст] - Надіслати одному
└ Приклад: /msg_id 123456789 Привіт!

/msg_all [текст] - Розсилка всім
├ Відправляє всім з notifications=1
├ Автоматично позначає заблокованих
└ Показує статистику доставки

❓ <b>ІНШЕ:</b>

/help

<b>💡 Підказки:</b>
• Бот автоматично зберігає username
• При розсилці позначає заблокованих як is_active=0
• last_activity оновлюється при кожній дії
• Всі повідомлення підтримують HTML розмітку"""
    
    bot.reply_to(message, help_text, parse_mode="HTML")

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
        
        with sqlite3.connect(DB_FILE) as conn:
            cursor = conn.cursor()
            
            for user in users:
                user_id = user[0]
                try:
                    bot.send_message(user_id, msg_text, parse_mode="HTML")
                    success += 1
                    time.sleep(1.05)
                except Exception as e:
                    blocked += 1
                    cursor.execute(
                        "UPDATE users SET is_active = 0 WHERE user_id = ?",
                        (user_id,)
                    )
                    logger.info(f"User {user_id} marked as inactive (blocked bot)")
            
            conn.commit()
        
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
            stats_lines = [f"📊 <b>Статистика</b>\n\nВсього користувачів: {total}\n"]
            
            for region_key, config in sorted(REGIONS_CONFIG.items(), key=lambda x: x[1]['name']):
                count = cursor.execute(
                    f"SELECT COUNT(*) FROM users WHERE region=?", (region_key,)
                ).fetchone()[0]
                if count > 0:
                    stats_lines.append(f"{config['name']}: {count}")
            
            no_group = cursor.execute(
                "SELECT COUNT(*) FROM users WHERE group_id IS NULL"
            ).fetchone()[0]
            stats_lines.append(f"\n❓ Без групи: {no_group}")
            
        bot.reply_to(message, "\n".join(stats_lines), parse_mode="HTML")
    except Exception as e:
        bot.reply_to(message, f"❌ {e}")

@bot.message_handler(commands=['analytics'])
def admin_analytics(message):
    if message.from_user.id != ADMIN_ID:
        return
    try:
        with sqlite3.connect(DB_FILE) as conn:
            cursor = conn.cursor()

            total = cursor.execute("SELECT COUNT(*) FROM users").fetchone()[0]
            active = cursor.execute("SELECT COUNT(*) FROM users WHERE is_active = 1").fetchone()[0]
            blocked = cursor.execute("SELECT COUNT(*) FROM users WHERE is_active = 0").fetchone()[0]
            with_username = cursor.execute("SELECT COUNT(*) FROM users WHERE username IS NOT NULL").fetchone()[0]
            without_username = cursor.execute("SELECT COUNT(*) FROM users WHERE username IS NULL").fetchone()[0]

            now = datetime.now(KYIV_TZ)
            
            day_ago = (now - timedelta(days=1)).isoformat()
            active_24h = cursor.execute(
                "SELECT COUNT(*) FROM users WHERE last_activity >= ? AND is_active = 1",
                (day_ago,)
            ).fetchone()[0]
            
            week_ago = (now - timedelta(days=7)).isoformat()
            active_7d = cursor.execute(
                "SELECT COUNT(*) FROM users WHERE last_activity >= ? AND is_active = 1",
                (week_ago,)
            ).fetchone()[0]
            
            month_ago = (now - timedelta(days=30)).isoformat()
            active_30d = cursor.execute(
                "SELECT COUNT(*) FROM users WHERE last_activity >= ? AND is_active = 1",
                (month_ago,)
            ).fetchone()[0]

            top_regions = cursor.execute('''
                SELECT region, COUNT(*) as cnt 
                FROM users 
                WHERE last_activity >= ? AND is_active = 1
                GROUP BY region 
                ORDER BY cnt DESC 
                LIMIT 5
            ''', (week_ago,)).fetchall()

            top_groups = cursor.execute('''
                SELECT group_id, COUNT(*) as cnt 
                FROM users 
                WHERE group_id IS NOT NULL AND is_active = 1
                GROUP BY group_id 
                ORDER BY cnt DESC 
                LIMIT 5
            ''').fetchall()

            recent_users = cursor.execute('''
                SELECT user_id, username, region, group_id, last_activity 
                FROM users 
                ORDER BY last_activity DESC 
                LIMIT 10
            ''').fetchall()

            msg = f"""📈 <b>Детальна Аналітика</b>

👥 <b>Загальна статистика:</b>
• Всього користувачів: {total}
• ✅ Активні (не заблокували): {active} ({active*100//total if total > 0 else 0}%)
• 🚫 Заблокували бота: {blocked} ({blocked*100//total if total > 0 else 0}%)
• 📝 З username: {with_username} ({with_username*100//total if total > 0 else 0}%)
• ❓ Без username: {without_username}

⏰ <b>Активність:</b>
• За останні 24 год: {active_24h}
• За останні 7 днів: {active_7d}
• За останні 30 днів: {active_30d}

🏆 <b>Топ-5 областей (7 днів):</b>"""
            
            for region, cnt in top_regions:
                region_name = REGIONS_CONFIG.get(region, {}).get('name', region)
                msg += f"\n• {region_name}: {cnt}"
            
            msg += "\n\n🔢 <b>Топ-5 груп:</b>"
            for group, cnt in top_groups:
                msg += f"\n• Група {group}: {cnt}"
            
            msg += "\n\n👤 <b>Останні 10 користувачів:</b>"
            for uid, uname, reg, grp, last_act in recent_users:
                username_str = f"@{uname}" if uname else "без username"
                region_name = REGIONS_CONFIG.get(reg, {}).get('name', reg) if reg else "?"
                group_str = grp if grp else "?"

                try:
                    last_time = datetime.fromisoformat(last_act) if last_act else None
                    if last_time:
                        time_ago = now - last_time
                        if time_ago.days > 0:
                            time_str = f"{time_ago.days}д"
                        elif time_ago.seconds // 3600 > 0:
                            time_str = f"{time_ago.seconds // 3600}г"
                        else:
                            time_str = f"{time_ago.seconds // 60}хв"
                    else:
                        time_str = "?"
                except:
                    time_str = "?"
                
                msg += f"\n• {uid} ({username_str}) - {region_name} гр.{group_str} [{time_str}]"
            
        bot.reply_to(message, msg, parse_mode="HTML")
    except Exception as e:
        logger.error(f"Analytics error: {e}")
        bot.reply_to(message, f"❌ Помилка: {e}")

@bot.message_handler(commands=['export_no_username'])
def export_no_username(message):
    if message.from_user.id != ADMIN_ID:
        return
    try:
        with sqlite3.connect(DB_FILE) as conn:
            cursor = conn.cursor()
            
            users = cursor.execute('''
                SELECT user_id, region, group_id, last_activity, is_active
                FROM users 
                WHERE username IS NULL
                ORDER BY last_activity DESC
            ''').fetchall()
            
            if not users:
                bot.reply_to(message, "✅ Всі користувачі мають username!")
                return
            
            msg = f"📋 <b>Користувачі без username: {len(users)}</b>\n\n"
            
            for uid, reg, grp, last_act, is_active in users[:50]:  # Обмежуємо 50-ма
                status = "✅" if is_active else "🚫"
                region_name = REGIONS_CONFIG.get(reg, {}).get('name', reg) if reg else "?"
                group_str = grp if grp else "?"
                
                try:
                    last_time = datetime.fromisoformat(last_act) if last_act else None
                    if last_time:
                        time_str = last_time.strftime("%d.%m %H:%M")
                    else:
                        time_str = "немає"
                except:
                    time_str = "?"
                
                msg += f"{status} <code>{uid}</code> | {region_name} {group_str} | {time_str}\n"
            
            if len(users) > 50:
                msg += f"\n... та ще {len(users) - 50} користувачів"
            
        bot.reply_to(message, msg, parse_mode="HTML")
    except Exception as e:
        bot.reply_to(message, f"❌ {e}")

@bot.message_handler(commands=['export_blocked'])
def export_blocked(message):
    if message.from_user.id != ADMIN_ID:
        return
    try:
        with sqlite3.connect(DB_FILE) as conn:
            cursor = conn.cursor()
            
            users = cursor.execute('''
                SELECT user_id, username, region, group_id, last_activity
                FROM users 
                WHERE is_active = 0
                ORDER BY last_activity DESC
            ''').fetchall()
            
            if not users:
                bot.reply_to(message, "✅ Немає заблокованих користувачів!")
                return
            
            msg = f"🚫 <b>Заблоковані користувачі: {len(users)}</b>\n\n"
            
            for uid, uname, reg, grp, last_act in users[:50]:
                username_str = f"@{uname}" if uname else "немає"
                region_name = REGIONS_CONFIG.get(reg, {}).get('name', reg) if reg else "?"
                group_str = grp if grp else "?"
                
                try:
                    last_time = datetime.fromisoformat(last_act) if last_act else None
                    if last_time:
                        time_str = last_time.strftime("%d.%m")
                    else:
                        time_str = "?"
                except:
                    time_str = "?"
                
                msg += f"<code>{uid}</code> ({username_str}) | {region_name} {group_str} | {time_str}\n"
            
            if len(users) > 50:
                msg += f"\n... та ще {len(users) - 50} користувачів"
            
        bot.reply_to(message, msg, parse_mode="HTML")
    except Exception as e:
        bot.reply_to(message, f"❌ {e}")

def update_all_schedules():
    logger.info("Updating schedules...")
    groups = [f"{i}.{j}" for i in range(1, 7) for j in range(1, 3)]
    all_users = db_get_all_users_with_groups()

    with sqlite3.connect(DB_FILE) as conn:
        cursor = conn.cursor()
        
        for region_key in REGIONS_CONFIG.keys():
            for gr in groups:
                key = get_cache_key(region_key, gr)
                new_data = fetch_schedule(region_key, gr)
                if not new_data:
                    continue

                old_data = schedules_cache.get(key)
                schedules_cache[key] = new_data

                if old_data is None:
                    continue

                targets = [
                    (uid,) for uid, r, gid in all_users
                    if r == region_key and gid == gr
                ]
                if not targets:
                    continue

                old_today = old_data.get('today')
                new_today = new_data.get('today')
                if new_today and new_today != old_today:
                    rname = REGIONS_CONFIG.get(region_key, {}).get('name', region_key)
                    text = format_schedule_list(new_today, region_key)
                    for (uid,) in targets:
                        try:
                            bot.send_message(
                                uid,
                                f"⚠️ <b>Графік на СЬОГОДНІ ({new_data['today_date']}) змінився!</b>\n"
                                f"<i>{rname}, Група {gr}</i>\n\n{text}",
                                parse_mode="HTML"
                            )
                        except Exception as e:
                            cursor.execute(
                                "UPDATE users SET is_active = 0 WHERE user_id = ?",
                                (uid,)
                            )
                            logger.info(f"User {uid} marked as inactive during schedule update")

                old_tomorrow = old_data.get('tomorrow')
                new_tomorrow = new_data.get('tomorrow')
                if new_tomorrow and (old_tomorrow is None or new_tomorrow != old_tomorrow):
                    rname = REGIONS_CONFIG.get(region_key, {}).get('name', region_key)
                    text = format_schedule_list(new_tomorrow, region_key)
                    markup = types.InlineKeyboardMarkup()
                    markup.add(types.InlineKeyboardButton(
                        f"📅 Переглянути графік на {new_data['tomorrow_date']}",
                        callback_data=f"show_tomorrow_{gr}"
                    ))
                    for (uid,) in targets:
                        try:
                            if old_tomorrow is None:
                                header = f"📅 <b>З'явився графік на ЗАВТРА ({new_data['tomorrow_date']})!</b>"
                            else:
                                header = f"⚠️ <b>Графік на ЗАВТРА ({new_data['tomorrow_date']}) змінився!</b>"
                            bot.send_message(
                                uid,
                                f"{header}\n<i>{rname}, Група {gr}</i>\n\n{text}",
                                parse_mode="HTML",
                                reply_markup=markup
                            )
                        except Exception as e:
                            cursor.execute(
                                "UPDATE users SET is_active = 0 WHERE user_id = ?",
                                (uid,)
                            )
                            logger.info(f"User {uid} marked as inactive during schedule update")
        
        conn.commit()

def check_upcoming_changes():
    try:
        now = datetime.now(KYIV_TZ)

        if now.minute not in [0, 30]:
            return

        future_time = now + timedelta(minutes=30)
        current_slot = f"{now.hour:02}:{30 if now.minute >= 30 else 0:02}"
        future_slot = f"{future_time.hour:02}:{30 if future_time.minute >= 30 else 0:02}"
        
        users = db_get_all_users_with_groups()
        
        with sqlite3.connect(DB_FILE) as conn:
            cursor = conn.cursor()
            
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

                show_maybe = (region == "ternopil")
                
                if not show_maybe:
                    if current_status == "10":
                        current_status = "0"
                    if future_status == "10":
                        future_status = "0"
                
                current_has_light = current_status in ["0", "10"] if show_maybe else current_status == "0"
                future_has_light = future_status in ["0", "10"] if show_maybe else future_status == "0"

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
                    if not show_maybe and slot_code == "10":
                        slot_code = "0"
                    
                    if show_maybe:
                        slot_has_light = slot_code in ["0", "10"]
                    else:
                        slot_has_light = slot_code == "0"
                    
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
                    cursor.execute(
                        "UPDATE users SET is_active = 0 WHERE user_id = ?",
                        (user_id,)
                    )
                    logger.error(f"Failed to send alert to {user_id}, marked as inactive: {e}")
            
            conn.commit()
    
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
    logger.info("Bot started with all regions!")
    while True:
        try:
            bot.infinity_polling(timeout=10, long_polling_timeout=5)
        except Exception as e:
            logger.critical(f"Bot crash: {e}. Restarting...")
            time.sleep(5)