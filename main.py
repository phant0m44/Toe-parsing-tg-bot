import telebot
from telebot import types
import requests
import sqlite3
import time
import threading
import schedule
import logging
import io
import sys
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo
from bs4 import BeautifulSoup
import re
import concurrent.futures
from PIL import Image, ImageDraw, ImageFont

# --- Unicode fix for Debian (stdout/stderr encoding) ---
if sys.stdout.encoding and sys.stdout.encoding.lower() != 'utf-8':
    import codecs
    sys.stdout = codecs.getwriter('utf-8')(sys.stdout.buffer, 'strict')
    sys.stderr = codecs.getwriter('utf-8')(sys.stderr.buffer, 'strict')

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    handlers=[
        logging.StreamHandler(sys.stdout),
    ]
)
logger = logging.getLogger(__name__)

BOT_TOKEN = "Your Token Here"
DB_FILE = "bot_users.db"
ADMIN_ID = 5292087312

# --- bezsvitla.com.ua base URL ---
BEZSVITLA_BASE = "https://bezsvitla.com.ua"

# --- Ternopil API ---
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

BEZSVITLA_SLUGS = {
    "vinnytsia":       "vinnytska-oblast",
    "volyn":           "volynska-oblast",
    "dnipro":          "dnipropetrovska-oblast",
    "zhytomyr":        "zhytomyrska-oblast",
    "zakarpattia":     "zakarpatska-oblast",
    "zaporizhzhia":    "zaporizka-oblast",
    "ivano-frankivsk": "ivano-frankivska-oblast",
    "kyiv":            "kyivska-oblast",
    "kirovohrad":      "kirovohradska-oblast",
    "lviv":            "lvivska-oblast",
    "mykolaiv":        "mykolaivska-oblast",
    "odesa":           "odeska-oblast",
    "poltava":         "poltavska-oblast",
    "rivne":           "rivnenska-oblast",
    "sumy":            "sumska-oblast",
    "ternopil":        "ternopilska-oblast",
    "kharkiv":         "kharkivska-oblast",
    "kherson":         "khersonska-oblast",
    "khmelnytskyi":    "khmelnytska-oblast",
    "cherkasy":        "cherkaska-oblast",
    "chernivtsi":      "chernivetska-oblast",
    "chernihiv":       "chernihivska-oblast",
    "kyiv-city":       "kyiv",
}

KYIV_CITY_MAX_GROUP = 60

REGIONS_CONFIG = {
    "vinnytsia":       {"name": "🍇 Вінницька",        "region_id": 2},
    "volyn":           {"name": "🌲 Волинська",        "region_id": 3},
    "dnipro":          {"name": "⚙️ Дніпропетровська", "region_id": 4},
    "zhytomyr":        {"name": "🌳 Житомирська",      "region_id": 6},
    "zakarpattia":     {"name": "🏔️ Закарпатська",      "region_id": 7},
    "zaporizhzhia":    {"name": "🏭 Запорізька",        "region_id": 8},
    "ivano-frankivsk": {"name": "⛰️ Івано-Франківська", "region_id": 9},
    "kyiv":            {"name": "🏛️ Київська",         "region_id": 10},
    "kirovohrad":      {"name": "🌾 Кіровоградська",    "region_id": 11},
    "lviv":            {"name": "🦁 Львівська",         "region_id": 13},
    "mykolaiv":        {"name": "⚓ Миколаївська",      "region_id": 14},
    "odesa":           {"name": "🌊 Одеська",           "region_id": 15},
    "poltava":         {"name": "🌻 Полтавська",        "region_id": 16},
    "rivne":           {"name": "🌲 Рівненська",        "region_id": 17},
    "sumy":            {"name": "🌾 Сумська",           "region_id": 18},
    "ternopil":        {"name": "🏔 Тернопільська",      "region_id": 19},
    "kharkiv":         {"name": "🎓 Харківська",       "region_id": 20},
    "kherson":         {"name": "🍉 Херсонська",       "region_id": 21},
    "khmelnytskyi":    {"name": "🏰 Хмельницька",      "region_id": 22},
    "cherkasy":        {"name": "🌊 Черкаська",        "region_id": 23},
    "chernivtsi":      {"name": "🏔️ Чернівецька",     "region_id": 24},
    "chernihiv":       {"name": "🌲 Чернігівська",     "region_id": 25},
    "kyiv-city":       {"name": "🏙️ м. Київ",          "region_id": 26},
}

try:
    KYIV_TZ = ZoneInfo("Europe/Kyiv")
except Exception:
    KYIV_TZ = timezone.utc

bot = telebot.TeleBot(BOT_TOKEN)
schedules_cache = {}
last_sent_alerts = {}

# ─────────────────────────────────────────────
# DB helpers
# ─────────────────────────────────────────────

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
            
            columns_to_add = [
                ("region", "'ternopil'"),
                ("username", "NULL"),
                ("is_active", "1"),
                ("last_activity", "NULL")
            ]
            
            for col, default in columns_to_add:
                try:
                    cursor.execute(f"ALTER TABLE users ADD COLUMN {col} TEXT DEFAULT {default}")
                except sqlite3.OperationalError:
                    pass
            
            conn.commit()
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
                        "UPDATE users SET username=?, is_active=1, last_activity=? WHERE user_id=?", 
                        (username, now, user_id)
                    )
                else:
                    cursor.execute(
                        "UPDATE users SET is_active=1, last_activity=? WHERE user_id=?", 
                        (now, user_id)
                    )
            else:
                cursor.execute(
                    "INSERT INTO users (user_id, username, is_active, last_activity, notifications) VALUES (?,?,1,?,1)", 
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
                "INSERT OR IGNORE INTO users (user_id, region, notifications, username, is_active, last_activity) VALUES (?,?,1,?,1,?)", 
                (user_id, region, username, now)
            )
            
            if username:
                cursor.execute(
                    "UPDATE users SET region=?, group_id=NULL, username=?, is_active=1, last_activity=? WHERE user_id=?", 
                    (region, username, now, user_id)
                )
            else:
                cursor.execute(
                    "UPDATE users SET region=?, group_id=NULL, is_active=1, last_activity=? WHERE user_id=?", 
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
                    "VALUES (?, COALESCE((SELECT region FROM users WHERE user_id=?),'ternopil'), ?, COALESCE((SELECT notifications FROM users WHERE user_id=?),1), ?, 1, ?)", 
                    (user_id, user_id, group_id, user_id, username, now)
                )
            else:
                cursor.execute(
                    "INSERT OR REPLACE INTO users (user_id, region, group_id, notifications, is_active, last_activity) "
                    "VALUES (?, COALESCE((SELECT region FROM users WHERE user_id=?),'ternopil'), ?, COALESCE((SELECT notifications FROM users WHERE user_id=?),1), 1, ?)", 
                    (user_id, user_id, group_id, user_id, now)
                )
            conn.commit()
    except Exception as e:
        logger.error(f"Error setting group: {e}")

def db_get_user(user_id):
    try:
        with sqlite3.connect(DB_FILE) as conn:
            cursor = conn.cursor()
            return cursor.execute("SELECT region, group_id, notifications FROM users WHERE user_id=?", (user_id,)).fetchone()
    except Exception:
        return None

def db_toggle_notification(user_id):
    try:
        with sqlite3.connect(DB_FILE) as conn:
            cursor = conn.cursor()
            res = cursor.execute("SELECT notifications FROM users WHERE user_id=?", (user_id,)).fetchone()
            
            new_status = 0 if res and res[0] == 1 else 1
            cursor.execute("UPDATE users SET notifications=? WHERE user_id=?", (new_status, user_id))
            conn.commit()
            return new_status
    except Exception:
        return 1

def db_get_all_users_with_groups():
    try:
        with sqlite3.connect(DB_FILE) as conn:
            cursor = conn.cursor()
            return cursor.execute("SELECT user_id, region, group_id FROM users WHERE notifications=1 AND group_id IS NOT NULL").fetchall()
    except Exception:
        return []

# ─────────────────────────────────────────────
# Group helpers
# ─────────────────────────────────────────────

def get_groups_for_region(region):
    if region == "kyiv-city":
        return [f"{i}.1" for i in range(1, KYIV_CITY_MAX_GROUP + 1)]
    else:
        return [f"{i}.{j}" for i in range(1, 7) for j in range(1, 3)]

def _t2m(t_str):
    h, m = t_str.strip().split(":")
    return int(h) * 60 + int(m)

# ─────────────────────────────────────────────
# OPTIMIZED Schedule fetchers
# ─────────────────────────────────────────────

def fetch_and_cache_bezsvitla(region):
    today = datetime.now(KYIV_TZ)
    tomorrow = today + timedelta(days=1)
    today_str = today.strftime("%Y-%m-%d")
    tomorrow_str = tomorrow.strftime("%Y-%m-%d")

    slug = BEZSVITLA_SLUGS.get(region)
    if not slug:
        return

    def process_page(url, date_key, date_str):
        try:
            r = requests.get(url, headers=WEB_HEADERS, timeout=15)
            r.raise_for_status()
            soup = BeautifulSoup(r.text, "html.parser")
            
            for card in soup.find_all(class_="card"):
                header = card.find(class_="card-header")
                if not header:
                    continue
                    
                text = header.get_text(strip=True)
                m = re.search(r'(\d+\.\d+|\d+)', text)
                if not m:
                    continue
                grp = m.group(1)
                
                slots = {}
                for li in card.find_all("li"):
                    t_text = li.get_text(separator=" ", strip=True)
                    tm = re.search(r'(\d{2}:\d{2})\s*[–\-]\s*(\d{2}:\d{2})', t_text)
                    if not tm:
                        continue
                        
                    is_on = li.find(class_="icon-on") is not None
                    code = "0" if is_on else "1"
                    
                    cur = _t2m(tm.group(1))
                    end_m = _t2m(tm.group(2)) if tm.group(2) != "24:00" else 24 * 60
                    
                    while cur < end_m:
                        h, mn = divmod(cur, 60)
                        slots[f"{h:02}:{mn:02}"] = code
                        cur += 30
                
                if slots:
                    k = get_cache_key(region, grp)
                    if k not in schedules_cache:
                        schedules_cache[k] = {
                            'today': None, 'today_date': today_str, 
                            'tomorrow': None, 'tomorrow_date': tomorrow_str
                        }
                    schedules_cache[k][date_key] = slots
        except Exception:
            pass

    process_page(f"{BEZSVITLA_BASE}/{slug}", 'today', today_str)
    process_page(f"{BEZSVITLA_BASE}/{slug}/grafik-na-{tomorrow_str}", 'tomorrow', tomorrow_str)

def fetch_ternopil_schedule(group_id):
    from urllib.parse import quote
    utc_now = datetime.now(timezone.utc)
    db = (utc_now + timedelta(days=2)).strftime("%Y-%m-%dT00:00:00+00:00")
    da = (utc_now - timedelta(days=1)).strftime("%Y-%m-%dT00:00:00+00:00")
    
    creds = GROUP_CREDS.get(group_id, GROUP_CREDS["3.1"])
    url = f"{API_BASE}?before={quote(db)}&after={quote(da)}&group[]={group_id}&time={creds['time']}"
    
    headers = BASE_HEADERS.copy()
    headers["X-debug-key"] = creds["key"]
    
    try:
        r = requests.get(url, headers=headers, timeout=20)
        if r.status_code == 404:
            return None
        r.raise_for_status()
        
        items = r.json().get('hydra:member', [])
        if not items:
            return None
            
        today_str = datetime.now(KYIV_TZ).strftime("%Y-%m-%d")
        tom_str = (datetime.now(KYIV_TZ) + timedelta(days=1)).strftime("%Y-%m-%d")
        res = {'today': None, 'today_date': today_str, 'tomorrow': None, 'tomorrow_date': tom_str}
        
        for item in items:
            dg = item.get('dateGraph', '').split('T')[0]
            times = item['dataJson'].get(group_id, {}).get('times') or list(item['dataJson'].values())[0]['times']
            
            if dg == today_str:
                res['today'] = times
            elif dg == tom_str:
                res['tomorrow'] = times
        return res
    except Exception:
        return None

def prefetch_region_schedules(region):
    groups = get_groups_for_region(region)
    missing = [g for g in groups if not schedules_cache.get(get_cache_key(region, g))]
    
    if not missing:
        return

    if region != "ternopil":
        fetch_and_cache_bezsvitla(region)
    else:
        with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
            futures = {executor.submit(fetch_ternopil_schedule, g): g for g in missing}
            for fut in concurrent.futures.as_completed(futures):
                g = futures[fut]
                try:
                    res = fut.result()
                    if res:
                        schedules_cache[get_cache_key(region, g)] = res
                except Exception:
                    pass

def get_cache_key(region, group_id):
    return f"{region}_{group_id}"

def get_cached_schedule(region, group_id):
    key = get_cache_key(region, group_id)
    cached = schedules_cache.get(key)
    
    if not cached:
        prefetch_region_schedules(region)
        cached = schedules_cache.get(key)
        
    return cached

# ─────────────────────────────────────────────
# EXACT EXACT Chart generator (Pillow) - High Quality
# ─────────────────────────────────────────────

def create_bulb_icon(is_on, size=24):
    scale = 4
    full_size = size * scale

    img = Image.new('RGBA', (full_size, full_size), (0, 0, 0, 0))
    draw = ImageDraw.Draw(img)
    
    color = "#1F2336" if is_on else "#FFFFFF"
    cx, cy = full_size // 2, full_size // 2
    lw = max(1, 1 * scale)

    draw.ellipse([cx - 4*scale, cy - 5*scale, cx + 4*scale, cy + 3*scale], outline=color, width=lw)
    draw.rectangle([cx - 2*scale, cy + 3*scale, cx + 2*scale, cy + 6*scale], fill=color)
    
    if is_on:
        draw.line([cx, cy - 9*scale, cx, cy - 6*scale], fill=color, width=lw)
        draw.line([cx - 7*scale, cy - 3*scale, cx - 4*scale, cy - 2*scale], fill=color, width=lw)
        draw.line([cx + 4*scale, cy - 2*scale, cx + 7*scale, cy - 3*scale], fill=color, width=lw)
    else:
        draw.line([cx - 6*scale, cy - 6*scale, cx + 6*scale, cy + 6*scale], fill=color, width=lw + 2)

    return img.resize((size, size), Image.Resampling.LANCZOS)

def create_lightning_icon(size=24):
    scale = 4
    full_size = size * scale
    
    img = Image.new('RGBA', (full_size, full_size), (0, 0, 0, 0))
    draw = ImageDraw.Draw(img)

    cx, cy = full_size // 2, full_size // 2
    points = [
        (cx + 2*scale, cy - 8*scale),   
        (cx - 6*scale, cy + 1*scale),   
        (cx - 1*scale, cy + 1*scale),   
        (cx - 3*scale, cy + 9*scale),   
        (cx + 6*scale, cy - 2*scale),   
        (cx + 1*scale, cy - 2*scale),   
    ]
    
    draw.polygon(points, fill="#FFC107")
    
    return img.resize((size, size), Image.Resampling.LANCZOS)

def create_lightning_icon(size=24):
    scale = 4
    full_size = size * scale
    
    img = Image.new('RGBA', (full_size, full_size), (0, 0, 0, 0))
    draw = ImageDraw.Draw(img)

    cx, cy = full_size // 2, full_size // 2
    points = [
        (cx + 2*scale, cy - 8*scale),   
        (cx - 6*scale, cy + 1*scale),   
        (cx - 1*scale, cy + 1*scale),   
        (cx - 3*scale, cy + 9*scale),   
        (cx + 6*scale, cy - 2*scale),   
        (cx + 1*scale, cy - 2*scale),   
    ]
    
    draw.polygon(points, fill="#FFC107")
    
    return img.resize((size, size), Image.Resampling.LANCZOS)

def generate_general_schedule_chart(region, target_day, date_str):
    prefetch_region_schedules(region)
    groups = get_groups_for_region(region)
    show_maybe = (region == "ternopil")

    row_h = 36          
    bar_h = 22          
    bar_pad = (row_h - bar_h) // 2
    
    col_w = 30          
    left_margin = 80
    right_margin = 35   
    top_margin = 125    
    bottom_margin = 35
    
    width = left_margin + 24 * col_w + right_margin
    height = top_margin + len(groups) * row_h + bottom_margin
    
    img = Image.new('RGBA', (width, height), color=(255, 255, 255, 255))
    draw = ImageDraw.Draw(img)
    
    def get_font(size, bold=False):
        fonts = ["arialbd.ttf" if bold else "arial.ttf", "Segoe UI Bold.ttf", "DejaVuSans-Bold.ttf"]
        for f in fonts:
            try:
                return ImageFont.truetype(f, size)
            except IOError:
                pass
        return ImageFont.load_default()
        
    font_main = get_font(18, bold=True)
    font_sm = get_font(12, bold=False)
    font_hdr_bold = get_font(20, bold=True)
    font_hdr = get_font(16, bold=False)
    
    C_ON = "#FCCC44"       
    C_OFF = "#3B425E"      
    C_MAYBE = "#F39C12"
    C_BG_1 = "#F2F2F2"     
    C_BG_2 = "#FFFFFF"     
    C_GRID = "#D3D3D3"
    C_TEXT = "#000000"
    C_SUBTEXT = "#666666"
    
    months = ["січня", "лютого", "березня", "квітня", "травня", "червня", "липня", "серпня", "вересня", "жовтня", "листопада", "грудня"]
    try:
        dt = datetime.strptime(date_str, "%Y-%m-%d")
        date_ua = f"{dt.day} {months[dt.month - 1]} {dt.year}р."
    except:
        date_ua = date_str

    rname = REGIONS_CONFIG.get(region, {}).get('name', region)
    clean_rname = re.sub(r'[^\w\sа-яА-ЯіІїЇєЄґҐ.-]', '', rname).strip()

    draw.text((15, 15), f"{clean_rname}   {date_ua}", font=font_hdr_bold, fill=C_TEXT)
    draw.text((15, 45), "Розклад відключень на день", font=font_hdr_bold, fill=C_TEXT)
    draw.text((15, 75), "За даними обленерго", font=font_hdr, fill=C_TEXT)

    bot_title = "СвітлоГрафіки" 
    bot_user = "@ToeElectricity_bot"
    
    w1 = int(draw.textlength(bot_title, font=font_hdr_bold))
    w2 = int(draw.textlength(bot_user, font=font_hdr))
    
    title_x = width - w1 - 35 
    draw.text((title_x, 15), bot_title, font=font_hdr_bold, fill=C_TEXT)
    draw.text((width - w2 - 15, 45), bot_user, font=font_hdr, fill=C_SUBTEXT)

    header_icon = create_lightning_icon(size=22).convert("RGBA")
    img.alpha_composite(header_icon, (title_x + w1 + 5, 14))

    icon_size = 18
    icon_on = create_bulb_icon(True, size=icon_size).convert("RGBA")
    icon_off = create_bulb_icon(False, size=icon_size).convert("RGBA")
    
    for i, grp in enumerate(groups):
        y0 = top_margin + i * row_h
        bg_color = C_BG_1 if i % 2 == 0 else C_BG_2
        
        draw.rectangle([0, y0, left_margin, y0 + row_h], fill=bg_color)
        draw.text((15, y0 + (row_h - 20)//2), grp, font=font_main, fill=C_TEXT)
        draw.line([(left_margin, y0), (width, y0)], fill=C_GRID, width=1)

    draw.line([(left_margin, top_margin + len(groups) * row_h), (width, top_margin + len(groups) * row_h)], fill=C_GRID, width=1)

    for i, grp in enumerate(groups):
        y0 = top_margin + i * row_h
        y0_bar = y0 + bar_pad
        y1_bar = y0 + row_h - bar_pad
        
        cached = schedules_cache.get(get_cache_key(region, grp))
        sched_data = cached[target_day] if cached and cached.get(target_day) else {}
        
        blocks = []
        cur_code = None
        start_m_idx = 0
        
        for m_idx in range(49): 
            if m_idx < 48:
                h = m_idx // 2
                m = (m_idx % 2) * 30
                code = sched_data.get(f"{h:02}:{m:02}", "0")
                if not show_maybe and code == "10": 
                    code = "0"
            else: 
                code = None
            
            if code != cur_code:
                if cur_code is not None:
                    blocks.append({'code': cur_code, 's': start_m_idx, 'e': m_idx})
                cur_code = code
                start_m_idx = m_idx

        for b in blocks:
            color = C_ON
            if b['code'] == "1": color = C_OFF
            elif b['code'] == "10": color = C_MAYBE
            
            x0 = left_margin + b['s'] * (col_w / 2)
            x1 = left_margin + b['e'] * (col_w / 2)
            
            draw.rectangle([x0, y0_bar, x1, y1_bar], fill=color)
            
            if (x1 - x0) >= col_w: 
                cx = int((x0 + x1) / 2)
                cy = int((y0_bar + y1_bar) / 2)
                icon = icon_on if b['code'] != "1" else icon_off
                img.alpha_composite(icon, (cx - icon_size // 2, cy - icon_size // 2))

    for h in range(25):
        x = left_margin + h * col_w
        draw.line([(x, top_margin), (x, height - bottom_margin)], fill=C_GRID, width=1)
        
        tw = 8 if h < 10 else 14
        draw.text((x - tw//2, top_margin - 20), str(h), font=font_sm, fill=C_SUBTEXT)
        draw.text((x - tw//2, height - bottom_margin + 8), str(h), font=font_sm, fill=C_SUBTEXT)

    draw.text((15, top_margin - 20), "Група ↓", font=font_sm, fill=C_SUBTEXT)
    draw.text((15, height - bottom_margin + 8), "Група ↑", font=font_sm, fill=C_SUBTEXT)

    wm_text = bot_user
    # Big centered watermark
    wm_font_lg = get_font(60, bold=True)
    wm_w_lg = int(draw.textlength(wm_text, font=wm_font_lg))
    wm_img_lg = Image.new('RGBA', (wm_w_lg, 80), (255, 255, 255, 0))

    ImageDraw.Draw(wm_img_lg).text((0, 0), wm_text, font=wm_font_lg, fill=(120, 120, 120, 45)) 
    wm_rot_lg = wm_img_lg.rotate(-30, expand=1)

    # Smaller watermarks for corners
    wm_font_sm = get_font(35, bold=True)
    wm_w_sm = int(draw.textlength(wm_text, font=wm_font_sm))
    wm_img_sm = Image.new('RGBA', (wm_w_sm, 50), (255, 255, 255, 0))
    # Transpancy is set in fill color (120, 120, 120, 45)
    ImageDraw.Draw(wm_img_sm).text((0, 0), wm_text, font=wm_font_sm, fill=(120, 120, 120, 45)) 
    wm_rot_sm = wm_img_sm.rotate(-30, expand=1)
    
    # 1. centered big watermark
    cx = width // 2 - wm_rot_lg.width // 2
    cy = height // 2 - wm_rot_lg.height // 2
    img.alpha_composite(wm_rot_lg, (cx, cy))
    
    # 2. down-left small watermark
    bl_x = left_margin + 10
    bl_y = height - bottom_margin - wm_rot_sm.height - 20
    img.alpha_composite(wm_rot_sm, (int(bl_x), int(bl_y)))
    
    # 3. up-right small watermark
    tr_x = width - right_margin - wm_rot_sm.width - 10
    tr_y = top_margin + 10
    img.alpha_composite(wm_rot_sm, (int(tr_x), int(tr_y)))

    img = img.convert('RGB')
    buf = io.BytesIO()
    img.save(buf, format="PNG")
    buf.seek(0)
    return buf

# ─────────────────────────────────────────────
# Schedule formatting
# ─────────────────────────────────────────────

def format_schedule_list(schedule, region):
    show_maybe = (region == "ternopil")
    
    STATUS_MAP = {
        "0": ("🟢", "Є світло"), 
        "1": ("🔴", "НЕМАЄ світла"), 
        "10": ("🟡", "Можливе вимкнення") if show_maybe else ("🟢", "Є світло")
    }
    
    lines = []
    for h in range(12):
        for m in (0, 30):
            t1 = f"{h:02}:{m:02}"
            t2 = f"{h+12:02}:{m:02}"
            
            c1 = schedule.get(t1, "?")
            c2 = schedule.get(t2, "?")
            
            icon1 = STATUS_MAP.get(c1, ('❓',))[0]
            icon2 = STATUS_MAP.get(c2, ('❓',))[0]
            
            lines.append(f"{t1} {icon1}    {t2} {icon2}")
            
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
            key = f"{h:02}:{m:02}"
            code = schedule.get(key, "10")
            if not show_maybe and code == "10":
                code = "0"
            full_schedule[key] = code
            
    times = sorted(list(full_schedule.keys()))
    cur_code = full_schedule.get(current_slot, "10" if show_maybe else "0")
    
    if show_maybe:
        is_light_now = cur_code in ["0", "10"]
    else:
        is_light_now = cur_code == "0"
    
    try: 
        curr_idx = times.index(current_slot)
    except ValueError: 
        return "⚠️ Не вдалося визначити час."

    start_idx = curr_idx
    while start_idx > 0:
        prev_code = full_schedule[times[start_idx - 1]]
        prev_is_light = prev_code in ["0", "10"] if show_maybe else prev_code == "0"
        
        if prev_is_light != is_light_now:
            break
        start_idx -= 1
        
    start_slot = times[start_idx]

    end_idx = curr_idx
    end_slot = "24:00"
    while end_idx < len(times) - 1:
        next_code = full_schedule[times[end_idx + 1]]
        next_is_light = next_code in ["0", "10"] if show_maybe else next_code == "0"
        
        if next_is_light != is_light_now:
            end_slot = times[end_idx + 1]
            break
        end_idx += 1

    total_m = (_t2m(end_slot) if end_slot != "24:00" else 24*60) - _t2m(start_slot)
    h, m = divmod(total_m, 60)
    dur_str = f"{h} год" + (f" {m} хв" if m else "")
    
    if show_maybe:
        SMAP = {"0": ("🟢", "Є світло"), "1": ("🔴", "НЕМАЄ світла"), "10": ("🟡", "Можливе відключення")}
    else:
        SMAP = {"0": ("🟢", "Є світло"), "1": ("🔴", "НЕМАЄ світла"), "10": ("🟢", "Є світло")}
        
    icon, text_status = SMAP.get(cur_code, ("❓", "Невідомо"))
    
    msg = f"Зараз ({current_slot}): {icon} <b>{text_status}</b>\n"
    if is_light_now:
        msg += f"Світло БУДЕ з {start_slot} по {end_slot}\n"
    else:
        msg += f"Світла НЕ БУДЕ з {start_slot} по {end_slot}\n"
    msg += f"⏱ Всього: <b>{dur_str}</b>"
    
    return msg

# ─────────────────────────────────────────────
# Keyboard builders
# ─────────────────────────────────────────────

def main_menu_kb():
    markup = types.ReplyKeyboardMarkup(resize_keyboard=True)
    markup.row("📅 Отримати графік", "💡 Стан")
    markup.row("📊 Загальний графік")
    markup.row("⚙️ Налаштування")
    return markup

def region_kb():
    markup = types.InlineKeyboardMarkup(row_width=2)
    buttons = []
    for rk, cfg in sorted(REGIONS_CONFIG.items(), key=lambda x: x[1]['name']):
        buttons.append(types.InlineKeyboardButton(cfg['name'], callback_data=f"set_region_{rk}"))
        
    for i in range(0, len(buttons), 2): 
        markup.row(*buttons[i:i+2])
        
    return markup

def groups_kb(region="ternopil"):
    markup = types.InlineKeyboardMarkup()
    groups = get_groups_for_region(region)
    
    if region == "kyiv-city":
        row = []
        for g in groups:
            row.append(types.InlineKeyboardButton(g, callback_data=f"set_group_{g}"))
            if len(row) == 5: 
                markup.row(*row)
                row = []
        if row: 
            markup.row(*row)
    else:
        for i in range(1, 7): 
            markup.row(
                types.InlineKeyboardButton(f"Група {i}.1", callback_data=f"set_group_{i}.1"), 
                types.InlineKeyboardButton(f"Група {i}.2", callback_data=f"set_group_{i}.2")
            )
            
    return markup

# ─────────────────────────────────────────────
# Bot handlers
# ─────────────────────────────────────────────

@bot.message_handler(commands=['start'])
def send_welcome(message):
    init_db()
    try:
        user_id = message.chat.id
        db_update_user_activity(user_id, message.from_user.username)
        user_data = db_get_user(user_id)
        
        if user_data and user_data[1]:
            region, group_id = user_data[0], user_data[1]
            rname = REGIONS_CONFIG.get(region, {}).get('name', region)
            bot.send_message(
                user_id, 
                f"Вітаю! 👋\nВаша область: <b>{rname}</b>\nВаша група: <b>{group_id}</b>\n\nДля зміни — скористайтесь <b>⚙️ Налаштування</b>", 
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
        db_set_region(call.message.chat.id, region, call.from_user.username)
        rname = REGIONS_CONFIG.get(region, {}).get('name', region)
        
        bot.answer_callback_query(call.id, f"Обрано: {rname}")
        bot.send_message(
            call.message.chat.id, 
            f"✅ Область: <b>{rname}</b>\n\nТепер оберіть вашу <b>групу</b>:", 
            reply_markup=groups_kb(region), 
            parse_mode="HTML"
        )
    except Exception: 
        pass

@bot.callback_query_handler(func=lambda call: call.data.startswith('set_group_'))
def callback_set_group(call):
    try:
        group_id = call.data.replace('set_group_', '')
        user_id = call.message.chat.id
        db_set_group(user_id, group_id, call.from_user.username)
        
        user_data = db_get_user(user_id)
        region = user_data[0] if user_data else "ternopil"
        rname = REGIONS_CONFIG.get(region, {}).get('name', "ternopil")
        
        bot.answer_callback_query(call.id, "Групу збережено!")
        bot.send_message(
            user_id, 
            f"✅ Область: <b>{rname}</b>\nГрупа: <b>{group_id}</b>", 
            reply_markup=main_menu_kb(), 
            parse_mode="HTML"
        )
    except Exception: 
        pass

@bot.callback_query_handler(func=lambda call: call.data.startswith('show_tomorrow_'))
def callback_show_tomorrow(call):
    try:
        udata = db_get_user(call.message.chat.id)
        if not udata: 
            return
            
        region, group_id = udata[0], udata[1]
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
    except Exception: 
        pass

@bot.message_handler(func=lambda message: message.text == "📅 Отримати графік")
def send_schedule(message):
    try:
        user_id = message.chat.id
        db_update_user_activity(user_id, message.from_user.username)
        
        udata = db_get_user(user_id)
        if not udata or not udata[1]: 
            return bot.send_message(user_id, "Спочатку натисніть /start")
            
        region, group_id = udata[0], udata[1]
        cached = get_cached_schedule(region, group_id)
        rname = REGIONS_CONFIG.get(region, {}).get('name', region)

        if cached and cached.get('today'):
            markup = types.InlineKeyboardMarkup()
            if cached.get('tomorrow'): 
                markup.add(types.InlineKeyboardButton(f"➡️ Графік на Завтра ({cached['tomorrow_date']})", callback_data=f"show_tomorrow_{group_id}"))
                
            text = format_schedule_list(cached['today'], region)
            bot.send_message(
                user_id, 
                f"📅 <b>Графік на СЬОГОДНІ ({cached['today_date']}):</b>\n<i>{rname}, Група {group_id}</i>\n\n{text}", 
                parse_mode="HTML", 
                reply_markup=markup if cached.get('tomorrow') else None
            )
        elif cached and cached.get('tomorrow'):
            markup = types.InlineKeyboardMarkup()
            markup.add(types.InlineKeyboardButton(f"➡️ Графік на Завтра ({cached['tomorrow_date']})", callback_data=f"show_tomorrow_{group_id}"))
            bot.send_message(
                user_id, 
                f"<i>{rname}, Група {group_id}</i>\n\n❌ Графік на сьогодні не знайдений.\nАле є графік на завтра:", 
                parse_mode="HTML", 
                reply_markup=markup
            )
        else: 
            bot.send_message(user_id, "❌ Графік зараз недоступний.")
    except Exception as e: 
        logger.error(f"Send schedule error: {e}")

@bot.message_handler(func=lambda message: message.text == "💡 Стан")
def send_status(message):
    try:
        user_id = message.chat.id
        db_update_user_activity(user_id, message.from_user.username)
        
        udata = db_get_user(user_id)
        if not udata or not udata[1]: 
            return bot.send_message(user_id, "Спочатку оберіть групу через /start")
            
        region, group_id = udata[0], udata[1]
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
    except Exception: 
        pass

@bot.message_handler(func=lambda message: message.text == "📊 Загальний графік")
def send_chart(message):
    try:
        user_id = message.chat.id
        db_update_user_activity(user_id, message.from_user.username)
        
        udata = db_get_user(user_id)
        if not udata or not udata[0]: 
            return bot.send_message(user_id, "Спочатку натисніть /start")
        
        region = udata[0]
        bot.send_chat_action(user_id, 'upload_photo')
        
        groups = get_groups_for_region(region)
        if not groups: 
            return bot.send_message(user_id, "❌ Немає груп для цієї області.")
        
        cached = get_cached_schedule(region, groups[0])
        target_day, date_str, label = None, None, None
        
        if cached and cached.get('today'):
            target_day, date_str, label = 'today', cached['today_date'], "СЬОГОДНІ"
        elif cached and cached.get('tomorrow'):
            target_day, date_str, label = 'tomorrow', cached['tomorrow_date'], "ЗАВТРА"
        else: 
            return bot.send_message(user_id, "Немає даних для графіка.")
            
        buf = generate_general_schedule_chart(region, target_day, date_str)
        rname = REGIONS_CONFIG.get(region, {}).get('name', region)
        bot.send_photo(
            user_id, 
            buf, 
            caption=f"📊 <b>Загальний графік на {label}</b>\n{rname} | {date_str}",
            parse_mode="HTML"
        )
    except Exception as e:
        logger.error(f"Chart error: {e}")
        bot.send_message(message.chat.id, "❌ Не вдалося згенерувати графік.")

@bot.message_handler(func=lambda message: message.text == "⚙️ Налаштування")
def settings(message):
    try:
        user_id = message.chat.id
        udata = db_get_user(user_id)
        if not udata: 
            return bot.send_message(user_id, "Натисніть /start")
            
        region, group_id, notifications = udata
        rname = REGIONS_CONFIG.get(region, {}).get('name', region)
        notif_status = "Увімкнено ✅" if notifications else "Вимкнено 🔕"
        group_str = group_id if group_id else "не обрано"
        
        markup = types.InlineKeyboardMarkup()
        markup.add(types.InlineKeyboardButton(f"Сповіщення: {notif_status}", callback_data="toggle_notif"))
        markup.add(types.InlineKeyboardButton("🔄 Змінити групу", callback_data="change_group"))
        markup.add(types.InlineKeyboardButton("🗺 Змінити область", callback_data="change_region"))
        
        bot.send_message(
            user_id, 
            f"⚙️ <b>Налаштування</b>\n\nОбласть: <b>{rname}</b>\nГрупа: <b>{group_str}</b>\n\nСповіщення про зміни у графіку:", 
            reply_markup=markup, 
            parse_mode="HTML"
        )
    except Exception: 
        pass

@bot.callback_query_handler(func=lambda call: call.data == "change_region")
def change_region_btn(call): 
    bot.send_message(call.message.chat.id, "🗺 Оберіть нову область:", reply_markup=region_kb())
    bot.answer_callback_query(call.id)

@bot.callback_query_handler(func=lambda call: call.data == "change_group")
def change_group_btn(call): 
    user_data = db_get_user(call.message.chat.id)
    region = user_data[0] if user_data else "ternopil"
    bot.send_message(call.message.chat.id, "Виберіть нову групу:", reply_markup=groups_kb(region))
    bot.answer_callback_query(call.id)

@bot.callback_query_handler(func=lambda call: call.data == "toggle_notif")
def toggle_notifications(call):
    ns = db_toggle_notification(call.message.chat.id)
    notif_status = "Увімкнено ✅" if ns else "Вимкнено 🔕"
    
    markup = types.InlineKeyboardMarkup()
    markup.add(types.InlineKeyboardButton(f"Сповіщення: {notif_status}", callback_data="toggle_notif"))
    markup.add(types.InlineKeyboardButton("🔄 Змінити групу", callback_data="change_group"))
    markup.add(types.InlineKeyboardButton("🗺 Змінити область", callback_data="change_region"))
    
    try: 
        bot.edit_message_reply_markup(
            call.message.chat.id, 
            call.message.message_id, 
            reply_markup=markup
        )
    except Exception: 
        pass

# ─────────────────────────────────────────────
# Admin commands
# ─────────────────────────────────────────────
@bot.message_handler(commands=['stats'])
def admin_stats(m):
    if m.from_user.id != ADMIN_ID: 
        return
    with sqlite3.connect(DB_FILE) as c:
        count = c.execute('SELECT COUNT(*) FROM users').fetchone()[0]
        bot.reply_to(m, f"Користувачів: {count}")

@bot.message_handler(commands=['msg_all'])
def admin_broadcast(m):
    if m.from_user.id != ADMIN_ID: 
        return
        
    parts = m.text.split(maxsplit=1)
    if len(parts) < 2:
        return
    msg_text = parts[1]
    
    users = db_get_all_users_with_groups()
    bot.reply_to(m, f"Розсилка на {len(users)}...")
    
    for u in users:
        try: 
            bot.send_message(u[0], msg_text, parse_mode="HTML")
        except Exception: 
            pass

# ─────────────────────────────────────────────
# Background scheduler
# ─────────────────────────────────────────────

def update_all_schedules():
    all_users = db_get_all_users_with_groups()
    needed = set((u[1], u[2]) for u in all_users if u[2])
    
    with sqlite3.connect(DB_FILE) as conn:
        for (r, g) in needed:
            old = schedules_cache.get(get_cache_key(r, g))
            schedules_cache.pop(get_cache_key(r, g), None) 
            new = get_cached_schedule(r, g)
            
            if not new or not old: 
                continue
            
            targets = [u[0] for u in all_users if u[1] == r and u[2] == g]
            
            if new.get('today') and new.get('today') != old.get('today'):
                text = format_schedule_list(new['today'], r)
                for uid in targets:
                    try: 
                        bot.send_message(
                            uid, 
                            f"⚠️ <b>Графік на СЬОГОДНІ ЗМІНИВСЯ!</b>\n\n{text}", 
                            parse_mode="HTML"
                        )
                    except Exception: 
                        pass

def check_upcoming_changes():
    now = datetime.now(KYIV_TZ)
    if now.minute not in (0, 30): 
        return
        
    future_time = now + timedelta(minutes=30)
    fs = f"{future_time.hour:02}:{30 if future_time.minute >= 30 else 0:02}"
    cs = f"{now.hour:02}:{30 if now.minute >= 30 else 0:02}"
    
    for uid, r, g in db_get_all_users_with_groups():
        ak = f"{uid}_{fs}"
        if last_sent_alerts.get(ak) or not schedules_cache.get(get_cache_key(r, g)): 
            continue
            
        td = schedules_cache[get_cache_key(r, g)].get('today', {})
        if not td: 
            continue
            
        sm = (r == "ternopil")
        cc = td.get(cs)
        fc = td.get(fs)
        if cc is None or fc is None: 
            continue
        
        cl = cc in ["0", "10"] if sm else cc == "0"
        fl = fc in ["0", "10"] if sm else fc == "0"
        
        if cl != fl:
            times_to_check = sorted(td.keys())[sorted(td.keys()).index(fs):]
            dur = sum(30 for t in times_to_check if (td[t] in ["0", "10"] if sm else td[t] == "0") == fl)
            
            if fl:
                m = f"🟢 Через 30 хв ({fs}) буде світло!\n⏱ Всього: {dur//60} год {dur%60} хв"
            else:
                m = f"🔴 Через 30 хв ({fs}) вимкнення світла!\n⏱ Всього: {dur//60} год {dur%60} хв"
                
            try: 
                bot.send_message(uid, m, parse_mode="HTML")
                last_sent_alerts[ak] = True
            except Exception: 
                pass

def scheduler_loop():
    schedule.every(15).minutes.do(update_all_schedules)
    schedule.every(1).minutes.do(check_upcoming_changes)
    
    while True: 
        schedule.run_pending()
        time.sleep(1)

if __name__ == "__main__":
    init_db()
    threading.Thread(target=scheduler_loop, daemon=True).start()
    
    while True:
        try: 
            bot.infinity_polling(timeout=10, long_polling_timeout=5)
        except Exception as e: 
            logger.critical(f"Bot crash: {e}. Restarting...")
            time.sleep(5)