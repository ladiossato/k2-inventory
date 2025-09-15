# k2_inventory_app.py
# -------------------------------------------------------------
# Enhanced Multi-Page Inventory Management System
# - Container-based calculations (cases, quarts, trays, bags, bottles)
# - Mobile-first responsive design
# - FIXED form validation and consumption days display
# - Entry Form, Analytics Dashboard, Admin Settings
# - 3-month data retention with historical trends
# - Two-message auto-request system
#
# Pages:
# 1. Entry - Mobile-optimized data entry (On-Hand, Received only)
# 2. Analytics - Historical trends and insights  
# 3. Admin - Item management, system settings, test functions
#
# Run:
#   pip install streamlit apscheduler requests python-dotenv plotly pandas
#   streamlit run k2_inventory_app.py
# -------------------------------------------------------------
import json
import os
import math
import sqlite3
import threading
import logging
import time as time_module
import pandas as pd
from datetime import datetime, date, time, timedelta
from zoneinfo import ZoneInfo
from typing import Dict, List, Optional, Tuple, NamedTuple, Any
from dataclasses import dataclass

import requests
import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from dotenv import load_dotenv
conversation_states: Dict[int, Dict[str, Any]] = {} # Global state for user conversations

# Configure Streamlit for mobile-first design
st.set_page_config(
    page_title="K2 Inventory",
    page_icon="🪣",
    layout="wide",
    initial_sidebar_state="collapsed"
)

# Mobile-first CSS injection
st.markdown("""
<style>
    /* Mobile-first responsive design */
    @media (max-width: 768px) {
        .main .block-container {
            padding-top: 1rem;
            padding-left: 1rem;
            padding-right: 1rem;
        }
        
        .stButton > button {
            width: 100%;
            height: 3rem;
            font-size: 1.1rem;
        }
        
        .stTextInput > div > div > input {
            font-size: 1.1rem;
            height: 3rem;
        }
        
        .stSelectbox > div > div > select {
            font-size: 1.1rem;
            height: 3rem;
        }
        
        .stTextArea textarea {
            font-size: 1.1rem;
        }
        
        .stDateInput > div > div > input {
            font-size: 1.1rem;
            height: 3rem;
        }
    }
    
    /* Tab styling for mobile */
    .stTabs [data-baseweb="tab-list"] {
        gap: 2px;
    }
    
    .stTabs [data-baseweb="tab"] {
        padding: 12px 20px;
        font-size: 1rem;
        font-weight: 600;
    }
    
    /* Status indicators */
    .status-green { color: #00C851; font-weight: bold; }
    .status-yellow { color: #FFB300; font-weight: bold; }
    .status-red { color: #FF3547; font-weight: bold; }
    .status-missing { color: #6C757D; font-weight: bold; }
    
    /* Metric cards */
    .metric-card {
        background: #F8F9FA;
        padding: 1rem;
        border-radius: 8px;
        border-left: 4px solid #007BFF;
        margin: 0.5rem 0;
    }
</style>
""", unsafe_allow_html=True)

# ------------------------- Enhanced Logging -------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(funcName)s | %(message)s",
)
LOG = logging.getLogger("k2")

# ----------------------- Environment & Validation -----------------------
load_dotenv()
TZ = ZoneInfo(os.getenv("TZ", "America/Chicago"))

def validate_environment():
    """Validate required environment variables on startup."""
    BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
    if not BOT_TOKEN:
        LOG.warning("TELEGRAM_BOT_TOKEN not set - Telegram features disabled")
        return ""
    
    # Validate chat IDs are integers
    chat_vars = ["CHAT_ONHAND", "CHAT_AUTOREQUEST", "CHAT_RECEIVED", "CHAT_REASSURANCE"]
    for var in chat_vars:
        val = os.getenv(var)
        if val and not val.lstrip('-').isdigit():
            LOG.warning(f"{var} must be a valid integer chat ID")
    
    LOG.info("Environment validation completed")
    return BOT_TOKEN

BOT_TOKEN = validate_environment()

# Toggle testing vs production
USE_TEST_CHAT = True   # <-- flip this between True/False
TEST_CHAT = 6904183057 # <-- your personal chat ID

if USE_TEST_CHAT:
    CHAT_ONHAND = CHAT_AUTOREQUEST = CHAT_RECEIVED = CHAT_REASSURANCE = TEST_CHAT
    LOG.info("Using TEST_CHAT mode - all messages go to %s", TEST_CHAT)
else:
    CHAT_ONHAND = int(os.getenv("CHAT_ONHAND", "-1002819958218"))
    CHAT_AUTOREQUEST = int(os.getenv("CHAT_AUTOREQUEST", "-1002819958218"))
    CHAT_RECEIVED = int(os.getenv("CHAT_RECEIVED", "-4957164054"))
    CHAT_REASSURANCE = int(os.getenv("CHAT_REASSURANCE", "6904183057"))

# Business rules
DELIVERY_WEEKDAYS = {0, 3}  # Monday=0, Thursday=3
DELIVERY_NOON = time(12, 0)
REQUEST_WINDOWS = {
    1: {"label": "Thursday Delivery", "total_days": 6.5},  # Tuesday 08:00
    5: {"label": "Monday Delivery", "total_days": 5.5},   # Saturday 08:00
}

AVONDALE_DELIVERY_WEEKDAYS = {0, 3}  # Monday=0, Thursday=3 (existing)
COMMISSARY_DELIVERY_WEEKDAYS = {0, 2, 4}  # Monday=0, Wednesday=2, Friday=4

AVONDALE_REQUEST_WINDOWS = {
    1: {"label": "Thursday Delivery", "total_days": 6.5},  # Tuesday 08:00
    5: {"label": "Monday Delivery", "total_days": 5.5},   # Saturday 08:00
}

COMMISSARY_REQUEST_WINDOWS = {
    6: {"label": "Monday Delivery", "total_days": 2.5},    # Sunday 08:00
    1: {"label": "Wednesday Delivery", "total_days": 2.5}, # Tuesday 08:00  
    3: {"label": "Friday Delivery", "total_days": 2.5},    # Thursday 08:00
}

# Keep original for backward compatibility
DELIVERY_WEEKDAYS = AVONDALE_DELIVERY_WEEKDAYS
REQUEST_WINDOWS = AVONDALE_REQUEST_WINDOWS

RUN_REASSURANCE = time(17, 0)
RUN_MISSING = time(23, 59)
RUN_REQ_HOUR = 8
DEFAULT_BUFFER_DAYS = 1.0

# Data retention policy
DATA_RETENTION_DAYS = 90  # 3 months

# Container-based items with ADU (containers per day) and unit types
AVONDALE_ITEMS = {
    "Steak": {"adu": 1.8, "unit_type": "case", "par_level": 6},
    "Salmon": {"adu": 0.9, "unit_type": "case", "par_level": 3},
    "Chipotle Aioli": {"adu": 8.0, "unit_type": "quart", "par_level": 24},
    "Garlic Aioli": {"adu": 6.0, "unit_type": "quart", "par_level": 18},
    "Jalapeno Aioli": {"adu": 5.0, "unit_type": "quart", "par_level": 15},
    "Sriracha Aioli": {"adu": 2.0, "unit_type": "quart", "par_level": 6},
    "Ponzu Sauce": {"adu": 3.0, "unit_type": "quart", "par_level": 9},
    "Teriyaki/Soyu Sauce": {"adu": 3.0, "unit_type": "quart", "par_level": 9},
    "Orange Sauce": {"adu": 4.0, "unit_type": "quart", "par_level": 12},
    "Bulgogi Sauce": {"adu": 3.0, "unit_type": "quart", "par_level": 9},
    "Fried Rice Sauce": {"adu": 4.0, "unit_type": "quart", "par_level": 12},
    "Honey": {"adu": 2.0, "unit_type": "bottle", "par_level": 6},
}

COMMISSARY_ITEMS = {
    "Fish": {"adu": 0.3, "unit_type": "tray", "par_level": 1},
    "Shrimp": {"adu": 0.5, "unit_type": "tray", "par_level": 2},
    "Grilled Chicken": {"adu": 2.5, "unit_type": "case", "par_level": 8},
    "Crispy Chicken": {"adu": 3.5, "unit_type": "case", "par_level": 11},
    "Crab Ragoon": {"adu": 1.9, "unit_type": "bag", "par_level": 6},
    "Nutella Ragoon": {"adu": 0.7, "unit_type": "bag", "par_level": 3},
    "Ponzu Cups": {"adu": 0.8, "unit_type": "quart", "par_level": 3},
}

# Combine for backward compatibility
ITEMS_CONFIG = {**AVONDALE_ITEMS, **COMMISSARY_ITEMS}

# Unit type mappings for display
UNIT_TYPES = {
    "case": "cases",
    "quart": "quarts", 
    "tray": "trays",
    "bag": "bags",
    "bottle": "bottles",
    "container": "containers"  # ADDED
}

# ----------------------- Enhanced Data Types -----------------------
@dataclass
class ItemStatus:
    name: str
    qty: Optional[float]
    status: str  # Green, Yellow, Red, Missing
    consumption_need: float
    par_gap: float
    par_level: float
    adu: float
    unit_type: str
    days_to_delivery: float
    days_coverage: float  # How many days current stock will last

@dataclass
class MessageStyle:
    """Consistent styling for Telegram messages."""
    HEADER = "🪣"
    SUCCESS = "✅"
    WARNING = "⚠️"
    CRITICAL = "🚨"
    INFO = "ℹ️"
    GREEN = "🟢"
    YELLOW = "🟡"
    RED = "🔴"
    MISSING = "❌"

# ------------------------- Enhanced Database -------------------------
DB_PATH = os.path.join(os.path.dirname(__file__), "k2.db")

SCHEMA_SQL = """
PRAGMA foreign_keys = ON;
PRAGMA journal_mode = WAL;

CREATE TABLE IF NOT EXISTS items (
    id INTEGER PRIMARY KEY,
    name TEXT UNIQUE NOT NULL,
    adu REAL NOT NULL,
    unit_type TEXT NOT NULL DEFAULT 'case',
    buffer_days REAL NOT NULL DEFAULT 1.0,
    par_level REAL NOT NULL,
    active BOOLEAN DEFAULT TRUE,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS nightly_on_hand (
    id INTEGER PRIMARY KEY,
    item_id INTEGER NOT NULL REFERENCES items(id) ON DELETE CASCADE,
    d TEXT NOT NULL,
    qty REAL,
    manager TEXT,
    notes TEXT,
    created_at TEXT NOT NULL
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_onhand_unique ON nightly_on_hand(item_id, d);
CREATE INDEX IF NOT EXISTS idx_onhand_date ON nightly_on_hand(d);

CREATE TABLE IF NOT EXISTS transfers (
    id INTEGER PRIMARY KEY,
    item_id INTEGER NOT NULL REFERENCES items(id) ON DELETE CASCADE,
    d TEXT NOT NULL,
    type TEXT NOT NULL CHECK (type IN ('Received')),
    received_qty REAL,
    notes TEXT,
    created_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_transfers_item_date ON transfers(item_id, d);
CREATE INDEX IF NOT EXISTS idx_transfers_date ON transfers(d);

CREATE TABLE IF NOT EXISTS auto_requests (
    id INTEGER PRIMARY KEY,
    item_id INTEGER NOT NULL REFERENCES items(id) ON DELETE CASCADE,
    request_date TEXT NOT NULL,
    delivery_date TEXT NOT NULL,
    requested_qty REAL NOT NULL,
    on_hand_qty REAL NOT NULL,
    created_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_auto_requests_date ON auto_requests(request_date);
CREATE INDEX IF NOT EXISTS idx_auto_requests_delivery ON auto_requests(delivery_date);

CREATE TABLE IF NOT EXISTS system_log (
    id INTEGER PRIMARY KEY,
    event_type TEXT NOT NULL,
    message TEXT NOT NULL,
    metadata TEXT,
    created_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_system_log_date ON system_log(created_at);

CREATE TABLE IF NOT EXISTS settings (
    key TEXT PRIMARY KEY,
    value TEXT,
    updated_at TEXT NOT NULL
);
"""

def get_db_connection() -> sqlite3.Connection:
    """Get a new database connection for each operation."""
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn

def init_db():
    """Initialize database with schema and seed data - UPDATED with location support."""
    try:
        with get_db_connection() as conn:
            conn.executescript(SCHEMA_SQL)
            
            # Check if we need to migrate from old schema (case_size to unit_type)
            try:
                conn.execute("SELECT unit_type FROM items LIMIT 1")
            except sqlite3.OperationalError:
                LOG.info("Migrating database schema to container-based system...")
                # Add unit_type column if it doesn't exist
                conn.execute("ALTER TABLE items ADD COLUMN unit_type TEXT DEFAULT 'case'")
                # Drop case_size column if it exists
                try:
                    conn.execute("ALTER TABLE items DROP COLUMN case_size")
                except:
                    pass
                conn.commit()
            
            # FIXED: Use the proper location migration function
            check_and_migrate_location_columns(conn)
            
            # Seed items if empty or update existing items
            cur = conn.execute("SELECT COUNT(*) AS n FROM items")
            if cur.fetchone()["n"] == 0:
                LOG.info("Seeding items table with container-based configuration...")
                ts = datetime.now(TZ).isoformat()
                for name, config in ITEMS_CONFIG.items():
                    conn.execute(
                        "INSERT INTO items(name, adu, unit_type, buffer_days, par_level, created_at, updated_at) VALUES (?,?,?,?,?,?,?)",
                        (name, config["adu"], config["unit_type"], DEFAULT_BUFFER_DAYS, config["par_level"], ts, ts),
                    )
                conn.commit()
                LOG.info("Database initialized with container-based system")
            else:
                # Update existing items with new configuration
                LOG.info("Updating existing items with container-based configuration...")
                ts = datetime.now(TZ).isoformat()
                for name, config in ITEMS_CONFIG.items():
                    conn.execute(
                        "UPDATE items SET adu=?, unit_type=?, par_level=?, updated_at=? WHERE name=?",
                        (config["adu"], config["unit_type"], config["par_level"], ts, name)
                    )
                conn.commit()
                LOG.info("Items updated to container-based system")
                
    except Exception as e:
        LOG.error("Database initialization failed: %s", e)
        raise

def check_and_migrate_location_columns(conn: sqlite3.Connection):
    """Fixed location column migration with proper data population."""
    try:
        # Check if location columns exist
        cursor = conn.execute("PRAGMA table_info(nightly_on_hand)")
        onhand_columns = [col[1] for col in cursor.fetchall()]
        
        cursor = conn.execute("PRAGMA table_info(transfers)")  
        transfers_columns = [col[1] for col in cursor.fetchall()]
        
        cursor = conn.execute("PRAGMA table_info(auto_requests)")
        requests_columns = [col[1] for col in cursor.fetchall()]
        
        migrations_needed = []
        
        # Add missing location columns
        if 'location' not in onhand_columns:
            conn.execute("ALTER TABLE nightly_on_hand ADD COLUMN location TEXT DEFAULT 'Avondale'")
            migrations_needed.append('nightly_on_hand')
            
        if 'location' not in transfers_columns:
            conn.execute("ALTER TABLE transfers ADD COLUMN location TEXT DEFAULT 'Avondale'")
            migrations_needed.append('transfers')
            
        if 'location' not in requests_columns:
            conn.execute("ALTER TABLE auto_requests ADD COLUMN location TEXT DEFAULT 'Avondale'")
            migrations_needed.append('auto_requests')
        
        if migrations_needed:
            # Update existing NULL records
            conn.execute("UPDATE nightly_on_hand SET location = 'Avondale' WHERE location IS NULL")
            conn.execute("UPDATE transfers SET location = 'Avondale' WHERE location IS NULL")  
            conn.execute("UPDATE auto_requests SET location = 'Avondale' WHERE location IS NULL")
            
            conn.commit()
            LOG.info(f"Location columns added to: {', '.join(migrations_needed)}")
        else:
            LOG.info("Location columns already exist")
            
    except Exception as e:
        LOG.error(f"Location migration failed: {e}")
        raise

def cleanup_old_data():
    """Clean up data older than retention period."""
    try:
        cutoff_date = (datetime.now(TZ) - timedelta(days=DATA_RETENTION_DAYS)).date()
        with get_db_connection() as conn:
            # Clean old on-hand records
            result1 = conn.execute("DELETE FROM nightly_on_hand WHERE d < ?", (cutoff_date.isoformat(),))
            # Clean old transfers
            result2 = conn.execute("DELETE FROM transfers WHERE d < ?", (cutoff_date.isoformat(),))
            # Clean old auto-requests
            result3 = conn.execute("DELETE FROM auto_requests WHERE request_date < ?", (cutoff_date.isoformat(),))
            # Clean old system logs
            result4 = conn.execute("DELETE FROM system_log WHERE date(created_at) < ?", (cutoff_date.isoformat(),))
            conn.commit()
            
            total_cleaned = result1.rowcount + result2.rowcount + result3.rowcount + result4.rowcount
            if total_cleaned > 0:
                LOG.info(f"Cleaned up {total_cleaned} old records (older than {cutoff_date})")
                
    except Exception as e:
        LOG.error("Data cleanup failed: %s", e)

def check_system_health():
    """Complete system health check for deployment monitoring."""
    health_status = {
        'database': False,
        'telegram': False,
        'items_configured': False,
        'overall': False
    }
    
    try:
        # Test database connection
        with get_db_connection() as conn:
            conn.execute("SELECT 1").fetchone()
            health_status['database'] = True
            
            # Check if items are properly configured
            item_count = conn.execute("SELECT COUNT(*) as count FROM items").fetchone()['count']
            if item_count > 0:
                health_status['items_configured'] = True
    except Exception as e:
        LOG.error(f"Database health check failed: {e}")
    
    try:
        # Test Telegram connection (if token available)
        if BOT_TOKEN:
            test_msg = f"🔍 Health check from K2 Inventory\n📅 {now_local().strftime('%Y-%m-%d %H:%M:%S')}"
            # Don't actually send, just validate token format
            if len(BOT_TOKEN.split(':')) == 2:
                health_status['telegram'] = True
    except Exception as e:
        LOG.error(f"Telegram health check failed: {e}")
    
    # Overall health
    health_status['overall'] = all([
        health_status['database'],
        health_status['telegram'],
        health_status['items_configured']
    ])
    
    return health_status


# ---------------------- Time Helpers -----------------------
def today_local() -> date:
    return datetime.now(TZ).date()

def now_local() -> datetime:
    return datetime.now(TZ)

def next_delivery_after_location(d: date, location: str) -> date:
    """Return next delivery date based on location."""
    delivery_weekdays = COMMISSARY_DELIVERY_WEEKDAYS if location == 'Commissary' else AVONDALE_DELIVERY_WEEKDAYS
    
    for i in range(1, 8):
        cand = d + timedelta(days=i)
        if cand.weekday() in delivery_weekdays:
            return cand
    return d + timedelta(days=1)  # fallback

def next_two_deliveries_from(d: date) -> Tuple[date, date]:
    first = next_delivery_after(d)
    second = next_delivery_after(first)
    return first, second

def days_until_delivery_location(count_date: date, location: str) -> float:
    """Calculate days until delivery for specific location."""
    next_del = next_delivery_after_location(count_date, location)
    start = datetime.combine(count_date + timedelta(days=1), time(0, 0), TZ)
    end = datetime.combine(next_del, DELIVERY_NOON, TZ)
    delta = end - start
    days = max(0.0, delta.total_seconds() / 86400.0)
    LOG.debug(f"Days until delivery from {count_date} to {next_del}: {days:.2f} days")
    return days

# ------------------- Fixed Container-Based Calculations ----------------

def calculate_item_status_location(name: str, qty: Optional[float], count_date: date, adu: float, par_level: float, unit_type: str, location: str) -> ItemStatus:
    """Calculate item status with location-specific delivery schedule."""
    days_to_delivery = days_until_delivery_location(count_date, location)
    
    # Consumption need based on location delivery schedule
    consumption_need = adu * (days_to_delivery + DEFAULT_BUFFER_DAYS)
    
    # Days coverage
    days_coverage = (qty / adu) if qty and adu > 0 else 0.0
    
    # Par gap (still calculated but not displayed)
    par_gap = max(0.0, par_level - (qty or 0.0))
    
    if qty is None:
        return ItemStatus(name, None, "Missing", consumption_need, par_gap, par_level, adu, unit_type, days_to_delivery, 0.0)
    
    # Status logic based on consumption need, not par
    if qty < consumption_need:
        status = "Red"
    elif qty < par_level:  # Still using par for yellow threshold
        status = "Yellow"
    else:
        status = "Green"
    
    return ItemStatus(name, qty, status, consumption_need, par_gap, par_level, adu, unit_type, days_to_delivery, days_coverage)


# Add these functions right here:

def next_delivery_after(d: date) -> date:
    """Backward compatibility - defaults to Avondale schedule"""
    return next_delivery_after_location(d, 'Avondale')

def days_until_delivery(count_date: date) -> float:
    """Backward compatibility - defaults to Avondale schedule"""
    return days_until_delivery_location(count_date, 'Avondale')

def calculate_item_status(name: str, qty: Optional[float], count_date: date, adu: float, par_level: float, unit_type: str) -> ItemStatus:
    """Backward compatibility - defaults to Avondale location"""
    return calculate_item_status_location(name, qty, count_date, adu, par_level, unit_type, 'Avondale')

def get_item_status_for_date(conn: sqlite3.Connection, d: date) -> List[ItemStatus]:
    """Backward compatibility - defaults to Avondale location"""
    return get_item_status_for_date_location(conn, d, 'Avondale')

def handle_submit(entry_type: str, entry_date: date, manager: str, notes: str, qty_inputs: Dict[str, Optional[float]]) -> int:
    """Backward compatibility - defaults to Avondale location"""
    return handle_submit_with_location(entry_type, entry_date, manager, notes, qty_inputs, 'Avondale')

def format_reassurance_message(items_status: List[ItemStatus], check_date: date) -> str:
    """Backward compatibility - defaults to Avondale location"""
    return format_reassurance_message_location(items_status, check_date, 'Avondale')

def map_oh_for_date_flexible_location(conn: sqlite3.Connection, target_date: date, location: str = None) -> Dict[str, Tuple[float, str]]:
    """Return most recent On-Hand qty and unit_type per item on or before target_date for specific location."""
    out: Dict[str, Tuple[float, str]] = {}
    
    # Get items for specific location or all items
    if location:
        items_config = AVONDALE_ITEMS if location == 'Avondale' else COMMISSARY_ITEMS
        items_query = "SELECT name, unit_type FROM items WHERE active = TRUE AND name IN ({})".format(
            ','.join(['?'] * len(items_config))
        )
        items = conn.execute(items_query, list(items_config.keys())).fetchall()
    else:
        items = conn.execute("SELECT name, unit_type FROM items WHERE active = TRUE").fetchall()
    
    for item in items:
        name = item["name"]
        unit_type = item["unit_type"]
        
        # Look for most recent on-hand data on or before target_date for location
        if location:
            recent = conn.execute("""
                SELECT qty FROM nightly_on_hand oh
                JOIN items i ON oh.item_id = i.id
                WHERE i.name = ? AND oh.d <= ? AND oh.location = ?
                ORDER BY oh.d DESC, oh.created_at DESC
                LIMIT 1
            """, (name, target_date.isoformat(), location)).fetchone()
        else:
            recent = conn.execute("""
                SELECT qty FROM nightly_on_hand oh
                JOIN items i ON oh.item_id = i.id
                WHERE i.name = ? AND oh.d <= ?
                ORDER BY oh.d DESC, oh.created_at DESC
                LIMIT 1
            """, (name, target_date.isoformat())).fetchone()
        
        if recent and recent["qty"] is not None:
            qty = float(recent["qty"])
        else:
            qty = 0.0
            
        out[name] = (qty, unit_type)
    
    return out

# ADD this new location-aware auto-request data storage
def store_auto_request_data_location(requests: List[Tuple[str, float, float, float]], request_date: date, delivery_date: date, location: str):
    """Store auto-request data with location for shortage comparison later."""
    try:
        with get_db_connection() as conn:
            ts = now_local().isoformat()
            
            for name, req_qty, on_hand, adu in requests:
                if req_qty > 0:  # Only store items that were actually requested
                    item_id = conn.execute("SELECT id FROM items WHERE name = ?", (name,)).fetchone()
                    if item_id:
                        conn.execute(
                            "INSERT INTO auto_requests(item_id, request_date, delivery_date, requested_qty, on_hand_qty, location, created_at) VALUES (?,?,?,?,?,?,?)",
                            (item_id["id"], request_date.isoformat(), delivery_date.isoformat(), req_qty, on_hand, location, ts)
                        )
            
            conn.commit()
            LOG.info(f"Stored auto-request data for {len([r for r in requests if r[1] > 0])} {location} items")
            
    except Exception as e:
        LOG.error(f"Failed to store auto-request data for {location}: {e}")

# ADD this new location-aware info message formatter
def format_auto_request_info_message_location(request_data: List[Tuple[str, float, float, float, str]], run_weekday: int, request_date: date, location: str) -> str:
    """Location-aware detailed information message for managers."""
    style = MessageStyle()
    
    # Use location-specific request windows
    if location == 'Commissary':
        windows = COMMISSARY_REQUEST_WINDOWS
    else:
        windows = AVONDALE_REQUEST_WINDOWS
        
    window = windows.get(run_weekday, {"label": "Next Delivery", "total_days": 6.5})
    
    header = f"<b>🪣 AUTO-REQUEST ANALYSIS - {location.upper()}</b>\n"
    header += f"📅 {request_date.strftime('%a %b %d, %Y')}\n"
    header += f"🚚 For: <b>{window['label']}</b>\n"
    header += f"📊 Coverage: {window['total_days'] + DEFAULT_BUFFER_DAYS:.1f} days\n"
    header += f"📋 Based on most recent inventory count\n"
    
    lines = [header, f"\n<b>📊 DETAILED ANALYSIS:</b>"]
    
    for name, req_qty, on_hand, adu, unit_type in request_data:
        current_display = format_unit_display(on_hand, unit_type)
        
        if req_qty > 0:
            coverage_days = on_hand / adu if adu > 0 else 0
            need_display = format_unit_display(req_qty, unit_type)
            lines.append(f"\n<b>{name}</b>:")
            lines.append(f"  Current: {current_display} ({coverage_days:.1f} days)")
            lines.append(f"  Daily use: {adu:g} • Need: <b>{need_display}</b>")
        else:
            lines.append(f"\n<b>{name}</b>: Fully stocked ({current_display})")
    
    return "\n".join(lines)

# ------------------- Enhanced Telegram Messaging -------------------
def tg_send_with_retry(chat_id: int, text: str, max_retries: int = 3) -> bool:
    """Send Telegram message with retry logic."""
    if not BOT_TOKEN:
        LOG.info("Telegram disabled - would send: %s", text[:100])
        return True
    
    for attempt in range(max_retries):
        try:
            resp = requests.post(
                f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage",
                json={
                    "chat_id": chat_id, 
                    "text": text,
                    "parse_mode": "HTML"
                },
                timeout=10,
            )
            
            if resp.status_code == 200:
                LOG.info("Telegram sent to %s (%d chars)", chat_id, len(text))
                return True
            else:
                LOG.warning("Telegram error %s (attempt %d): %s", resp.status_code, attempt + 1, resp.text[:200])
                
        except Exception as e:
            LOG.warning("Telegram send failed (attempt %d): %s", attempt + 1, e)
        
        if attempt < max_retries - 1:
            wait_time = (2 ** attempt) * 1
            time_module.sleep(wait_time)
    
    LOG.error("Failed to send Telegram after %d attempts", max_retries)
    return False

def format_unit_display(qty: float, unit_type: str) -> str:
    """Format quantity with proper unit display - no unnecessary decimals."""
    unit_name = UNIT_TYPES.get(unit_type, unit_type)
    
    # FIXED: Display whole numbers without decimals
    if qty == int(qty):
        qty_str = str(int(qty))
    else:
        qty_str = f"{qty:g}"
    
    if qty == 1.0:
        # Use singular form
        unit_name = unit_name.rstrip('s')
    
    return f"{qty_str} {unit_name}"

def format_on_hand_message_with_location(items_status: List[ItemStatus], entry_date: date, manager_name: str, location: str) -> str:
    """FIXED format with consumption need instead of par references."""
    style = MessageStyle()
    
    reds = [item for item in items_status if item.status == "Red"]
    yellows = [item for item in items_status if item.status == "Yellow"]
    greens = [item for item in items_status if item.status == "Green"]
    missing = [item for item in items_status if item.status == "Missing"]
    
    next_delivery = next_delivery_after_location(entry_date, location)
    delivery_day = next_delivery.strftime('%a %b %d')
    
    header = f"<b>{style.HEADER} INVENTORY COUNT - {location.upper()}</b>\n"
    header += f"📅 {entry_date.strftime('%a %b %d, %Y')}\n"
    header += f"🚚 Next Delivery: {delivery_day}\n"
    if manager_name.strip():
        header += f"👤 Submitted by: <b>{manager_name}</b>\n"
    
    summary = f"📊 <b>Status:</b> {len(greens)}✅ {len(yellows)}🟡 {len(reds)}🔴"
    if missing:
        summary += f" {len(missing)}❌"
    
    lines = [header, summary, ""]
    
    # Critical items
    if reds:
        lines.append(f"<b>{style.CRITICAL} URGENT - WON'T LAST TO DELIVERY</b>")
        for item in reds:
            qty_display = format_unit_display(item.qty or 0, item.unit_type)
            shortage = max(0.0, item.consumption_need - (item.qty or 0.0))
            shortage_rounded = math.ceil(shortage)
            
            lines.append(f"{style.RED} <b>{item.name}</b>: {qty_display}")
            lines.append(f"   Need <b>{shortage_rounded} more</b> to reach delivery")
        lines.append("")
    
    # FIXED: Warning items - show consumption need instead of "below par"
    if yellows:
        lines.append(f"<b>{style.WARNING} CAUTION - WON'T REACH FULL CYCLE</b>")
        for item in yellows:
            qty_display = format_unit_display(item.qty or 0, item.unit_type)
            coverage_days_rounded = math.ceil(item.days_coverage)
            # Calculate how much more needed for full cycle
            shortage_for_cycle = max(0.0, item.consumption_need - (item.qty or 0.0))
            shortage_rounded = math.ceil(shortage_for_cycle)
            
            lines.append(f"{style.YELLOW} <b>{item.name}</b>: {qty_display}")
            lines.append(f"   Need <b>{shortage_rounded} more</b> for full cycle ({coverage_days_rounded} days coverage)")
        lines.append("")
    
    # Missing counts
    if missing:
        lines.append(f"<b>{style.CRITICAL} MISSING COUNTS</b>")
        for item in missing:
            lines.append(f"{style.MISSING} <b>{item.name}</b> - No count entered")
        lines.append("")
    
    # Fully stocked
    if greens:
        lines.append(f"<b>{style.SUCCESS} FULLY STOCKED</b>")
        for item in greens:
            qty_display = format_unit_display(item.qty, item.unit_type)
            lines.append(f"{style.GREEN} <b>{item.name}</b>: {qty_display}")
    
    return "\n".join(lines)

def format_received_message_with_shortages(items_received: List[Tuple[str, float]], entry_date: date, notes: str = "", manager_name: str = "") -> str:
    """Enhanced received message with shortage tracking and name accountability - COMPLETE VERSION."""
    style = MessageStyle()
    
    header = f"<b>📦 DELIVERY RECEIVED</b>\n📅 {entry_date.strftime('%a %b %d, %Y')}\n"
    if manager_name.strip():
        header += f"👤 Received by: <b>{manager_name}</b>\n"
    
    # Get most recent auto-request data for shortage comparison - COMPLETE LOGIC
    shortage_data = {}
    try:
        with get_db_connection() as conn:
            # Look for auto-requests from the last 7 days that could apply to this delivery
            lookback_date = (entry_date - timedelta(days=7)).isoformat()
            cur = conn.execute("""
                SELECT i.name, ar.requested_qty, ar.request_date, i.unit_type
                FROM auto_requests ar
                JOIN items i ON ar.item_id = i.id
                WHERE ar.request_date >= ? AND ar.delivery_date >= ?
                ORDER BY ar.request_date DESC
            """, (lookback_date, entry_date.isoformat()))
            
            for row in cur.fetchall():
                name = row["name"]
                if name not in shortage_data:  # Take most recent request
                    shortage_data[name] = {
                        "requested": float(row["requested_qty"]),
                        "unit_type": row["unit_type"],
                        "date": row["request_date"]
                    }
    except Exception as e:
        LOG.error("Failed to load shortage data: %s", e)
    
    lines = [header, f"\n<b>✅ ITEMS RECEIVED ({len(items_received)} items):</b>"]
    
    # Process each received item for shortage detection - COMPLETE LOGIC
    has_shortages = False
    for name, received_qty in items_received:
        # Get unit type for this item
        try:
            with get_db_connection() as conn:
                unit_row = conn.execute("SELECT unit_type FROM items WHERE name = ?", (name,)).fetchone()
                unit_type = unit_row["unit_type"] if unit_row else "case"
        except:
            unit_type = "case"
        
        received_display = format_unit_display(received_qty, unit_type)
        
        if name in shortage_data:
            requested_qty = shortage_data[name]["requested"]
            shortage = requested_qty - received_qty
            
            if shortage > 0.1:  # Small tolerance for rounding
                has_shortages = True
                requested_display = format_unit_display(requested_qty, unit_type)
                shortage_display = format_unit_display(shortage, unit_type)
                lines.append(f"• <b>{name}</b>: +{received_display} <b>(SHORTED: requested {requested_display}, short {shortage_display})</b> ⚠️")
            else:
                lines.append(f"• <b>{name}</b>: +{received_display} ✅")
        else:
            # No recent request found - just show received
            lines.append(f"• <b>{name}</b>: +{received_display}")
    
    # Shortage summary - COMPLETE LOGIC
    if has_shortages:
        lines.append(f"\n⚠️ <b>SHORTAGES DETECTED</b> - Review with supplier")
    
    # Notes section - COMPLETE LOGIC
    if notes.strip():
        lines.append(f"\n📝 <b>Notes:</b> {notes}")
    
    return "\n".join(lines)


def format_auto_request_info_message(request_data: List[Tuple[str, float, float, float, str]], run_weekday: int, request_date: date) -> str:
    """Detailed information message for managers (Message 1)."""
    style = MessageStyle()
    window = REQUEST_WINDOWS[run_weekday]
    
    header = f"<b>🪣 AUTO-REQUEST ANALYSIS</b>\n"
    header += f"📅 {request_date.strftime('%a %b %d, %Y')}\n"
    header += f"🚚 For: <b>{window['label']}</b>\n"
    header += f"📊 Coverage: {window['total_days'] + DEFAULT_BUFFER_DAYS:.1f} days\n"
    header += f"📋 Based on most recent inventory count\n"
    
    lines = [header, f"\n<b>📊 DETAILED ANALYSIS:</b>"]
    
    for name, req_qty, on_hand, adu, unit_type in request_data:
        current_display = format_unit_display(on_hand, unit_type)
        
        if req_qty > 0:
            coverage_days = on_hand / adu if adu > 0 else 0
            need_display = format_unit_display(req_qty, unit_type)
            lines.append(f"\n<b>{name}</b>:")
            lines.append(f"  Current: {current_display} ({coverage_days:.1f} days)")
            lines.append(f"  Daily use: {adu:g} • Need: <b>{need_display}</b>")
        else:
            lines.append(f"\n<b>{name}</b>: Fully stocked ({current_display})")
    
    return "\n".join(lines)

def format_auto_request_order_message(request_data: List[Tuple[str, float, float, float, str]], run_weekday: int, request_date: date) -> str:
    """Clean order message for prep team (Message 2)."""
    window = REQUEST_WINDOWS[run_weekday]
    
    header = f"<b>📋 ORDER REQUEST</b>\n"
    header += f"📅 {request_date.strftime('%a %b %d, %Y')}\n\n"
    header += f"Hey prep team! This is what we need for <b>{window['label']}</b>.\n"
    header += f"Please confirm at your earliest convenience:\n"
    
    # Only include items that need ordering
    needed_items = [(name, req_qty, unit_type) for name, req_qty, _, _, unit_type in request_data if req_qty > 0]
    
    if not needed_items:
        return header + f"\n✅ <b>NO ORDERS NEEDED</b>\nAll items are fully stocked!"
    
    lines = [header]
    for name, req_qty, unit_type in needed_items:
        qty_display = format_unit_display(req_qty, unit_type)
        lines.append(f"• <b>{name}</b>: {qty_display}")
    
    lines.append(f"\nTotal items: <b>{len(needed_items)}</b>")
    
    return "\n".join(lines)

def format_reassurance_message_location(items_status: List[ItemStatus], check_date: date, location: str) -> str:
    """Format reassurance message for specific location."""
    style = MessageStyle()
    
    reds = [item for item in items_status if item.status == "Red"]
    yellows = [item for item in items_status if item.status == "Yellow"]
    missing = [item for item in items_status if item.status == "Missing"]
    
    header = f"<b>{style.INFO} DAILY REASSURANCE - {location.upper()}</b>\n"
    header += f"📅 {check_date.strftime('%a %b %d, %Y')} • 5:00 PM\n"
    
    if not reds and not yellows and not missing:
        return header + f"\n{style.SUCCESS} <b>ALL SYSTEMS GREEN</b>\nAll {location} inventory levels are adequate ✅"
    
    lines = [header]
    
    if reds:
        lines.append(f"\n{style.CRITICAL} <b>CRITICAL ITEMS:</b>")
        for item in reds:
            qty_display = format_unit_display(item.qty or 0, item.unit_type)
            # Calculate remaining need for delivery
            remaining_need = max(0.0, item.consumption_need - (item.qty or 0.0))
            remaining_need_rounded = math.ceil(remaining_need)
            remaining_display = format_unit_display(remaining_need_rounded, item.unit_type)
            lines.append(f"• <b>{item.name}</b> - Only {qty_display} left (Need {remaining_display} more)")
    
    if yellows:
        lines.append(f"\n{style.WARNING} <b>WATCH LIST:</b>")
        for item in yellows:
            qty_display = format_unit_display(item.qty or 0, item.unit_type)
            coverage_days_rounded = math.ceil(item.days_coverage)
            lines.append(f"• <b>{item.name}</b> - {qty_display} ({coverage_days_rounded} days coverage)")
    
    if missing:
        lines.append(f"\n{style.MISSING} <b>MISSING COUNTS:</b>")
        lines.append("• " + ", ".join(item.name for item in missing))
    
    return "\n".join(lines)

# ------------------- Enhanced Data Lookup Functions -------------------
def map_oh_for_date_flexible(conn: sqlite3.Connection, target_date: date) -> Dict[str, Tuple[float, str]]:
    """Return most recent On-Hand qty and unit_type per item on or before target_date."""
    out: Dict[str, Tuple[float, str]] = {}
    
    # Get all active items first
    items = conn.execute("SELECT name, unit_type FROM items WHERE active = TRUE").fetchall()
    
    for item in items:
        name = item["name"]
        unit_type = item["unit_type"]
        
        # Look for most recent on-hand data on or before target_date
        recent = conn.execute("""
            SELECT qty FROM nightly_on_hand oh
            JOIN items i ON oh.item_id = i.id
            WHERE i.name = ? AND oh.d <= ?
            ORDER BY oh.d DESC, oh.created_at DESC
            LIMIT 1
        """, (name, target_date.isoformat())).fetchone()
        
        if recent and recent["qty"] is not None:
            qty = float(recent["qty"])
        else:
            qty = 0.0
            
        out[name] = (qty, unit_type)
    
    return out

# ----------------------- Submit Handlers -----------------------
def upsert_item_id(conn: sqlite3.Connection, name: str) -> int:
    """Get or create item ID."""
    cur = conn.execute("SELECT id FROM items WHERE name=?", (name,))
    row = cur.fetchone()
    if row:
        return row["id"]
    
    # Create missing item with default values
    ts = now_local().isoformat()
    conn.execute(
        "INSERT INTO items(name, adu, unit_type, buffer_days, par_level, created_at, updated_at) VALUES (?,?,?,?,?,?,?)",
        (name, 1.0, "case", DEFAULT_BUFFER_DAYS, 3.0, ts, ts),
    )
    return conn.execute("SELECT last_insert_rowid() AS id").fetchone()["id"]

def handle_submit_with_location(entry_type: str, entry_date: date, manager: str, notes: str, qty_inputs: Dict[str, Optional[float]], location: str) -> int:
    """UPDATED submit handler with location-aware delivery calculations."""
    created = 0
    ts = now_local().isoformat()
    
    # Get items config for this location
    items_config = AVONDALE_ITEMS if location == 'Avondale' else COMMISSARY_ITEMS
    
    # Validation
    valid_entries = {}
    for item_name, qty in qty_inputs.items():
        if item_name not in items_config or qty is None:
            continue
        try:
            float_qty = float(qty)
            if float_qty > 0:
                valid_entries[item_name] = float_qty
        except (ValueError, TypeError):
            continue
    
    if not valid_entries:
        return 0
    
    try:
        with get_db_connection() as conn:
            if entry_type == "On-Hand":
                items_status = []
                
                for item_name, qty in valid_entries.items():
                    item_id = upsert_item_id(conn, item_name)
                    
                    config = items_config[item_name]
                    adu = config["adu"]
                    par_level = config["par_level"]
                    unit_type = config["unit_type"]
                    
                    # Insert with location
                    conn.execute(
                        "INSERT OR REPLACE INTO nightly_on_hand(item_id, d, qty, manager, notes, location, created_at) VALUES (?,?,?,?,?,?,?)",
                        (item_id, entry_date.isoformat(), qty, manager, notes, location, ts),
                    )
                    created += 1
                    
                    # FIXED: Use location-aware status calculation
                    status = calculate_item_status_location(item_name, qty, entry_date, adu, par_level, unit_type, location)
                    items_status.append(status)
                
                conn.commit()
                
                if items_status:
                    message = format_on_hand_message_with_location(items_status, entry_date, manager, location)
                    success = tg_send_with_retry(CHAT_ONHAND, message)
                    
            elif entry_type == "Received":
                items_received = []
                
                for item_name, qty in valid_entries.items():
                    item_id = upsert_item_id(conn, item_name)
                    
                    conn.execute(
                        "INSERT INTO transfers(item_id, d, type, received_qty, notes, location, created_at) VALUES (?,?,?,?,?,?,?)",
                        (item_id, entry_date.isoformat(), "Received", qty, notes, location, ts),
                    )
                    created += 1
                    items_received.append((item_name, qty))
                
                conn.commit()
                
                if items_received:
                    message = format_received_message_with_location(items_received, entry_date, notes, manager, location)
                    success = tg_send_with_retry(CHAT_RECEIVED, message)
                
    except Exception as e:
        LOG.error("Submit failed for %s - %s: %s", entry_type, location, e)
        raise
        
    return created

# 2. ADD this function to store auto-request data for shortage tracking:
def store_auto_request_data(requests: List[Tuple[str, float, float, float]], request_date: date, delivery_date: date):
    """Store auto-request data for shortage comparison later."""
    try:
        with get_db_connection() as conn:
            ts = now_local().isoformat()
            
            for name, req_qty, on_hand, adu in requests:
                if req_qty > 0:  # Only store items that were actually requested
                    # Get item ID
                    item_id = conn.execute("SELECT id FROM items WHERE name = ?", (name,)).fetchone()
                    if item_id:
                        conn.execute(
                            "INSERT INTO auto_requests(item_id, request_date, delivery_date, requested_qty, on_hand_qty, created_at) VALUES (?,?,?,?,?,?)",
                            (item_id["id"], request_date.isoformat(), delivery_date.isoformat(), req_qty, on_hand, ts)
                        )
            
            conn.commit()
            LOG.info(f"Stored auto-request data for {len([r for r in requests if r[1] > 0])} items")
            
    except Exception as e:
        LOG.error(f"Failed to store auto-request data: {e}")

# ---------------------- Scheduled Jobs --------------------

def get_item_status_for_date_location(conn: sqlite3.Connection, d: date, location: str) -> List[ItemStatus]:
    """Get item status for specific location and date."""
    items_status = []
    
    # Get items for this location
    items_config = AVONDALE_ITEMS if location == 'Avondale' else COMMISSARY_ITEMS
    
    for name, config in items_config.items():
        adu = config["adu"]
        par_level = config["par_level"]
        unit_type = config["unit_type"]
        
        # Get item ID
        item_row = conn.execute("SELECT id FROM items WHERE name = ?", (name,)).fetchone()
        if not item_row:
            continue
            
        item_id = item_row["id"]
        
        # Get on-hand quantity for this date and location
        oh_row = conn.execute(
            "SELECT qty FROM nightly_on_hand WHERE item_id=? AND d=? AND location=?",
            (item_id, d.isoformat(), location),
        ).fetchone()
        
        qty = float(oh_row["qty"]) if oh_row and oh_row["qty"] is not None else None
        
        # Use location-aware status calculation
        status = calculate_item_status_location(name, qty, d, adu, par_level, unit_type, location)
        items_status.append(status)
    
    return items_status

def auto_request_quantities(run_weekday: int, oh_by_item: Dict[str, Tuple[float, str]]) -> List[Tuple[str, float, float, float, str]]:
    """Compute auto-request with properly rounded quantities."""
    with get_db_connection() as conn:
        cur = conn.execute("SELECT name, adu, unit_type FROM items WHERE active = TRUE")
        items_config = {row["name"]: {"adu": float(row["adu"]), "unit_type": row["unit_type"]} for row in cur.fetchall()}
    
    window = REQUEST_WINDOWS[run_weekday]
    total_days = window["total_days"] + DEFAULT_BUFFER_DAYS
    out: List[Tuple[str, float, float, float, str]] = []
    
    # Calculate delivery date
    request_date = today_local()
    delivery_date = next_delivery_after(request_date)
    
    with get_db_connection() as conn:
        ts = now_local().isoformat()
        
        for name, config in items_config.items():
            adu = config["adu"]
            unit_type = config["unit_type"]
            oh_qty, _ = oh_by_item.get(name, (0.0, unit_type))
            
            # Calculate containers needed
            needed = adu * total_days
            req = max(0.0, needed - oh_qty)
            
            # FIXED: Always round up to whole containers - no decimals
            rounded = math.ceil(req) if req > 0 else 0.0
            
            out.append((name, rounded, oh_qty, adu, unit_type))
            
            # Store auto-request in database for shortage tracking
            item_id = conn.execute("SELECT id FROM items WHERE name = ?", (name,)).fetchone()["id"]
            conn.execute(
                "INSERT INTO auto_requests(item_id, request_date, delivery_date, requested_qty, on_hand_qty, created_at) VALUES (?,?,?,?,?,?)",
                (item_id, request_date.isoformat(), delivery_date.isoformat(), rounded, oh_qty, ts)
            )
        
        conn.commit()
    
    return out

# FIXED: Update format_unit_display to handle whole numbers cleanly

def job_auto_request():
    """FIXED: Auto-request job with location-aware calculations."""
    try:
        with get_db_connection() as conn:
            now = now_local()
            wd = now.weekday()
            today = now.date()
            
            # Check both location schedules
            avondale_should_run = wd in AVONDALE_REQUEST_WINDOWS
            commissary_should_run = wd in COMMISSARY_REQUEST_WINDOWS
            
            if not avondale_should_run and not commissary_should_run:
                LOG.info(f"No auto-request scheduled for {['Mon','Tue','Wed','Thu','Fri','Sat','Sun'][wd]}")
                return
                
            LOG.info("Auto-request job running for applicable locations")
            
            messages_sent = 0
            
            # Process Avondale if scheduled
            if avondale_should_run:
                oh_map_avondale = map_oh_for_date_flexible_location(conn, today, 'Avondale')
                missing_items = [name for name, (qty, _) in oh_map_avondale.items() if qty == 0.0]
                
                if len(missing_items) <= len(oh_map_avondale) * 0.5:  # Less than 50% missing
                    delivery_date = next_delivery_after_location(today, 'Avondale')
                    avondale_requests = generate_location_requests('Avondale', wd, oh_map_avondale)
                    
                    # Store request data
                    store_auto_request_data_location(avondale_requests, today, delivery_date, 'Avondale')
                    
                    # Send messages
                    info_message = format_auto_request_info_message_location(avondale_requests, wd, today, 'Avondale')
                    order_message = format_location_order_message(avondale_requests, wd, today, 'Avondale')
                    
                    tg_send_with_retry(CHAT_AUTOREQUEST, info_message)
                    time_module.sleep(2)
                    tg_send_with_retry(CHAT_AUTOREQUEST, order_message)
                    messages_sent += 1
                else:
                    LOG.warning("Avondale: Too many items missing from recent counts")
            
            # Process Commissary if scheduled
            if commissary_should_run:
                if messages_sent > 0:
                    time_module.sleep(2)  # Delay between location messages
                    
                oh_map_commissary = map_oh_for_date_flexible_location(conn, today, 'Commissary')
                missing_items = [name for name, (qty, _) in oh_map_commissary.items() if qty == 0.0]
                
                if len(missing_items) <= len(oh_map_commissary) * 0.5:  # Less than 50% missing
                    delivery_date = next_delivery_after_location(today, 'Commissary')
                    commissary_requests = generate_location_requests('Commissary', wd, oh_map_commissary)
                    
                    # Store request data
                    store_auto_request_data_location(commissary_requests, today, delivery_date, 'Commissary')
                    
                    # Send messages
                    info_message = format_auto_request_info_message_location(commissary_requests, wd, today, 'Commissary')
                    order_message = format_location_order_message(commissary_requests, wd, today, 'Commissary')
                    
                    tg_send_with_retry(CHAT_AUTOREQUEST, info_message)
                    time_module.sleep(2)
                    tg_send_with_retry(CHAT_AUTOREQUEST, order_message)
                    messages_sent += 1
                else:
                    LOG.warning("Commissary: Too many items missing from recent counts")
            
            if messages_sent == 0:
                alert_msg = f"<b>⚠️ AUTO-REQUEST SKIPPED</b>\nInsufficient recent on-hand data for both locations on {['Mon','Tue','Wed','Thu','Fri','Sat','Sun'][wd]}"
                tg_send_with_retry(CHAT_AUTOREQUEST, alert_msg)
            
    except Exception as e:
        LOG.exception("Auto-request job failed: %s", e)

def job_reassurance():
    """FIXED: Reassurance job with location-aware messaging."""
    try:
        with get_db_connection() as conn:
            d = today_local()
            
            # Get status for both locations
            avondale_status = get_item_status_for_date_location(conn, d, 'Avondale')
            commissary_status = get_item_status_for_date_location(conn, d, 'Commissary')
            
            # Check if we have data for today for either location
            avondale_data = sum(1 for item in avondale_status if item.qty is not None)
            commissary_data = sum(1 for item in commissary_status if item.qty is not None)
            
            if avondale_data > 0:
                avondale_message = format_reassurance_message_location(avondale_status, d, 'Avondale')
                tg_send_with_retry(CHAT_REASSURANCE, avondale_message)
                
            if commissary_data > 0:
                commissary_message = format_reassurance_message_location(commissary_status, d, 'Commissary')
                tg_send_with_retry(CHAT_REASSURANCE, commissary_message)
                
            if avondale_data == 0 and commissary_data == 0:
                no_data_msg = f"⚠️ <b>REASSURANCE ALERT</b>\nNo inventory data for either location today ({d.strftime('%a %b %d, %Y')})\n\nPlease ensure inventory counts are entered for accurate monitoring."
                tg_send_with_retry(CHAT_REASSURANCE, no_data_msg)
            
    except Exception as e:
        LOG.exception("Reassurance job failed: %s", e)

def job_missing_counts():
    """Missing counts job."""
    try:
        with get_db_connection() as conn:
            d = today_local()
            missing = []
            cur = conn.execute("SELECT name FROM items WHERE active = TRUE ORDER BY name")
            for row in cur.fetchall():
                exists = conn.execute(
                    "SELECT 1 FROM nightly_on_hand WHERE item_id = (SELECT id FROM items WHERE name = ?) AND d = ?",
                    (row["name"], d.isoformat()),
                ).fetchone()
                if not exists:
                    missing.append(row["name"])
            
            if missing:
                header = f"<b>⚠️ MISSING NIGHTLY COUNTS</b>\n📅 {d.strftime('%a %b %d, %Y')} • 11:59 PM\n"
                message = header + f"\n<b>Items without counts:</b>\n" + "\n".join(f"• {name}" for name in missing)
                tg_send_with_retry(CHAT_REASSURANCE, message)
                
    except Exception as e:
        LOG.exception("Missing-counts job failed: %s", e)

def job_cleanup():
    """Daily cleanup job."""
    try:
        cleanup_old_data()
    except Exception as e:
        LOG.exception("Cleanup job failed: %s", e)

# 7. NEW: Average Daily Usage (ADU) command
def execute_adu_info_command(chat_id: int):
    """Show current ADU values for all items."""
    try:
        header = f"📊 <b>AVERAGE DAILY USAGE (ADU)</b>\n🤖 Command executed: /adu\n📅 {now_local().strftime('%Y-%m-%d %H:%M:%S')}\n"
        
        lines = [header, "\n<b>🏢 AVONDALE ITEMS:</b>"]
        for name, config in AVONDALE_ITEMS.items():
            unit_name = UNIT_TYPES.get(config["unit_type"], config["unit_type"])
            lines.append(f"• <b>{name}</b>: {config['adu']} {unit_name}/day")
        
        lines.append(f"\n<b>🏭 COMMISSARY ITEMS:</b>")
        for name, config in COMMISSARY_ITEMS.items():
            unit_name = UNIT_TYPES.get(config["unit_type"], config["unit_type"])
            lines.append(f"• <b>{name}</b>: {config['adu']} {unit_name}/day")
        
        lines.append(f"\n💡 Use /editadu to modify these values")
        
        message = "\n".join(lines)
        tg_send_with_retry(chat_id, message)
        
    except Exception as e:
        LOG.error(f"ADU info command failed: {e}")
        error_msg = f"❌ <b>ADU INFO FAILED</b>\n\nError: <code>{str(e)}</code>"
        tg_send_with_retry(chat_id, error_msg)

# 8. NEW: Edit ADU interactive workflow
def start_edit_adu_workflow(chat_id: int):
    """Start interactive ADU editing workflow."""
    state = ConversationState(chat_id)
    state.clear()
    
    state.set('current_flow', 'edit_adu')
    state.set('step', 'select_item')
    
    # Show all items with numbers
    msg = """✏️ <b>EDIT AVERAGE DAILY USAGE</b>

<b>🏢 AVONDALE ITEMS:</b>"""
    
    item_list = []
    counter = 1
    
    for name, config in AVONDALE_ITEMS.items():
        unit_name = UNIT_TYPES.get(config["unit_type"], config["unit_type"])
        msg += f"\n{counter}️⃣ <b>{name}</b>: {config['adu']} {unit_name}/day"
        item_list.append((name, 'Avondale'))
        counter += 1
    
    msg += f"\n\n<b>🏭 COMMISSARY ITEMS:</b>"
    for name, config in COMMISSARY_ITEMS.items():
        unit_name = UNIT_TYPES.get(config["unit_type"], config["unit_type"])
        msg += f"\n{counter}️⃣ <b>{name}</b>: {config['adu']} {unit_name}/day"
        item_list.append((name, 'Commissary'))
        counter += 1
    
    msg += f"\n\nSelect item to edit by number (1-{len(item_list)}):\n\n💡 Type /cancel to exit"
    
    state.set('item_list', item_list)
    tg_send_with_retry(chat_id, msg)

def handle_edit_adu_response(chat_id: int, response: str):
    """Handle ADU editing workflow."""
    state = ConversationState(chat_id)
    step = state.get('step')
    
    if step == 'select_item':
        handle_adu_item_selection(chat_id, response)
    elif step == 'enter_new_adu':
        handle_new_adu_entry(chat_id, response)

def handle_adu_item_selection(chat_id: int, response: str):
    """Handle item selection for ADU editing."""
    state = ConversationState(chat_id)
    item_list = state.get('item_list', [])
    
    try:
        selection = int(response.strip()) - 1
        if 0 <= selection < len(item_list):
            item_name, location = item_list[selection]
            state.set('selected_item', item_name)
            state.set('selected_location', location)
            state.set('step', 'enter_new_adu')
            
            # Get current ADU
            items_config = AVONDALE_ITEMS if location == 'Avondale' else COMMISSARY_ITEMS
            current_adu = items_config[item_name]["adu"]
            unit_type = items_config[item_name]["unit_type"]
            unit_name = UNIT_TYPES.get(unit_type, unit_type)
            
            msg = f"""✏️ <b>EDIT ADU - {item_name.upper()}</b>

<b>Current ADU:</b> {current_adu} {unit_name}/day
<b>Location:</b> {location}

Enter new ADU value (e.g., 2.5, 1.8):

⬅️ Type <b>back</b> to select different item
💡 Type <b>/cancel</b> to exit"""
            
            tg_send_with_retry(chat_id, msg)
        else:
            msg = f"❌ Invalid selection. Please enter a number between 1 and {len(item_list)}"
            tg_send_with_retry(chat_id, msg)
            
    except ValueError:
        msg = "❌ Please enter a valid number"
        tg_send_with_retry(chat_id, msg)

def handle_new_adu_entry(chat_id: int, response: str):
    """Handle new ADU value entry."""
    state = ConversationState(chat_id)
    
    if response.lower() == 'back':
        state.set('step', 'select_item')
        start_edit_adu_workflow(chat_id)
        return
    
    try:
        new_adu = float(response.strip())
        if new_adu <= 0:
            msg = "❌ ADU must be greater than 0"
            tg_send_with_retry(chat_id, msg)
            return
        
        item_name = state.get('selected_item')
        location = state.get('selected_location')
        
        # Update in database
        success = update_item_adu(item_name, new_adu)
        
        if success:
            # Update in-memory config
            if location == 'Avondale':
                AVONDALE_ITEMS[item_name]["adu"] = new_adu
            else:
                COMMISSARY_ITEMS[item_name]["adu"] = new_adu
            
            # Update combined config
            ITEMS_CONFIG[item_name]["adu"] = new_adu
            
            unit_type = AVONDALE_ITEMS[item_name]["unit_type"] if location == 'Avondale' else COMMISSARY_ITEMS[item_name]["unit_type"]
            unit_name = UNIT_TYPES.get(unit_type, unit_type)
            
            success_msg = f"""✅ <b>ADU UPDATED SUCCESSFULLY!</b>

<b>{item_name}</b> ADU changed to: <b>{new_adu} {unit_name}/day</b>
<b>Location:</b> {location}

💡 Use /adu to see all current values
🚀 Type /editadu to edit another item"""
            
            tg_send_with_retry(chat_id, success_msg)
        else:
            error_msg = "❌ Failed to update ADU in database"
            tg_send_with_retry(chat_id, error_msg)
        
        state.clear()
        
    except ValueError:
        msg = "❌ Please enter a valid number (e.g., 2.5, 1.8)"
        tg_send_with_retry(chat_id, msg)

def update_item_adu(item_name: str, new_adu: float) -> bool:
    """Update item ADU in database."""
    try:
        with get_db_connection() as conn:
            ts = now_local().isoformat()
            result = conn.execute(
                "UPDATE items SET adu=?, updated_at=? WHERE name=?",
                (new_adu, ts, item_name)
            )
            conn.commit()
            
            if result.rowcount > 0:
                LOG.info(f"Updated ADU for {item_name} to {new_adu}")
                return True
            else:
                LOG.warning(f"No item found with name: {item_name}")
                return False
                
    except Exception as e:
        LOG.error(f"Failed to update ADU for {item_name}: {e}")
        return False

# ---------------------- Enhanced Scheduler -------------------------
SCHED = BackgroundScheduler(timezone=str(TZ))

def start_scheduler():
    """Start scheduler with all jobs."""
    try:
        SCHED.remove_all_jobs()
        
        SCHED.add_job(job_auto_request, CronTrigger(day_of_week="tue,sat", hour=RUN_REQ_HOUR, minute=0), id="auto_request", max_instances=1)
        SCHED.add_job(job_reassurance, CronTrigger(hour=RUN_REASSURANCE.hour, minute=RUN_REASSURANCE.minute), id="reassurance", max_instances=1)
        SCHED.add_job(job_missing_counts, CronTrigger(hour=RUN_MISSING.hour, minute=RUN_MISSING.minute), id="missing_counts", max_instances=1)
        SCHED.add_job(job_cleanup, CronTrigger(hour=2, minute=0), id="cleanup", max_instances=1)  # Daily 2 AM
        
        SCHED.start()
        LOG.info("Scheduler started with %d jobs", len(SCHED.get_jobs()))
        
    except Exception as e:
        LOG.exception("Scheduler startup failed: %s", e)

# ========================== UI PAGES ==========================

def page_entry():
    """Mobile-optimized entry page with system health monitoring - FIXED form state management."""
    
    # REMOVED: The problematic form clearing logic that was at the beginning
    # This was causing race conditions and interfering with validation display
    
    st.title("📱 K2 Inventory Entry")
    
    # System status with health check
    col1, col2 = st.columns([2, 1])
    with col1:
        health = check_system_health()
        if health['overall']:
            st.success("🟢 System Online")
        else:
            st.error("🔴 System Issue")
            
        # Show detailed health status
        health_details = []
        if not health['database']:
            health_details.append("Database connection failed")
        if not health['telegram']:
            health_details.append("Telegram not configured")
        if not health['items_configured']:
            health_details.append("No items configured")
            
        if health_details:
            with st.expander("Health Issues"):
                for issue in health_details:
                    st.warning(f"⚠️ {issue}")
    
    with col2:
        if st.button("🔄", help="Refresh", use_container_width=True):
            st.rerun()
    
    # Show scheduler status
    USE_SCHEDULER = os.getenv('USE_SCHEDULER', 'false').lower() == 'true'
    if USE_SCHEDULER:
        if SCHED and SCHED.running:
            st.info("⏰ Scheduler: Active")
        else:
            st.warning("⏰ Scheduler: Inactive")
    else:
        st.info("🤖 Command Mode: Telegram slash commands enabled")
    
    # Entry configuration - outside form to prevent state conflicts
    st.subheader("📋 Entry Configuration")
    
    col1, col2 = st.columns(2)
    with col1:
        entry_date = st.date_input("📅 Date", value=today_local())
        entry_type = st.selectbox("📋 Type", ["On-Hand", "Received"], index=0, key="entry_type_selector")
    
    with col2:
        name = st.text_input("👤 Name", key="manager_name")
    
    notes = st.text_area("📝 Notes (optional)", height=60, key="entry_notes")
    
    # Entry form that preserves data on validation errors
    with st.form("entry_form", clear_on_submit=False):
        
        # Items section (mobile-optimized grid)
        st.subheader("📦 Items")
        
        # Get active items from database
        try:
            with get_db_connection() as conn:
                cur = conn.execute("SELECT name, adu, par_level, unit_type FROM items WHERE active = TRUE ORDER BY name")
                active_items = [(row["name"], float(row["adu"]), float(row["par_level"]), row["unit_type"]) for row in cur.fetchall()]
        except:
            active_items = [(name, config["adu"], config["par_level"], config["unit_type"]) for name, config in ITEMS_CONFIG.items()]
        
        qty_inputs = {}
        
        # Mobile-friendly item input (2 columns on mobile, 4 on desktop)
        cols = st.columns(2)
        for i, (item_name, adu, par, unit_type) in enumerate(active_items):
            with cols[i % 2]:
                unit_name = UNIT_TYPES.get(unit_type, unit_type)
                if entry_type == "On-Hand":
                    help_text = f"ADU: {adu} {unit_name}/day | Par: {par:g} {unit_name}"
                    placeholder = "qty"
                else:
                    help_text = f"Unit: {unit_name} | ADU: {adu}/day"
                    placeholder = "received"
                
                # Stable keys that don't depend on form state
                qty_input_str = st.text_input(
                    item_name, 
                    value="", 
                    placeholder=placeholder,
                    help=help_text,
                    key=f"item_{item_name}_{i}"
                )
                
                # Process input - empty string becomes None, valid numbers become float
                if qty_input_str.strip() == "":
                    qty_inputs[item_name] = None
                else:
                    try:
                        parsed_value = float(qty_input_str.strip())
                        if parsed_value > 0:
                            qty_inputs[item_name] = parsed_value
                        else:
                            qty_inputs[item_name] = None
                            if parsed_value <= 0:
                                st.error(f"{item_name}: Must be greater than 0")
                    except ValueError:
                        qty_inputs[item_name] = None
                        st.error(f"{item_name}: '{qty_input_str}' is not a valid number")
        
        # Submit button (mobile-friendly)
        submitted = st.form_submit_button("📤 Submit Entry", use_container_width=True, type="primary")
        
        if submitted:
            # Validation for partial submissions
            valid_entries = {}
            invalid_entries = {}
            
            for item_name, qty in qty_inputs.items():
                if qty is None:
                    # Empty field - this is fine for partial submissions
                    continue
                elif isinstance(qty, (int, float)) and qty > 0:
                    # Valid positive number
                    valid_entries[item_name] = float(qty)
                else:
                    # Invalid entry
                    invalid_entries[item_name] = qty
            
            # Debug output (remove this after testing)
            if st.session_state.get('show_debug', False):
                st.info(f"Debug: Entry Type: {entry_type}")
                st.info(f"Debug: Raw inputs: {qty_inputs}")
                st.info(f"Debug: Valid entries: {valid_entries}")
                st.info(f"Debug: Invalid entries: {invalid_entries}")
            
            # Validation checks
            if invalid_entries:
                for item_name, invalid_qty in invalid_entries.items():
                    st.error(f"{item_name}: '{invalid_qty}' is not valid")
            
            if not valid_entries:
                st.warning("⚠️ Please enter valid quantities (greater than 0) for at least one item")
            elif not name.strip():
                # Show error but don't clear form data
                st.error("❌ Please enter your name above")
                st.info("💡 Your item quantities have been preserved - just add your name and resubmit")
            else:
                try:
                    with st.spinner("Processing..."):
                        # Create a clean input dict with only valid entries + None for empty fields
                        clean_inputs = {}
                        for item_name in qty_inputs.keys():
                            if item_name in valid_entries:
                                clean_inputs[item_name] = valid_entries[item_name]
                            else:
                                clean_inputs[item_name] = None
                        
                        # FIXED: Use the backward compatibility function (defaults to Avondale)
                        created = handle_submit(entry_type, entry_date, name, notes, clean_inputs)
                    
                    if created > 0:
                        st.success(f"✅ Successfully created {created} entries")
                        st.info("📱 Telegram message sent")
                        
                        # Show confirmation
                        entered_items = []
                        for item_name, qty in valid_entries.items():
                            # Get unit type for display
                            unit_type = next((ut for n, _, _, ut in active_items if n == item_name), "case")
                            qty_display = format_unit_display(qty, unit_type)
                            entered_items.append(f"{item_name}: {qty_display}")
                        
                        with st.expander("📋 Submitted Items"):
                            st.write(f"**{entry_type}** for {entry_date}")
                            st.write(f"**Submitted by:** {name}")
                            st.write(", ".join(entered_items))
                            if notes.strip():
                                st.write(f"**Notes:** {notes}")
                        
                        # FIXED: Clear form state immediately after success
                        for key in list(st.session_state.keys()):
                            if key.startswith('item_') or key in ['manager_name', 'entry_notes']:
                                del st.session_state[key]
                        
                        st.info("🔄 Form cleared - ready for next entry")
                        time_module.sleep(2)  # Brief pause for user to see confirmation
                        st.rerun()
                    else:
                        st.error("❌ No entries were created - check logs")
                        
                except Exception as e:
                    st.error(f"❌ Error: {str(e)}")
                    LOG.exception("Entry submission failed")

def page_analytics():
    """Analytics and historical data page."""
    st.title("📊 Analytics Dashboard")
    
    try:
        with get_db_connection() as conn:
            # Get date range for data
            date_range = conn.execute(
                "SELECT MIN(d) as min_date, MAX(d) as max_date FROM nightly_on_hand"
            ).fetchone()
            
            if not date_range["min_date"]:
                st.info("🔍 No data available yet. Submit some entries first!")
                return
            
            # Date filter
            col1, col2, col3 = st.columns([1, 1, 1])
            with col1:
                start_date = st.date_input(
                    "From Date", 
                    value=datetime.fromisoformat(date_range["min_date"]).date(),
                    min_value=datetime.fromisoformat(date_range["min_date"]).date(),
                    max_value=datetime.fromisoformat(date_range["max_date"]).date()
                )
            with col2:
                end_date = st.date_input(
                    "To Date",
                    value=datetime.fromisoformat(date_range["max_date"]).date(),
                    min_value=datetime.fromisoformat(date_range["min_date"]).date(),
                    max_value=datetime.fromisoformat(date_range["max_date"]).date()
                )
            with col3:
                all_items = [row["name"] for row in conn.execute("SELECT DISTINCT name FROM items WHERE active = TRUE ORDER BY name").fetchall()]
                
                col3a, col3b = st.columns([1, 1])
                with col3a:
                    if st.button("Select All Items", use_container_width=True):
                        st.session_state.selected_items = all_items
                        st.rerun()
                
                with col3b:
                    if st.button("Clear Selection", use_container_width=True):
                        st.session_state.selected_items = []
                        st.rerun()
                
                # Use session state for persistence
                if 'selected_items' not in st.session_state:
                    st.session_state.selected_items = ["Steak", "Salmon", "Ponzu Sauce"]
                
                selected_items = st.multiselect(
                    "Items",
                    options=all_items,
                    default=st.session_state.selected_items,
                    key="items_multiselect"
                )
                
                # Update session state when multiselect changes
                if selected_items != st.session_state.selected_items:
                    st.session_state.selected_items = selected_items
            
            if not selected_items:
                st.warning("Please select at least one item to display")
                return
            
            # Get historical data
            query = """
            SELECT 
                oh.d as date,
                i.name as item,
                oh.qty as quantity,
                i.adu,
                i.par_level,
                i.unit_type,
                oh.manager,
                oh.location,
                oh.created_at
            FROM nightly_on_hand oh
            JOIN items i ON oh.item_id = i.id
            WHERE oh.d BETWEEN ? AND ?
            AND i.name IN ({})
            ORDER BY oh.d DESC, i.name
            """.format(','.join(['?'] * len(selected_items)))
            
            df = pd.read_sql_query(
                query, 
                conn, 
                params=[start_date.isoformat(), end_date.isoformat()] + selected_items
            )
            
            if df.empty:
                st.info("No data found for selected criteria")
                return
            
            # Calculate status for each entry
            def calculate_status(row):
                entry_date = datetime.fromisoformat(row['date']).date()
                location = row.get('location', 'Avondale')  # Default to Avondale if missing
                status = calculate_item_status_location(
                    row['item'], 
                    row['quantity'], 
                    entry_date, 
                    row['adu'], 
                    row['par_level'],
                    row['unit_type'],
                    location
                )
                return status.status
            
            df['status'] = df.apply(calculate_status, axis=1)
            
            # Summary metrics
            st.subheader("📈 Summary Metrics")
            col1, col2, col3, col4 = st.columns(4)
            
            total_entries = len(df)
            red_count = len(df[df['status'] == 'Red'])
            yellow_count = len(df[df['status'] == 'Yellow'])
            green_count = len(df[df['status'] == 'Green'])
            
            col1.metric("Total Entries", total_entries)
            col2.metric("🔴 Critical", red_count)
            col3.metric("🟡 Caution", yellow_count)
            col4.metric("🟢 Good", green_count)
            
            # Historical trend chart
            st.subheader("📊 Inventory Trends")
            
            if len(df) > 0:
                # Pivot data for charting
                df_pivot = df.pivot_table(
                    index='date', 
                    columns='item', 
                    values='quantity', 
                    aggfunc='mean'
                ).reset_index()
                
                fig = go.Figure()
                
                colors = px.colors.qualitative.Set3
                for i, item in enumerate(selected_items):
                    if item in df_pivot.columns:
                        fig.add_trace(go.Scatter(
                            x=df_pivot['date'],
                            y=df_pivot[item],
                            mode='lines+markers',
                            name=item,
                            line=dict(color=colors[i % len(colors)], width=3),
                            marker=dict(size=6)
                        ))
                
                fig.update_layout(
                    title="Container Levels Over Time",
                    xaxis_title="Date",
                    yaxis_title="Containers",
                    height=400,
                    hovermode='x unified'
                )
                
                st.plotly_chart(fig, use_container_width=True)
            
            # Status distribution chart
            col1, col2 = st.columns(2)
            
            with col1:
                st.subheader("🎯 Status Distribution")
                status_counts = df['status'].value_counts()
                
                colors = {
                    'Green': '#00C851',
                    'Yellow': '#FFB300', 
                    'Red': '#FF3547',
                    'Missing': '#6C757D'
                }
                
                fig_pie = go.Figure(data=[go.Pie(
                    labels=status_counts.index,
                    values=status_counts.values,
                    marker_colors=[colors.get(status, '#6C757D') for status in status_counts.index]
                )])
                
                fig_pie.update_layout(height=300)
                st.plotly_chart(fig_pie, use_container_width=True)
            
            with col2:
                st.subheader("📋 Recent Entries")
                
                # Recent entries table with status styling
                recent_df = df.head(10)[['date', 'item', 'quantity', 'unit_type', 'status', 'manager']].copy()
                
                # Add formatted quantity column
                recent_df['formatted_qty'] = recent_df.apply(
                    lambda row: format_unit_display(row['quantity'], row['unit_type']) if pd.notna(row['quantity']) else '',
                    axis=1
                )
                
                # Display as styled dataframe
                st.dataframe(
                    recent_df[['date', 'item', 'formatted_qty', 'status', 'manager']],
                    column_config={
                        "date": "Date",
                        "item": "Item", 
                        "formatted_qty": "Quantity",
                        "status": "Status",
                        "manager": "Name"
                    },
                    hide_index=True,
                    use_container_width=True
                )
            
            # Detailed entry log
            st.subheader("📋 Complete Entry Log")
            
            # Add filtering options
            col1, col2, col3 = st.columns(3)
            with col1:
                status_filter = st.multiselect(
                    "Filter by Status",
                    options=['Green', 'Yellow', 'Red', 'Missing'],
                    default=['Green', 'Yellow', 'Red', 'Missing']
                )
            with col2:
                managers = df['manager'].dropna().unique()
                manager_filter = st.multiselect(
                    "Filter by Name", 
                    options=managers,
                    default=list(managers)
                )
            
            # Apply filters
            filtered_df = df.copy()
            if status_filter:
                filtered_df = filtered_df[filtered_df['status'].isin(status_filter)]
            if manager_filter:
                filtered_df = filtered_df[filtered_df['manager'].isin(manager_filter)]
            
            # Display filtered data
            display_df = filtered_df[['date', 'item', 'quantity', 'unit_type', 'adu', 'par_level', 'status', 'manager']].copy()
            display_df['date'] = pd.to_datetime(display_df['date']).dt.strftime('%Y-%m-%d')
            
            # Add formatted columns
            display_df['formatted_qty'] = display_df.apply(
                lambda row: format_unit_display(row['quantity'], row['unit_type']) if pd.notna(row['quantity']) else '',
                axis=1
            )
            display_df['formatted_par'] = display_df.apply(
                lambda row: format_unit_display(row['par_level'], row['unit_type']),
                axis=1
            )
            
            st.dataframe(
                display_df[['date', 'item', 'formatted_qty', 'adu', 'formatted_par', 'status', 'manager']],
                column_config={
                    "date": "Date",
                    "item": "Item",
                    "formatted_qty": "Quantity",
                    "adu": st.column_config.NumberColumn("ADU", format="%.1f"),
                    "formatted_par": "Par Level",
                    "status": "Status",
                    "manager": "Name"
                },
                hide_index=True,
                use_container_width=True,
                height=400
            )
            
            # Export functionality
            if st.button("💾 Export Data (CSV)", use_container_width=True):
                csv = filtered_df.to_csv(index=False)
                st.download_button(
                    label="Download CSV",
                    data=csv,
                    file_name=f"inventory_data_{start_date}_{end_date}.csv",
                    mime="text/csv"
                )
    
    except Exception as e:
        st.error(f"❌ Error loading analytics: {str(e)}")
        LOG.exception("Analytics page error")

def page_admin():
    """Admin settings and item management page."""
    st.title("⚙️ Admin Settings")
    
    tab1, tab2, tab3, tab4 = st.tabs(["📦 Item Management", "🔧 System Settings", "📊 System Health", "🧪 Test Functions"])
    
    with tab1:
        st.subheader("Container-Based Item Management")
        st.info("All quantities are in containers (cases, quarts, trays, bags, bottles)")
        
        try:
            with get_db_connection() as conn:
                # Get current items
                items_df = pd.read_sql_query(
                    "SELECT id, name, adu, unit_type, par_level, active FROM items ORDER BY name",
                    conn
                )
                
                # Edit existing items
                st.subheader("🔍 Edit Items")
                
                if not items_df.empty:
                    # Create display dataframe with unit type options
                    unit_options = list(UNIT_TYPES.keys())
                    
                    edited_df = st.data_editor(
                        items_df[['name', 'adu', 'unit_type', 'par_level', 'active']],
                        column_config={
                            "name": "Item Name",
                            "adu": st.column_config.NumberColumn("ADU (Containers/Day)", min_value=0.0, format="%.2f"),
                            "unit_type": st.column_config.SelectboxColumn("Unit Type", options=unit_options),
                            "par_level": st.column_config.NumberColumn("Par Level (Containers)", min_value=0, format="%.0f"),
                            "active": st.column_config.CheckboxColumn("Active")
                        },
                        hide_index=True,
                        use_container_width=True,
                        key="items_editor"
                    )
                    
                    col1, col2 = st.columns(2)
                    with col1:
                        if st.button("💾 Save Changes", type="primary", use_container_width=True):
                            try:
                                ts = now_local().isoformat()
                                for idx, row in edited_df.iterrows():
                                    item_id = items_df.iloc[idx]['id']
                                    conn.execute(
                                        "UPDATE items SET adu=?, unit_type=?, par_level=?, active=?, updated_at=? WHERE id=?",
                                        (row['adu'], row['unit_type'], row['par_level'], row['active'], ts, item_id)
                                    )
                                conn.commit()
                                st.success("✅ Items updated successfully!")
                                st.rerun()
                            except Exception as e:
                                st.error(f"❌ Error updating items: {str(e)}")
                    
                    with col2:
                        if st.button("🔄 Reset to Defaults", use_container_width=True):
                            try:
                                ts = now_local().isoformat()
                                for name, config in ITEMS_CONFIG.items():
                                    conn.execute(
                                        "UPDATE items SET adu=?, unit_type=?, par_level=?, updated_at=? WHERE name=?",
                                        (config["adu"], config["unit_type"], config["par_level"], ts, name)
                                    )
                                conn.commit()
                                st.success("✅ Items reset to defaults!")
                                st.rerun()
                            except Exception as e:
                                st.error(f"❌ Error resetting items: {str(e)}")
                
                # Add new item
                st.subheader("➕ Add New Item")
                with st.form("add_item_form"):
                    col1, col2 = st.columns(2)
                    with col1:
                        new_name = st.text_input("Item Name")
                        new_adu = st.number_input("ADU (Containers/Day)", min_value=0.0, value=1.0, step=0.1)
                    with col2:
                        new_unit_type = st.selectbox("Unit Type", options=unit_options, index=0)
                        new_par = st.number_input("Par Level (Containers)", min_value=0, value=10)
                    
                    if st.form_submit_button("➕ Add Item", use_container_width=True):
                        if new_name.strip():
                            try:
                                ts = now_local().isoformat()
                                conn.execute(
                                    "INSERT INTO items(name, adu, unit_type, buffer_days, par_level, created_at, updated_at) VALUES (?,?,?,?,?,?,?)",
                                    (new_name.strip(), new_adu, new_unit_type, DEFAULT_BUFFER_DAYS, new_par, ts, ts)
                                )
                                conn.commit()
                                st.success(f"✅ Added '{new_name}' successfully!")
                                st.rerun()
                            except sqlite3.IntegrityError:
                                st.error("❌ Item name already exists!")
                            except Exception as e:
                                st.error(f"❌ Error adding item: {str(e)}")
                        else:
                            st.error("❌ Please enter an item name")
        
        except Exception as e:
            st.error(f"❌ Error loading items: {str(e)}")
    
    with tab2:
        st.subheader("System Configuration")
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.subheader("📱 Telegram Settings")
            if USE_TEST_CHAT:
                st.info(f"🧪 **Test Mode Active**\nAll messages → Chat ID: {TEST_CHAT}")
            else:
                st.success("🚀 **Production Mode**\nMessages routed to configured chats")
            
            st.subheader("🕐 Schedule")
            st.write("**Auto-Requests (2 messages):**")
            st.write("• Tuesday 8:00 AM (Thursday delivery)")
            st.write("• Saturday 8:00 AM (Monday delivery)")
            st.write("**Reassurance:** Daily 5:00 PM")
            st.write("**Missing Counts:** Daily 11:59 PM")
            
        with col2:
            st.subheader("💾 Data Management")
            st.info(f"**Data Retention:** {DATA_RETENTION_DAYS} days\n*Older data automatically cleaned up*")
            
            if st.button("🧹 Clean Old Data Now", use_container_width=True):
                try:
                    cleanup_old_data()
                    st.success("✅ Data cleanup completed!")
                except Exception as e:
                    st.error(f"❌ Cleanup failed: {str(e)}")
            
            if st.button("💾 Export All Data", use_container_width=True):
                try:
                    with get_db_connection() as conn:
                        # Export all tables
                        tables = ['items', 'nightly_on_hand', 'transfers', 'auto_requests']
                        export_data = {}
                        
                        for table in tables:
                            df = pd.read_sql_query(f"SELECT * FROM {table}", conn)
                            export_data[table] = df
                        
                        # Create combined export
                        with pd.ExcelWriter('k2_export.xlsx', engine='openpyxl') as writer:
                            for table_name, df in export_data.items():
                                df.to_excel(writer, sheet_name=table_name, index=False)
                        
                        st.success("✅ Export completed! Check k2_export.xlsx")
                        
                except Exception as e:
                    st.error(f"❌ Export failed: {str(e)}")
    
    with tab3:
        st.subheader("System Health Dashboard")
        
        # Scheduler status
        col1, col2 = st.columns(2)
        
        with col1:
            if SCHED and SCHED.running:
                st.success("✅ **Scheduler: Running**")
                
                # Show next job runs
                st.subheader("⏰ Upcoming Jobs")
                for job in SCHED.get_jobs():
                    next_run = job.next_run_time
                    if next_run:
                        st.write(f"**{job.id.replace('_', ' ').title()}**")
                        st.write(f"Next run: {next_run.strftime('%a %b %d, %H:%M')}")
                        st.write("")
            else:
                st.error("❌ **Scheduler: Not Running**")
        
        with col2:
            st.subheader("📊 Database Stats")
            try:
                with get_db_connection() as conn:
                    # Count records in each table
                    tables = ['items', 'nightly_on_hand', 'transfers', 'auto_requests']
                    for table in tables:
                        count = conn.execute(f"SELECT COUNT(*) as count FROM {table}").fetchone()['count']
                        st.metric(f"{table.replace('_', ' ').title()}", count)
                        
            except Exception as e:
                st.error(f"❌ Database error: {str(e)}")
        
        # System actions
        st.subheader("🔧 System Actions")
        col1, col2, col3 = st.columns(3)
        
        with col1:
            if st.button("🔄 Restart Scheduler", use_container_width=True):
                try:
                    if SCHED.running:
                        SCHED.shutdown()
                    start_scheduler()
                    st.success("✅ Scheduler restarted!")
                    st.rerun()
                except Exception as e:
                    st.error(f"❌ Restart failed: {str(e)}")
        
        with col2:
            if st.button("📱 Test Telegram", use_container_width=True):
                test_msg = f"🧪 Test message from K2 Inventory\n📅 {now_local().strftime('%Y-%m-%d %H:%M:%S')}"
                if tg_send_with_retry(TEST_CHAT, test_msg):
                    st.success("✅ Test message sent!")
                else:
                    st.error("❌ Test message failed!")
        
        with col3:
            if st.button("🔧 Database Integrity Check", use_container_width=True):
                try:
                    with get_db_connection() as conn:
                        integrity_check = conn.execute("PRAGMA integrity_check").fetchone()
                        if integrity_check[0] == "ok":
                            st.success("✅ Database integrity OK!")
                        else:
                            st.error(f"❌ Database issues: {integrity_check[0]}")
                except Exception as e:
                    st.error(f"❌ Check failed: {str(e)}")
    
    with tab4:
        st.subheader("🧪 Test Functions")
        st.info("Test all scheduled jobs manually. Messages will be sent to the configured test chat.")
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.subheader("📋 Auto-Request Test")
            st.write("Test the two-message auto-request system using current data")
            
            test_weekday = st.selectbox(
                "Test as which day?",
                options=[1, 5],
                format_func=lambda x: REQUEST_WINDOWS[x]["label"],
                key="test_weekday"
            )
            
            if st.button("🧪 Test Auto-Request", use_container_width=True, type="primary"):
                try:
                    with st.spinner("Running auto-request test..."):
                        # Use most recent available data (today first, then historical)
                        today = today_local()
                        
                        with get_db_connection() as conn:
                            oh_map = map_oh_for_date_flexible(conn, today)
                        
                        # Show what data we're using
                        data_found = sum(1 for qty, _ in oh_map.values() if qty > 0.0)
                        if data_found == 0:
                            st.warning("⚠️ No recent on-hand data found. Using zeros for test.")
                        else:
                            st.info(f"ℹ️ Using most recent data ({data_found} items with quantities)")
                        
                        # Generate test requests for both locations
                        avondale_requests = generate_location_requests('Avondale', test_weekday, oh_map)
                        commissary_requests = generate_location_requests('Commissary', test_weekday, oh_map)
                        
                        success1 = success2 = success3 = success4 = True
                        
                        # Send Avondale test messages
                        if avondale_requests:
                            avondale_info = format_auto_request_info_message(avondale_requests, test_weekday, today_local())
                            avondale_order = format_auto_request_order_message(avondale_requests, test_weekday, today_local())
                            success1 = tg_send_with_retry(TEST_CHAT, f"🧪 TEST AUTO-REQUEST - AVONDALE (Info)\n\n{avondale_info}")
                            time_module.sleep(1)
                            success2 = tg_send_with_retry(TEST_CHAT, f"🧪 TEST AUTO-REQUEST - AVONDALE (Order)\n\n{avondale_order}")
                            time_module.sleep(1)
                        
                        # Send Commissary test messages  
                        if commissary_requests:
                            commissary_info = format_auto_request_info_message(commissary_requests, test_weekday, today_local())
                            commissary_order = format_auto_request_order_message(commissary_requests, test_weekday, today_local())
                            success3 = tg_send_with_retry(TEST_CHAT, f"🧪 TEST AUTO-REQUEST - COMMISSARY (Info)\n\n{commissary_info}")
                            time_module.sleep(1)
                            success4 = tg_send_with_retry(TEST_CHAT, f"🧪 TEST AUTO-REQUEST - COMMISSARY (Order)\n\n{commissary_order}")
                        
                        if success1 and success2 and success3 and success4:
                            st.success("✅ Auto-request test completed! Check your Telegram.")
                            
                            # Show preview
                            with st.expander("📱 Message Preview"):
                                if avondale_requests:
                                    st.text("Avondale Info Message:")
                                    st.code(avondale_info)
                                    st.text("Avondale Order Message:")
                                    st.code(avondale_order)
                                if commissary_requests:
                                    st.text("Commissary Info Message:")
                                    st.code(commissary_info)
                                    st.text("Commissary Order Message:")
                                    st.code(commissary_order)
                        else:
                            st.error("❌ Test failed to send messages")
                            
                except Exception as e:
                    st.error(f"❌ Auto-request test failed: {str(e)}")
            
            st.subheader("📊 Reassurance Test")
            st.write("Test daily reassurance message using today's data")
            
            if st.button("🧪 Test Reassurance", use_container_width=True):
                try:
                    with st.spinner("Running reassurance test..."):
                        with get_db_connection() as conn:
                            today = today_local()
                            avondale_status = get_item_status_for_date_location(conn, today, 'Avondale')
                            commissary_status = get_item_status_for_date_location(conn, today, 'Commissary')
                        
                        avondale_message = format_reassurance_message_location(avondale_status, today, 'Avondale')
                        commissary_message = format_reassurance_message_location(commissary_status, today, 'Commissary')
                        
                        combined_message = f"{avondale_message}\n\n{commissary_message}"
                        
                        if tg_send_with_retry(TEST_CHAT, f"🧪 TEST REASSURANCE\n\n{combined_message}"):
                            st.success("✅ Reassurance test completed! Check your Telegram.")
                            
                            with st.expander("📱 Message Preview"):
                                st.code(combined_message)
                        else:
                            st.error("❌ Reassurance test failed")
                            
                except Exception as e:
                    st.error(f"❌ Reassurance test failed: {str(e)}")
        
        with col2:
            st.subheader("⚠️ Missing Counts Test")
            st.write("Test missing counts detection for today")
            
            if st.button("🧪 Test Missing Counts", use_container_width=True):
                try:
                    with st.spinner("Running missing counts test..."):
                        with get_db_connection() as conn:
                            d = today_local()
                            missing = []
                            cur = conn.execute("SELECT name FROM items WHERE active = TRUE ORDER BY name")
                            for row in cur.fetchall():
                                exists = conn.execute(
                                    "SELECT 1 FROM nightly_on_hand WHERE item_id = (SELECT id FROM items WHERE name = ?) AND d = ?",
                                    (row["name"], d.isoformat()),
                                ).fetchone()
                                if not exists:
                                    missing.append(row["name"])
                        
                        if missing:
                            header = f"🧪 TEST MISSING COUNTS\n\n<b>⚠️ MISSING NIGHTLY COUNTS</b>\n📅 {d.strftime('%a %b %d, %Y')} • 11:59 PM\n"
                            message = header + f"\n<b>Items without counts:</b>\n" + "\n".join(f"• {name}" for name in missing)
                        else:
                            message = f"🧪 TEST MISSING COUNTS\n\n✅ <b>ALL COUNTS PRESENT</b>\n📅 {d.strftime('%a %b %d, %Y')}\nNo missing inventory counts detected."
                        
                        if tg_send_with_retry(TEST_CHAT, message):
                            st.success("✅ Missing counts test completed! Check your Telegram.")
                            
                            with st.expander("📱 Message Preview"):
                                st.code(message)
                        else:
                            st.error("❌ Missing counts test failed")
                            
                except Exception as e:
                    st.error(f"❌ Missing counts test failed: {str(e)}")
            
            st.subheader("📦 Shortage Tracking Test")
            st.write("Test shortage detection with sample data")
            
            if st.button("🧪 Test Shortage Tracking", use_container_width=True):
                try:
                    # Create sample received data for testing with current data
                    test_received = [("Honey", 5.0), ("Fish", 2.0)]  # Different items for testing
                    test_date = today_local()
                    
                    message = format_received_message_with_shortages(test_received, test_date, "Test shortage tracking")
                    
                    if tg_send_with_retry(TEST_CHAT, f"🧪 TEST SHORTAGE TRACKING\n\n{message}"):
                        st.success("✅ Shortage tracking test completed! Check your Telegram.")
                        
                        with st.expander("📱 Message Preview"):
                            st.code(message)
                    else:
                        st.error("❌ Shortage tracking test failed")
                        
                except Exception as e:
                    st.error(f"❌ Shortage tracking test failed: {str(e)}")
        
        # Test summary section
        st.subheader("🔍 Test Data Summary")
        try:
            with get_db_connection() as conn:
                # Show current test data status
                today = today_local()
                yesterday = today - timedelta(days=1)
                
                col1, col2, col3 = st.columns(3)
                
                with col1:
                    today_count = conn.execute(
                        "SELECT COUNT(*) as count FROM nightly_on_hand WHERE d = ?",
                        (today.isoformat(),)
                    ).fetchone()['count']
                    st.metric("Today's Entries", today_count)
                
                with col2:
                    yesterday_count = conn.execute(
                        "SELECT COUNT(*) as count FROM nightly_on_hand WHERE d = ?", 
                        (yesterday.isoformat(),)
                    ).fetchone()['count']
                    st.metric("Yesterday's Entries", yesterday_count)
                
                with col3:
                    requests_count = conn.execute(
                        "SELECT COUNT(*) as count FROM auto_requests WHERE request_date >= ?",
                        ((today - timedelta(days=7)).isoformat(),)
                    ).fetchone()['count']
                    st.metric("Recent Requests", requests_count)
                    
        except Exception as e:
            st.error(f"❌ Error loading test data: {str(e)}")

# Add debug toggle in sidebar for development
if st.sidebar.button("🔍 Toggle Debug"):
    st.session_state.show_debug = not st.session_state.get('show_debug', False)
    st.rerun()

# ========================== TELEGRAM COMMAND HANDLER ==========================

# COMPLETE INTERACTIVE TELEGRAM DATA ENTRY SYSTEM
# Replace your existing Telegram command functions with these

def start_telegram_command_handler():
    """Enhanced Telegram command handler with interactive conversation support."""
    if not BOT_TOKEN:
        LOG.info("No Telegram bot token - command handler disabled")
        return
    
    def command_handler():
        """Background thread for handling Telegram commands and conversations."""
        last_update_id = 0
        
        while True:
            try:
                # Get updates from Telegram
                response = requests.get(
                    f"https://api.telegram.org/bot{BOT_TOKEN}/getUpdates",
                    params={
                        'offset': last_update_id + 1,
                        'timeout': 10,
                        'allowed_updates': ['message']
                    }
                )
                
                if response.status_code == 200:
                    data = response.json()
                    
                    for update in data.get('result', []):
                        last_update_id = update['update_id']
                        
                        if 'message' in update:
                            message = update['message']
                            chat_id = message['chat']['id']
                            text = message.get('text', '')
                            
                            # Handle both commands and conversation responses
                            if text.startswith('/'):
                                # It's a command
                                command_parts = text.split(' ', 1)
                                command = command_parts[0]
                                full_message = text if len(command_parts) > 1 else command
                                
                                handle_telegram_command(chat_id, command, full_message)
                            else:
                                # It's a conversation response
                                handle_telegram_command(chat_id, text, text)
                
            except Exception as e:
                LOG.error(f"Telegram command handler error: {e}")
                time_module.sleep(30)  # FIXED: Use time_module instead of time
            
            time_module.sleep(1)  # FIXED: Use time_module instead of time
    
    # Start command handler in background thread
    thread = threading.Thread(target=command_handler, daemon=True)
    thread.start()
    LOG.info("Enhanced Telegram command handler started with interactive conversation support")


class ConversationState:
    """Manages conversation state for interactive data entry."""
    
    def __init__(self, chat_id: int):
        self.chat_id = chat_id
        self.data = conversation_states.get(chat_id, {})
        
    def get(self, key: str, default=None):
        return self.data.get(key, default)
    
    def set(self, key: str, value):
        self.data[key] = value
        conversation_states[self.chat_id] = self.data
    
    def clear(self):
        if self.chat_id in conversation_states:
            del conversation_states[self.chat_id]

def handle_telegram_command(chat_id: int, command: str, message_text: str = ""):
    """Enhanced command handler with ADU commands."""
    try:
        state = ConversationState(chat_id)
        
        # Check if user is in middle of conversation
        current_flow = state.get('current_flow')
        
        if current_flow and not command.startswith('/'):
            # User is responding to a question in active flow
            if current_flow == 'entry':
                handle_conversation_response(chat_id, message_text)
            elif current_flow == 'edit_adu':
                handle_edit_adu_response(chat_id, message_text)
            return
        
        # Handle new commands
        command = command.lower().strip()
        
        if command == '/entry':
            start_entry_workflow(chat_id)
        elif command == '/adu':
            execute_adu_info_command(chat_id)
        elif command == '/editadu':
            start_edit_adu_workflow(chat_id)
        elif command == '/cancel':
            cancel_workflow(chat_id)
        elif command == '/info':
            execute_auto_request_info(chat_id)
        elif command == '/order':
            execute_auto_request_order(chat_id)
        elif command == '/reassurance':
            execute_reassurance_command(chat_id)
        elif command == '/missing':
            execute_missing_counts_command(chat_id)
        elif command == '/help':
            send_help_message(chat_id)
        elif command == '/commands':
            send_commands_list(chat_id)
        else:
            error_msg = f"Unknown command: {command}\n\nUse /help to see available commands"
            tg_send_with_retry(chat_id, error_msg)
            
    except Exception as e:
        LOG.error(f"Error handling command {command}: {e}")
        error_msg = f"Error executing command: {str(e)}"
        tg_send_with_retry(chat_id, error_msg)

def start_entry_workflow(chat_id: int):
    """Start workflow with location selection."""
    state = ConversationState(chat_id)
    state.clear()
    
    state.set('current_flow', 'entry')
    state.set('step', 'entry_type')
    
    welcome_msg = """🚀 <b>INTERACTIVE DATA ENTRY</b>

Let's walk through entering your inventory data step by step.

<b>Choose entry type:</b>
1️⃣ On-Hand Avondale (inventory count)
2️⃣ On-Hand Commissary (inventory count)
3️⃣ Received Avondale (delivery log)
4️⃣ Received Commissary (delivery log)

Reply with <b>1</b>, <b>2</b>, <b>3</b>, or <b>4</b>

💡 Type /cancel anytime to exit"""
    
    tg_send_with_retry(chat_id, welcome_msg)

def handle_conversation_response(chat_id: int, response: str):
    """Handle user responses during interactive conversation."""
    state = ConversationState(chat_id)
    current_step = state.get('step')
    
    if current_step == 'entry_type':
        handle_entry_type_response(chat_id, response)
    elif current_step == 'date':
        handle_date_response(chat_id, response)
    elif current_step == 'manager_name':
        handle_name_response(chat_id, response)
    elif current_step.startswith('item_'):
        handle_item_response(chat_id, response)
    elif current_step == 'notes':
        handle_notes_response(chat_id, response)
    elif current_step == 'review':
        handle_review_response(chat_id, response)

def handle_entry_type_response(chat_id: int, response: str):
    """Handle entry type and location selection."""
    state = ConversationState(chat_id)
    response = response.strip()
    
    if response in ['1', '1️⃣']:
        state.set('entry_type', 'On-Hand')
        state.set('location', 'Avondale')
    elif response in ['2', '2️⃣']:
        state.set('entry_type', 'On-Hand')
        state.set('location', 'Commissary')
    elif response in ['3', '3️⃣']:
        state.set('entry_type', 'Received')
        state.set('location', 'Avondale')
    elif response in ['4', '4️⃣']:
        state.set('entry_type', 'Received')
        state.set('location', 'Commissary')
    else:
        msg = "❌ Invalid selection. Please reply with <b>1</b>, <b>2</b>, <b>3</b>, or <b>4</b>"
        tg_send_with_retry(chat_id, msg)
        return
    
    # Move to date selection
    state.set('step', 'date')
    ask_for_date(chat_id)

def ask_for_date(chat_id: int):
    """Ask for entry date with proper HTML formatting."""
    today = today_local()
    yesterday = today - timedelta(days=1)
    
    msg = f"""📅 <b>SELECT DATE</b>

<b>Choose date for this entry:</b>
1️⃣ Today ({today.strftime('%b %d')})
2️⃣ Yesterday ({yesterday.strftime('%b %d')})
3️⃣ Custom date

Reply with <b>1</b>, <b>2</b>, or <b>3</b>

⬅️ Type <b>back</b> to go back
💡 Type <b>/cancel</b> to exit"""
    
    tg_send_with_retry(chat_id, msg)

def handle_date_response(chat_id: int, response: str):
    """Handle date selection with proper HTML formatting."""
    state = ConversationState(chat_id)
    response = response.strip().lower()
    
    if response == 'back':
        state.set('step', 'entry_type')
        start_entry_workflow(chat_id)
        return
    
    today = today_local()
    
    if response in ['1', '1️⃣', 'today']:
        selected_date = today
    elif response in ['2', '2️⃣', 'yesterday']:
        selected_date = today - timedelta(days=1)
    elif response in ['3', '3️⃣', 'custom']:
        msg = """📅 <b>ENTER CUSTOM DATE</b>

Format: YYYY-MM-DD (e.g., 2024-09-12)
Or: MM-DD (e.g., 09-12 for this year)

⬅️ Type <b>back</b> to go back"""
        tg_send_with_retry(chat_id, msg)
        state.set('step', 'custom_date')
        return
    else:
        # Try to parse as custom date
        try:
            if len(response) == 5 and '-' in response:  # MM-DD format
                month, day = response.split('-')
                selected_date = date(today.year, int(month), int(day))
            else:  # YYYY-MM-DD format
                selected_date = date.fromisoformat(response)
        except:
            msg = "❌ Invalid date format. Please use 1, 2, 3, or YYYY-MM-DD format"
            tg_send_with_retry(chat_id, msg)
            return
    
    state.set('entry_date', selected_date.isoformat())
    state.set('step', 'manager_name')
    ask_for_name(chat_id)

def ask_for_name(chat_id: int):
    """Ask for manager name with proper HTML formatting."""
    state = ConversationState(chat_id)
    entry_type = state.get('entry_type')
    
    action = "counting" if entry_type == "On-Hand" else "receiving"
    
    msg = f"""👤 <b>ENTER YOUR NAME</b>

Who is {action} this inventory?

Enter your first name or full name:

⬅️ Type <b>back</b> to go back
💡 Type <b>/cancel</b> to exit"""
    
    tg_send_with_retry(chat_id, msg)

def handle_name_response(chat_id: int, response: str):
    """Handle name entry."""
    state = ConversationState(chat_id)
    response = response.strip()
    
    if response.lower() == 'back':
        state.set('step', 'date')
        ask_for_date(chat_id)
        return
    
    if len(response) < 2:
        msg = "❌ Please enter a valid name (at least 2 characters)"
        tg_send_with_retry(chat_id, msg)
        return
    
    state.set('manager_name', response)
    
    # Initialize items data
    state.set('items_data', {})
    state.set('current_item_index', 0)
    
    # Start item entry
    start_item_entry(chat_id)

def start_item_entry(chat_id: int):
    """Start entering items based on selected location."""
    state = ConversationState(chat_id)
    location = state.get('location', 'Avondale')
    
    # Get items for selected location
    if location == 'Avondale':
        items_config = AVONDALE_ITEMS
    else:
        items_config = COMMISSARY_ITEMS
    
    # Convert to list format with item details
    items = []
    for name, config in items_config.items():
        items.append((name, config["adu"], config["par_level"], config["unit_type"]))
    
    state.set('available_items', items)
    state.set('current_item_index', 0)
    ask_next_item(chat_id)

def ask_next_item(chat_id: int):
    """Ask for next item with location context."""
    state = ConversationState(chat_id)
    items = state.get('available_items', [])
    current_index = state.get('current_item_index', 0)
    entry_type = state.get('entry_type')
    location = state.get('location', 'Avondale')
    entry_date = state.get('entry_date')
    
    if current_index >= len(items):
        state.set('step', 'notes')
        ask_for_notes(chat_id)
        return
    
    item_name, adu, par, unit_type = items[current_index]
    unit_name = UNIT_TYPES.get(unit_type, unit_type)
    
    # Progress indicator
    progress = f"({current_index + 1}/{len(items)})"
    
    # Calculate consumption need for this item
    if entry_date:
        date_obj = date.fromisoformat(entry_date)
        days_to_delivery = days_until_delivery_location(date_obj, location)
        consumption_need = adu * (days_to_delivery + DEFAULT_BUFFER_DAYS)
        consumption_need_rounded = math.ceil(consumption_need)
    else:
        consumption_need_rounded = math.ceil(adu * 7.5)
    
    if entry_type == "On-Hand":
        action = "in stock"
        help_info = f"ADU: {adu} {unit_name}/day | Need: {consumption_need_rounded} {unit_name} to reach delivery"
    else:
        action = "received"
        help_info = f"Daily usage: {adu} {unit_name}/day"
    
    msg = f"""📦 <b>{location.upper()} - ITEM ENTRY {progress}</b>

<b>{item_name}</b>
How many {unit_name} {action}?

💡 {help_info}

Enter quantity (e.g., 5, 2.5) or <b>0</b> if none:

⬅️ Type <b>back</b> for previous item
⏭️ Type <b>skip</b> to skip this item
💡 Type <b>/cancel</b> to exit"""
    
    state.set('step', f'item_{current_index}')
    tg_send_with_retry(chat_id, msg)

def handle_item_response(chat_id: int, response: str):
    """Handle item quantity entry."""
    state = ConversationState(chat_id)
    items = state.get('available_items', [])
    current_index = state.get('current_item_index', 0)
    items_data = state.get('items_data', {})
    
    response = response.strip().lower()
    
    if response == 'back':
        if current_index > 0:
            # Go to previous item
            state.set('current_item_index', current_index - 1)
            ask_next_item(chat_id)
        else:
            # Go back to name entry
            state.set('step', 'manager_name')
            ask_for_name(chat_id)
        return
    
    if response == 'skip':
        # Skip this item
        state.set('current_item_index', current_index + 1)
        ask_next_item(chat_id)
        return
    
    # Try to parse quantity
    item_name = items[current_index][0]
    
    try:
        if response == '0':
            qty = 0
        else:
            qty = float(response)
            if qty < 0:
                msg = "❌ Quantity cannot be negative. Please enter a positive number or 0:"
                tg_send_with_retry(chat_id, msg)
                return
    except ValueError:
        msg = "❌ Invalid quantity. Please enter a number (e.g., 5, 2.5, or 0):"
        tg_send_with_retry(chat_id, msg)
        return
    
    # Store the quantity
    if qty > 0:
        items_data[item_name] = qty
    # If qty is 0, don't store (same as skipping)
    
    state.set('items_data', items_data)
    state.set('current_item_index', current_index + 1)
    
    # Move to next item
    ask_next_item(chat_id)

def ask_for_notes(chat_id: int):
    """Ask for optional notes with proper HTML formatting."""
    state = ConversationState(chat_id)
    entry_type = state.get('entry_type')
    
    msg = f"""📝 <b>OPTIONAL NOTES</b>

Any notes about this {entry_type.lower()} entry?

Examples:
• "Steak looked fresh"
• "Short on Aioli deliveries"  
• "Busy night, quick count"

Type your notes or <b>skip</b> for none:

⬅️ Type <b>back</b> to modify items
💡 Type <b>/cancel</b> to exit"""
    
    tg_send_with_retry(chat_id, msg)

def handle_notes_response(chat_id: int, response: str):
    """Handle notes entry."""
    state = ConversationState(chat_id)
    response = response.strip()
    
    if response.lower() == 'back':
        # Go back to last item
        items = state.get('available_items', [])
        state.set('current_item_index', len(items) - 1)
        ask_next_item(chat_id)
        return
    
    if response.lower() == 'skip':
        notes = ""
    else:
        notes = response
    
    state.set('notes', notes)
    state.set('step', 'review')
    show_entry_review(chat_id)

def show_entry_review(chat_id: int):
    """Show entry review with location information."""
    state = ConversationState(chat_id)
    entry_type = state.get('entry_type')
    location = state.get('location', 'Avondale')
    entry_date = state.get('entry_date')
    manager_name = state.get('manager_name')
    items_data = state.get('items_data', {})
    notes = state.get('notes', '')
    
    date_obj = date.fromisoformat(entry_date)
    formatted_date = date_obj.strftime('%a %b %d, %Y')
    
    review_msg = f"""📋 <b>ENTRY REVIEW</b>

<b>Location:</b> {location}
<b>Type:</b> {entry_type}
<b>Date:</b> {formatted_date}
<b>Name:</b> {manager_name}

<b>Items ({len(items_data)} entered):</b>"""
    
    if items_data:
        # Get item details for display
        items_config = AVONDALE_ITEMS if location == 'Avondale' else COMMISSARY_ITEMS
        
        for item_name, qty in items_data.items():
            if item_name in items_config:
                unit_type = items_config[item_name]["unit_type"]
                unit_name = UNIT_TYPES.get(unit_type, unit_type)
                
                if qty == 1.0:
                    unit_name = unit_name.rstrip('s')
                
                review_msg += f"\n• {item_name}: {qty:g} {unit_name}"
    else:
        review_msg += "\n• (No items entered)"
    
    if notes:
        review_msg += f"\n\n<b>Notes:</b> {notes}"
    
    review_msg += f"""

<b>Actions:</b>
✅ <b>Submit</b> - Save this entry
✏️ <b>Edit</b> - Go back and modify
❌ <b>Cancel</b> - Discard entry

Reply with: <b>submit</b>, <b>edit</b>, or <b>cancel</b>"""
    
    tg_send_with_retry(chat_id, review_msg)

def handle_review_response(chat_id: int, response: str):
    """Handle review actions."""
    state = ConversationState(chat_id)
    response = response.strip().lower()
    
    if response in ['submit', 'save', 'confirm']:
        submit_entry(chat_id)
    elif response in ['edit', 'modify', 'back']:
        # Go back to notes
        state.set('step', 'notes')
        ask_for_notes(chat_id)
    elif response in ['cancel', 'discard']:
        cancel_workflow(chat_id)
    else:
        msg = "❌ Invalid action. Please reply with **submit**, **edit**, or **cancel**"
        tg_send_with_retry(chat_id, msg)

def submit_entry(chat_id: int):
    """Submit entry with location information."""
    state = ConversationState(chat_id)
    
    try:
        entry_type = state.get('entry_type')
        location = state.get('location', 'Avondale')
        entry_date = date.fromisoformat(state.get('entry_date'))
        manager_name = state.get('manager_name')
        items_data = state.get('items_data', {})
        notes = state.get('notes', '')
        
        if not items_data:
            msg = "❌ No items to submit. Entry cancelled."
            tg_send_with_retry(chat_id, msg)
            state.clear()
            return
        
        # Prepare submission data for all items (location-specific)
        items_config = AVONDALE_ITEMS if location == 'Avondale' else COMMISSARY_ITEMS
        
        qty_inputs = {}
        for item_name in items_config.keys():
            qty_inputs[item_name] = items_data.get(item_name, None)
        
        # Add location to notes
        full_notes = f"Entered via Telegram - {location}. {notes}".strip()
        
        # Submit with location parameter
        created = handle_submit_with_location(entry_type, entry_date, manager_name, full_notes, qty_inputs, location)
        
        if created > 0:
            success_msg = f"""✅ <b>ENTRY SUBMITTED SUCCESSFULLY!</b>

<b>{entry_type} - {location}</b> entry saved:
• <b>Date:</b> {entry_date.strftime('%a %b %d, %Y')}
• <b>Items:</b> {len(items_data)} entered
• <b>Manager:</b> {manager_name}

📱 Message sent to management channels

🚀 Type <b>/entry</b> to create another entry"""
            
            tg_send_with_retry(chat_id, success_msg)
        else:
            error_msg = "❌ Failed to save entry. Please check logs or try again."
            tg_send_with_retry(chat_id, error_msg)
        
    except Exception as e:
        LOG.error(f"Failed to submit Telegram entry: {e}")
        error_msg = f"❌ Submission failed: {str(e)}"
        tg_send_with_retry(chat_id, error_msg)
    
    state.clear()

def handle_submit_with_location(entry_type: str, entry_date: date, manager: str, notes: str, qty_inputs: Dict[str, Optional[float]], location: str) -> int:
    """Submit handler with location support."""
    created = 0
    ts = now_local().isoformat()
    
    LOG.info(f"=== SUBMIT HANDLER START - {location} ===")
    LOG.info(f"Entry type: {entry_type}, Location: {location}, Manager: {manager}")
    
    # Get items config for this location
    items_config = AVONDALE_ITEMS if location == 'Avondale' else COMMISSARY_ITEMS
    
    # Clean validation for partial submissions
    valid_entries = {}
    for item_name, qty in qty_inputs.items():
        if item_name not in items_config:
            continue  # Skip items not relevant to this location
        if qty is None:
            continue
        try:
            float_qty = float(qty)
            if float_qty > 0:
                valid_entries[item_name] = float_qty
        except (ValueError, TypeError):
            continue
    
    if not valid_entries:
        return 0
    
    try:
        with get_db_connection() as conn:
            if entry_type == "On-Hand":
                items_status = []
                
                for item_name, qty in valid_entries.items():
                    item_id = upsert_item_id(conn, item_name)
                    
                    config = items_config[item_name]
                    adu = config["adu"]
                    par_level = config["par_level"]
                    unit_type = config["unit_type"]
                    
                    # Insert with location
                    conn.execute(
                        "INSERT OR REPLACE INTO nightly_on_hand(item_id, d, qty, manager, notes, location, created_at) VALUES (?,?,?,?,?,?,?)",
                        (item_id, entry_date.isoformat(), qty, manager, notes, location, ts),
                    )
                    created += 1
                    
                    status = calculate_item_status_location(item_name, qty, entry_date, adu, par_level, unit_type, location)
                    items_status.append(status)
                
                conn.commit()
                
                if items_status:
                    message = format_on_hand_message_with_location(items_status, entry_date, manager, location)
                    success = tg_send_with_retry(CHAT_ONHAND, message)
                    
            elif entry_type == "Received":
                items_received = []
                
                for item_name, qty in valid_entries.items():
                    item_id = upsert_item_id(conn, item_name)
                    
                    # Insert with location
                    conn.execute(
                        "INSERT INTO transfers(item_id, d, type, received_qty, notes, location, created_at) VALUES (?,?,?,?,?,?,?)",
                        (item_id, entry_date.isoformat(), "Received", qty, notes, location, ts),
                    )
                    created += 1
                    items_received.append((item_name, qty))
                
                conn.commit()
                
                if items_received:
                    message = format_received_message_with_location(items_received, entry_date, notes, manager, location)
                    success = tg_send_with_retry(CHAT_RECEIVED, message)
                
    except Exception as e:
        LOG.error("Submit failed for %s - %s: %s", entry_type, location, e)
        raise
        
    return created

def format_received_message_with_location(items_received: List[Tuple[str, float]], entry_date: date, notes: str, manager_name: str, location: str) -> str:
    """Format received message with location identification."""
    style = MessageStyle()
    
    header = f"<b>📦 DELIVERY RECEIVED - {location.upper()}</b>\n📅 {entry_date.strftime('%a %b %d, %Y')}\n"
    if manager_name.strip():
        header += f"👤 Received by: <b>{manager_name}</b>\n"
    
    lines = [header, f"\n<b>✅ ITEMS RECEIVED ({len(items_received)} items):</b>"]
    
    # Get items config for location to determine unit types
    items_config = AVONDALE_ITEMS if location == 'Avondale' else COMMISSARY_ITEMS
    
    for name, received_qty in items_received:
        if name in items_config:
            unit_type = items_config[name]["unit_type"]
            received_display = format_unit_display(received_qty, unit_type)
            lines.append(f"• <b>{name}</b>: +{received_display}")
    
    if notes.strip():
        lines.append(f"\n📝 <b>Notes:</b> {notes}")
    
    return "\n".join(lines)

# NEW: Location-aware message formatting
def format_on_hand_message_with_location(items_status: List[ItemStatus], entry_date: date, manager_name: str, location: str) -> str:
    """Format on-hand message with location identification."""
    style = MessageStyle()
    
    reds = [item for item in items_status if item.status == "Red"]
    yellows = [item for item in items_status if item.status == "Yellow"]
    greens = [item for item in items_status if item.status == "Green"]
    missing = [item for item in items_status if item.status == "Missing"]
    
    next_delivery = next_delivery_after(entry_date)
    delivery_day = next_delivery.strftime('%a %b %d')
    
    header = f"<b>{style.HEADER} INVENTORY COUNT - {location.upper()}</b>\n"
    header += f"📅 {entry_date.strftime('%a %b %d, %Y')}\n"
    header += f"🚚 Next Delivery: {delivery_day}\n"
    if manager_name.strip():
        header += f"👤 Submitted by: <b>{manager_name}</b>\n"
    
    summary = f"📊 <b>Status:</b> {len(greens)}✅ {len(yellows)}🟡 {len(reds)}🔴"
    if missing:
        summary += f" {len(missing)}❌"
    
    lines = [header, summary, ""]
    
    # Critical items
    if reds:
        lines.append(f"<b>{style.CRITICAL} URGENT - WON'T LAST TO DELIVERY</b>")
        for item in reds:
            qty_display = format_unit_display(item.qty or 0, item.unit_type)
            shortage = max(0.0, item.consumption_need - (item.qty or 0.0))
            shortage_rounded = math.ceil(shortage)
            
            lines.append(f"{style.RED} <b>{item.name}</b>: {qty_display}")
            lines.append(f"   Need <b>{shortage_rounded} more</b> to reach delivery")
        lines.append("")
    
    # Warning items
    if yellows:
        lines.append(f"<b>{style.WARNING} CAUTION - BELOW PAR LEVEL</b>")
        for item in yellows:
            qty_display = format_unit_display(item.qty or 0, item.unit_type)
            coverage_days_rounded = math.ceil(item.days_coverage)
            lines.append(f"{style.YELLOW} <b>{item.name}</b>: {qty_display}")
            lines.append(f"   ({coverage_days_rounded} days coverage)")
        lines.append("")
    
    # Missing counts
    if missing:
        lines.append(f"<b>{style.CRITICAL} MISSING COUNTS</b>")
        for item in missing:
            lines.append(f"{style.MISSING} <b>{item.name}</b> - No count entered")
        lines.append("")
    
    # Fully stocked
    if greens:
        lines.append(f"<b>{style.SUCCESS} FULLY STOCKED</b>")
        for item in greens:
            qty_display = format_unit_display(item.qty, item.unit_type)
            lines.append(f"{style.GREEN} <b>{item.name}</b>: {qty_display}")
    
    return "\n".join(lines)


def cancel_workflow(chat_id: int):
    """Cancel current workflow with proper HTML formatting."""
    state = ConversationState(chat_id)
    state.clear()
    
    msg = """❌ <b>ENTRY CANCELLED</b>

Your entry has been discarded.

🚀 Type <b>/entry</b> to start a new entry
💡 Type <b>/help</b> for other commands"""
    
    tg_send_with_retry(chat_id, msg)

def send_help_message(chat_id: int):
    """Updated help message with new ADU commands."""
    help_text = """🤖 <b>K2 INVENTORY BOT - HELP</b>

<b>🔥 MAIN COMMAND:</b>
/entry - Interactive data entry
• Step-by-step guided process
• Enter On-Hand counts or Received deliveries
• Choose Avondale or Commissary location
• Review before submitting

<b>📊 REPORTING COMMANDS:</b>
/info - Auto-request analysis (detailed)
/order - Clean order list for suppliers (both locations)
/reassurance - Daily status check
/missing - Check for missing counts

<b>⚙️ MANAGEMENT COMMANDS:</b>
/adu - Show all Average Daily Usage values
/editadu - Edit ADU values for any item

<b>📋 OTHER COMMANDS:</b>
/commands - Quick command reference
/cancel - Exit current process
/help - This help message

<b>🚀 START HERE:</b> Type <b>/entry</b> to begin interactive data entry!</b>"""

    tg_send_with_retry(chat_id, help_text)

def send_commands_list(chat_id: int):
    """Updated commands list."""
    commands_text = """🤖 <b>K2 INVENTORY BOT - COMMANDS</b>

<b>🔥 MAIN:</b>
/entry - Interactive data entry (Avondale/Commissary)

<b>📊 REPORTS:</b>
/info - Auto-request analysis
/order - Order lists (both locations)
/reassurance - Daily status check  
/missing - Missing counts check

<b>⚙️ MANAGEMENT:</b>
/adu - Show all ADU values
/editadu - Edit ADU values

<b>📋 HELP:</b>
/commands - This list
/help - Detailed help
/cancel - Exit current process

<b>💡 TIP:</b> Type <b>/entry</b> for guided data entry</b>"""

    tg_send_with_retry(chat_id, commands_text)

# ========================= TELEGRAM REPORTING FUNCTIONS ==========================

def execute_auto_request_info(chat_id: int):
    """FIXED: Execute auto-request info with location-aware calculations."""
    try:
        now = now_local()
        wd = now.weekday()
        
        if wd not in AVONDALE_REQUEST_WINDOWS and wd not in COMMISSARY_REQUEST_WINDOWS:
            wd = 1  # Default to Tuesday
        
        command_header = f"🤖 COMMAND EXECUTED: /info\n📅 {now.strftime('%Y-%m-%d %H:%M:%S')}\n\n"
        
        with get_db_connection() as conn:
            today = now.date()
            
            # Check for data availability per location
            oh_map_avondale = map_oh_for_date_flexible_location(conn, today, 'Avondale')
            oh_map_commissary = map_oh_for_date_flexible_location(conn, today, 'Commissary')
            
            avondale_data = sum(1 for name, (qty, _) in oh_map_avondale.items() if qty > 0)
            commissary_data = sum(1 for name, (qty, _) in oh_map_commissary.items() if qty > 0)
            
            if avondale_data == 0 and commissary_data == 0:
                no_data_msg = command_header + """⚠️ <b>No inventory data available</b>

To generate auto-request analysis, you need recent inventory counts for at least one location.

<b>Next steps:</b>
1️⃣ Use /entry to add inventory counts  
2️⃣ Or add counts via the web app
3️⃣ Then try /info again

<b>What this command does:</b>
• Shows detailed request analysis
• Calculates consumption needs by location  
• Identifies items requiring orders
• Provides coverage analysis"""
                
                tg_send_with_retry(chat_id, no_data_msg)
                return
            
            # Generate location-specific requests
            messages_sent = 0
            
            if avondale_data > 0:
                avondale_requests = generate_location_requests('Avondale', wd, oh_map_avondale)
                avondale_message = format_auto_request_info_message_location(avondale_requests, wd, today, 'Avondale')
                full_message = command_header + avondale_message if messages_sent == 0 else avondale_message
                tg_send_with_retry(chat_id, full_message)
                messages_sent += 1
                
            if commissary_data > 0:
                if messages_sent > 0:
                    time_module.sleep(1)  # Brief delay between messages
                
                commissary_requests = generate_location_requests('Commissary', wd, oh_map_commissary)
                commissary_message = format_auto_request_info_message_location(commissary_requests, wd, today, 'Commissary')
                full_message = command_header + commissary_message if messages_sent == 0 else commissary_message
                tg_send_with_retry(chat_id, full_message)
                messages_sent += 1
            
            LOG.info(f"Auto-request info sent via command to {chat_id} ({messages_sent} messages)")
            
    except Exception as e:
        LOG.error(f"Failed to execute auto-request info command: {e}")
        error_msg = f"❌ <b>AUTO-REQUEST INFO FAILED</b>\n\nError: <code>{str(e)}</code>"
        tg_send_with_retry(chat_id, error_msg)

def execute_auto_request_order(chat_id: int):
    """Execute auto-request order with separate messages for both locations."""
    try:
        now = now_local()
        wd = now.weekday()
        
        if wd not in REQUEST_WINDOWS:
            wd = 1  # Default to Tuesday
        
        with get_db_connection() as conn:
            today = now.date()
            oh_map = map_oh_for_date_flexible(conn, today)
            
            # Check for data availability
            avondale_data = sum(1 for name, (qty, _) in oh_map.items() if name in AVONDALE_ITEMS and qty > 0)
            commissary_data = sum(1 for name, (qty, _) in oh_map.items() if name in COMMISSARY_ITEMS and qty > 0)
            
            if avondale_data == 0 and commissary_data == 0:
                no_data_msg = f"""📋 <b>ORDER REQUEST</b>
🤖 Command executed: /order
📅 {now.strftime('%Y-%m-%d %H:%M:%S')}

⚠️ <b>No inventory data available</b>

To generate order requests, you need recent inventory counts for both locations.

<b>Next steps:</b>
1️⃣ Use /entry to add inventory counts
2️⃣ Or add counts via the web app
3️⃣ Then try /order again"""
                
                tg_send_with_retry(chat_id, no_data_msg)
                return
            
            # Generate requests for each location
            if avondale_data > 0:
                avondale_requests = generate_location_requests('Avondale', wd, oh_map)
                avondale_message = format_location_order_message(avondale_requests, wd, today, 'Avondale')
                tg_send_with_retry(chat_id, avondale_message)
                time_module.sleep(1)  # Brief delay between messages
            
            if commissary_data > 0:
                commissary_requests = generate_location_requests('Commissary', wd, oh_map)
                commissary_message = format_location_order_message(commissary_requests, wd, today, 'Commissary')
                tg_send_with_retry(chat_id, commissary_message)
            
            LOG.info(f"Location-specific order messages sent via command to {chat_id}")
            
    except Exception as e:
        LOG.error(f"Failed to execute auto-request order command: {e}")
        error_msg = f"❌ <b>ORDER REQUEST FAILED</b>\n\nError: <code>{str(e)}</code>"
        tg_send_with_retry(chat_id, error_msg)

def generate_location_requests(location: str, run_weekday: int, oh_by_item: Dict[str, Tuple[float, str]]) -> List[Tuple[str, float, float, float, str]]:
    """FIXED: Generate requests for specific location with proper delivery windows."""
    items_config = AVONDALE_ITEMS if location == 'Avondale' else COMMISSARY_ITEMS
    
    # Use location-specific request windows
    if location == 'Commissary':
        windows = COMMISSARY_REQUEST_WINDOWS
    else:
        windows = AVONDALE_REQUEST_WINDOWS
        
    window = windows.get(run_weekday, {"label": "Next Delivery", "total_days": 6.5})
    total_days = window["total_days"] + DEFAULT_BUFFER_DAYS
    
    requests = []
    for name, config in items_config.items():
        adu = config["adu"]
        unit_type = config["unit_type"]
        oh_qty, _ = oh_by_item.get(name, (0.0, unit_type))
        
        needed = adu * total_days
        req = max(0.0, needed - oh_qty)
        rounded = math.ceil(req) if req > 0 else 0.0
        
        requests.append((name, rounded, oh_qty, adu, unit_type))
    
    return requests

def format_location_order_message(requests: List[Tuple[str, float, float, float, str]], run_weekday: int, request_date: date, location: str) -> str:
    """Format order message for specific location."""
    window = REQUEST_WINDOWS[run_weekday]
    team_name = f"{location} Prep Team"
    
    header = f"🤖 COMMAND EXECUTED: /order - {location.upper()}\n📅 {request_date.strftime('%Y-%m-%d %H:%M:%S')}\n\n"
    header += f"<b>📋 ORDER REQUEST - {location.upper()}</b>\n"
    header += f"📅 {request_date.strftime('%a %b %d, %Y')}\n\n"
    header += f"Hey {team_name}! This is what we need for <b>{window['label']}</b>.\n"
    header += f"Please confirm at your earliest convenience:\n"
    
    # Filter items that need ordering
    needed_items = [(name, req_qty, unit_type) for name, req_qty, _, _, unit_type in requests if req_qty > 0]
    
    if not needed_items:
        return header + f"\n✅ <b>NO ORDERS NEEDED</b>\nAll {location} items are fully stocked!"
    
    lines = [header]
    for name, req_qty, unit_type in needed_items:
        qty_display = format_unit_display(req_qty, unit_type)
        lines.append(f"• <b>{name}</b>: {qty_display}")
    
    lines.append(f"\nTotal items: <b>{len(needed_items)}</b>")
    
    return "\n".join(lines)

def format_location_info_message(requests: List[Tuple[str, float, float, float, str]], run_weekday: int, request_date: date, location: str) -> str:
    """Format detailed info message for specific location."""
    window = REQUEST_WINDOWS[run_weekday]
    
    header = f"🤖 COMMAND EXECUTED: /info - {location.upper()}\n📅 {request_date.strftime('%Y-%m-%d %H:%M:%S')}\n\n"
    header += f"<b>🪣 AUTO-REQUEST ANALYSIS - {location.upper()}</b>\n"
    header += f"📅 {request_date.strftime('%a %b %d, %Y')}\n"
    header += f"🚚 For: <b>{window['label']}</b>\n"
    header += f"📊 Coverage: {window['total_days'] + DEFAULT_BUFFER_DAYS:.1f} days\n"
    header += f"📋 Based on most recent inventory count\n\n"
    
    if not requests:
        return header + f"⚠️ No data available for {location}"
    
    # Filter and categorize items
    needed_items = [(name, req_qty, oh_qty, adu, unit_type) for name, req_qty, oh_qty, adu, unit_type in requests if req_qty > 0]
    good_items = [(name, req_qty, oh_qty, adu, unit_type) for name, req_qty, oh_qty, adu, unit_type in requests if req_qty == 0]
    
    lines = [header]
    
    if needed_items:
        lines.append("🔴 <b>ITEMS TO ORDER:</b>")
        for name, req_qty, oh_qty, adu, unit_type in needed_items:
            qty_display = format_unit_display(req_qty, unit_type)
            on_hand_display = format_unit_display(oh_qty, unit_type)
            lines.append(f"• <b>{name}</b>: {qty_display}")
            lines.append(f"  📦 On-hand: {on_hand_display} | ADU: {adu}")
        lines.append("")
    
    if good_items:
        lines.append("✅ <b>ITEMS WELL STOCKED:</b>")
        for name, _, oh_qty, adu, unit_type in good_items:
            on_hand_display = format_unit_display(oh_qty, unit_type)
            lines.append(f"• <b>{name}</b>: {on_hand_display} (ADU: {adu})")
    
    lines.append(f"\n📊 <b>SUMMARY:</b>")
    lines.append(f"🔴 Need to order: <b>{len(needed_items)}</b> items")
    lines.append(f"✅ Well stocked: <b>{len(good_items)}</b> items")
    lines.append(f"📦 Total tracked: <b>{len(requests)}</b> items")
    
    return "\n".join(lines)

def execute_reassurance_command(chat_id: int):
    """Execute reassurance message with location-aware calculations."""
    try:
        with get_db_connection() as conn:
            d = today_local()
            
            # Get status for both locations
            avondale_status = get_item_status_for_date_location(conn, d, 'Avondale')
            commissary_status = get_item_status_for_date_location(conn, d, 'Commissary')
            
            # Check if we have any data for today
            avondale_data = sum(1 for item in avondale_status if item.qty is not None)
            commissary_data = sum(1 for item in commissary_status if item.qty is not None)
            
            if avondale_data == 0 and commissary_data == 0:
                no_data_msg = f"""📊 <b>DAILY REASSURANCE</b>
🤖 Command executed: /reassurance
📅 {d.strftime('%a %b %d, %Y')} • {now_local().strftime('%H:%M')}

⚠️ <b>No inventory data for today</b>

To get reassurance status, you need today's inventory counts.

<b>Next steps:</b>
1️⃣ Use /entry to add today's inventory
2️⃣ Or add counts via the web app
3️⃣ Then try /reassurance again

<b>What this command does:</b>
• Shows current inventory health
• Identifies critical items
• Lists items on watch list
• Confirms if all systems are green"""
                
                tg_send_with_retry(chat_id, no_data_msg)
                return
            
            # Send separate messages for each location if they have data
            command_header = f"🤖 COMMAND EXECUTED: /reassurance\n📅 {now_local().strftime('%Y-%m-%d %H:%M:%S')}\n\n"
            
            if avondale_data > 0:
                avondale_message = format_reassurance_message_location(avondale_status, d, 'Avondale')
                full_message = command_header + avondale_message
                tg_send_with_retry(chat_id, full_message)
                
                if commissary_data > 0:
                    time_module.sleep(1)  # Brief delay between messages
            
            if commissary_data > 0:
                commissary_message = format_reassurance_message_location(commissary_status, d, 'Commissary')
                if avondale_data == 0:  # Only add header if first message
                    full_message = command_header + commissary_message
                else:
                    full_message = commissary_message
                tg_send_with_retry(chat_id, full_message)
            
            LOG.info(f"Reassurance message sent via command to {chat_id}")
            
    except Exception as e:
        LOG.error(f"Failed to execute reassurance command: {e}")
        error_msg = f"""❌ <b>REASSURANCE CHECK FAILED</b>

An error occurred while checking inventory status:
<code>{str(e)}</code>

<b>Possible causes:</b>
• Database connection issue
• System configuration problem

<b>Try:</b>
1️⃣ Try again in a few moments
2️⃣ Check system health in web app
3️⃣ Contact support if problem persists"""
        tg_send_with_retry(chat_id, error_msg)

def execute_missing_counts_command(chat_id: int):
    """Execute missing counts check with location support."""
    try:
        with get_db_connection() as conn:
            d = today_local()
            
            # Check both locations
            avondale_missing = []
            commissary_missing = []
            
            # Check Avondale items
            for name in AVONDALE_ITEMS.keys():
                exists = conn.execute(
                    "SELECT 1 FROM nightly_on_hand WHERE item_id = (SELECT id FROM items WHERE name = ?) AND d = ? AND location = 'Avondale'",
                    (name, d.isoformat()),
                ).fetchone()
                if not exists:
                    avondale_missing.append(name)
            
            # Check Commissary items
            for name in COMMISSARY_ITEMS.keys():
                exists = conn.execute(
                    "SELECT 1 FROM nightly_on_hand WHERE item_id = (SELECT id FROM items WHERE name = ?) AND d = ? AND location = 'Commissary'",
                    (name, d.isoformat()),
                ).fetchone()
                if not exists:
                    commissary_missing.append(name)
            
            # Format message
            command_header = f"🤖 COMMAND EXECUTED: /missing\n📅 {now_local().strftime('%Y-%m-%d %H:%M:%S')}\n\n"
            
            if not avondale_missing and not commissary_missing:
                message = f"✅ <b>ALL COUNTS PRESENT</b>\n📅 {d.strftime('%a %b %d, %Y')}\n\nNo missing inventory counts detected for either location.\n\n🎉 Great job keeping data complete!"
            else:
                style = MessageStyle()
                lines = []
                
                if avondale_missing:
                    lines.append(f"<b>{style.WARNING} AVONDALE - MISSING COUNTS ({len(avondale_missing)} items):</b>")
                    lines.extend(f"• {name}" for name in avondale_missing)
                    lines.append("")
                
                if commissary_missing:
                    lines.append(f"<b>{style.WARNING} COMMISSARY - MISSING COUNTS ({len(commissary_missing)} items):</b>")
                    lines.extend(f"• {name}" for name in commissary_missing)
                    lines.append("")
                
                lines.append(f"<b>Next steps:</b>")
                lines.append(f"1️⃣ Use /entry to add missing counts")
                lines.append(f"2️⃣ Or add via web app")
                lines.append(f"3️⃣ Complete counts for accurate reporting")
                
                message = "\n".join(lines)
            
            full_message = command_header + message
            tg_send_with_retry(chat_id, full_message)
            LOG.info(f"Missing counts check sent via command to {chat_id}")
            
    except Exception as e:
        LOG.error(f"Failed to execute missing counts command: {e}")
        error_msg = f"""❌ <b>MISSING COUNTS CHECK FAILED</b>

An error occurred while checking for missing counts:
<code>{str(e)}</code>

<b>Try:</b>
1️⃣ Try again in a few moments
2️⃣ Check system health in web app
3️⃣ Contact support if problem persists"""
        tg_send_with_retry(chat_id, error_msg)

# ========================== MAIN APPLICATION ==========================

def main():
    """Main application entry point - FIXED for production deployment."""
    try:
        LOG.info("Starting K2 Inventory App...")
        
        # Initialize database
        init_db()
        
        # Only start scheduler in local development
        USE_SCHEDULER = os.getenv('USE_SCHEDULER', 'false').lower() == 'true'
        
        if USE_SCHEDULER:
            start_scheduler()
            LOG.info("Scheduler started for local development")
        else:
            LOG.info("Scheduler disabled for production deployment")
            # Start Telegram command handler instead
            start_telegram_command_handler()
        
        # FIXED: Call the correct function name
        page_entry()  # Changed from ui_entry_form() to page_entry()
        
    except Exception as e:
        LOG.exception("Application startup failed: %s", e)
        st.error(f"🚨 System startup failed: {str(e)}")
        st.stop()

if __name__ == "__main__":
    main()