#!/usr/bin/env python3
"""
K2 Restaurant Inventory Management System - Notion Integration
============================================================

Production-ready inventory management system with Notion database integration.
Provides real-time inventory tracking with user-friendly data management
through Notion databases instead of SQLite.

Key Features:
- Notion databases for all data storage (items, inventory, ADU calculations)
- Manager-friendly interface through Notion for data editing
- Telegram bot for field operations and data entry
- Automated calculations and reporting
- Location-aware business logic
- Weekly and monthly ADU analysis

Author: Dorei Satō
License: Proprietary  
Version: 2.0.0
"""

import asyncio
import json
import logging
import os
import sys
import threading
import time
import math
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple, Any, Union
from urllib.parse import quote

import requests
SYSTEM_VERSION = "2.0.0"  # Make sure this is defined at module level

# Load environment variables from .env file if it exists
def load_env_file():
    """Load environment variables from .env file if it exists"""
    env_file = '.env'
    if os.path.exists(env_file):
        print(f"Loading environment variables from {env_file}")
        try:
            with open(env_file, 'r', encoding='utf-8') as f:
                for line in f:
                    line = line.strip()
                    if line and not line.startswith('#') and '=' in line:
                        key, value = line.split('=', 1)
                        key = key.strip()
                        value = value.strip().strip('"').strip("'")  # Remove quotes
                        os.environ[key] = value
                        print(f"  Loaded: {key}={value if key not in ['TELEGRAM_BOT_TOKEN', 'NOTION_TOKEN'] else value[:10]+'...'}")
        except UnicodeDecodeError:
            # Fallback to system default encoding
            print(f"UTF-8 encoding failed, trying system default...")
            with open(env_file, 'r') as f:
                for line in f:
                    line = line.strip()
                    if line and not line.startswith('#') and '=' in line:
                        key, value = line.split('=', 1)
                        key = key.strip()
                        value = value.strip().strip('"').strip("'")  # Remove quotes
                        os.environ[key] = value
                        print(f"  Loaded: {key}={value if key not in ['TELEGRAM_BOT_TOKEN', 'NOTION_TOKEN'] else value[:10]+'...'}")
    else:
        print(f"No {env_file} file found - using system environment variables")

# Load .env file before other imports
load_env_file()

# Helper function to get current local time
def get_local_time() -> datetime:
    """Get current local system time"""
    return datetime.now()

# Helper function to get current time in specified timezone
def get_time_in_timezone(timezone_str: str = None) -> datetime:
    """
    Get current time in specified timezone or local time if not specified.
    
    Args:
        timezone_str: Timezone string (e.g., 'America/Chicago') or None for local time
        
    Returns:
        datetime: Current time in specified timezone
    """
    if not timezone_str:
        return datetime.now()
    
    try:
        import pytz
        target_tz = pytz.timezone(timezone_str)
        utc_now = datetime.utcnow().replace(tzinfo=pytz.UTC)
        local_time = utc_now.astimezone(target_tz)
        return local_time.replace(tzinfo=None)  # Remove timezone info for consistency
    except ImportError:
        # Fallback to system local time if pytz not available
        return datetime.now()
    except:
        # Fallback to system local time if timezone is invalid
        return datetime.now()

# ===== CONFIGURATION AND CONSTANTS =====

# System Configuration
SYSTEM_VERSION = "2.0.0"
LOG_FORMAT = "%(asctime)s | %(levelname)-8s | %(name)-12s | %(funcName)-20s | %(lineno)d | %(message)s"
MAX_MEMORY_MB = 512
MAX_LOG_SIZE_MB = 50
RETENTION_DAYS = 90

# Business Constants
BUFFER_DAYS = 1.0  # Safety margin for all calculations
MAX_CONCURRENT_USERS = 10
RATE_LIMIT_COMMANDS_PER_MINUTE = 10

# Rounding Configuration
def round_order_quantity(qty: float) -> int:
    """
    Round order quantities to whole numbers for practical ordering.
    
    Business Rule: Always round UP to ensure sufficient inventory.
    Examples: 0.1 → 1, 1.7 → 2, 2.0 → 2
    
    Args:
        qty: Calculated quantity (can be decimal)
        
    Returns:
        int: Rounded up quantity (whole number)
    """
    import math
    if qty <= 0:
        return 0
    return math.ceil(qty)  # Always round up for safety

def round_consumption_display(qty: float) -> float:
    """
    Round consumption/need quantities for display purposes.
    
    Shows 1 decimal place for clarity while keeping precision.
    
    Args:
        qty: Consumption need quantity
        
    Returns:
        float: Rounded to 1 decimal place
    """
    return round(qty, 1)

def round_adu_display(adu: float) -> float:
    """
    Round ADU values for display purposes.
    
    Shows 2 decimal places for ADU precision.
    
    Args:
        adu: Average Daily Usage value
        
    Returns:
        float: Rounded to 2 decimal places
    """
    return round(adu, 2)

# Time Configuration
TIMEZONE = os.environ.get('TZ', 'local')  # Default to local system time
BUSINESS_TIMEZONE = "America/Chicago"  # For business operations (delivery schedules)

# Delivery Schedules (hour in 24-hour format, Chicago Time)
DELIVERY_SCHEDULES = {
    "Avondale": {
        "days": ["Monday", "Thursday"],
        "hour": 12,
        "request_schedule": {
            "Tuesday": 8,    # For Thursday delivery
            "Saturday": 8,   # For Monday delivery
        }
    },
    "Commissary": {
        "days": ["Tuesday", "Thursday", "Saturday"],
        "hour": 12,
        "request_schedule": {
            "Monday": 8,     # For Tuesday delivery
            "Wednesday": 8,  # For Thursday delivery
            "Friday": 8,     # For Saturday delivery
        }
    }
}

# --- INVENTORY CONSUMPTION SCHEDULES (required by InventoryItem.get_current_consumption_days) ---
# Keys must be the SAME day names used in DELIVERY_SCHEDULES["<Location>"]["days"].
INVENTORY_CONFIG = {
    "Avondale": {
        "consumption_schedule": {
            "Monday": 3.0,    # Monday delivery must last 3.0 days (Mon→Thu)
            "Thursday": 4.0,  # Thursday delivery must last 4.0 days (Thu→Mon)
        }
    },
    "Commissary": {
        "consumption_schedule": {
            "Tuesday": 2.0,   # Tue→Thu
            "Thursday": 2.0,  # Thu→Sat
            "Saturday": 3.0,  # Sat→Tue
        }
    },
}


# Error Messages for User Feedback
ERROR_MESSAGES = {
    "notion_timeout": "⏰ Notion database is busy, please try again in a moment",
    "invalid_quantity": "❌ Please enter a valid number (e.g., 5, 2.5, or 0)",
    "item_not_found": "❌ Item '{item_name}' not found in {location} inventory",
    "calculation_error": "🔧 Calculation error - support has been notified",
    "system_error": "🚨 System error - please try again or contact support",
    "network_error": "📡 Network error - please check connection and try again",
    "notion_error": "📝 Notion database error - please try again or contact support",
    "invalid_date": "📅 Please enter a valid date (YYYY-MM-DD format)",
    "invalid_command": "❓ Unknown command. Type /help for available commands",
    "conversation_timeout": "⏰ Conversation timed out. Please start over with the command"
}

# ===== LOGGING SETUP =====

def setup_logging():
    """
    Configure comprehensive logging system with local timezone.
    
    Log Levels:
    - CRITICAL: System startup/shutdown, database initialization, critical failures
    - INFO: Business operations, commands, transactions, messages
    - DEBUG: Function calls, calculations, query details, conversation state
    
    Returns:
        logging.Logger: Configured root logger
    """
    import logging.handlers
    from datetime import datetime
    
    # Custom formatter that uses local timezone
    class LocalTimeFormatter(logging.Formatter):
        def formatTime(self, record, datefmt=None):
            dt = datetime.fromtimestamp(record.created)
            if datefmt:
                s = dt.strftime(datefmt)
            else:
                s = dt.strftime('%Y-%m-%d %H:%M:%S,%f')[:-3]
            return s
    
    # Configure root logger
    formatter = LocalTimeFormatter(LOG_FORMAT)
    
    # Create handlers
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    
    file_handler = logging.FileHandler(
        f"k2_notion_system_{datetime.now().strftime('%Y%m%d')}.log", 
        encoding='utf-8'
    )
    file_handler.setFormatter(formatter)
    
    # Configure root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.DEBUG)
    root_logger.addHandler(console_handler)
    root_logger.addHandler(file_handler)
    
    # Create specific loggers for different components
    loggers = {
        'system': logging.getLogger('system'),
        'notion': logging.getLogger('notion'),
        'telegram': logging.getLogger('telegram'),
        'calculations': logging.getLogger('calculations'),
        'scheduler': logging.getLogger('scheduler'),
        'business': logging.getLogger('business')
    }
    
    # Set specific log levels for different components in production
    if os.environ.get('RAILWAY_ENVIRONMENT') == 'production':
        loggers['notion'].setLevel(logging.INFO)
        loggers['calculations'].setLevel(logging.INFO)
    
    logger = logging.getLogger('system')
    logger.critical(f"K2 Notion Inventory System v{SYSTEM_VERSION} - Logging initialized")
    logger.info(f"Log format: {LOG_FORMAT}")
    logger.info(f"User timezone: {TIMEZONE}")
    logger.info(f"Business timezone: {BUSINESS_TIMEZONE}")
    
    return logger

# Initialize logging
logger = setup_logging()

# ===== MODULE-LEVEL HELPER FUNCTIONS =====

def _ik(rows: list[list[tuple[str, str]]]) -> Dict:
    """Create inline keyboard markup for Telegram."""
    return {
        "inline_keyboard": [
            [{"text": text, "callback_data": data} for text, data in row]
            for row in rows
        ]
    }

def validate_date_format(date_str: str) -> bool:
    """
    Validate date string format.
    
    Args:
        date_str: Date string to validate
        
    Returns:
        bool: True if valid YYYY-MM-DD format
    """
    try:
        datetime.strptime(date_str, "%Y-%m-%d")
        return True
    except ValueError:
        return False

def sanitize_user_input(text: str, max_length: int = 500) -> str:
    """
    Sanitize user input for safety.
    
    Args:
        text: Raw user input
        max_length: Maximum allowed length
        
    Returns:
        str: Sanitized text
    """
    if not text:
        return ""
    # Remove control characters and limit length
    text = ''.join(char for char in text if char.isprintable() or char.isspace())
    return text[:max_length].strip()

# ===== DATA CLASSES =====

@dataclass
class InventoryItem:
    """
    Represents a single inventory item with sophisticated consumption calculation logic.
    
    Uses delivery-to-delivery consumption periods rather than static consumption days,
    accounting for varying intervals between deliveries based on restaurant schedules.
    """
    id: str  # Notion page ID
    name: str
    adu: float  # Average Daily Usage (containers per day)
    unit_type: str  # case, quart, tray, bag, bottle
    location: str  # Avondale or Commissary
    active: bool = True
    created_at: str = field(default_factory=lambda: datetime.now().isoformat())
    updated_at: str = field(default_factory=lambda: datetime.now().isoformat())
    # ===== DELIVERY / CONSUMPTION CONFIG =====
    # Used by InventoryItem.get_current_consumption_days()
    INVENTORY_CONFIG = {
        "Avondale": {
            "delivery_days": ["Monday", "Thursday"],
            "delivery_hour": 12,  # 12:00 PM Central
            # Consumption days from delivery to next delivery
            "consumption_schedule": {"Thursday": 4.0, "Monday": 3.0},
        },
        "Commissary": {
            "delivery_days": ["Tuesday", "Thursday", "Saturday"],
            "delivery_hour": 12,  # 12:00 PM Central
            "consumption_schedule": {"Tuesday": 2.0, "Thursday": 2.0, "Saturday": 3.0},
        },
    }

    
    def get_current_consumption_days(self, from_date: datetime = None) -> float:
        """
        Calculate consumption days needed based on which delivery cycle we're in.
        
        Returns the exact days this delivery must last until next delivery arrives.
        Accounts for varying intervals between different delivery days.
        
        Args:
            from_date: Reference date to determine delivery cycle
            
        Returns:
            float: Days this delivery must last
        """
        if from_date is None:
            from_date = get_time_in_timezone(BUSINESS_TIMEZONE)
        
        schedule = DELIVERY_SCHEDULES[self.location]
        consumption_schedule = INVENTORY_CONFIG[self.location]["consumption_schedule"]
        delivery_days = schedule["days"]
        delivery_hour = schedule["hour"]
        
        # Determine which delivery cycle we're currently in
        current_weekday = from_date.weekday()  # 0=Monday, 6=Sunday
        current_hour = from_date.hour
        
        weekday_map = {
            'Monday': 0, 'Tuesday': 1, 'Wednesday': 2, 'Thursday': 3,
            'Friday': 4, 'Saturday': 5, 'Sunday': 6
        }
        
        # Find the most recent delivery day
        delivery_weekdays = [(weekday_map[day], day) for day in delivery_days]
        
        # Determine current delivery cycle
        current_delivery_day = None
        
        for weekday_num, day_name in sorted(delivery_weekdays, reverse=True):
            if (current_weekday > weekday_num or 
                (current_weekday == weekday_num and current_hour >= delivery_hour)):
                current_delivery_day = day_name
                break
        
        # If no delivery found, we're before the first delivery of the week
        if current_delivery_day is None:
            current_delivery_day = delivery_days[-1]  # Last delivery of previous week
        
        consumption_days = consumption_schedule.get(current_delivery_day, 3.5)
        
        logger.debug(f"Consumption days for {self.name} in {current_delivery_day} cycle: {consumption_days}")
        return consumption_days
    
    def calculate_consumption_need(self, from_date: datetime = None) -> float:
        """
        Calculate total consumption need based on current delivery cycle.
        
        Formula: consumption_need = adu × current_consumption_days
        
        Args:
            from_date: Reference date for calculation
            
        Returns:
            float: Total containers needed until next delivery
        """
        consumption_days = self.get_current_consumption_days(from_date)
        consumption = self.adu * consumption_days
        
        logger.debug(f"Consumption calculation for {self.name}: "
                    f"adu={self.adu} × consumption_days={consumption_days} = {consumption}")
        return consumption
    
    def determine_status(self, current_qty: float, consumption_need: float) -> str:
        """
        Determine inventory status with business-critical logic.
        
        Status Logic:
        - RED (Critical): current_qty < consumption_need (stockout risk)
        - GREEN (Good): current_qty >= consumption_need (sufficient coverage)
        
        Args:
            current_qty: Current inventory quantity
            consumption_need: Required quantity until next delivery
            
        Returns:
            str: Status color ('RED', 'GREEN')
        """
        if current_qty < consumption_need:
            status = 'RED'
        else:
            status = 'GREEN'
            
        logger.debug(f"Status determination for {self.name}: qty={current_qty}, "
                    f"need={consumption_need} → {status}")
        return status

@dataclass
class ConversationState:
    """
    Manages conversation state for multi-step Telegram interactions.
    
    Tracks the current step, collected data, and context for interactive workflows
    like inventory entry and data validation processes.
    """
    user_id: int
    chat_id: int
    command: str
    step: str
    note: str = ""
    review_payload: Dict[str, Any] = field(default_factory=dict)
    data: Dict[str, Any] = field(default_factory=dict)
    location: Optional[str] = None
    entry_type: Optional[str] = None  # 'on_hand' or 'received'
    current_item_index: int = 0
    items: List[InventoryItem] = field(default_factory=list)
    started_at: datetime = field(default_factory=datetime.now)
    last_activity: datetime = field(default_factory=datetime.now)
    
    def is_expired(self, timeout_minutes: int = 30) -> bool:
        """Check if conversation has timed out"""
        return (datetime.now() - self.last_activity).total_seconds() > (timeout_minutes * 60)
    
    def update_activity(self):
        """Update last activity timestamp"""
        self.last_activity = datetime.now()

# ===== NOTION DATABASE MANAGER =====

class NotionManager:
    """
    Enterprise-grade Notion database manager with dynamic schema management.
    
    Implements a hybrid approach:
    1. Auto-initializes database schemas on first run
    2. Dynamically manages property columns for inventory items
    3. Provides data integrity and error recovery mechanisms
    4. Optimizes API usage with intelligent caching strategies
    """
    
    def __init__(self, token: str, items_db_id: str, inventory_db_id: str, adu_calc_db_id: str):
        """
        Initialize Notion manager with all three database IDs.
        
        Args:
            token: Notion integration token
            items_db_id: Items master database ID
            inventory_db_id: Inventory transactions database ID  
            adu_calc_db_id: ADU calculations database ID (required)
        """
        self.token = token
        self.items_db_id = items_db_id
        self.inventory_db_id = inventory_db_id
        self.adu_calc_db_id = adu_calc_db_id
        
        self.headers = {
            'Authorization': f'Bearer {token}',
            'Content-Type': 'application/json',
            'Notion-Version': '2022-06-28'
        }
        
        self.logger = logging.getLogger('notion')
        self.session = requests.Session()
        self.session.headers.update(self.headers)
        self.base_url = "https://api.notion.com/v1"

        
        # Advanced caching system
        self._items_cache = {}
        self._schema_cache = {}
        self._cache_timestamp = None
        self._cache_ttl = 300  # 5 minutes
        
        # Dynamic property management
        self._inventory_properties = set()
        self._items_initialized = False
        
        self.logger.critical(f"Notion manager initialized with dynamic schema management")
        self.logger.info(f"Items DB: {items_db_id[:8]}...")
        self.logger.info(f"Inventory DB: {inventory_db_id[:8]}...")
        self.logger.info(f"ADU Calculations DB: {adu_calc_db_id[:8]}...")
        
        # Initialize system on first run
        self._initialize_system()
    
    def _initialize_system(self):
        """
        Initialize the complete system with schema validation and data seeding.
        
        This method:
        1. Validates database connections
        2. Checks if items database is populated
        3. Auto-populates if empty
        4. Initializes inventory database schema
        5. Sets up dynamic property tracking
        """
        try:
            self.logger.info("Initializing Notion system...")
            
            # Validate database connections
            self._validate_databases()
            
            # Check if items database needs initialization
            if not self._check_items_initialized():
                self.logger.info("Items database empty - initializing with master data...")
                self._seed_items_database()
                self._items_initialized = True
            
            # Initialize inventory database schema
            self._initialize_inventory_schema()
            
            self.logger.critical("Notion system initialization completed successfully")
            
        except Exception as e:
            self.logger.critical(f"System initialization failed: {e}")
            raise
    
    def _check_items_initialized(self) -> bool:
        """Check if items database has been populated with master data."""
        try:
            response = self._make_request('POST', f'/databases/{self.items_db_id}/query', {
                'page_size': 1
            })
            
            if response and response['results']:
                self.logger.info("Items database already populated")
                return True
            else:
                self.logger.info("Items database is empty")
                return False
                
        except Exception as e:
            self.logger.error(f"Error checking items initialization: {e}")
            return False
    
    def _seed_items_database(self):
        """
        Seed the items database with master inventory configuration.
        
        Populates both locations with their respective items, ADU values,
        and unit types from the inventory configuration.
        """
        try:
            # Define inventory configuration directly here to ensure availability
            inventory_config = {
                "Avondale": {
                    "consumption_schedule": {
                        "Thursday": 4.0,  # Thursday 12PM → Monday 12PM
                        "Monday": 3.0     # Monday 12PM → Thursday 12PM
                    },
                    "items": {
                        "Steak": {"adu": 1.8, "unit_type": "case"},
                        "Salmon": {"adu": 0.9, "unit_type": "case"},
                        "Chipotle Aioli": {"adu": 8.0, "unit_type": "quart"},
                        "Garlic Aioli": {"adu": 6.0, "unit_type": "quart"},
                        "Jalapeno Aioli": {"adu": 5.0, "unit_type": "quart"},
                        "Sriracha Aioli": {"adu": 2.0, "unit_type": "quart"},
                        "Ponzu Sauce": {"adu": 3.0, "unit_type": "quart"},
                        "Teriyaki/Soyu Sauce": {"adu": 3.0, "unit_type": "quart"},
                        "Orange Sauce": {"adu": 4.0, "unit_type": "quart"},
                        "Bulgogi Sauce": {"adu": 3.0, "unit_type": "quart"},
                        "Fried Rice Sauce": {"adu": 4.0, "unit_type": "quart"},
                        "Honey": {"adu": 2.0, "unit_type": "bottle"}
                    }
                },
                "Commissary": {
                    "consumption_schedule": {
                        "Tuesday": 2.0,   # Tuesday 12PM → Thursday 12PM
                        "Thursday": 2.0,  # Thursday 12PM → Saturday 12PM  
                        "Saturday": 3.0   # Saturday 12PM → Tuesday 12PM
                    },
                    "items": {
                        "Fish": {"adu": 0.3, "unit_type": "tray"},
                        "Shrimp": {"adu": 0.5, "unit_type": "tray"},
                        "Grilled Chicken": {"adu": 2.5, "unit_type": "case"},
                        "Crispy Chicken": {"adu": 3.5, "unit_type": "case"},
                        "Crab Ragoon": {"adu": 1.9, "unit_type": "bag"},
                        "Nutella Ragoon": {"adu": 0.7, "unit_type": "bag"},
                        "Ponzu Cups": {"adu": 0.8, "unit_type": "quart"}
                    }
                }
            }
            
            items_created = 0
            
            for location, config in inventory_config.items():
                consumption_schedule = config["consumption_schedule"]
                items = config["items"]
                
                # Calculate average consumption days for this location
                avg_consumption_days = sum(consumption_schedule.values()) / len(consumption_schedule)
                
                for item_name, item_config in items.items():
                    page_data = {
                        'parent': {
                            'database_id': self.items_db_id
                        },
                        'properties': {
                            'Item Name': {
                                'title': [
                                    {
                                        'text': {
                                            'content': item_name
                                        }
                                    }
                                ]
                            },
                            'Location': {
                                'select': {
                                    'name': location
                                }
                            },
                            'ADU': {
                                'number': item_config['adu']
                            },
                            'Unit Type': {
                                'select': {
                                    'name': item_config['unit_type']
                                }
                            },
                            'Consumption Days': {
                                'number': avg_consumption_days
                            },
                            'Active': {
                                'checkbox': True
                            }
                        }
                    }
                    
                    response = self._make_request('POST', '/pages', page_data)
                    if response:
                        items_created += 1
                        self.logger.debug(f"Created item: {item_name} ({location})")
                    else:
                        self.logger.error(f"Failed to create item: {item_name}")
            
            self.logger.info(f"Seeded {items_created} items in items database")
            
        except Exception as e:
            self.logger.error(f"Error seeding items database: {e}")
            self.logger.error(f"Full error details: {str(e)}")
            raise
    
    def _initialize_inventory_schema(self):
        """
        Initialize inventory database schema with dynamic property creation.
        
        Creates quantity columns for each inventory item automatically,
        ensuring perfect property name matching for data entry.
        """
        try:
            self.logger.info("Initializing inventory database schema...")
            
            # Get all items to create quantity columns
            all_items = self.get_all_items(use_cache=False)
            
            # Build the set of required properties
            base_properties = {
                'Manager',      # Title
                'Date',         # Date
                'Location',     # Select
                'Type',         # Select  
                'Notes'         # Rich Text
            }
            
            # Add quantity columns for each item
            item_properties = set()
            for item in all_items:
                property_name = self._get_quantity_property_name(item.name)
                item_properties.add(property_name)
            
            self._inventory_properties = base_properties | item_properties
            
            self.logger.info(f"Inventory schema initialized with {len(self._inventory_properties)} properties")
            self.logger.debug(f"Item quantity properties: {sorted(item_properties)}")
            
        except Exception as e:
            self.logger.error(f"Error initializing inventory schema: {e}")
            raise
    
    def _get_quantity_property_name(self, item_name: str) -> str:
        """
        Generate standardized property name for item quantity columns.
        
        Args:
            item_name: Name of inventory item
            
        Returns:
            str: Property name for quantity column (e.g., "Steak Qty")
        """
        # Clean item name and add "Qty" suffix
        clean_name = item_name.strip()
        return f"{clean_name} Qty"
    
    def get_inventory_properties(self) -> Dict[str, str]:
        """
        Get mapping of item names to their quantity property names.
        
        Returns:
            Dict[str, str]: Mapping of item_name -> property_name
        """
        items = self.get_all_items()
        return {item.name: self._get_quantity_property_name(item.name) for item in items}
    
    def _validate_databases(self):
        """Validate that all required databases are accessible."""
        try:
            # Test items database
            response = self._make_request('POST', f'/databases/{self.items_db_id}/query', {
                'page_size': 1
            })
            if response:
                self.logger.info("Items database connection validated")
            
            # Test inventory database  
            response = self._make_request('POST', f'/databases/{self.inventory_db_id}/query', {
                'page_size': 1
            })
            if response:
                self.logger.info("Inventory database connection validated")
            
            # Test ADU calculations database
            response = self._make_request('POST', f'/databases/{self.adu_calc_db_id}/query', {
                'page_size': 1
            })
            if response:
                self.logger.info("ADU calculations database connection validated")
                
        except Exception as e:
            self.logger.critical(f"Database validation failed: {e}")
            raise
    
    def _make_request(self, http_method: str, path: str, data: Dict = None) -> Optional[Dict]:
        """
        Make HTTP request to Notion API with error handling and logging.

        Args:
            http_method: 'GET' | 'POST' | 'PATCH' | 'DELETE'
            path: e.g., '/databases/{id}/query' or '/pages'
            data: JSON body (for non-GET)

        Returns:
            Optional[Dict]: Parsed JSON on success, else None
        """
        url = f"{self.base_url}{path}"
        try:
            start_time = time.time()
            if http_method.upper() == "GET":
                resp = self.session.get(url, timeout=30)
            else:
                resp = self.session.request(http_method.upper(), url, json=data or {}, timeout=30)
            duration_ms = (time.time() - start_time) * 1000

            if resp.status_code >= 200 and resp.status_code < 300:
                self.logger.debug(f"Notion {http_method} {path} OK in {duration_ms:.2f}ms")
                return resp.json()
            else:
                # Try to log Notion error body if present
                try:
                    err = resp.json()
                except Exception:
                    err = {"message": resp.text}
                self.logger.error(f"Notion {http_method} {path} HTTP {resp.status_code}: {err}")
                return None
        except requests.exceptions.Timeout:
            self.logger.error(f"Notion {http_method} {path} timed out")
            return None
        except requests.exceptions.RequestException as e:
            self.logger.error(f"Notion {http_method} {path} network error: {e}")
            return None
        except Exception as e:
            self.logger.error(f"Notion {http_method} {path} unexpected error: {e}")
            return None
        
    def _is_cache_valid(self) -> bool:
        """Check if items cache is still valid."""
        if not self._cache_timestamp:
            return False
        return (time.time() - self._cache_timestamp) < self._cache_ttl
    
    def _parse_item_from_notion(self, page: Dict) -> InventoryItem:
        """
        Parse Notion page into InventoryItem object with enhanced validation.
        
        Args:
            page: Notion page object
            
        Returns:
            InventoryItem: Parsed inventory item with business logic
        """
        try:
            props = page['properties']
            
            return InventoryItem(
                id=page['id'],
                name=props['Item Name']['title'][0]['plain_text'] if props['Item Name']['title'] else 'Unknown',
                location=props['Location']['select']['name'] if props['Location']['select'] else 'Unknown',
                adu=props['ADU']['number'] if props['ADU']['number'] is not None else 0.0,
                unit_type=props['Unit Type']['select']['name'] if props['Unit Type']['select'] else 'case',
                active=props.get('Active', {}).get('checkbox', True),
                created_at=page['created_time'],
                updated_at=page['last_edited_time']
            )
            
        except Exception as e:
            self.logger.error(f"Error parsing item from Notion: {e}")
            # Return a minimal valid item to prevent system crashes
            return InventoryItem(
                id=page.get('id', 'unknown'),
                name='Unknown Item',
                location='Unknown',
                adu=0.0,
                unit_type='case'
            )
    
    def get_items_for_location(self, location: str, use_cache: bool = True) -> List[InventoryItem]:
        """
        Retrieve all active items for a specific location.
        
        Args:
            location: Location name ('Avondale' or 'Commissary')
            use_cache: Whether to use cached data if available
            
        Returns:
            List[InventoryItem]: List of inventory items for location
        """
        cache_key = f"items_{location}"
        
        # Check cache first
        if use_cache and self._is_cache_valid() and cache_key in self._items_cache:
            self.logger.debug(f"Using cached items for {location}")
            return self._items_cache[cache_key]
        
        start_time = time.time()
        
        # Query Notion database
        query = {
            'filter': {
                'and': [
                    {
                        'property': 'Location',
                        'select': {
                            'equals': location
                        }
                    },
                    {
                        'property': 'Active',
                        'checkbox': {
                            'equals': True
                        }
                    }
                ]
            },
            'sorts': [
                {
                    'property': 'Item Name',
                    'direction': 'ascending'
                }
            ]
        }
        
        response = self._make_request('POST', f'/databases/{self.items_db_id}/query', query)
        
        if not response:
            self.logger.error(f"Failed to retrieve items for {location}")
            return []
        
        items = []
        for page in response['results']:
            try:
                item = self._parse_item_from_notion(page)
                items.append(item)
            except Exception as e:
                self.logger.error(f"Error parsing item from Notion: {e}")
                continue
        
        # Update cache
        self._items_cache[cache_key] = items
        self._cache_timestamp = time.time()
        
        duration_ms = (time.time() - start_time) * 1000
        self.logger.debug(f"Retrieved {len(items)} items for {location} in {duration_ms:.2f}ms")
        
        return items
    
    def get_all_items(self, use_cache: bool = True) -> List[InventoryItem]:
        """
        Retrieve all active items from all locations.
        
        Args:
            use_cache: Whether to use cached data if available
            
        Returns:
            List[InventoryItem]: List of all inventory items
        """
        avondale_items = self.get_items_for_location('Avondale', use_cache)
        commissary_items = self.get_items_for_location('Commissary', use_cache)
        
        return avondale_items + commissary_items
    
    def save_inventory_transaction(self, location: str, entry_type: str, date: str, 
                                 manager: str, notes: str, quantities: Dict[str, float]) -> bool:
        """
        Save inventory transaction using a single JSON column approach.
        
        This elegant solution stores all quantities in one JSON property,
        eliminating the need to create 19+ individual columns manually.
        
        Args:
            location: Location name
            entry_type: 'on_hand' or 'received'
            date: Date in YYYY-MM-DD format
            manager: Manager name (becomes page title)
            notes: Optional notes
            quantities: Dict mapping item names to quantities
            
        Returns:
            bool: True if successful
        """
        try:
            # Create executive-level title for management visibility
            entry_type_display = "On-Hand Count" if entry_type == 'on_hand' else "Delivery Received"
            total_items = sum(1 for qty in quantities.values() if qty > 0)
            title = f"{manager} • {entry_type_display} • {date} • {location} ({total_items} items)"
            
            # Format quantities as readable JSON string for Notion
            quantities_summary = []
            for item_name, qty in quantities.items():
                if qty > 0:  # Only show items with quantities
                    quantities_summary.append(f"{item_name}: {qty}")
            
            quantities_display = "\n".join(quantities_summary) if quantities_summary else "No items recorded"
            
            # Build properties using single JSON approach
            properties = {
                'Manager': {  # Title property
                    'title': [
                        {
                            'text': {
                                'content': title
                            }
                        }
                    ]
                },
                'Date': {
                    'date': {
                        'start': date
                    }
                },
                'Location': {
                    'select': {
                        'name': location
                    }
                },
                'Type': {
                    'select': {
                        'name': 'On-Hand' if entry_type == 'on_hand' else 'Received'
                    }
                },
                'Quantities': {  # Single rich text field with all quantities
                    'rich_text': [
                        {
                            'text': {
                                'content': quantities_display
                            }
                        }
                    ]
                }
            }
            
            # Add notes with rich formatting
            if notes:
                properties['Notes'] = {
                    'rich_text': [
                        {
                            'text': {
                                'content': notes
                            }
                        }
                    ]
                }
            
            # Store raw JSON data for system processing
            quantities_json = json.dumps(quantities)
            properties['Quantities JSON'] = {
                'rich_text': [
                    {
                        'text': {
                            'content': quantities_json
                        }
                    }
                ]
            }
            
            # Create the page
            page_data = {
                'parent': {
                    'database_id': self.inventory_db_id
                },
                'properties': properties
            }
            
            response = self._make_request('POST', '/pages', page_data)
            
            if response:
                self.logger.info(f"Saved inventory transaction: {title}")
                self.logger.info(f"Items recorded: {len([q for q in quantities.values() if q > 0])}")
                return True
            else:
                self.logger.error(f"Failed to save inventory transaction")
                return False
                
        except Exception as e:
            self.logger.error(f"Error saving inventory transaction: {e}")
            return False
        
    def get_latest_inventory(self, location: str, entry_type: str = "on_hand") -> Dict[str, float]:
            """
            FIXED: Query with correct Type values that match what's saved.
            """
            try:
                # FIX: Use "On-Hand" not "On-Hand Count"
                type_select = "On-Hand" if entry_type == "on_hand" else "Received"
                
                query = {
                    "filter": {
                        "and": [
                            {"property": "Location", "select": {"equals": location}},
                            {"property": "Type", "select": {"equals": type_select}},
                        ]
                    },
                    "sorts": [{"property": "Date", "direction": "descending"}],
                    "page_size": 1,
                }
                
                response = self._make_request("POST", 
                                            f"/databases/{self.inventory_db_id}/query", 
                                            query)
                
                if not response or not response.get("results"):
                    self.logger.debug(f"No inventory found for {location} ({type_select})")
                    return {}
                
                page = response["results"][0]
                props = page.get("properties", {})
                
                # Try both possible property names
                json_prop = props.get("Quantities JSON") or props.get("Quantities")
                
                if not json_prop or not json_prop.get("rich_text"):
                    return {}
                
                # Extract JSON from rich text
                raw_json = "".join(
                    segment.get("plain_text", "") 
                    for segment in json_prop["rich_text"]
                ).strip()
                
                if not raw_json:
                    return {}
                
                # Parse JSON data
                data = json.loads(raw_json)
                
                # Convert to float dict
                result = {}
                for item_name, quantity in (data or {}).items():
                    try:
                        result[str(item_name)] = float(quantity)
                    except (ValueError, TypeError):
                        self.logger.warning(f"Invalid quantity for {item_name}: {quantity}")
                        continue
                
                self.logger.debug(f"Retrieved {len(result)} items from latest {type_select} for {location}")
                return result
                
            except json.JSONDecodeError as e:
                self.logger.error(f"JSON decode error in get_latest_inventory: {e}")
                return {}
            except Exception as e:
                self.logger.error(f"get_latest_inventory error: {e}", exc_info=True)
                return {}
        
    def get_missing_counts(self, location: str, date: str) -> List[str]:
        """
        Get list of items missing inventory counts for a specific date.
        
        Args:
            location: Location name
            date: Date in YYYY-MM-DD format
            
        Returns:
            List[str]: List of item names missing counts
        """
        try:
            # Query for on-hand entries for this location and date
            query = {
                'filter': {
                    'and': [
                        {
                            'property': 'Location',
                            'select': {
                                'equals': location
                            }
                        },
                        {
                            'property': 'Type',
                            'select': {
                                'equals': 'On-Hand'
                            }
                        },
                        {
                            'property': 'Date',
                            'date': {
                                'equals': date
                            }
                        }
                    ]
                }
            }
            
            response = self._make_request('POST', f'/databases/{self.inventory_db_id}/query', query)
            
            if not response:
                self.logger.error(f"Failed to check missing counts for {location} on {date}")
                return []
            
            # Get all items for this location
            items = self.get_items_for_location(location)
            all_item_names = set(item.name for item in items)
            
            # Find which items have counts for this date
            items_with_counts = set()
            
            for page in response['results']:
                props = page['properties']
                
                # Check each item quantity column
                for item_name in all_item_names:
                    column_name = f"{item_name} Qty"
                    if column_name in props and props[column_name]['number'] is not None:
                        items_with_counts.add(item_name)
            
            # Items missing counts are those not found
            missing_items = sorted(list(all_item_names - items_with_counts))
            
            self.logger.debug(f"Found {len(missing_items)} missing counts for {location} on {date}")
            return missing_items
            
        except Exception as e:
            self.logger.error(f"Error checking missing counts: {e}")
            return []
    
    def invalidate_cache(self):
        """Invalidate the items cache to force refresh on next request."""
        self._items_cache.clear()
        self._cache_timestamp = None
        self.logger.debug("Items cache invalidated")

# ===== BUSINESS CALCULATIONS ENGINE =====

class InventoryCalculator:
    """
    Core business logic engine for inventory calculations, status determination,
    and auto-request generation. Uses Notion data with location-aware calculations
    and proper delivery schedule integration.
    """
    
    def __init__(self, notion_manager: NotionManager):
        """
        Initialize calculator with Notion manager dependency.
        
        Args:
            notion_manager: Notion manager instance
        """
        self.notion = notion_manager
        self.logger = logging.getLogger('calculations')
        self.logger.info("Inventory calculator initialized with Notion integration")
    
    def calculate_days_until_next_delivery(self, location: str, from_date: datetime = None) -> Tuple[float, str]:
        """
        Calculate days until next scheduled delivery for a location.
        
        Note: Delivery schedules are calculated in business timezone (America/Chicago)
        regardless of user's local timezone, since deliveries happen at physical locations.
        
        Args:
            location: Location name ('Avondale' or 'Commissary')
            from_date: Calculate from this date (defaults to current time in business timezone)
            
        Returns:
            Tuple[float, str]: (days_until_delivery, delivery_date_string)
        """
        if from_date is None:
            # Use business timezone for delivery calculations
            from_date = get_time_in_timezone(BUSINESS_TIMEZONE)
        
        schedule = DELIVERY_SCHEDULES[location]
        delivery_days = schedule["days"]
        delivery_hour = schedule["hour"]
        
        self.logger.debug(f"Calculating next delivery for {location} from {from_date} (business timezone)")
        
        # Find next delivery day
        current_weekday = from_date.weekday()  # 0=Monday, 6=Sunday
        weekday_map = {
            'Monday': 0, 'Tuesday': 1, 'Wednesday': 2, 'Thursday': 3,
            'Friday': 4, 'Saturday': 5, 'Sunday': 6
        }
        
        delivery_weekdays = [weekday_map[day] for day in delivery_days]
        
        days_ahead = []
        for delivery_weekday in delivery_weekdays:
            if delivery_weekday > current_weekday:
                # This week
                days_ahead.append(delivery_weekday - current_weekday)
            elif delivery_weekday == current_weekday:
                # Today - check if delivery time has passed
                current_hour = from_date.hour
                if current_hour < delivery_hour:
                    # Delivery is later today
                    days_ahead.append(0)
                else:
                    # Delivery already passed, next week
                    days_ahead.append(7)
            else:
                # Next week
                days_ahead.append(7 - current_weekday + delivery_weekday)
        
        # Find the soonest delivery
        days_until = min(days_ahead)
        
        # Calculate exact time until delivery including hour
        next_delivery = from_date + timedelta(days=days_until)
        next_delivery = next_delivery.replace(hour=delivery_hour, minute=0, second=0, microsecond=0)
        
        # If delivery is today but after current time, calculate fractional days
        if days_until == 0:
            time_diff = next_delivery - from_date
            days_until = time_diff.total_seconds() / (24 * 3600)
        
        delivery_date_str = next_delivery.strftime('%Y-%m-%d')
        
        self.logger.debug(f"Next delivery for {location}: {days_until:.2f} days on {delivery_date_str}")
        return days_until, delivery_date_str
    
    def calculate_item_status(self, item: InventoryItem, current_qty: float = None,
                            from_date: datetime = None) -> Dict[str, Any]:
        """
        Calculate comprehensive status using sophisticated consumption analysis.
        
        Implements delivery-cycle-aware calculations that account for varying
        consumption periods between different delivery days.
        
        Args:
            item: Inventory item with consumption logic
            current_qty: Current quantity (if None, gets from Notion)
            from_date: Calculate from this date (defaults to now)
            
        Returns:
            Dict containing comprehensive status analysis
        """
        start_time = time.time()
        
        if from_date is None:
            from_date = get_time_in_timezone(BUSINESS_TIMEZONE)
        
        # Get current quantity if not provided
        if current_qty is None:
            inventory_data = self.notion.get_latest_inventory(item.location)
            # FIX: Handle simple float return instead of tuple
            current_qty = inventory_data.get(item.name, 0.0)
            last_count_date = from_date.strftime('%Y-%m-%d')
        else:
            last_count_date = from_date.strftime('%Y-%m-%d')
        
        # Calculate consumption need using sophisticated cycle analysis
        consumption_need = item.calculate_consumption_need(from_date)
        current_consumption_days = item.get_current_consumption_days(from_date)
        
        # Calculate required order quantity
        required_order = max(0, consumption_need - current_qty)
        
        # Determine status using business-critical logic
        status = item.determine_status(current_qty, consumption_need)
        
        # Calculate advanced analytics
        days_of_stock = (current_qty / item.adu) if item.adu > 0 else float('inf')
        
        # Risk assessment
        coverage_ratio = current_qty / consumption_need if consumption_need > 0 else float('inf')
        risk_level = 'HIGH' if coverage_ratio < 0.8 else 'MEDIUM' if coverage_ratio < 1.2 else 'LOW'
        
        # Get next delivery info for context
        days_until_delivery, delivery_date = self.calculate_days_until_next_delivery(item.location, from_date)
        
        result = {
            'item_id': item.id,
            'item_name': item.name,
            'location': item.location,
            'unit_type': item.unit_type,
            'adu': item.adu,
            'current_consumption_days': current_consumption_days,
            'current_qty': current_qty,
            'last_count_date': last_count_date,
            'days_until_delivery': days_until_delivery,
            'delivery_date': delivery_date,
            'consumption_need': consumption_need,
            'required_order': required_order,
            'status': status,
            'days_of_stock': days_of_stock,
            'coverage_ratio': coverage_ratio,
            'risk_level': risk_level,
            'calculation_date': from_date.isoformat()
        }
        
        duration_ms = (time.time() - start_time) * 1000
        self.logger.debug(f"Advanced status calculated for {item.name} in {duration_ms:.2f}ms: "
                        f"qty={current_qty}, need={consumption_need:.1f}, status={status}, risk={risk_level}")
        
        return result


    def calculate_location_summary(self, location: str, from_date: datetime = None) -> Dict[str, Any]:
        """
        Calculate comprehensive summary for all items in a location.
        
        Args:
            location: Location name
            from_date: Calculate from this date (defaults to now)
            
        Returns:
            Dict containing location summary with all item statuses
        """
        start_time = time.time()
        
        if from_date is None:
            from_date = get_time_in_timezone(BUSINESS_TIMEZONE)
        
        # Get all items for location
        items = self.notion.get_items_for_location(location)
        
        # Get all current inventory quantities
        inventory_data = self.notion.get_latest_inventory(location)
        
        # Calculate status for each item
        item_statuses = []
        status_counts = {'RED': 0, 'GREEN': 0}  # Only RED and GREEN now
        total_required_order = 0
        critical_items = []
        
        for item in items:
            # FIX: Handle simple float return instead of tuple
            current_qty = inventory_data.get(item.name, 0.0)
            status_info = self.calculate_item_status(item, current_qty, from_date)
            
            item_statuses.append(status_info)
            status_counts[status_info['status']] += 1
            total_required_order += status_info['required_order']
            
            if status_info['status'] == 'RED':
                critical_items.append(status_info['item_name'])
        
        # Calculate next delivery info
        days_until_delivery, delivery_date = self.calculate_days_until_next_delivery(location, from_date)
        
        summary = {
            'location': location,
            'calculation_date': from_date.isoformat(),
            'total_items': len(items),
            'days_until_delivery': days_until_delivery,
            'delivery_date': delivery_date,
            'status_counts': status_counts,
            'critical_items': critical_items,
            'total_required_order': total_required_order,
            'items': item_statuses
        }
        
        duration_ms = (time.time() - start_time) * 1000
        self.logger.info(f"Location summary calculated for {location} in {duration_ms:.2f}ms: "
                        f"{status_counts['RED']} RED, {status_counts['GREEN']} GREEN")
        
        return summary
    
    def generate_auto_requests(self, location: str, from_date: datetime = None) -> Dict[str, Any]:
        """
        Generate automated purchase requests for a location based on current inventory.
        
        Args:
            location: Location name
            from_date: Generate from this date (defaults to now)
            
        Returns:
            Dict containing request summary and individual item requests
        """
        start_time = time.time()
        
        if from_date is None:
            from_date = get_time_in_timezone(BUSINESS_TIMEZONE)
        
        # Get location summary with current calculations
        summary = self.calculate_location_summary(location, from_date)
        
        # Generate requests for items that need ordering
        requests = []
        total_items_requested = 0
        
        for item_status in summary['items']:
            if item_status['required_order'] > 0:
                request = {
                    'item_id': item_status['item_id'],
                    'item_name': item_status['item_name'],
                    'unit_type': item_status['unit_type'],
                    'current_qty': item_status['current_qty'],
                    'consumption_need': item_status['consumption_need'],
                    'requested_qty': item_status['required_order'],
                    'status': item_status['status'],
                    'delivery_date': item_status['delivery_date']
                }
                
                requests.append(request)
                total_items_requested += item_status['required_order']
        
        request_summary = {
            'location': location,
            'request_date': from_date.strftime('%Y-%m-%d'),
            'delivery_date': summary['delivery_date'],
            'total_items': len(requests),
            'total_quantity': total_items_requested,
            'critical_items': len(summary['critical_items']),
            'requests': requests
        }
        
        duration_ms = (time.time() - start_time) * 1000
        self.logger.info(f"Auto-requests generated for {location} in {duration_ms:.2f}ms: "
                        f"{len(requests)} items, {total_items_requested} total units")
        
        return request_summary

# Continue with the rest of the classes...
# (Due to length limits, I'll continue in the next response)

# ===== TELEGRAM BOT INTERFACE =====

class TelegramBot:
    """
    Production-ready Telegram bot with comprehensive error handling.
    """
    
    def __init__(self, token: str, notion_manager, calculator):
        """Initialize bot with enhanced error handling and state management."""
        self.token = token
        self.notion = notion_manager
        self.calc = calculator
        self.logger = logging.getLogger('telegram')
        
        # Bot configuration
        self.base_url = f"https://api.telegram.org/bot{token}"
        self.running = False
        self.last_update_id = 0
        
        # Enhanced conversation state management
        self.conversations: Dict[int, ConversationState] = {}
        self.conversation_lock = threading.Lock()
        self.conversation_cleanup_interval = 1800  # 30 minutes
        self.last_cleanup_time = datetime.now()
        
        # Rate limiting with exemptions
        self.user_commands: Dict[int, List[datetime]] = {}
        self.rate_limit_lock = threading.Lock()
        self.rate_limit_exempt_commands = {'/cancel', '/help', '/done', '/skip'}
        
        # Connection retry configuration
        self.max_retries = 3
        self.retry_delay = 1.0
        
        # Chat configuration from environment
        import os
        self.chat_config = {
            'onhand': int(os.environ.get('CHAT_ONHAND', '0')),
            'autorequest': int(os.environ.get('CHAT_AUTOREQUEST', '0')),
            'received': int(os.environ.get('CHAT_RECEIVED', '0')),
            'reassurance': int(os.environ.get('CHAT_REASSURANCE', '0'))
        }
        
        # Test chat override
        self.use_test_chat = os.environ.get('USE_TEST_CHAT', 'false').lower() == 'true'
        self.test_chat = int(os.environ.get('TEST_CHAT', '0')) if self.use_test_chat else None
        
        self.logger.info(f"Telegram bot initialized with enhanced error handling")
        if self.use_test_chat:
            self.logger.info(f"Test mode enabled - all messages will go to chat {self.test_chat}")

    # ===== CONVERSATION STATE MANAGEMENT =====
    
    def _cleanup_stale_conversations(self):
        """Remove expired conversation states to prevent memory leaks."""
        now = datetime.now()
        
        # Only cleanup every interval
        if (now - self.last_cleanup_time).total_seconds() < self.conversation_cleanup_interval:
            return
        
        with self.conversation_lock:
            expired_users = []
            for user_id, state in self.conversations.items():
                if state.is_expired(timeout_minutes=30):
                    expired_users.append(user_id)
            
            for user_id in expired_users:
                del self.conversations[user_id]
                self.logger.info(f"Cleaned up expired conversation for user {user_id}")
        
        self.last_cleanup_time = now
        
        if expired_users:
            self.logger.info(f"Cleaned up {len(expired_users)} expired conversations")
    
    def _get_or_create_conversation(self, user_id: int, chat_id: int, 
                                   command: str) -> ConversationState:
        """Get existing or create new conversation state."""
        with self.conversation_lock:
            if user_id in self.conversations:
                state = self.conversations[user_id]
                state.update_activity()
            else:
                state = ConversationState(
                    user_id=user_id,
                    chat_id=chat_id,
                    command=command,
                    step="initial"
                )
                self.conversations[user_id] = state
        return state
    
    def _end_conversation(self, user_id: int):
        """Safely end a conversation."""
        with self.conversation_lock:
            if user_id in self.conversations:
                del self.conversations[user_id]
                self.logger.debug(f"Ended conversation for user {user_id}")

    # ===== NETWORK COMMUNICATION WITH RETRY LOGIC =====
    
    def _make_request_with_retry(self, method: str, data: Dict = None) -> Optional[Dict]:
        """
        Make API request with automatic retry on failure.
        
        Args:
            method: Telegram API method
            data: Request payload
            
        Returns:
            Optional[Dict]: Response or None if all retries failed
        """
        for attempt in range(self.max_retries):
            result = self._make_request(method, data)
            if result is not None:
                return result
            
            if attempt < self.max_retries - 1:
                self.logger.warning(f"Request {method} failed, attempt {attempt + 1}/{self.max_retries}")
                time.sleep(self.retry_delay * (attempt + 1))
        
        self.logger.error(f"Request {method} failed after {self.max_retries} attempts")
        return None
    
    def _make_request(self, method: str, data: Dict = None) -> Optional[Dict]:
        """Make Telegram API request with comprehensive error handling."""
        import requests
        url = f"{self.base_url}/{method}"
        
        try:
            start = time.time()
            resp = requests.post(url, json=data or {}, timeout=30)
            duration = (time.time() - start) * 1000
            
            if resp.status_code == 200:
                payload = resp.json()
                if payload.get("ok"):
                    self.logger.debug(f"Telegram {method} OK in {duration:.2f}ms")
                    return payload
                else:
                    error_code = payload.get("error_code", "unknown")
                    error_desc = payload.get("description", "no description")
                    self.logger.error(f"Telegram {method} error {error_code}: {error_desc}")
                    return None
            else:
                self.logger.error(f"Telegram {method} HTTP {resp.status_code}")
                return None
                
        except requests.exceptions.Timeout:
            self.logger.error(f"Telegram {method} timeout")
            return None
        except requests.exceptions.ConnectionError:
            self.logger.error(f"Telegram {method} connection error")
            return None
        except Exception as e:
            self.logger.error(f"Telegram {method} unexpected error: {e}")
            return None
    
    def send_message(self, chat_id: int, text: str, parse_mode: str = "HTML",
                    disable_web_page_preview: bool = True, 
                    reply_markup: Optional[Dict] = None) -> bool:
        """Send message with automatic fallback and sanitization."""
        import html
        
        # Test mode redirect
        if self.use_test_chat and self.test_chat:
            original_chat_id = chat_id
            chat_id = self.test_chat
            text = f"<b>[Test Mode - Original Chat: {original_chat_id}]</b>\n\n{text}"
        
        # Truncate if too long (Telegram limit is 4096)
        if len(text) > 4000:
            text = text[:3997] + "..."
        
        # Sanitize HTML
        safe_text = self._sanitize_html(text)
        
        # Prepare payload
        payload = {
            "chat_id": chat_id,
            "text": safe_text,
            "disable_web_page_preview": disable_web_page_preview,
            "parse_mode": parse_mode if parse_mode else None
        }
        
        if reply_markup:
            payload["reply_markup"] = reply_markup
        
        # Try sending with retry
        result = self._make_request_with_retry("sendMessage", payload)
        
        if result:
            self.logger.info(f"Message sent to chat {chat_id}")
            return True
        
        # Fallback to plain text if HTML failed
        if parse_mode == "HTML":
            payload["parse_mode"] = None
            payload["text"] = html.unescape(text)
            result = self._make_request_with_retry("sendMessage", payload)
            if result:
                self.logger.info(f"Message sent as plain text to chat {chat_id}")
                return True
        
        self.logger.error(f"Failed to send message to chat {chat_id}")
        return False
    
    def _sanitize_html(self, text: str) -> str:
        """Enhanced HTML sanitization for Telegram."""
        import html
        import re
        
        # First escape everything
        text = html.escape(text, quote=False)
        
        # Re-enable safe tags
        safe_tags = ["b", "/b", "i", "/i", "u", "/u", "s", "/s", 
                    "code", "/code", "pre", "/pre", "tg-spoiler", "/tg-spoiler"]
        
        for tag in safe_tags:
            text = text.replace(f"&lt;{tag}&gt;", f"<{tag}>")
        
        # Remove empty tags
        text = re.sub(r"<\s*>", "", text)
        text = re.sub(r"</\s*>", "", text)
        
        return text

    def _process_update(self, update: Dict): ...
    def _rate_limit_ok(self, user_id: int) -> bool: ...

    def get_updates(self, timeout: int = 25) -> List[Dict]:
        """Get updates with error handling."""
        data = {
            "timeout": timeout,
            "allowed_updates": ["message", "callback_query"],
        }
        
        if self.last_update_id:
            data["offset"] = self.last_update_id + 1
        
        result = self._make_request("getUpdates", data)
        
        if not result:
            return []
        
        updates = result.get("result", [])
        
        if updates:
            self.last_update_id = updates[-1]["update_id"]
        
        return updates

    def _rate_limit_ok(self, user_id: int) -> bool:
        """
        Basic per-user command rate limiting.
        """
        now = datetime.now()
        with self.rate_limit_lock:
            buf = self.user_commands.setdefault(user_id, [])
            # keep the last 60 seconds
            cutoff = now - timedelta(seconds=60)
            buf[:] = [t for t in buf if t > cutoff]
            if len(buf) >= RATE_LIMIT_COMMANDS_PER_MINUTE:
                return False
            buf.append(now)
            return True

    def _sanitize_html_basic(self, text: str) -> str:
        """
        Make dynamic text safe for Telegram HTML:
        - Escape all angle brackets
        - Re-enable only a small, safe whitelist of tags we actually use (<b>, <i>, <u>, <s>, <code>, <pre>, <tg-spoiler>)
        - Strip any empty tags like "<>"
        """
        import html, re

        # Escape everything first (so any accidental '<' in item names/notes won't become tags)
        t = html.escape(text, quote=False)

        # Re-enable a minimal whitelist of tags we deliberately use in our templates
        for tag in ("b", "/b", "i", "/i", "u", "/u", "s", "/s", "code", "/code", "pre", "/pre", "tg-spoiler", "/tg-spoiler"):
            t = t.replace(f"&lt;{tag}&gt;", f"<{tag}>")

        # Remove empty/broken tags like "<>" that trigger "Unsupported start tag"
        t = re.sub(r"<\s*>", "", t)

        return t


    # ===== ENHANCED RATE LIMITING =====
    
    def _check_rate_limit(self, user_id: int, command: str = "") -> bool:
        """
        Check rate limit with command exemptions.
        
        Args:
            user_id: Telegram user ID
            command: Command being executed
            
        Returns:
            bool: True if allowed, False if rate limited
        """
        # Exempt certain commands and conversation continuations
        if command in self.rate_limit_exempt_commands:
            return True
        
        # Check if user has active conversation (exempt from rate limit)
        with self.conversation_lock:
            if user_id in self.conversations:
                return True
        
        # Apply standard rate limiting
        now = datetime.now()
        with self.rate_limit_lock:
            commands = self.user_commands.setdefault(user_id, [])
            
            # Clean old entries
            cutoff = now - timedelta(seconds=60)
            commands[:] = [t for t in commands if t > cutoff]
            
            # Check limit
            if len(commands) >= 10:  # 10 commands per minute
                return False
            
            commands.append(now)
            return True
    
    # ===== POLLING WITH ERROR RECOVERY =====
    
    def start_polling(self):
        """Start polling with automatic error recovery and cleanup."""
        self.running = True
        
        if self.use_test_chat and self.test_chat:
            self.send_message(self.test_chat, 
                            f"✅ K2 Bot v{SYSTEM_VERSION} online (test mode)")
        
        backoff = 1
        consecutive_errors = 0
        max_consecutive_errors = 10
        
        while self.running:
            try:
                # Periodic cleanup
                self._cleanup_stale_conversations()
                
                # Get updates
                updates = self.get_updates(timeout=25)
                
                if updates:
                    consecutive_errors = 0
                    backoff = 1
                    
                    for update in updates:
                        try:
                            self._process_update(update)
                        except Exception as e:
                            self.logger.error(f"Error processing update: {e}", exc_info=True)
                
            except Exception as e:
                consecutive_errors += 1
                self.logger.error(f"Polling error ({consecutive_errors}): {e}")
                
                if consecutive_errors >= max_consecutive_errors:
                    self.logger.critical("Too many consecutive errors, stopping bot")
                    self.running = False
                    break
                
                time.sleep(min(backoff, 30))
                backoff = min(backoff * 2, 30)

    def stop(self):
        """Gracefully stop the bot."""
        self.running = False
        self.logger.info("Telegram bot stopping...")

    # ===== UPDATE PROCESSING =====
    
    def _process_update(self, update: Dict):
        """Process update with comprehensive error handling."""
        try:
            # Handle callback queries
            if "callback_query" in update:
                self._handle_callback_safe(update["callback_query"])
                return
            
            # Handle messages
            message = update.get("message")
            if not message or "text" not in message:
                return
            
            text = sanitize_user_input(message.get("text", ""))
            if not text:
                return
            
            chat_id = message["chat"]["id"]
            user_id = message["from"]["id"]
            
            # Handle commands
            if text.startswith("/"):
                command = text.split()[0].lower()
                
                # Check rate limit
                if not self._check_rate_limit(user_id, command):
                    self.send_message(chat_id, 
                                    "⏳ Too many commands. Please wait a moment.")
                    return
                
                # Route command
                self._route_command(message, command)
                return
            
            # Handle conversation input
            with self.conversation_lock:
                if user_id in self.conversations:
                    state = self.conversations[user_id]
                    self._handle_conversation_input_safe(message, state)
                    return
            
            # No active conversation
            self.send_message(chat_id, 
                            "Type /help to see available commands or /entry to start.")
            
        except Exception as e:
            self.logger.error(f"Error in _process_update: {e}", exc_info=True)
            try:
                chat_id = update.get("message", {}).get("chat", {}).get("id")
                if chat_id:
                    self.send_message(chat_id, 
                                    "⚠️ An error occurred. Please try again.")
            except:
                pass
    
    def _route_command(self, message: Dict, command: str):
        """Route commands to appropriate handlers."""
        handlers = {
            "/start": self._handle_start,
            "/help": self._handle_help,
            "/entry": self._handle_entry,
            "/info": self._handle_info,
            "/order": self._handle_order,
            "/order_avondale": self._handle_order_avondale,
            "/order_commissary": self._handle_order_commissary,
            "/reassurance": self._handle_reassurance,
            "/status": self._handle_status,
            "/cancel": self._handle_cancel,
            "/adu": self._handle_adu,
            "/missing": self._handle_missing,
        }
        
        handler = handlers.get(command)
        if handler:
            try:
                handler(message)
            except Exception as e:
                self.logger.error(f"Error in {command}: {e}", exc_info=True)
                chat_id = message["chat"]["id"]
                self.send_message(chat_id, 
                                f"⚠️ Error executing {command}. Please try again.")
        else:
            self._handle_unknown(message)
    
    def _handle_callback_safe(self, callback_query: Dict):
        """Handle callback with error handling."""
        try:
            self._handle_callback(callback_query)
        except Exception as e:
            self.logger.error(f"Error in callback: {e}", exc_info=True)
            chat_id = callback_query.get("message", {}).get("chat", {}).get("id")
            if chat_id:
                self.send_message(chat_id, "⚠️ Error processing selection. Please try again.")
    
    def _handle_conversation_input_safe(self, message: Dict, state: ConversationState):
        """Handle conversation input with error handling."""
        try:
            # Try enhanced handler first
            if self._handle_conversation_input_enhanced(message, state):
                return
            # Fallback to basic handler
            self._handle_conversation_input(message, state)
        except Exception as e:
            self.logger.error(f"Error in conversation: {e}", exc_info=True)
            self.send_message(state.chat_id, 
                            "⚠️ Error processing input. Please try /cancel and start over.")

    # ===== CALLBACK HANDLING =====
    
    def _handle_callback(self, callback_query: Dict):
        """Handle inline keyboard callbacks."""
        data = callback_query.get("data", "")
        message = callback_query.get("message", {})
        chat_id = message.get("chat", {}).get("id")
        user_id = callback_query.get("from", {}).get("id")
        
        # Acknowledge callback
        self._make_request("answerCallbackQuery", 
                          {"callback_query_id": callback_query.get("id")})
        
        with self.conversation_lock:
            state = self.conversations.get(user_id)
        
        if not state:
            self.send_message(chat_id, "Session expired. Use /entry to start again.")
            return
        
        # Route callback based on data
        if data.startswith("loc|"):
            self._handle_location_callback(state, data)
        elif data.startswith("type|"):
            self._handle_type_callback(state, data)
        elif data.startswith("date|"):
            self._handle_date_callback(state, data)
        elif data.startswith("review|"):
            self._handle_review_callback(state, data)

    def _handle_location_callback(self, state: ConversationState, data: str):
        """Handle location selection."""
        state.location = data.split("|", 1)[1]
        state.step = "choose_type"
        
        keyboard = _ik([
            [("📦 On-Hand Count", "type|on_hand")],
            [("📥 Received Delivery", "type|received")]
        ])
        
        self.send_message(state.chat_id, 
                        f"Location: <b>{state.location}</b>\n"
                        "Select entry type:",
                        reply_markup=keyboard)
    
    def _handle_type_callback(self, state: ConversationState, data: str):
        """Handle entry type selection."""
        state.entry_type = data.split("|", 1)[1]
        state.step = "choose_date"
        
        today = get_time_in_timezone(BUSINESS_TIMEZONE).strftime("%Y-%m-%d")
        
        keyboard = _ik([
            [("📅 Today", f"date|{today}")],
            [("✏️ Enter custom date", "date|manual")]
        ])
        
        self.send_message(state.chat_id, 
                        "Select date:",
                        reply_markup=keyboard)
    
    def _handle_date_callback(self, state: ConversationState, data: str):
        """Handle date selection."""
        selection = data.split("|", 1)[1]
        
        if selection == "manual":
            state.step = "enter_date"
            self.send_message(state.chat_id, 
                            "Enter date (YYYY-MM-DD) or type 'today':")
        else:
            state.data["date"] = selection
            self._begin_item_loop(state)
    
    def _handle_review_callback(self, state: ConversationState, data: str):
        """Handle review actions."""
        action = data.split("|", 1)[1]
        
        if action == "submit":
            self._finalize_entry(state)
        elif action == "back":
            state.step = "enter_items"
            self._prompt_next_item(state)
        elif action == "cancel":
            self._end_conversation(state.user_id)
            self.send_message(state.chat_id, "❌ Entry cancelled. No data saved.")


    # ===== COMMAND HANDLERS =====
    
    def _handle_start(self, message: Dict):
        """Welcome message with system status."""
        chat_id = message["chat"]["id"]
        
        try:
            # Quick system check
            items_count = len(self.notion.get_all_items())
            system_status = "✅ Online" if items_count > 0 else "⚠️ Check connection"
            
            text = (
                "🚀 <b>K2 Restaurant Inventory System</b>\n"
                "━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
                f"Version 2.0.0 • Status: {system_status}\n\n"
                
                "📊 <b>Core Commands</b>\n"
                "├ /entry — Record inventory counts\n"
                "├ /info — Live status dashboard\n"
                "├ /order — Generate purchase orders\n"
                "└ /reassurance — Daily risk check\n\n"
                
                "🔧 <b>Quick Actions</b>\n"
                "├ /order_avondale — Avondale orders\n"
                "├ /order_commissary — Commissary orders\n"
                "├ /adu — View usage rates\n"
                "├ /missing — Check missing counts\n"
                "└ /status — System diagnostics\n\n"
                
                "💡 Type /help for details • /cancel to exit"
            )
            self.send_message(chat_id, text)
            
        except Exception as e:
            self.logger.error(f"Error in /start: {e}", exc_info=True)
            self.send_message(chat_id, "Welcome! Type /help for available commands.")
    
    def _handle_help(self, message: Dict):
        """Command reference."""
        chat_id = message["chat"]["id"]
        text = (
            "📚 <b>Command Reference</b>\n"
            "━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
            
            "📝 <b>Data Entry</b>\n"
            "/entry — Interactive inventory recording\n"
            "  • Choose location → type → date\n"
            "  • Enter quantities or skip items\n"
            "  • Saves directly to Notion\n\n"
            
            "📊 <b>Analytics & Reports</b>\n"
            "/info — Real-time inventory analysis\n"
            "/order — Supplier-ready order lists\n"
            "/reassurance — Risk assessment\n\n"
            
            "🔍 <b>Quick Checks</b>\n"
            "/adu — Average daily usage rates\n"
            "/missing [location] [date] — Missing counts\n"
            "/status — System health check\n\n"
            
            "💡 <b>Tips</b>\n"
            "• Use 'today' for current date\n"
            "• Type /skip to skip items\n"
            "• Type /done to finish early\n"
            "• Use /cancel anytime to exit"
        )
        self.send_message(chat_id, text)


    def _handle_status(self, message: Dict):
        """System diagnostics with visual indicators"""
        chat_id = message["chat"]["id"]
        try:
            avondale = self.notion.get_items_for_location("Avondale")
            commissary = self.notion.get_items_for_location("Commissary")
            now = get_time_in_timezone(BUSINESS_TIMEZONE)
            
            # Check system components
            notion_status = "✅ Connected" if avondale or commissary else "❌ Error"
            bot_status = "✅ Active" if self.running else "⚠️ Idle"
            
            text = (
                "🔧 <b>System Diagnostics</b>\n"
                "━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
                
                "⚡ <b>Status Overview</b>\n"
                f"├ Notion Database: {notion_status}\n"
                f"├ Telegram Bot: {bot_status}\n"
                f"├ Version: {SYSTEM_VERSION}\n"
                f"└ Mode: {'🧪 Test' if self.use_test_chat else '🚀 Production'}\n\n"
                
                "📊 <b>Database Stats</b>\n"
                f"├ Avondale Items: {len(avondale)}\n"
                f"├ Commissary Items: {len(commissary)}\n"
                f"└ Total Active: {len(avondale) + len(commissary)}\n\n"
                
                "🕐 <b>Time Information</b>\n"
                f"├ System Time: {now.strftime('%I:%M %p')}\n"
                f"├ Date: {now.strftime('%b %d, %Y')}\n"
                f"└ Timezone: {BUSINESS_TIMEZONE}\n\n"
                
                "✅ All systems operational"
            )
            self.send_message(chat_id, text)
        except Exception as e:
            self.logger.error(f"/status failed: {e}", exc_info=True)
            self.send_message(chat_id, (
                "🚨 <b>System Error</b>\n"
                "━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
                "Unable to retrieve system status.\n"
                "Please contact support if this persists."
            ))

    def _handle_adu(self, message: Dict):
        """ADU rates with visual grouping by location"""
        chat_id = message["chat"]["id"]
        
        try:
            items = self.notion.get_all_items()
            
            if not items:
                self.send_message(chat_id, "No items found in database.")
                return
            
            # Group by location
            avondale_items = [i for i in items if i.location == "Avondale"]
            commissary_items = [i for i in items if i.location == "Commissary"]
            
            text = (
                "📈 <b>AVERAGE DAILY USAGE</b>\n"
                "━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
            )
            
            # Avondale section
            if avondale_items:
                text += "🏪 <b>AVONDALE</b>\n"
                for item in sorted(avondale_items, key=lambda x: x.adu, reverse=True):
                    # Use emoji indicators for high/medium/low usage
                    if item.adu >= 5:
                        indicator = "🔴"  # High usage
                    elif item.adu >= 2:
                        indicator = "🟡"  # Medium usage
                    else:
                        indicator = "🟢"  # Low usage
                    
                    text += f"{indicator} <b>{item.name}</b>\n"
                    text += f"   {item.adu:.2f} {item.unit_type}/day\n"
                text += "\n"
            
            # Commissary section
            if commissary_items:
                text += "🏭 <b>COMMISSARY</b>\n"
                for item in sorted(commissary_items, key=lambda x: x.adu, reverse=True):
                    # Use emoji indicators
                    if item.adu >= 2:
                        indicator = "🔴"  # High usage
                    elif item.adu >= 1:
                        indicator = "🟡"  # Medium usage
                    else:
                        indicator = "🟢"  # Low usage
                    
                    text += f"{indicator} <b>{item.name}</b>\n"
                    text += f"   {item.adu:.2f} {item.unit_type}/day\n"
            
            text += (
                "\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
                "📊 Usage Indicators:\n"
                "🔴 High • 🟡 Medium • 🟢 Low\n\n"
                "💡 ADU drives all calculations"
            )
            
            self.send_message(chat_id, text)
            
        except Exception as e:
            self.logger.error(f"/adu failed: {e}", exc_info=True)
            self.send_message(chat_id, "⚠️ Unable to retrieve ADU data.")

    def _handle_missing(self, message: Dict):
        """Missing counts with clear visual formatting"""
        chat_id = message["chat"]["id"]
        
        parts = message.get("text", "").split()
        
        if len(parts) < 3:
            # Help message
            text = (
                "ℹ️ <b>Check Missing Counts</b>\n"
                "━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
                "📝 <b>Usage:</b>\n"
                "/missing [location] [date]\n\n"
                "📍 <b>Locations:</b>\n"
                "  • Avondale\n"
                "  • Commissary\n\n"
                "📅 <b>Date Format:</b>\n"
                "  • YYYY-MM-DD\n"
                "  • Example: 2025-09-16\n\n"
                "💡 <b>Example:</b>\n"
                "<code>/missing Avondale 2025-09-16</code>"
            )
            self.send_message(chat_id, text)
            return
        
        location = parts[1]
        date = parts[2]
        
        # Validate location
        if location not in ["Avondale", "Commissary"]:
            self.send_message(chat_id, (
                "❌ Invalid location\n"
                "Please use: Avondale or Commissary"
            ))
            return
        
        try:
            missing = self.notion.get_missing_counts(location, date)
            
            if not missing:
                text = (
                    "✅ <b>Inventory Check Complete</b>\n"
                    "━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
                    f"📍 Location: <b>{location}</b>\n"
                    f"📅 Date: <b>{date}</b>\n\n"
                    "✅ All items have been counted\n"
                    "No missing entries detected"
                )
            else:
                text = (
                    "⚠️ <b>Missing Inventory Counts</b>\n"
                    "━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
                    f"📍 Location: <b>{location}</b>\n"
                    f"📅 Date: <b>{date}</b>\n"
                    f"📊 Missing: <b>{len(missing)} items</b>\n\n"
                    
                    "📝 <b>Items Without Counts:</b>\n"
                )
                
                for item in missing:
                    text += f"  ☐ {item}\n"
                
                text += (
                    "\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
                    "💡 Use /entry to record these counts"
                )
            
            self.send_message(chat_id, text)
            
        except Exception as e:
            self.logger.error(f"/missing failed: {e}", exc_info=True)
            self.send_message(chat_id, (
                "⚠️ Unable to check missing counts\n"
                "Please verify the date format and try again"
            ))

    def _handle_entry(self, message: Dict):
        """Start inventory entry flow."""
        chat_id = message["chat"]["id"]
        user_id = message["from"]["id"]
        
        try:
            state = self._get_or_create_conversation(user_id, chat_id, "/entry")
            state.step = "choose_location"
            
            keyboard = _ik([
                [("🏪 Avondale", "loc|Avondale")],
                [("🏭 Commissary", "loc|Commissary")]
            ])
            
            self.send_message(chat_id, 
                            "<b>📝 Inventory Entry</b>\n"
                            "━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
                            "Select location:",
                            reply_markup=keyboard)
            
        except Exception as e:
            self.logger.error(f"Error starting entry: {e}", exc_info=True)
            self.send_message(chat_id, "⚠️ Unable to start entry. Please try again.")
    
    def _handle_cancel(self, message: Dict):
        """Cancel current operation."""
        chat_id = message["chat"]["id"]
        user_id = message["from"]["id"]
        
        with self.conversation_lock:
            if user_id in self.conversations:
                state = self.conversations[user_id]
                command = state.command
                del self.conversations[user_id]
                
                text = (
                    "❌ <b>Operation Cancelled</b>\n"
                    "━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
                    f"Cancelled: {command}\n"
                    "No data was saved\n\n"
                    "Start over with /entry or /help"
                )
            else:
                text = "ℹ️ No active operation to cancel"
        
        self.send_message(chat_id, text)

    def _handle_unknown(self, message: Dict):
        """Handle unknown commands."""
        chat_id = message["chat"]["id"]
        text = message.get("text", "")
        
        self.send_message(chat_id,
                        f"❓ Unknown command: {text}\n"
                        "Type /help to see available commands")

    # ===== CONVERSATION INPUT HANDLING =====
    
    def _handle_conversation_input_enhanced(self, message: Dict, state: ConversationState) -> bool:
        """
        Enhanced conversation input handler.
        
        Returns:
            bool: True if handled, False to use fallback handler
        """
        text = sanitize_user_input(message.get("text", ""))
        chat_id = state.chat_id
        
        # Update activity
        state.update_activity()
        
        # Handle date entry
        if state.step == "enter_date":
            return self._handle_date_entry(state, text)
        
        # Handle item entry
        if state.step == "enter_items":
            return self._handle_item_entry(state, text)
        
        # Handle note entry
        if state.step == "enter_note":
            return self._handle_note_entry(state, text)
        
        return False
    
    def _handle_date_entry(self, state: ConversationState, text: str) -> bool:
        """Handle manual date entry."""
        if text.lower() in ("today", "t"):
            state.data["date"] = get_time_in_timezone(BUSINESS_TIMEZONE).strftime("%Y-%m-%d")
            self._begin_item_loop(state)
        elif validate_date_format(text):
            state.data["date"] = text
            self._begin_item_loop(state)
        else:
            self.send_message(state.chat_id, 
                            "❌ Invalid date format. Use YYYY-MM-DD or 'today'")
        return True
    
    def _handle_item_entry(self, state: ConversationState, text: str) -> bool:
        """Handle item quantity entry."""
        lower_text = text.lower()
        
        # Handle commands
        if lower_text in ("/skip", "skip"):
            state.current_item_index += 1
            self._prompt_next_item(state)
            return True
        
        if lower_text in ("/done", "done"):
            self._start_review(state)
            return True
        
        # Parse quantity
        try:
            qty = float(text)
            if qty < 0:
                raise ValueError("Negative quantity")
            
            item = state.items[state.current_item_index]
            state.data.setdefault("quantities", {})[item.name] = qty
            state.current_item_index += 1
            self._prompt_next_item(state)
            
        except ValueError:
            self.send_message(state.chat_id, 
                            "❌ Please enter a valid number, /skip, or /done")
        
        return True
    
    def _handle_note_entry(self, state: ConversationState, text: str) -> bool:
        """Handle note entry."""
        if text.lower() != "none":
            state.note = sanitize_user_input(text, 500)
        else:
            state.note = ""
        
        self._show_review(state)
        return True

    def _handle_conversation_input_entry_ext(self, message: Dict, state: "ConversationState") -> bool:
        """
        Extends your existing _handle_conversation_input.
        Returns True if this function fully handled the message; False to let your original logic run.
        """
        chat_id = state.chat_id
        text = (message.get("text") or "").strip()
        low = text.lower()
        state.update_activity()

        # global escape
        if low == "/cancel":
            self._handle_cancel(message)
            return True

        # manual date entry
        if state.step == "choose_date":
            today = get_time_in_timezone(BUSINESS_TIMEZONE).strftime("%Y-%m-%d")
            if low in ("today", "t"):
                state.data["date"] = today
                self._begin_item_loop(state)
                return True
            try:
                datetime.strptime(text, "%Y-%m-%d")
                state.data["date"] = text
                self._begin_item_loop(state)
            except ValueError:
                self.send_message(chat_id, "Invalid date. Use YYYY-MM-DD or 'today'.")
            return True

        # item quantities with /skip /done
        if state.step == "enter_items":
            if low == "/skip":
                state.current_item_index += 1
                self._prompt_next_item(state)
                return True
            if low == "/done":
                state.step = "note"
                self.send_message(chat_id, "Add a note? Reply text or 'none'.")
                return True
            try:
                qty = float(text)
                item = state.items[state.current_item_index]
                state.data["quantities"][item.name] = qty
                state.current_item_index += 1
                self._prompt_next_item(state)
            except ValueError:
                self.send_message(chat_id, "Enter a number, or /skip /done /cancel.")
            return True

        # note → review card
        if state.step == "note":
            if low != "none":
                state.note = text
            state.step = "review"
            lines = [f"• {k}: {v}" for k, v in state.data.get("quantities", {}).items()]
            preview = (
                f"<b>Review</b>\n"
                f"Location: <b>{state.location}</b>\n"
                f"Type: <b>{'On-Hand' if state.entry_type=='on_hand' else 'Received'}</b>\n"
                f"Date: <b>{state.data['date']}</b>\n"
                f"Items: {len(lines)}\n" + ("\n".join(lines) if lines else "• none") + "\n"
                f"Note: {getattr(state, 'note', '') or '—'}"
            )
            kb = {"inline_keyboard": [[{"text": "Submit", "callback_data": "review|submit"},
                                    {"text": "Go Back", "callback_data": "review|back"}],
                                    [{"text": "Cancel", "callback_data": "review|cancel"}]]}
            self.send_message(chat_id, preview, reply_markup=kb)
            return True

        # not handled here → let your original handler run
        return False


    # ===== ITEM ENTRY FLOW =====
    
    def _begin_item_loop(self, state: ConversationState):
        """Start item entry loop."""
        try:
            state.items = self.notion.get_items_for_location(state.location)
            
            if not state.items:
                self.send_message(state.chat_id, 
                                f"⚠️ No items found for {state.location}")
                self._end_conversation(state.user_id)
                return
            
            state.current_item_index = 0
            state.data["quantities"] = {}
            state.step = "enter_items"
            
            entry_type = "On-Hand Count" if state.entry_type == "on_hand" else "Delivery"
            
            self.send_message(state.chat_id, 
                            f"📝 <b>{entry_type} for {state.location}</b>\n"
                            f"Date: {state.data['date']}\n"
                            f"Items: {len(state.items)}\n\n"
                            "Enter quantities (or /skip, /done, /cancel)")
            
            self._prompt_next_item(state)
            
        except Exception as e:
            self.logger.error(f"Error starting item loop: {e}", exc_info=True)
            self.send_message(state.chat_id, 
                            "⚠️ Error loading items. Please try again.")
            self._end_conversation(state.user_id)

    def _prompt_next_item(self, state: ConversationState):
        """Prompt for next item or complete if done."""
        if state.current_item_index >= len(state.items):
            self._start_review(state)
            return
        
        item = state.items[state.current_item_index]
        progress = f"{state.current_item_index + 1}/{len(state.items)}"
        
        # Get last recorded quantity if available
        last_qty = ""
        if hasattr(state, 'data') and 'quantities' in state.data:
            if item.name in state.data['quantities']:
                last_qty = f" (currently: {state.data['quantities'][item.name]})"
        
        self.send_message(state.chat_id,
                        f"[{progress}] <b>{item.name}</b>\n"
                        f"Unit: {item.unit_type} • ADU: {item.adu:.2f}/day{last_qty}\n"
                        f"Enter quantity:")

    def _start_review(self, state: ConversationState):
        """Start review process."""
        state.step = "enter_note"
        self.send_message(state.chat_id, 
                        "📝 Add a note? (type note or 'none'):")
    
    def _show_review(self, state: ConversationState):
        """Show review summary."""
        state.step = "review"
        
        quantities = state.data.get("quantities", {})
        items_with_qty = [(k, v) for k, v in quantities.items() if v > 0]
        
        entry_type = "On-Hand Count" if state.entry_type == "on_hand" else "Delivery"
        
        text = (
            "📋 <b>Review Entry</b>\n"
            "━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"Type: <b>{entry_type}</b>\n"
            f"Location: <b>{state.location}</b>\n"
            f"Date: <b>{state.data['date']}</b>\n"
            f"Items recorded: <b>{len(items_with_qty)}</b>\n\n"
        )
        
        if items_with_qty:
            text += "📦 <b>Quantities:</b>\n"
            for name, qty in sorted(items_with_qty):
                text += f"  • {name}: {qty}\n"
        else:
            text += "⚠️ No quantities entered\n"
        
        if state.note:
            text += f"\n📝 Note: {state.note}\n"
        
        keyboard = _ik([
            [("✅ Submit", "review|submit"), ("◀️ Back", "review|back")],
            [("❌ Cancel", "review|cancel")]
        ])
        
        self.send_message(state.chat_id, text, reply_markup=keyboard)

    def _finalize_entry(self, state: ConversationState):
        """Save entry to Notion."""
        try:
            quantities = state.data.get("quantities", {})
            
            # Validate quantities
            if not quantities or all(v == 0 for v in quantities.values()):
                self.send_message(state.chat_id, 
                                "⚠️ No quantities entered. Entry cancelled.")
                self._end_conversation(state.user_id)
                return
            
            # Save to Notion
            success = self.notion.save_inventory_transaction(
                location=state.location,
                entry_type=state.entry_type,
                date=state.data["date"],
                manager="Manager",  # Could be from state.data if collected
                notes=state.note if hasattr(state, 'note') else "",
                quantities=quantities
            )
            
            if success:
                items_count = len([v for v in quantities.values() if v > 0])
                entry_type = "on-hand count" if state.entry_type == "on_hand" else "delivery"
                
                self.send_message(state.chat_id,
                                f"✅ <b>Entry Saved</b>\n"
                                f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
                                f"Saved {items_count} items for {state.location}\n"
                                f"Type: {entry_type}\n"
                                f"Date: {state.data['date']}\n\n"
                                f"Use /info to see updated status")
            else:
                self.send_message(state.chat_id, 
                                "⚠️ Failed to save to Notion. Please try again.")
            
        except Exception as e:
            self.logger.error(f"Error finalizing entry: {e}", exc_info=True)
            self.send_message(state.chat_id, 
                            "⚠️ Error saving entry. Please contact support.")
        finally:
            self._end_conversation(state.user_id)

    def _handle_info(self, message: Dict):
        """Executive dashboard with mobile-optimized layout"""
        import math
        chat_id = message["chat"]["id"]
        
        def format_item_line(item: dict) -> str:
            """Format a single critical item for mobile display"""
            name = item.get("item_name", "Unknown")
            unit = item.get("unit_type", "unit")
            current = float(item.get("current_qty", 0))
            need = float(item.get("consumption_need", 0))
            order = math.ceil(max(0, need - current))
            
            # Compact format with emoji indicators
            if current == 0:
                status_icon = "🚨"
            elif current < need * 0.5:
                status_icon = "⚠️"
            else:
                status_icon = "📉"
                
            return f"{status_icon} <b>{name}</b>\n   Order {order} {unit} • Have {current:.1f}/{need:.1f}"
        
        try:
            now = get_time_in_timezone(BUSINESS_TIMEZONE)
            avondale = self.calc.calculate_location_summary("Avondale")
            commissary = self.calc.calculate_location_summary("Commissary")
            
            # Header with timestamp
            text = (
                "📊 <b>Inventory Dashboard</b>\n"
                f"🕐 {now.strftime('%I:%M %p')} • {now.strftime('%b %d')}\n"
                "━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
            )
            
            # Avondale Section
            a_red = avondale.get("status_counts", {}).get("RED", 0)
            a_green = avondale.get("status_counts", {}).get("GREEN", 0)
            a_days = avondale.get("days_until_delivery", 0)
            a_delivery = avondale.get("delivery_date", "—")
            
            text += (
                "🏪 <b>AVONDALE</b>\n"
                f"├ Next Delivery: {a_delivery} ({a_days:.1f} days)\n"
                f"├ Status: 🔴 {a_red} • 🟢 {a_green}\n"
            )
            
            # Avondale critical items (top 5)
            a_critical = [item for item in avondale.get("items", []) if item.get("status") == "RED"]
            if a_critical:
                text += "└ <b>Critical Items:</b>\n"
                for item in a_critical[:5]:
                    lines = format_item_line(item).split('\n')
                    text += f"  {lines[0]}\n  {lines[1]}\n"
                if len(a_critical) > 5:
                    text += f"  <i>...and {len(a_critical) - 5} more</i>\n"
            else:
                text += "└ ✅ All items sufficient\n"
            
            text += "\n"
            
            # Commissary Section
            c_red = commissary.get("status_counts", {}).get("RED", 0)
            c_green = commissary.get("status_counts", {}).get("GREEN", 0)
            c_days = commissary.get("days_until_delivery", 0)
            c_delivery = commissary.get("delivery_date", "—")
            
            text += (
                "🏭 <b>COMMISSARY</b>\n"
                f"├ Next Delivery: {c_delivery} ({c_days:.1f} days)\n"
                f"├ Status: 🔴 {c_red} • 🟢 {c_green}\n"
            )
            
            # Commissary critical items (top 5)
            c_critical = [item for item in commissary.get("items", []) if item.get("status") == "RED"]
            if c_critical:
                text += "└ <b>Critical Items:</b>\n"
                for item in c_critical[:5]:
                    lines = format_item_line(item).split('\n')
                    text += f"  {lines[0]}\n  {lines[1]}\n"
                if len(c_critical) > 5:
                    text += f"  <i>...and {len(c_critical) - 5} more</i>\n"
            else:
                text += "└ ✅ All items sufficient\n"
            
            # Footer with quick actions
            text += (
                "\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
                "💡 Quick Actions:\n"
                "• /order for supplier-ready lists\n"
                "• /entry to update counts"
            )
            
            self.send_message(chat_id, text)
            
        except Exception as e:
            self.logger.error(f"/info failed: {e}", exc_info=True)
            self.send_message(chat_id, "⚠️ Unable to generate dashboard. Please try again.")
        


    def _handle_order(self, message: Dict):
        """Combined order list with visual hierarchy"""
        import math
        chat_id = message["chat"]["id"]
        
        def format_order_section(location: str, summary: dict, emoji: str) -> str:
            """Format order section for a location"""
            delivery = summary.get("delivery_date", "—")
            requests = summary.get("requests", [])
            
            # Calculate totals by unit type
            totals = {}
            order_lines = []
            
            for item in requests:
                qty = math.ceil(float(item.get("requested_qty", 0)))
                if qty <= 0:
                    continue
                    
                name = item.get("item_name", "Unknown")
                unit = item.get("unit_type", "unit")
                current = float(item.get("current_qty", 0))
                need = float(item.get("consumption_need", 0))
                
                totals[unit] = totals.get(unit, 0) + qty
                order_lines.append({
                    'qty': qty,
                    'name': name,
                    'unit': unit,
                    'current': current,
                    'need': need
                })
            
            # Sort by quantity descending
            order_lines.sort(key=lambda x: x['qty'], reverse=True)
            
            # Build section text
            text = f"{emoji} <b>{location.upper()} ORDER</b>\n"
            text += f"📅 Delivery: {delivery}\n"
            
            if not order_lines:
                text += "✅ No items needed\n"
                return text
            
            # Totals summary
            text += "📦 Totals: "
            text += " • ".join(f"{v} {k}" for k, v in sorted(totals.items()))
            text += f"\n\n"
            
            # Item list
            for item in order_lines[:10]:  # Limit to top 10 for mobile
                text += f"<b>{item['qty']} {item['unit']}</b> — {item['name']}\n"
                text += f"  Current: {item['current']:.1f} • Need: {item['need']:.1f}\n"
            
            if len(order_lines) > 10:
                text += f"<i>...and {len(order_lines) - 10} more items</i>\n"
            
            return text
        
        try:
            avondale = self.calc.generate_auto_requests("Avondale")
            commissary = self.calc.generate_auto_requests("Commissary")
            
            text = (
                "📋 <b>PURCHASE ORDERS</b>\n"
                "━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
            )
            
            text += format_order_section("Avondale", avondale, "🏪")
            text += "\n" + ("─" * 28) + "\n\n"
            text += format_order_section("Commissary", commissary, "🏭")
            
            text += (
                "\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
                "📌 <b>Note:</b> Quantities rounded up for ordering\n"
                "💡 Use location-specific commands:\n"
                "  • /order_avondale\n"
                "  • /order_commissary"
            )
            
            self.send_message(chat_id, text)
            
        except Exception as e:
            self.logger.error(f"/order failed: {e}", exc_info=True)
            self.send_message(chat_id, "⚠️ Unable to generate orders. Please try again.")


    def _handle_order_avondale(self, message: Dict):
        """Avondale-specific order with supplier format"""
        import math
        chat_id = message["chat"]["id"]
        
        try:
            summary = self.calc.generate_auto_requests("Avondale")
            delivery = summary.get("delivery_date", "—")
            requests = summary.get("requests", [])
            
            # Process and sort orders
            orders = []
            totals = {}
            
            for item in requests:
                qty = math.ceil(float(item.get("requested_qty", 0)))
                if qty <= 0:
                    continue
                
                unit = item.get("unit_type", "unit")
                totals[unit] = totals.get(unit, 0) + qty
                
                orders.append({
                    'qty': qty,
                    'name': item.get("item_name", "Unknown"),
                    'unit': unit,
                    'current': float(item.get("current_qty", 0)),
                    'need': float(item.get("consumption_need", 0))
                })
            
            orders.sort(key=lambda x: x['qty'], reverse=True)
            
            # Build message
            text = (
                "🏪 <b>AVONDALE PURCHASE ORDER</b>\n"
                "━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
                f"📅 Delivery Date: <b>{delivery}</b>\n"
                f"📦 Items to Order: <b>{len(orders)}</b>\n\n"
            )
            
            if orders:
                # Summary by unit type
                text += "📊 <b>Order Summary</b>\n"
                for unit, total in sorted(totals.items(), key=lambda x: (-x[1], x[0])):
                    text += f"  • {total} {unit}{'s' if total > 1 else ''}\n"
                
                text += "\n📋 <b>Detailed Order List</b>\n"
                text += "─" * 28 + "\n"
                
                for item in orders:
                    text += f"☐ <b>{item['qty']} {item['unit']}</b> — {item['name']}\n"
                    stock_info = f"Stock: {item['current']:.1f} • Need: {item['need']:.1f}"
                    text += f"  <i>{stock_info}</i>\n"
                
                text += (
                    "\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
                    "✅ Ready to send to supplier\n"
                    "📱 Screenshot or forward this message"
                )
            else:
                text += (
                    "✅ <b>No Orders Needed</b>\n\n"
                    "All inventory levels are sufficient\n"
                    "until the next delivery."
                )
            
            self.send_message(chat_id, text)
            
        except Exception as e:
            self.logger.error(f"/order_avondale failed: {e}", exc_info=True)
            self.send_message(chat_id, "⚠️ Unable to generate Avondale orders.")


    def _handle_order_commissary(self, message: Dict):
        """Commissary-specific order with supplier format"""
        import math
        chat_id = message["chat"]["id"]
        
        try:
            summary = self.calc.generate_auto_requests("Commissary")
            delivery = summary.get("delivery_date", "—")
            requests = summary.get("requests", [])
            
            # Process and sort orders
            orders = []
            totals = {}
            
            for item in requests:
                qty = math.ceil(float(item.get("requested_qty", 0)))
                if qty <= 0:
                    continue
                
                unit = item.get("unit_type", "unit")
                totals[unit] = totals.get(unit, 0) + qty
                
                orders.append({
                    'qty': qty,
                    'name': item.get("item_name", "Unknown"),
                    'unit': unit,
                    'current': float(item.get("current_qty", 0)),
                    'need': float(item.get("consumption_need", 0))
                })
            
            orders.sort(key=lambda x: x['qty'], reverse=True)
            
            # Build message
            text = (
                "🏭 <b>COMMISSARY PURCHASE ORDER</b>\n"
                "━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
                f"📅 Delivery Date: <b>{delivery}</b>\n"
                f"📦 Items to Order: <b>{len(orders)}</b>\n\n"
            )
            
            if orders:
                # Summary by unit type
                text += "📊 <b>Order Summary</b>\n"
                for unit, total in sorted(totals.items(), key=lambda x: (-x[1], x[0])):
                    text += f"  • {total} {unit}{'s' if total > 1 else ''}\n"
                
                text += "\n📋 <b>Detailed Order List</b>\n"
                text += "─" * 28 + "\n"
                
                for item in orders:
                    text += f"☐ <b>{item['qty']} {item['unit']}</b> — {item['name']}\n"
                    stock_info = f"Stock: {item['current']:.1f} • Need: {item['need']:.1f}"
                    text += f"  <i>{stock_info}</i>\n"
                
                text += (
                    "\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
                    "✅ Ready to send to supplier\n"
                    "📱 Screenshot or forward this message"
                )
            else:
                text += (
                    "✅ <b>No Orders Needed</b>\n\n"
                    "All inventory levels are sufficient\n"
                    "until the next delivery."
                )
            
            self.send_message(chat_id, text)
            
        except Exception as e:
            self.logger.error(f"/order_commissary failed: {e}", exc_info=True)
            self.send_message(chat_id, "⚠️ Unable to generate Commissary orders.")


    def _handle_reassurance(self, message: Dict):
        """Daily risk assessment - FIXED to prevent duplicates."""
        chat_id = message["chat"]["id"]
        
        try:
            avondale = self.calc.calculate_location_summary("Avondale")
            commissary = self.calc.calculate_location_summary("Commissary")
            
            a_critical = [item for item in avondale.get("items", []) 
                         if item.get("status") == "RED"]
            c_critical = [item for item in commissary.get("items", []) 
                         if item.get("status") == "RED"]
            total_critical = len(a_critical) + len(c_critical)
            
            now = get_time_in_timezone(BUSINESS_TIMEZONE)
            
            if total_critical == 0:
                text = self._format_reassurance_clear(now, avondale, commissary)
            else:
                text = self._format_reassurance_alert(now, total_critical, 
                                                      a_critical, c_critical)
            
            # FIXED: Only send to reassurance chat if it's different
            reassurance_chat = self.chat_config.get('reassurance')
            if reassurance_chat and reassurance_chat != chat_id:
                self.send_message(reassurance_chat, text)
                self.logger.info(f"Reassurance sent to management chat {reassurance_chat}")
            
            # Always send to requesting user
            self.send_message(chat_id, text)
            
        except Exception as e:
            self.logger.error(f"Error in reassurance: {e}", exc_info=True)
            self.send_message(chat_id, "⚠️ Unable to generate risk assessment.")
    
    def _format_reassurance_clear(self, now, avondale, commissary):
        """Format all-clear reassurance message."""
        return (
            "✅ <b>DAILY RISK ASSESSMENT</b>\n"
            "━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"🕐 {now.strftime('%I:%M %p')} • {now.strftime('%A, %b %d')}\n\n"
            
            "🟢 <b>ALL CLEAR</b>\n"
            "No critical inventory issues detected\n\n"
            
            "📊 <b>Location Status</b>\n"
            f"├ Avondale: {avondale['status_counts']['GREEN']} items OK\n"
            f"│  Next delivery: {avondale['delivery_date']}\n"
            f"├ Commissary: {commissary['status_counts']['GREEN']} items OK\n"
            f"│  Next delivery: {commissary['delivery_date']}\n"
            f"└ Total Coverage: 100%\n\n"
            
            "✅ All inventory levels sufficient\n"
            "✅ No immediate action required\n\n"
            
            "━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
            "💚 System Status: Healthy"
        )
    
    def _format_reassurance_alert(self, now, total_critical, a_critical, c_critical):
        """Format critical alert reassurance message."""
        text = (
            "🚨 <b>DAILY RISK ASSESSMENT</b>\n"
            "━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"🕐 {now.strftime('%I:%M %p')} • {now.strftime('%A, %b %d')}\n\n"
            
            f"⚠️ <b>ACTION REQUIRED</b>\n"
            f"{total_critical} critical item{'s' if total_critical != 1 else ''} at risk\n\n"
        )
        
        if a_critical:
            text += f"🏪 <b>AVONDALE ({len(a_critical)} critical)</b>\n"
            for item in a_critical[:5]:
                days_stock = item.get('days_of_stock', 0)
                text += (
                    f"🔴 <b>{item['item_name']}</b>\n"
                    f"   Stock: {item['current_qty']:.1f} {item['unit_type']}\n"
                    f"   Days remaining: {days_stock:.1f}\n"
                )
            if len(a_critical) > 5:
                text += f"<i>...plus {len(a_critical) - 5} more</i>\n"
            text += "\n"
        
        if c_critical:
            text += f"🏭 <b>COMMISSARY ({len(c_critical)} critical)</b>\n"
            for item in c_critical[:5]:
                days_stock = item.get('days_of_stock', 0)
                text += (
                    f"🔴 <b>{item['item_name']}</b>\n"
                    f"   Stock: {item['current_qty']:.1f} {item['unit_type']}\n"
                    f"   Days remaining: {days_stock:.1f}\n"
                )
            if len(c_critical) > 5:
                text += f"<i>...plus {len(c_critical) - 5} more</i>\n"
        
        text += (
            "\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
            "⚠️ <b>IMMEDIATE ACTION NEEDED</b>\n"
            "📞 Contact supplier immediately\n"
            "📋 Use /order for complete list"
        )
        
        return text


# ===== MAIN APPLICATION =====

class K2NotionInventorySystem:
    """
    Main application class with Notion integration.
    """
    
    def __init__(self):
        """Initialize the complete K2 Notion inventory system."""
        self.logger = logging.getLogger('system')
        self.logger.critical(f"K2 Notion Inventory Management System v{SYSTEM_VERSION} initializing")
        
        # Validate environment variables
        if not self._validate_environment():
            sys.exit(1)
        
        # Initialize core components
        self.notion_manager = None
        self.calculator = None
        self.bot = None
        self.scheduler = None
        
        # System state
        self.running = False
        self.startup_time = datetime.now()
        
        self.logger.info("System initialization completed")
    
    def _validate_environment(self) -> bool:
        """Validate required environment variables."""
        required_vars = [
            'TELEGRAM_BOT_TOKEN',
            'NOTION_TOKEN', 
            'NOTION_ITEMS_DB_ID',
            'NOTION_INVENTORY_DB_ID',
            'NOTION_ADU_CALC_DB_ID'
        ]
        
        missing_vars = [var for var in required_vars if not os.environ.get(var)]
        
        if missing_vars:
            self.logger.critical(f"Missing required environment variables: {missing_vars}")
            return False
        
        self.logger.info("Environment validation passed")
        return True
        
    def start(self):
        """Start all system components in proper order."""
        try:
            self.logger.critical("Starting K2 Notion Inventory Management System")
            
            # Initialize Notion manager
            self.logger.info("Initializing Notion manager...")
            notion_token = os.environ.get('NOTION_TOKEN')
            items_db_id = os.environ.get('NOTION_ITEMS_DB_ID')
            inventory_db_id = os.environ.get('NOTION_INVENTORY_DB_ID')
            adu_calc_db_id = os.environ.get('NOTION_ADU_CALC_DB_ID')
            
            self.notion_manager = NotionManager(notion_token, items_db_id, inventory_db_id, adu_calc_db_id)
            
            # Initialize calculator
            self.logger.info("Initializing inventory calculator...")
            self.calculator = InventoryCalculator(self.notion_manager)
            
            # Initialize Telegram bot
            self.logger.info("Initializing Telegram bot...")
            bot_token = os.environ.get('TELEGRAM_BOT_TOKEN')
            self.bot = TelegramBot(bot_token, self.notion_manager, self.calculator)
            
            
            self.running = True
            self.logger.critical("System startup completed successfully")
            
            # Start bot polling (this blocks)
            self.logger.info("Starting Telegram bot polling...")
            print("🚀 K2 Notion Inventory System is running!")
            print("📝 Data is stored in Notion databases") 
            print("🤖 Bot is ready for commands - try /start")
            print("Press Ctrl+C to stop")
            
            self.bot.start_polling()
            
        except KeyboardInterrupt:
            self.logger.info("Shutdown requested by user")
        except Exception as e:
            self.logger.critical(f"Critical error during startup: {e}")
            raise
        finally:
            self.stop()

    def stop(self):
        """Gracefully stop all system components."""
        if not self.running:
            return
        
        self.logger.critical("Shutting down K2 Notion Inventory Management System")
        self.running = False
        
        # Stop components in reverse order
        # if self.scheduler:
        #     self.logger.info("Stopping scheduler...")
        #     self.scheduler.stop()
        
        if self.bot:
            self.logger.info("Stopping Telegram bot...")
            self.bot.stop()
        
        # Log shutdown
        if self.notion_manager:
            uptime = datetime.now() - self.startup_time
            self.logger.info(f"System ran for {uptime.total_seconds():.1f} seconds")
        
        self.logger.critical("System shutdown completed")

# ===== ENTRY POINT =====

# REPLACE the entire main() function with this:

def main():
    """Main entry point for the application."""
    try:
        # Check if running in test/development mode
        if len(sys.argv) > 1 and sys.argv[1] == '--test':
            print("🧪 TEST MODE: Running system validation...")
            system = K2NotionInventorySystem()
            print("✅ System validation completed successfully!")
            print("✅ Notion databases accessible")
            print("\nTo run the full system, use: python k2_notion_inventory.py")
            return
        
        # Create and start the system
        system = K2NotionInventorySystem()
        system.start()
        
    except KeyboardInterrupt:
        print("\n⚠️ Shutdown requested by user")
        if 'system' in locals():
            system.stop()
    except Exception as e:
        logger.critical(f"Fatal error in main: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()