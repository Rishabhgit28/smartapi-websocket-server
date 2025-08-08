# =====================================================================================================================
# Combined Trading Dashboard and Signal Generator
# Merges the functionality of smartWebSocketV2.py (live dashboard) and ORH.py (trade signal detection).
# This single script provides real-time P&L tracking and simultaneously monitors for ORH and 3% Down setups.
# VERSION: Concurrent (Threaded) with Advanced Order Status Tracking and Dynamic Setups
# =====================================================================================================================

# --- Core Python and System Imports ---
import os
import sys
import json
import time
import struct
import ssl
import logging
import threading
import collections
import datetime
import re
from datetime import timedelta
from dateutil.relativedelta import relativedelta
# --- START: MODIFIED SECTION ---
from zoneinfo import ZoneInfo
# --- END: MODIFIED SECTION ---


# --- Flask for Web Service Deployment ---
from flask import Flask

# --- Third-Party Library Imports ---
import gspread
import pyotp
import websocket
import requests
import yfinance as yf # <-- ADDED FOR YAHOO FINANCE
import logzero
from logzero import logger
from oauth2client.service_account import ServiceAccountCredentials

# --- Windows-Specific Import for Sound Alerts ---
try:
    import winsound
except ImportError:
    # Handle the case where winsound is not available (e.g., on non-Windows systems)
    winsound = None
    logger.warning("Could not import 'winsound'. Sound alerts for ORH setup will be disabled.")

# --- SmartAPI Imports ---
from SmartApi import SmartConnect
from SmartApi.smartExceptions import DataException

# =====================================================================================================================
#
#                                         --- FLASK AND LOGGING CONFIGURATION ---
#
# =====================================================================================================================

# --- Setup Flask App for Deployment ---
# A simple web server is required for deployment on platforms like Render to confirm the service is running.
app = Flask(__name__)

# --- Configure Centralized Logging ---
# All logs will be written to a daily file in the 'logs' directory.
log_folder = time.strftime("%Y-%m-%d", time.localtime())
log_folder_path = os.path.join("logs", log_folder)
os.makedirs(log_folder_path, exist_ok=True)
log_path = os.path.join(log_folder_path, "app.log")

# FIX: Configure file logger and console logger separately for compatibility.

# 1. Configure a rotating file handler. The `level` will be inherited from the default logger setup below.
# This will keep the 3 most recent log files, each up to 1MB in size.
logzero.logfile(log_path, maxBytes=1e6, backupCount=3, encoding='utf-8')

# 2. Configure the default logger (which includes the console) and set the global logging level.
logzero.setup_default_logger(level=logging.INFO)


# Explicitly add a StreamHandler for console output to ensure messages are always visible.
if not any(isinstance(h, logging.StreamHandler) for h in logger.handlers):
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO)
    formatter = logging.Formatter('[%(levelname)s %(asctime)s %(filename)s:%(lineno)d] %(message)s')
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

# --- Flask Health Check Route ---
@app.route('/ping')
def ping():
    """A simple route for deployment services to check if the app is alive."""
    return "Combined Trading Server is running", 200

# =====================================================================================================================
#
#                                         --- GLOBAL VARIABLES AND CONFIGURATION ---
#
# =====================================================================================================================

# --- SmartAPI Credentials ---
API_KEY = "oNNHQHKU"
CLIENT_CODE = "D355432"
MPIN = "1234"
TOTP_SECRET = "QHO5IWOISV56Z2BFTPFSRSQVRQ"

# --- Google Sheets Configuration ---
GOOGLE_SHEET_ID = '1cYBpsVKCbrYCZzrj8NAMEgUG4cXy5Q5r9BtQE1Cjmz0'
DASHBOARD_SHEET_NAME = 'Dashboard'
ATH_CACHE_SHEET_NAME = 'ATH Cache'
ORDERS_SHEET_NAME = 'Orders'

# --- Apps Script Web App URL for Instant Triggers ---
APPS_SCRIPT_WEB_APP_URL = "https://script.google.com/macros/s/AKfycbz_fhouljmK5Ad1q9wOfG0JjvJhZVAcAUupH8mmowbcUlMDG0wMP6h9XSfBsmihYvYh/exec" # <-- PASTE YOUR URL HERE

# --- MODIFIED: Instrument master list URL and global variable ---
INSTRUMENT_LIST_URL = "https://margincalculator.angelbroking.com/OpenAPI_File/files/OpenAPIScripMaster.json"
instrument_master_list = []
stock_name_cache = {} # Cache for full stock names from Yahoo Finance


# Set the path to the Google credentials JSON file
if os.path.exists("/etc/secrets/creds.json"):
    JSON_KEY_FILE_PATH = "/etc/secrets/creds.json"
else:
    # Fallback for local development
    current_dir = os.path.dirname(__file__) if "__file__" in locals() else os.getcwd()
    JSON_KEY_FILE_PATH = os.path.join(current_dir, "the-money-method-ad6d7-6d23c192b74e.json")


SCOPE = [
    "https://spreadsheets.google.com/feeds",
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive"
]

# --- Global Objects (Initialized in main logic) ---
smart_api_obj = None
smart_ws = None
gsheet = None
Dashboard = None
ATHCache = None
OrdersSheet = None

# --- Threading Lock for Data Safety ---
data_lock = threading.Lock()

# --- Data Caching and State Management Variables ---
# For Live Dashboard
latest_tick_data = collections.defaultdict(dict)
latest_quote_data = collections.defaultdict(dict)
excel_dashboard_details = collections.defaultdict(list)
previous_ltp_data = {}
previous_percentage_change_data = {}
cells_to_clear_color = set()

# For ORH and 3% Down Setups
excel_setup_details = collections.defaultdict(list)
excel_3pct_setup_details = collections.defaultdict(list)
excel_dynamic_setups = collections.defaultdict(list) # <-- NEW: For dynamic setups
interval_ohlc_data = collections.defaultdict(lambda: collections.defaultdict(dict))
completed_3min_candles = collections.defaultdict(list)
volume_history_3pct = collections.defaultdict(lambda: collections.defaultdict(list))
previous_day_high_cache = {}
monthly_high_cache = {}

# For Subscription Management
subscribed_tokens = set()
scan_memory_cache = {} # <-- FIX: In-memory cache to prevent repetitive logging

# --- Configuration Constants ---
# General
START_ROW_DATA = 5
EXCEL_RETRY_ATTEMPTS = 3
PREV_DAY_HIGH_CACHE_FILE = 'previous_day_high_cache.json'
TEST_3PCT_DOWN_HISTORICAL_FETCH = False
SCRIP_SEARCH_RETRY_ATTEMPTS = 5
SCRIP_SEARCH_RETRY_DELAY = 2.0
SCRIP_SEARCH_RETRY_MULTIPLIER = 1.5
HISTORICAL_DATA_RETRY_ATTEMPTS = 3
HISTORICAL_DATA_RETRY_DELAY = 1.0
HISTORICAL_DATA_RETRY_MULTIPLIER = 1.5
QUOTE_API_MAX_TOKENS = 50

# For Live Dashboard
INDEX_START_ROW = 100
QUARTER_POSITIONS_START_ROW = 100
FOCUS_EXCHANGE_COL = 'B'
FOCUS_SYMBOL_COL = 'C'
FOCUS_LTP_COL = 'D'
FOCUS_CHG_COL = 'E'
# --- START: MODIFIED SECTION ---
ATH_CACHE_Y_COL_DASH = 'AG' # Changed from 'Y'
ATH_CACHE_Z_COL_DASH = 'AH' # Changed from 'Z'
# --- END: MODIFIED SECTION ---
FULL_EXCHANGE_COL = 'L'
FULL_SYMBOL_COL = 'M'
FULL_QTY_COL = 'N'
FULL_PRICE_COL = 'O'
FULL_LTP_COL = 'P'
FULL_RETURN_AMT_COL = 'Q'
FULL_RETURN_PCT_COL = 'R'

FULL_ENTRY_DATE_COL = 'S'
FULL_DAYS_DURATION_COL = 'T'
MONTH_SORT_COL = 'V'
SWING_LOW_INPUT_COL = 'W'
PERCENT_FROM_SWING_LOW_COL = 'X'

HIGHEST_UP_CANDLE_COL = 'Z'
HIGHEST_UP_CANDLE_STATUS_COL = 'AA'
HIGH_VOL_RESULT_COL = 'AB'
HIGH_VOL_STATUS_COL = 'AC'
PCT_DOWN_RESULT_COL = 'AD'
PCT_DOWN_STATUS_COL = 'AE'
SUGGESTION_COL = 'AF'
FULL_POSITIONS_END_COL = 'AG'

INDEX_EXCHANGE_COL = 'B'
INDEX_SYMBOL_COL = 'C'
INDEX_LTP_COL = 'D'
INDEX_CHG_COL = 'E'
QUARTER_EXCHANGE_COL = 'M'
QUARTER_SYMBOL_COL = 'N'
QUARTER_LTP_COL = 'P'
QUARTER_CHG_COL = 'Q'

# For ORH and 3% Down Setups
ORH_EXCHANGE_COL = 'B'
ORH_SYMBOL_COL = 'C'
ORH_QTY_COL = 'I'
ORH_TOKEN_COL = 'Y'
ORH_RESULT_COL = 'G'
ORH_BUY_STOP_COL = 'H'
PCT_EXCHANGE_COL_3PCT = 'L'
PCT_SYMBOL_COL_3PCT = 'M'
PCT_TOKEN_COL_3PCT = 'Z'

CANDLE_INTERVALS_3PCT_API = ['FIFTEEN_MINUTE', 'THIRTY_MINUTE', 'ONE_HOUR']
CANDLE_INTERVAL_MAP_DISPLAY = {
    'FIFTEEN_MINUTE': '15 min',
    'THIRTY_MINUTE': '30 min',
    'ONE_HOUR': '60 min'
}

ORH_MAX_CANDLES = 5
ORH_MAX_ROW = 17

# =====================================================================================================================
#
#                                         --- HELPER AND UTILITY FUNCTIONS ---
#
# =====================================================================================================================

def get_ist_time():
    """Returns the current time in Indian Standard Time."""
    return datetime.datetime.now(ZoneInfo("Asia/Kolkata"))

def normalize_status(api_status):
    """
    Maps raw API status strings to a user-friendly, unified format, as per the research document.
    """
    if not api_status or not isinstance(api_status, str):
        return 'Unknown'

    status_lower = api_status.lower()

    status_mapping = {
        # GTT Statuses from gttLists
        'active': 'Pending Trigger',
        'new': 'Pending Trigger',
        'triggered': 'Triggered - Awaiting ID',
        'cancelled': 'Cancelled',
        'expired': 'Expired',
        'rejected': 'Rejected',

        # Order Statuses from getOrderBook
        'open': 'Pending Execution',
        'pending': 'Pending Execution',
        'complete': 'Completed',
        'executed': 'Completed',
        # 'rejected' and 'cancelled' are already mapped above

        # Custom statuses for tracking
        'not_found_in_gtt_list': 'GTT Rule Not Found',
        'not_found_in_order_book': 'Order Not in Book'
    }
    return status_mapping.get(status_lower, api_status.title())


def get_full_name_from_yahoo(symbol, exchange):
    """
    Fetches the full company name from Yahoo Finance with a fallback mechanism.
    Uses a cache to avoid repeated API calls.
    """
    global stock_name_cache
    if not symbol or not exchange:
        return symbol # Return original symbol if data is missing

    base_symbol = re.sub(r'-(EQ|BE)$', '', symbol).upper()
    if base_symbol in stock_name_cache:
        return stock_name_cache[base_symbol] or symbol # Return cached name or original symbol

    # --- MODIFIED: Robust lookup logic ---
    tickers_to_try = []
    if exchange.upper() == 'NSE':
        tickers_to_try.extend([f"{base_symbol}.NS", f"{base_symbol}.BO"])
    elif exchange.upper() == 'BSE':
        tickers_to_try.extend([f"{base_symbol}.BO", f"{base_symbol}.NS"])
    else:
        tickers_to_try.append(f"{base_symbol}.NS") # Default to NS for others

    for ticker in tickers_to_try:
        try:
            ticker_info = yf.Ticker(ticker)
            long_name = ticker_info.info.get('longName')
            if long_name:
                logger.info(f"Fetched full name for {symbol} using ticker {ticker}: {long_name}")
                stock_name_cache[base_symbol] = long_name
                return long_name
        except Exception:
            logger.warning(f"Could not fetch data from Yahoo Finance for ticker {ticker}.")
            continue # Try the next ticker in the list

    logger.warning(f"Could not find 'longName' for {symbol} on any exchange. Falling back to symbol.")
    stock_name_cache[base_symbol] = None # Cache the failure to avoid re-querying
    return symbol


def trigger_apps_script_alert(alert_type, row, symbol, exchange):
    """
    Sends a POST request to the Google Apps Script Web App URL to trigger an alert.
    MODIFIED: Now fetches the full stock name from Yahoo Finance just-in-time.
    """
    def _send_request():
        try:
            if not APPS_SCRIPT_WEB_APP_URL or "PASTE YOUR URL HERE" in APPS_SCRIPT_WEB_APP_URL:
                logger.warning("Apps Script Web App URL is not configured. Skipping trigger.")
                return

            # --- MODIFIED: Just-in-time fetching ---
            full_stock_name = get_full_name_from_yahoo(symbol, exchange)

            payload = {
                "alertType": alert_type,
                "row": row,
                "stockName": full_stock_name or symbol # Fallback to symbol if name not found
            }

            logger.info(f"Triggering Apps Script for {alert_type} on row {row} with payload: {payload}")

            response = requests.post(APPS_SCRIPT_WEB_APP_URL, json=payload, timeout=20)

            if response.status_code == 200:
                logger.info(f"Successfully triggered Apps Script. Response: {response.text}")
            else:
                logger.error(f"Failed to trigger Apps Script. Status: {response.status_code}, Response: {response.text}")

        except Exception as e:
            logger.exception(f"An error occurred while trying to trigger the Apps Script alert: {e}")

    # Run the request in a daemon thread so it doesn't block the main script
    trigger_thread = threading.Thread(target=_send_request, daemon=True)
    trigger_thread.start()


def update_connection_status(status_message):
    """Updates a file with the current connection status for external monitoring."""
    try:
        folder_path = os.path.dirname(sys.executable) if getattr(sys, 'frozen', False) else os.path.dirname(os.path.abspath(__file__))
        file_path = os.path.join(folder_path, "connection_status.txt")
        with open(file_path, "w") as f:
            f.write(status_message)
    except Exception as e:
        logger.warning(f"Failed to write connection status file: {e}")

def load_previous_day_high_cache():
    """Loads the previous day's high data from a JSON cache file for the ORH setup."""
    global previous_day_high_cache
    cache_dir = os.path.dirname(sys.executable) if getattr(sys, 'frozen', False) else os.path.dirname(os.path.abspath(__file__))
    cache_path = os.path.join(cache_dir, PREV_DAY_HIGH_CACHE_FILE)
    if os.path.exists(cache_path):
        try:
            with open(cache_path, 'r') as f:
                previous_day_high_cache = json.load(f)
            logger.info(f"Loaded previous day high cache from {cache_path}.")
        except Exception as e:
            logger.error(f"Error loading cache file {cache_path}: {e}. Starting with empty cache.")
            previous_day_high_cache = {}
    else:
        logger.info("Previous day high cache file not found. Starting with empty cache.")
        previous_day_high_cache = {}

def save_previous_day_high_cache():
    """Saves the previous day's high data to a JSON cache file."""
    cache_dir = os.path.dirname(sys.executable) if getattr(sys, 'frozen', False) else os.path.dirname(os.path.abspath(__file__))
    cache_path = os.path.join(cache_dir, PREV_DAY_HIGH_CACHE_FILE)
    try:
        with open(cache_path, 'w') as f:
            json.dump(previous_day_high_cache, f, indent=4)
        logger.info(f"Saved previous day high cache to {cache_path}.")
    except Exception as e:
        logger.error(f"Error saving cache file {cache_path}: {e}")

def is_market_hours():
    """Checks if the current time is within Indian market hours (Mon-Fri, 9:15 AM - 3:30 PM IST)."""
    now = get_ist_time()
    return (0 <= now.weekday() <= 4) and (datetime.time(9, 15) <= now.time() <= datetime.time(15, 30))

def col_to_num(letter):
    """Converts a column letter (e.g., 'A', 'B', 'AA') to a 1-based index."""
    index = 0
    for char in letter.upper():
        index = index * 26 + (ord(char) - ord('A') + 1)
    return index

def get_last_row_in_column(sheet, column_letter):
    """Finds the last row with data in a given column for a gspread worksheet."""
    try:
        column_index = col_to_num(column_letter)
        column_values = sheet.col_values(column_index)
        last_non_empty_index = -1
        for i, val in enumerate(column_values):
            if val and str(val).strip() != '':
                last_non_empty_index = i
        return last_non_empty_index + 1 if last_non_empty_index != -1 else START_ROW_DATA - 1
    except Exception as e:
        logger.error(f"Error in get_last_row_in_column for sheet '{sheet.title}', column '{column_letter}': {e}")
        return START_ROW_DATA - 1

def rgb_to_float(rgb_tuple):
    """Converts an RGB tuple (0-255) to a float dictionary (0-1) for the Google Sheets API."""
    if rgb_tuple is None:
        return {"red": 1.0, "green": 1.0, "blue": 1.0} # Default to white
    return {"red": rgb_tuple[0] / 255.0, "green": rgb_tuple[1] / 255.0, "blue": rgb_tuple[2] / 255.0}

# =====================================================================================================================
#
#                                         --- SMARTAPI WEBSOCKET CLIENT ---
#
# =====================================================================================================================

class SmartWebSocketV2(object):
    """Handles low-level WebSocket communication with the Angel One SmartAPI."""
    ROOT_URI = "wss://smartapisocket.angelone.in/smart-stream"
    HEART_BEAT_MESSAGE = "ping"
    HEART_BEAT_INTERVAL = 10
    LITTLE_ENDIAN_BYTE_ORDER = "<"
    RESUBSCRIBE_FLAG = False
    SUBSCRIBE_ACTION = 1
    UNSUBSCRIBE_ACTION = 0
    LTP_MODE = 1
    QUOTE = 2
    SNAP_QUOTE = 3
    DEPTH = 4
    NSE_CM = 1
    NSE_FO = 2
    BSE_CM = 3
    BSE_FO = 4
    MCX_FO = 5
    NCX_FO = 7
    CDE_FO = 13
    SUBSCRIPTION_MODE_MAP = {1: "LTP", 2: "QUOTE", 3: "SNAP_QUOTE", 4: "DEPTH"}
    wsapp = None
    input_request_dict = {}
    current_retry_attempt = 0

    def __init__(self, auth_token, api_key, client_code, feed_token, max_retry_attempt=1, retry_strategy=0, retry_delay=10, retry_multiplier=2, retry_duration=60):
        self.auth_token = auth_token
        self.api_key = api_key
        self.client_code = client_code
        self.feed_token = feed_token
        self.DISCONNECT_FLAG = True
        self.last_pong_timestamp = None
        self.MAX_RETRY_ATTEMPT = max_retry_attempt
        self.retry_strategy = retry_strategy
        self.retry_delay = retry_delay
        self.retry_multiplier = retry_multiplier
        self.retry_duration = retry_duration
        self._is_connected_flag = False
        if not all([self.auth_token, self.api_key, self.client_code, self.feed_token]):
            raise Exception("Provide valid value for all the tokens")

    def _on_message(self, wsapp, message):
        if message == "pong":
            self._on_pong(wsapp, message)
        else:
            try:
                parsed_message = self._parse_binary_data(message)
                self.on_data(wsapp, parsed_message)
            except Exception as e:
                logger.error(f"Error parsing or handling binary message: {e}. Raw message (first 50 bytes): {message[:50]}...")
                self.on_error(wsapp, f"Data parsing error: {e}")

    def _on_open(self, wsapp):
        self._is_connected_flag = True
        update_connection_status("connected")
        if self.RESUBSCRIBE_FLAG:
            self.resubscribe()
        self.on_open(wsapp)

    def _on_pong(self, wsapp, data):
        self.last_pong_timestamp = time.time()
        logger.info(f"Pong received at {time.strftime('%H:%M:%S', time.localtime(self.last_pong_timestamp))}")

    def _on_ping(self, wsapp, data):
        logger.info("Ping sent.")

    def subscribe(self, correlation_id, mode, token_list):
        try:
            request_data = {"correlationID": correlation_id, "action": self.SUBSCRIBE_ACTION, "params": {"mode": mode, "tokenList": token_list}}
            if self.wsapp and self.wsapp.sock and self.wsapp.sock.connected:
                self.wsapp.send(json.dumps(request_data))
                self.RESUBSCRIBE_FLAG = True
            else:
                logger.warning("WebSocket not connected. Subscription request deferred.")
        except Exception as e:
            logger.error(f"Error occurred during subscribe: {e}")

    def unsubscribe(self, correlation_id, mode, token_list):
        try:
            request_data = {"correlationID": correlation_id, "action": self.UNSUBSCRIBE_ACTION, "params": {"mode": mode, "tokenList": token_list}}
            if self.wsapp and self.wsapp.sock and self.wsapp.sock.connected:
                self.wsapp.send(json.dumps(request_data))
            else:
                logger.warning("WebSocket not connected. Unsubscribe request deferred.")
        except Exception as e:
            logger.error(f"Error occurred during unsubscribe: {e}")

    def resubscribe(self):
        # Implementation for resubscribing if connection drops
        pass

    def connect(self):
        """
        Establishes a WebSocket connection and implements a reconnect mechanism.
        """
        headers = {"Authorization": self.auth_token, "x-api-key": self.api_key, "x-client-code": self.client_code, "x-feed-token": self.feed_token}

        # --- FIX: Implement reconnect loop ---
        self.DISCONNECT_FLAG = False
        while not self.DISCONNECT_FLAG:
            try:
                self._is_connected_flag = False
                update_connection_status("connecting")
                logger.info("Attempting to connect WebSocket...")
                self.wsapp = websocket.WebSocketApp(self.ROOT_URI, header=headers, on_open=self._on_open, on_error=self._on_error, on_close=self._on_close, on_message=self._on_message, on_ping=self._on_ping, on_pong=self._on_pong)
                self.wsapp.run_forever(sslopt={"cert_reqs": ssl.CERT_NONE}, ping_interval=self.HEART_BEAT_INTERVAL)
            except Exception as e:
                logger.error(f"Error occurred during WebSocket connection: {e}")

            if not self.DISCONNECT_FLAG:
                logger.warning("WebSocket connection lost. Retrying in 5 seconds...")
                time.sleep(5)

    def close_connection(self):
        self.RESUBSCRIBE_FLAG = False
        self.DISCONNECT_FLAG = True
        if self.wsapp:
            self.wsapp.close()
        self._is_connected_flag = False
        update_connection_status("disconnected")
        logger.info("WebSocket connection explicitly closed.")

    def _on_error(self, wsapp, error):
        self._is_connected_flag = False
        update_connection_status("disconnecting")
        logger.error(f"Internal WebSocket error: {error}")
        self.on_error(wsapp, error)

    def _on_close(self, wsapp, close_status_code, close_msg):
        self._is_connected_flag = False
        update_connection_status("disconnected")
        logger.warning(f"WebSocket closed. Code: {close_status_code}, Message: {close_msg}")
        self.on_close(wsapp, close_status_code, close_msg)

    def _parse_binary_data(self, binary_data):
        parsed_data = {
            "subscription_mode": self._unpack_data(binary_data, 0, 1, byte_format="B")[0],
            "exchange_type": self._unpack_data(binary_data, 1, 2, byte_format="B")[0],
            "token": self._parse_token_value(binary_data[2:27]),
            "sequence_number": self._unpack_data(binary_data, 27, 35, byte_format="q")[0],
            "exchange_timestamp": self._unpack_data(binary_data, 35, 43, byte_format="q")[0],
            "last_traded_price": self._unpack_data(binary_data, 43, 51, byte_format="q")[0]
        }
        try:
            parsed_data["subscription_mode_val"] = self.SUBSCRIPTION_MODE_MAP.get(parsed_data["subscription_mode"])
            if parsed_data["subscription_mode"] in [self.QUOTE, self.SNAP_QUOTE]:
                parsed_data["last_traded_quantity"] = self._unpack_data(binary_data, 51, 59, byte_format="q")[0]
                parsed_data["average_traded_price"] = self._unpack_data(binary_data, 59, 67, byte_format="q")[0]
                parsed_data["volume_trade_for_the_day"] = self._unpack_data(binary_data, 67, 75, byte_format="q")[0]
                parsed_data["total_buy_quantity"] = self._unpack_data(binary_data, 75, 83, byte_format="d")[0]
                parsed_data["total_sell_quantity"] = self._unpack_data(binary_data, 83, 91, byte_format="d")[0]
                parsed_data["open_price_of_the_day"] = self._unpack_data(binary_data, 91, 99, byte_format="q")[0]
                parsed_data["high_price_of_the_day"] = self._unpack_data(binary_data, 99, 107, byte_format="q")[0]
                parsed_data["low_price_of_the_day"] = self._unpack_data(binary_data, 107, 115, byte_format="q")[0]
                parsed_data["closed_price"] = self._unpack_data(binary_data, 115, 123, byte_format="q")[0]
            return parsed_data
        except Exception as e:
            logger.error(f"Error occurred during binary data parsing: {e}. Data: {binary_data}")
            raise e

    def _unpack_data(self, binary_data, start, end, byte_format="I"):
        return struct.unpack(self.LITTLE_ENDIAN_BYTE_ORDER + byte_format, binary_data[start:end])

    @staticmethod
    def _parse_token_value(binary_packet):
        token = ""
        for i in range(len(binary_packet)):
            if chr(binary_packet[i]) == '\x00': return token
            token += chr(binary_packet[i])
        return token

    # --- Abstract methods to be implemented by child class ---
    def on_open(self, wsapp): pass
    def on_error(self, wsapp, error_message): pass
    def on_close(self, wsapp, close_status_code, close_msg): pass
    def on_data(self, wsapp, data): pass


class MyWebSocketClient(SmartWebSocketV2):
    """
    Custom WebSocket client that implements the combined logic for both the dashboard and the signal setups.
    """
    def on_open(self, wsapp):
        logger.info("WebSocket connection opened.")
        print("[INFO] WebSocket connection opened.")

    def on_error(self, wsapp, error_message):
        logger.error(f"WebSocket error: {error_message}")
        print(f"[ERROR] WebSocket error: {error_message}")

    def on_close(self, wsapp, close_status_code, close_msg):
        logger.warning(f"WebSocket closed. Code: {close_status_code}, Message: {close_msg}")
        print(f"[WARNING] WebSocket closed. Code: {close_status_code}, Message: {close_msg}")

    def on_data(self, wsapp, data):
        """
        This is the unified data handler. It processes each incoming tick for both the
        live dashboard and the 3-minute candle construction for the ORH setup.
        """
        token = data.get('token')
        ltp_raw = data.get('last_traded_price')

        # Scale prices (API provides them as integers, e.g., 12345 is 123.45)
        ltp_scaled = ltp_raw / 100.0 if isinstance(ltp_raw, (int, float)) else None

        if token and ltp_scaled is not None:
            # --- Logic for Live Dashboard (applies to all tokens) ---
            # We only need the LTP from the websocket now.
            latest_tick_data[token]['ltp'] = ltp_scaled

            # --- Logic for ORH Setup (applies ONLY to ORH tokens) ---
            # To make this thread-safe, we check if the token is in a local copy of the setup details.
            with data_lock:
                is_orh_token = token in excel_setup_details

            if is_orh_token:
                current_time = get_ist_time()
                interval = '3min'
                candle_info = interval_ohlc_data[token][interval]

                minute_floor = (current_time.minute // 3) * 3
                candle_start_dt = current_time.replace(minute=minute_floor, second=0, microsecond=0)

                # Check if a new 3-minute candle needs to be started
                if candle_info.get('start_time') is None or candle_start_dt > candle_info['start_time']:
                    if candle_info.get('start_time') is not None:
                        # Finalize and store the completed candle
                        completed_candle = {
                            'open': candle_info['open'], 'high': candle_info['high'], 'low': candle_info['low'],
                            'close': candle_info['last_ltp'], 'start_time': candle_info['start_time']
                        }
                        completed_3min_candles[token].append(completed_candle)

                        # MODIFIED: Use ORH_MAX_CANDLES
                        if len(completed_3min_candles[token]) > ORH_MAX_CANDLES:
                            completed_3min_candles[token].pop(0)

                        logger.info(f"Completed 3min candle for {token}: O={completed_candle['open']:.2f}, H={completed_candle['high']:.2f}, L={completed_candle['low']:.2f}, C={completed_candle['close']:.2f}")

                    # Initialize the new candle
                    candle_info.update({'open': ltp_scaled, 'high': ltp_scaled, 'low': ltp_scaled, 'start_time': candle_start_dt})

                # Update the current (ongoing) candle with the latest tick
                candle_info['high'] = max(candle_info.get('high', ltp_scaled), ltp_scaled)
                candle_info['low'] = min(candle_info.get('low', ltp_scaled), ltp_scaled)
                candle_info['last_ltp'] = ltp_scaled

# =====================================================================================================================
#
#                                         --- SETUP-SPECIFIC FUNCTIONS (ORH & 3% DOWN) ---
#
# =====================================================================================================================

def fetch_initial_candle_data(smart_api_obj, symbols_to_fetch):
    """Fetches historical 3-min candle data for today to pre-populate candles for ORH setup."""
    logger.info("Fetching initial historical 3-min candle data for today (ORH setup)...")

    now_ist = get_ist_time()
    from_date = now_ist.replace(hour=9, minute=15, second=0, microsecond=0).strftime("%Y-%m-%d %H:%M")
    to_date = now_ist.strftime("%Y-%m-%d %H:%M")
    MAX_RETRIES, RETRY_DELAY_SECONDS = 5, 20

    with data_lock:
        symbols_to_fetch_copy = symbols_to_fetch.copy()

    for token, entries in symbols_to_fetch_copy.items():
        if not entries: continue
        symbol_name, exchange_type = entries[0]['symbol'], entries[0]['exchange_type']
        exchange_str = {1: "NSE", 3: "BSE"}.get(exchange_type)
        if not exchange_str:
            logger.warning(f"Cannot fetch history for token {token}, unknown exchange type {exchange_type}"); time.sleep(1); continue

        for attempt in range(MAX_RETRIES):
            try:
                historic_param = {"exchange": exchange_str, "symboltoken": token, "interval": "THREE_MINUTE", "fromdate": from_date, "todate": to_date}
                response = smart_api_obj.getCandleData(historic_param)
                if response and response.get("status") and response.get("data"):
                    completed_3min_candles[token] = [{'start_time': datetime.datetime.fromisoformat(c[0]), 'open': c[1], 'high': c[2], 'low': c[3], 'close': c[4]} for c in response["data"]]
                    # MODIFIED: Use ORH_MAX_CANDLES
                    if len(completed_3min_candles[token]) > ORH_MAX_CANDLES:
                        completed_3min_candles[token] = completed_3min_candles[token][-ORH_MAX_CANDLES:]
                    logger.info(f"Fetched {len(completed_3min_candles[token])} 3-min candles for {symbol_name} (Token: {token}).")
                    break
                else:
                    logger.warning(f"Attempt {attempt + 1}/{MAX_RETRIES}: Could not fetch 3-min data for {symbol_name}. Message: {response.get('message', 'Unknown error')}")
                    if attempt < MAX_RETRIES - 1: time.sleep(RETRY_DELAY_SECONDS)
            except Exception as e:
                logger.warning(f"Attempt {attempt + 1}/{MAX_RETRIES}: Error fetching 3-min data for {symbol_name}: {e}")
                if attempt < MAX_RETRIES - 1: time.sleep(RETRY_DELAY_SECONDS)
        time.sleep(0.4) # Add delay between each symbol fetch to avoid rate limiting

def fetch_previous_day_candle_data(smart_api_obj, symbols_to_fetch):
    """Fetches the previous day's ONE_DAY candle data for ORH setup, using a local cache first."""
    logger.info("Fetching previous day's candle data (ORH setup)...")

    yesterday = datetime.date.today() - timedelta(days=1)
    while yesterday.weekday() >= 5: yesterday -= timedelta(days=1)
    from_date = datetime.datetime.combine(yesterday, datetime.time.min).strftime("%Y-%m-%d %H:%M")
    to_date = datetime.datetime.combine(yesterday, datetime.time.max).strftime("%Y-%m-%d %H:%M")
    yesterday_str = yesterday.strftime("%Y-%m-%d")
    MAX_RETRIES, RETRY_DELAY_SECONDS = 5, 30

    with data_lock:
        symbols_to_fetch_copy = symbols_to_fetch.copy()

    for token, entries in symbols_to_fetch_copy.items():
        if not entries: continue
        symbol_name, exchange_type = entries[0]['symbol'], entries[0]['exchange_type']
        if token in previous_day_high_cache and previous_day_high_cache[token].get('date') == yesterday_str:
            logger.info(f"Previous Day's High for {symbol_name} (Token: {token}): {previous_day_high_cache[token]['high']:.2f} (from cache)"); time.sleep(0.1); continue

        exchange_str = {1: "NSE", 3: "BSE"}.get(exchange_type)
        if not exchange_str:
            logger.warning(f"Cannot fetch previous day history for token {token}, unknown exchange type {exchange_type}"); time.sleep(1); continue

        for attempt in range(MAX_RETRIES):
            try:
                historic_param = {"exchange": exchange_str, "symboltoken": token, "interval": "ONE_DAY", "fromdate": from_date, "todate": to_date}
                response = smart_api_obj.getCandleData(historic_param)
                if response and response.get("status") and response.get("data"):
                    if response["data"]:
                        previous_day_high = response["data"][0][2]
                        logger.info(f"Previous Day's High for {symbol_name} (Token: {token}): {previous_day_high:.2f} (fetched from API)")
                        previous_day_high_cache[token] = {'date': yesterday_str, 'high': previous_day_high}
                        save_previous_day_high_cache()
                        break
                    else: logger.warning(f"No previous day's candle data found for {token} ({symbol_name})."); break
                else:
                    logger.warning(f"Attempt {attempt + 1}/{MAX_RETRIES}: Could not fetch data for {symbol_name}. Message: {response.get('message', 'Unknown error')}")
                    if attempt < MAX_RETRIES - 1: time.sleep(RETRY_DELAY_SECONDS)
            except Exception as e:
                logger.warning(f"Attempt {attempt + 1}/{MAX_RETRIES}: Exception while fetching data for {symbol_name}: {e}")
                if attempt < MAX_RETRIES - 1: time.sleep(RETRY_DELAY_SECONDS)
        time.sleep(0.4) # Add delay between each symbol fetch to avoid rate limiting

def fetch_historical_candles_for_3pct_down(smart_api_obj, tokens_to_fetch, interval_api):
    """
    Fetches historical data for price/volume setups from the previous week's Monday to now.
    """
    today = get_ist_time()
    if today.weekday() == 5: # If Saturday
        to_dt = today - timedelta(days=1)
    elif today.weekday() == 6: # If Sunday
        to_dt = today - timedelta(days=2)
    else:
        to_dt = today

    to_dt = to_dt.replace(hour=23, minute=59, second=59)
    days_to_last_monday = to_dt.weekday() + 7
    from_dt = datetime.datetime.combine(to_dt.date() - timedelta(days=days_to_last_monday), datetime.time.min)

    from_date_str = from_dt.strftime("%Y-%m-%d %H:%M")
    to_date_str = to_dt.strftime("%Y-%m-%d %H:%M")

    logger.info(f"Fetching {interval_api} candles from {from_date_str} to {to_date_str}...")

    with data_lock:
        setup_details_copy = excel_3pct_setup_details.copy()
        dynamic_setup_copy = excel_dynamic_setups.copy()

        # Combine tokens from both setup types to avoid duplicate API calls
        all_tokens_to_fetch = set(tokens_to_fetch)
        for token, details in dynamic_setup_copy.items():
            if details:
                all_tokens_to_fetch.add((token, details[0]['exchange_type']))

        tokens_to_fetch_unique = list(all_tokens_to_fetch)

    for token_info in tokens_to_fetch_unique:
        token, exchange_type = token_info[0], token_info[1]

        # Get symbol name from either dictionary
        symbol_name = 'Unknown'
        if token in setup_details_copy and setup_details_copy[token]:
            symbol_name = setup_details_copy[token][0].get('symbol', 'Unknown')
        elif token in dynamic_setup_copy and dynamic_setup_copy[token]:
            symbol_name = dynamic_setup_copy[token][0].get('symbol', 'Unknown')

        exchange_str = {1: "NSE", 3: "BSE"}.get(exchange_type)
        if not exchange_str:
            logger.warning(f"Cannot fetch history for token {token}, unknown exchange type {exchange_type}")
            time.sleep(1)
            continue

        try:
            historic_param = {"exchange": exchange_str, "symboltoken": token, "interval": interval_api, "fromdate": from_date_str, "todate": to_date_str}
            response = smart_api_obj.getCandleData(historic_param)

            if response and response.get("status") and response.get("data"):
                candle_data = response["data"]

                candle_history = []
                for c in candle_data:
                    candle_history.append({
                        'start_time': datetime.datetime.fromisoformat(c[0]), 'open': c[1], 'high': c[2],
                        'low': c[3], 'close': c[4], 'volume': c[5] if len(c) > 5 else 0
                    })

                with data_lock:
                    volume_history_3pct[token][interval_api] = candle_history

                logger.info(f"Fetched {len(candle_history)} candles for {symbol_name} ({interval_api}).")
            else:
                logger.warning(f"Fetch error for {symbol_name} ({interval_api}). Message: {response.get('message', 'Unknown error')}")
        except Exception as e:
            logger.error(f"Exception fetching data for {symbol_name} ({interval_api}): {e}")

        time.sleep(0.5)

def fetch_monthly_highs(smart_api_obj, tokens_to_fetch):
    """
    Fetches the high of the current and previous month for given tokens.
    Stores the higher of the two in the monthly_high_cache.
    """
    global monthly_high_cache
    logger.info("Fetching monthly high data for Swing Low setup...")

    today = datetime.date.today()
    # Previous month start
    prev_month_start = (today.replace(day=1) - timedelta(days=1)).replace(day=1)

    from_date_str = prev_month_start.strftime("%Y-%m-%d %H:%M")
    to_date_str = today.strftime("%Y-%m-%d %H:%M")

    with data_lock:
        tokens_to_fetch_copy = list(tokens_to_fetch)

    for token, exchange_type in tokens_to_fetch_copy:
        exchange_str = {1: "NSE", 3: "BSE"}.get(exchange_type)
        if not exchange_str:
            continue

        try:
            historic_param = {
                "exchange": exchange_str,
                "symboltoken": token,
                "interval": "ONE_DAY", # Fetch daily data to calculate monthly high
                "fromdate": from_date_str,
                "todate": to_date_str
            }
            response = smart_api_obj.getCandleData(historic_param)

            if response and response.get("status") and response.get("data"):
                data = response["data"]
                current_month_high = 0
                prev_month_high = 0

                for c in data:
                    candle_date = datetime.datetime.fromisoformat(c[0]).date()
                    candle_high = c[2]

                    if candle_date.month == today.month and candle_date.year == today.year:
                        current_month_high = max(current_month_high, candle_high)
                    elif candle_date.month == prev_month_start.month and candle_date.year == prev_month_start.year:
                        prev_month_high = max(prev_month_high, candle_high)

                final_high = max(current_month_high, prev_month_high)
                if final_high > 0:
                    with data_lock:
                        monthly_high_cache[token] = final_high
                    logger.info(f"Updated monthly high for token {token}: {final_high:.2f}")

            else:
                logger.warning(f"Could not fetch monthly data for token {token}. Response: {response.get('message', 'Unknown error')}")
        except Exception as e:
            logger.error(f"Exception fetching monthly data for token {token}: {e}")

        time.sleep(0.5) # API rate limiting

def check_and_update_orh_setup():
    """
    Checks the latest completed 3-min candle for ORH setup, updates Google Sheet,
    and creates a pre-filled order row in the 'Orders' sheet.
    """
    logger.info("Checking latest 3-min candle for ORH setup...")
    updates_queued = []

    dashboard_data = Dashboard.get_all_values()

    with data_lock:
        setup_details_copy = excel_setup_details.copy()

    for token, symbol_entries in setup_details_copy.items():
        filtered_symbol_entries = [entry for entry in symbol_entries if entry['row'] <= ORH_MAX_ROW]
        if not filtered_symbol_entries:
            continue

        candles, prev_high_entry = completed_3min_candles.get(token, []), previous_day_high_cache.get(token)
        if not candles or not prev_high_entry or not prev_high_entry.get("high"):
            continue

        prev_high = prev_high_entry["high"]
        latest_candle = candles[-1]
        high, low, close = latest_candle['high'], latest_candle['low'], latest_candle['close']

        for entry in filtered_symbol_entries:
            row, row_idx = entry["row"], entry["row"] - 1

            current_orh_value = dashboard_data[row_idx][col_to_num(ORH_RESULT_COL) - 1] if len(dashboard_data) > row_idx and len(dashboard_data[row_idx]) >= col_to_num(ORH_RESULT_COL) else ""

            if "Yes" in current_orh_value:
                continue

            if high != low and close >= (low + 0.7 * (high - low)) and close > prev_high:
                trigger_time_str = latest_candle['start_time'].strftime('%H:%M')
                buy_stop_value = round(low * 0.995, 2)

                logger.info(f"ORH Triggered for {entry['symbol']} at {latest_candle['start_time']:%Y-%m-%d %H:%M:%S} > H:{high}, L:{low}, C:{close}")

                if winsound:
                    try:
                        winsound.Beep(1000, 400)
                    except Exception as e:
                        logger.warning(f"Sound alert failed: {e}")

                updates_queued.append({"range": f"{ORH_RESULT_COL}{row}", "values": [[f"Yes, {high:.2f} ({trigger_time_str})"]]})

                if high > 0:
                    stop_loss_pct = (high - buy_stop_value) / high
                    buy_stop_output = f"{buy_stop_value:.2f} ({stop_loss_pct:.2%})"
                else:
                    buy_stop_output = f"{buy_stop_value:.2f}"
                updates_queued.append({"range": f"{ORH_BUY_STOP_COL}{row}", "values": [[buy_stop_output]]})

                # --- TRIGGER APPS SCRIPT FOR NEW TRADE ---
                trigger_apps_script_alert("new_trade", row, entry['symbol'], entry['exchange'])
                time.sleep(2) # Add a delay to prevent overwhelming the Apps Script

                try:
                    symbol = dashboard_data[row_idx][col_to_num(ORH_SYMBOL_COL) - 1]
                    exchange = dashboard_data[row_idx][col_to_num(ORH_EXCHANGE_COL) - 1]
                    quantity = dashboard_data[row_idx][col_to_num(ORH_QTY_COL) - 1]

                    if not quantity or int(quantity) <= 0:
                        logger.warning(f"Cannot create order for {symbol}, quantity is missing or zero in column I.")
                        continue

                    trigger_price = round(high * 1.005, 2)

                    new_order_row = [
                        get_ist_time().strftime("%Y-%m-%d %H:%M:%S"),
                        symbol,
                        exchange,
                        "BUY",
                        "STOPLOSS_MARKET",
                        quantity,
                        trigger_price,
                        ""
                    ]

                    OrdersSheet.append_row(new_order_row, value_input_option='USER_ENTERED')
                    logger.info(f"Successfully created a pre-filled order row for {symbol} in the 'Orders' sheet.")

                except Exception as e:
                    logger.exception(f"Failed to create order row for {symbol}: {e}")

    if updates_queued:
        Dashboard.batch_update(updates_queued)
        logger.info(f"Applied {len(updates_queued)} ORH updates to Dashboard.")
    else:
        logger.info("No ORH setup updates needed.")


def check_and_update_price_volume_setups():
    """
    Checks for 3% down, high volume, and highest up candle setups based on historical data.
    Implements proximity consolidation and price-based sorting for the output.
    """
    logger.info("Checking for Price/Volume setups with proximity consolidation and price sorting...")
    updates_queued = []

    try:
        # Fetch current values from the sheet to prevent redundant updates
        pct_down_result_col_values = Dashboard.col_values(col_to_num(PCT_DOWN_RESULT_COL))
        high_volume_result_col_values = Dashboard.col_values(col_to_num(HIGH_VOL_RESULT_COL))
        highest_up_col_values = Dashboard.col_values(col_to_num(HIGHEST_UP_CANDLE_COL))
    except Exception as e:
        logger.error(f"Could not fetch column values for setup checks: {e}")
        pct_down_result_col_values, high_volume_result_col_values, highest_up_col_values = [], [], []

    with data_lock:
        setup_details_copy = excel_3pct_setup_details.copy()
        volume_history_copy = volume_history_3pct.copy()

    def format_consolidated_output(candle_dict):
        """
        New formatting logic:
        1. Consolidates signals if their lows are within a 2% threshold.
        2. For consolidated signals, keeps only the highest timeframe.
        3. Sorts the final, unique signals by low price (high to low).
        4. Formats the output string as "price (timeframe)".
        """
        # 1. Get all valid candidate candles
        available_candles = [
            {'candle': c, 'interval_api': interval_api}
            for interval_api, c in candle_dict.items() if c
        ]
        if not available_candles:
            return ""

        # Sort by low price initially to make grouping easier
        available_candles.sort(key=lambda x: x['candle']['low'])

        # 2. Group candles by proximity (2% threshold)
        groups = []
        if available_candles:
            current_group = [available_candles[0]]
            for i in range(1, len(available_candles)):
                # Check if current candle's low is within 2% of the first candle in the group
                if available_candles[i]['candle']['low'] <= current_group[0]['candle']['low'] * 1.02:
                    current_group.append(available_candles[i])
                else:
                    groups.append(current_group)
                    current_group = [available_candles[i]]
            groups.append(current_group)

        # 3. For each group, select the one with the highest timeframe
        unique_signals = []
        for group in groups:
            best_in_group = max(group, key=lambda info: CANDLE_INTERVALS_3PCT_API.index(info['interval_api']))
            unique_signals.append(best_in_group)

        # 4. Sort the final unique signals by low price, descending (high to low)
        sorted_unique_signals = sorted(unique_signals, key=lambda info: info['candle']['low'], reverse=True)

        # 5. Format the final output string
        output_parts = []
        for signal_info in sorted_unique_signals:
            candle = signal_info['candle']
            interval_name = CANDLE_INTERVAL_MAP_DISPLAY[signal_info['interval_api']]
            output_parts.append(f"{candle['low']:.2f} ({interval_name})")

        return ", ".join(output_parts)

    for token, symbol_entries in setup_details_copy.items():
        three_pct_down_candles = {}
        high_vol_candles = {}
        highest_up_candles = {}

        # --- Phase 1: Find the best candidate candle for each timeframe ---
        for interval_api in CANDLE_INTERVALS_3PCT_API:
            candle_history = volume_history_copy.get(token, {}).get(interval_api)
            if not candle_history:
                continue

            triggered_3pct_candles = [c for c in candle_history if c.get('high', 0) > 0 and (c['high'] - c['close']) / c['high'] >= 0.03]
            if triggered_3pct_candles:
                # Prioritize the most recent candle, not the one with the lowest price.
                three_pct_down_candles[interval_api] = triggered_3pct_candles[-1]

            if any(c.get('volume', 0) > 0 for c in candle_history):
                high_vol_candles[interval_api] = max(candle_history, key=lambda c: c.get('volume', 0))

            gainer_candles = [c for c in candle_history if c.get('open', 0) > 0]
            if gainer_candles:
                highest_up_candles[interval_api] = max(gainer_candles, key=lambda c: (c['close'] - c['open']) / c['open'])

        # --- Phase 2: Consolidate and format outputs using the new logic ---
        final_output_3_pct = format_consolidated_output(three_pct_down_candles)
        final_output_high_vol = format_consolidated_output(high_vol_candles)
        final_output_highest_up = format_consolidated_output(highest_up_candles)

        # --- Phase 3: Prepare updates for the sheet, only if the value has changed ---
        for entry in symbol_entries:
            row, row_idx = entry["row"], entry["row"] - 1

            if str(pct_down_result_col_values[row_idx] if row_idx < len(pct_down_result_col_values) else "").strip() != final_output_3_pct.strip():
                updates_queued.append({"range": f"{PCT_DOWN_RESULT_COL}{row}", "values": [[final_output_3_pct]]})

            if str(high_volume_result_col_values[row_idx] if row_idx < len(high_volume_result_col_values) else "").strip() != final_output_high_vol.strip():
                updates_queued.append({"range": f"{HIGH_VOL_RESULT_COL}{row}", "values": [[final_output_high_vol]]})

            if str(highest_up_col_values[row_idx] if row_idx < len(highest_up_col_values) else "").strip() != final_output_highest_up.strip():
                updates_queued.append({"range": f"{HIGHEST_UP_CANDLE_COL}{row}", "values": [[final_output_highest_up]]})

    if updates_queued:
        Dashboard.batch_update(updates_queued)
        logger.info(f"Applied {len(updates_queued)} Price/Volume setup updates to Dashboard.")
    else:
        logger.info("No Price/Volume setup updates were needed.")


# =====================================================================================================================
#
#                                --- START: NEW DYNAMIC SETUP LOGIC ---
#
# =====================================================================================================================

def check_and_update_dynamic_setups():
    """
    Finds the dynamic setup section in the sheet and updates it based on specialized
    candle analysis, now with intelligent consolidation logic.
    """
    logger.info("Checking for dynamic setups with consolidation...")

    with data_lock:
        dynamic_setups_copy = excel_dynamic_setups.copy()
        volume_history_copy = volume_history_3pct.copy()

    if not dynamic_setups_copy:
        logger.info("No symbols found in the dynamic setup list. Skipping.")
        return

    try:
        # --- Part 1: Dynamically find the location of the setup headers ---
        all_sheet_data = Dashboard.get_all_values()
        header_row_idx = -1
        header_map = {}

        for i, row_data in enumerate(all_sheet_data):
            if "Low of Highest Down candle" in row_data and "High of Highest Up candle" in row_data:
                header_row_idx = i
                for j, cell_value in enumerate(row_data):
                    col_letter = chr(ord('A') + j)
                    if "Symbol" in cell_value: header_map['symbol'] = col_letter
                    if "Exch." in cell_value: header_map['exchange'] = col_letter
                    if "Low of Highest Down candle" in cell_value: header_map['low_down'] = col_letter
                    if "High of Highest Up candle" in cell_value: header_map['high_up'] = col_letter
                logger.info(f"Found dynamic setup headers at row {header_row_idx + 1}. Header map: {header_map}")
                break

        if header_row_idx == -1 or not all(k in header_map for k in ['symbol', 'exchange', 'low_down', 'high_up']):
            logger.warning("Could not find the required headers for the dynamic setup on the Dashboard sheet. Skipping.")
            return

        updates_queued = []

        # --- NEW: Reusable consolidation function ---
        def format_dynamic_consolidated_output(candle_dict, price_key_to_group, price_key_to_display):
            if not candle_dict: return ""

            available_candles = [{'candle': c, 'interval_api': interval_api} for interval_api, c in candle_dict.items() if c]
            if not available_candles: return ""

            available_candles.sort(key=lambda x: x['candle'][price_key_to_group])

            groups = []
            if available_candles:
                current_group = [available_candles[0]]
                for i in range(1, len(available_candles)):
                    if available_candles[i]['candle'][price_key_to_group] <= current_group[0]['candle'][price_key_to_group] * 1.02:
                        current_group.append(available_candles[i])
                    else:
                        groups.append(current_group)
                        current_group = [available_candles[i]]
                groups.append(current_group)

            unique_signals = [max(group, key=lambda info: CANDLE_INTERVALS_3PCT_API.index(info['interval_api'])) for group in groups]
            sorted_unique_signals = sorted(unique_signals, key=lambda info: info['candle'][price_key_to_group], reverse=True)

            output_parts = []
            for signal_info in sorted_unique_signals:
                candle = signal_info['candle']
                interval_name = CANDLE_INTERVAL_MAP_DISPLAY[signal_info['interval_api']]
                output_parts.append(f"{candle[price_key_to_display]:.2f} ({interval_name})")

            return ", ".join(output_parts)

        # --- Part 2: Process each symbol in the dynamic list ---
        for token, symbol_entries in dynamic_setups_copy.items():
            if not symbol_entries: continue

            highest_down_candles = {}
            highest_up_candles = {}

            for interval_api in CANDLE_INTERVALS_3PCT_API:
                candle_history = volume_history_copy.get(token, {}).get(interval_api)
                if not candle_history: continue

                candles_with_drop = [c for c in candle_history if c.get('high', 0) > 0]
                if candles_with_drop:
                    highest_down_candles[interval_api] = max(candles_with_drop, key=lambda c: (c['high'] - c['close']) / c['high'])

                candles_with_bounce = [c for c in candle_history if c.get('low', 0) > 0]
                if candles_with_bounce:
                    highest_up_candles[interval_api] = max(candles_with_bounce, key=lambda c: (c['close'] - c['low']) / c['low'])

            # --- Part 3: Format outputs using the new consolidation logic ---
            final_output_low_down = format_dynamic_consolidated_output(highest_down_candles, 'low', 'low')
            final_output_high_up = format_dynamic_consolidated_output(highest_up_candles, 'high', 'high')

            # --- Part 4: Queue updates for the sheet ---
            for entry in symbol_entries:
                row = entry["row"]
                row_idx = row - 1

                if row_idx >= len(all_sheet_data): continue

                current_low_down_val = all_sheet_data[row_idx][col_to_num(header_map['low_down']) - 1] if len(all_sheet_data[row_idx]) >= col_to_num(header_map['low_down']) else ""
                current_high_up_val = all_sheet_data[row_idx][col_to_num(header_map['high_up']) - 1] if len(all_sheet_data[row_idx]) >= col_to_num(header_map['high_up']) else ""

                if final_output_low_down.strip() != str(current_low_down_val).strip():
                    updates_queued.append({'range': f"{header_map['low_down']}{row}", 'values': [[final_output_low_down]]})

                if final_output_high_up.strip() != str(current_high_up_val).strip():
                    updates_queued.append({'range': f"{header_map['high_up']}{row}", 'values': [[final_output_high_up]]})

        if updates_queued:
            Dashboard.batch_update(updates_queued)
            logger.info(f"Applied {len(updates_queued)} dynamic setup updates to Dashboard.")
        else:
            logger.info("No dynamic setup updates were needed.")

    except Exception as e:
        logger.exception(f"An error occurred in check_and_update_dynamic_setups: {e}")

# =====================================================================================================================
#
#                                --- END: NEW DYNAMIC SETUP LOGIC ---
#
# =====================================================================================================================


# =====================================================================================================================
#
#                                --- START: NEW ORDER TRACKING LOGIC BASED ON DOCUMENT ---
#
# =====================================================================================================================

def check_and_place_orders():
    """
    Scans the 'Orders' sheet for rows marked 'PENDING' in column L, validates,
    and submits them to the broker. Sets an initial status of 'ACCEPTED' or 'ERROR'.
    MODIFIED: Uses batch updates to avoid API quota errors.
    """
    try:
        if not OrdersSheet:
            logger.warning("OrdersSheet object not initialized. Skipping order placement.")
            return

        all_values = OrdersSheet.get_all_values()
        if len(all_values) < 3: return

        updates_to_make = []

        for idx, row_data in enumerate(all_values):
            row_num = idx + 1
            if row_num < 3: continue

            try:
                # Trigger from Column L (index 11)
                if len(row_data) < 12: continue
                trigger_status = str(row_data[11]).strip().upper()

                if trigger_status != 'PENDING': continue

                logger.info(f"Found a pending submission on row {row_num}: {row_data}")

                # Immediately update status to 'PROCESSING' to prevent re-submission
                updates_to_make.append({'range': f'J{row_num}', 'values': [['PROCESSING']]})
                # Clear the trigger column immediately in the same batch
                updates_to_make.append({'range': f'L{row_num}', 'values': [['']]})

                symbol = str(row_data[2])
                exchange = str(row_data[3])
                action = str(row_data[4]).upper()
                order_type = str(row_data[5]).upper()
                quantity = int(row_data[6])
                price_or_trigger = float(row_data[7] or 0)

                if not all([symbol, exchange, action, order_type, quantity > 0]):
                    raise ValueError("Missing or invalid required fields (Symbol, Exchange, Action, Type, Qty)")

                token_cache = {}
                instrument_info = get_or_fetch_instrument_details(symbol, exchange, token_cache)
                if not instrument_info:
                    raise ValueError(f"Could not find token for symbol {symbol}")
                token = instrument_info['token']

                order_id_or_rule_id = None
                response_data = None # Initialize response data for logging

                if order_type == 'GTT':
                    logger.info(f"Submitting GTT order for row {row_num}...")
                    gtt_params = {
                        "tradingsymbol": symbol, "symboltoken": token, "exchange": exchange,
                        "transactiontype": action, "producttype": "DELIVERY", "price": price_or_trigger,
                        "qty": quantity, "triggerprice": price_or_trigger, "disclosedqty": 0, "timeperiod": 365
                    }
                    response_data = smart_api_obj.gttCreateRule(gtt_params)

                    if isinstance(response_data, int) and response_data > 0:
                        order_id_or_rule_id = response_data
                    elif isinstance(response_data, dict) and response_data.get("data"):
                        data_payload = response_data["data"]
                        order_id_or_rule_id = data_payload.get("ruleid") if isinstance(data_payload, dict) else data_payload

                    if not order_id_or_rule_id:
                        error_message = "GTT creation failed."
                        if isinstance(response_data, dict):
                            error_message = response_data.get("message", error_message)
                        logger.error(f"Full API response for failed GTT: {response_data}")
                        raise DataException(error_message)

                else: # Handle regular orders
                    logger.info(f"Submitting regular order for row {row_num}...")
                    order_params = {
                        "variety": "NORMAL", "tradingsymbol": symbol, "symboltoken": token,
                        "transactiontype": action, "exchange": exchange, "ordertype": order_type,
                        "producttype": "DELIVERY", "duration": "DAY", "quantity": quantity
                    }
                    if order_type == 'LIMIT':
                        order_params["price"] = price_or_trigger
                    elif order_type == 'STOPLOSS_MARKET':
                        order_params["triggerprice"] = price_or_trigger
                        order_params["price"] = 0.0
                    else: # MARKET order
                        order_params["price"] = 0.0

                    response_data = smart_api_obj.placeOrder(order_params)

                    if isinstance(response_data, dict) and response_data.get("data", {}).get("orderid"):
                        order_id_or_rule_id = response_data["data"]["orderid"]
                    else:
                        error_message = "Order placement failed."
                        if isinstance(response_data, dict):
                            error_message = response_data.get("message", error_message)
                        logger.error(f"Full API response for failed order: {response_data}")
                        raise DataException(error_message)

                # --- UNIFIED SUCCESS UPDATE ---
                logger.info(f"Order for row {row_num} accepted by broker. ID: {order_id_or_rule_id}")
                updates_to_make.append({'range': f'J{row_num}:K{row_num}', 'values': [['ACCEPTED', str(order_id_or_rule_id)]]})

            except Exception as e:
                error_text = str(e.message) if hasattr(e, 'message') and e.message else str(e)
                logger.error(f"Failed to process order for row {row_num}: {error_text}")
                updates_to_make.append({'range': f'J{row_num}:K{row_num}', 'values': [['ERROR', error_text]]})

            time.sleep(1) # Rate limit API calls

        # --- BATCH UPDATE AT THE END ---
        if updates_to_make:
            OrdersSheet.batch_update(updates_to_make, value_input_option='USER_ENTERED')
            logger.info(f"Applied {len(updates_to_make)} order placement updates to the Orders sheet.")

    except Exception as e:
        logger.exception(f"An error occurred in the main order placement function: {e}")


def check_and_update_order_statuses():
    """
    Efficiently queries the broker for the live status of all trackable orders,
    normalizes the status, and updates the Google Sheet. Handles the GTT-to-order transition.
    """
    try:
        if not OrdersSheet or not smart_api_obj: return

        # --- 1. Fetch all data from sheet and APIs upfront with robust error handling ---
        all_order_rows = OrdersSheet.get_all_values()
        if len(all_order_rows) < 3: return

        # Initialize with default empty values
        all_gtt_rules_raw = {}
        order_book_raw = {}

        try:
            # CORRECTED: Use gttLists with the mandatory status parameter
            all_gtt_rules_raw = smart_api_obj.gttLists(status=['active', 'triggered'], page=1, count=200)
        except DataException as e:
            logger.warning(f"Could not fetch GTT list, API returned an error or empty response: {e}. Assuming no active GTTs.")
        except Exception as e:
            logger.error(f"An unexpected error occurred while fetching GTT list: {e}")

        try:
            order_book_raw = smart_api_obj.orderBook()
        except DataException as e:
            logger.warning(f"Could not fetch order book, API returned an error or empty response: {e}. Assuming no open orders.")
        except Exception as e:
            logger.error(f"An unexpected error occurred while fetching the order book: {e}")

        # --- 2. Create efficient lookup maps ---
        gtt_status_map = {str(rule['id']): rule for rule in all_gtt_rules_raw.get('data', [])} if all_gtt_rules_raw and all_gtt_rules_raw.get('data') else {}
        order_status_map = {order['orderid']: order for order in order_book_raw.get('data', [])} if order_book_raw and order_book_raw.get('data') else {}

        updates_queued = []

        # --- 3. Iterate through sheet rows and update status ---
        for idx, row in enumerate(all_order_rows):
            row_num = idx + 1
            if row_num < 3: continue

            try:
                # Columns: C=Symbol(2), D=Exch(3), E=Action(4), F=Type(5), G=Qty(6), J=Status(9), K=ID(10)
                if len(row) < 11: continue

                current_status_on_sheet = str(row[9]).strip().upper()
                order_id = str(row[10]).strip()
                order_type = str(row[5]).strip().upper()

                # Skip rows that are in a terminal state or don't need tracking
                if current_status_on_sheet in ['COMPLETED', 'REJECTED', 'CANCELLED', 'ERROR', 'EXPIRED'] or not order_id:
                    continue

                new_status_normalized = None
                raw_api_status = None

                # --- Logic for GTT Orders ---
                if order_type == 'GTT':
                    if order_id in gtt_status_map:
                        raw_api_status = gtt_status_map[order_id]['status']
                        new_status_normalized = normalize_status(raw_api_status)

                        # --- "Bridge the Gap": Handle GTT-to-Order transition ---
                        if new_status_normalized == 'Triggered - Awaiting ID':
                            logger.info(f"GTT rule {order_id} triggered. Searching for matching exchange order...")

                            gtt_rule = gtt_status_map[order_id]
                            found_match = False
                            for live_order in order_status_map.values():
                                if (live_order['tradingsymbol'] == gtt_rule['tradingsymbol'] and
                                    str(live_order['quantity']) == str(gtt_rule['qty']) and
                                    live_order['transactiontype'] == gtt_rule['transactiontype']):

                                    new_exchange_id = live_order['orderid']
                                    logger.info(f"Found matching exchange order for GTT {order_id}. New Order ID: {new_exchange_id}")

                                    # Update Order ID to new exchange ID and Type to GTT-LIMIT
                                    updates_queued.append({'range': f'F{row_num}', 'values': [['GTT-LIMIT']]})
                                    updates_queued.append({'range': f'K{row_num}', 'values': [[new_exchange_id]]})

                                    # Get status of the new order
                                    raw_api_status = live_order['status']
                                    new_status_normalized = normalize_status(raw_api_status)
                                    found_match = True
                                    break
                            if not found_match:
                                logger.warning(f"GTT {order_id} is triggered, but no matching order found in order book yet.")

                    else: # GTT ID not found in active/triggered list
                        # --- RESILIENT LOGIC FOR RACE CONDITION ---
                        if current_status_on_sheet == 'ACCEPTED':
                            logger.info(f"GTT Rule {order_id} not yet in list (propagation delay). Keeping status as ACCEPTED.")
                            new_status_normalized = 'ACCEPTED'
                        else:
                            raw_api_status = 'not_found_in_gtt_list'
                            new_status_normalized = normalize_status(raw_api_status)

                # --- Logic for Regular Exchange Orders (and triggered GTTs) ---
                elif order_type in ['LIMIT', 'MARKET', 'STOPLOSS_MARKET', 'GTT-LIMIT']:
                    if order_id in order_status_map:
                        raw_api_status = order_status_map[order_id]['status']
                        new_status_normalized = normalize_status(raw_api_status)
                    else:
                        raw_api_status = 'not_found_in_order_book'
                        new_status_normalized = normalize_status(raw_api_status)

                # --- Queue update if status has changed ---
                if new_status_normalized and new_status_normalized.upper() != current_status_on_sheet:
                    logger.info(f"Updating status for ID {order_id} (Row {row_num}) from '{current_status_on_sheet}' to '{new_status_normalized}'")
                    updates_queued.append({'range': f'J{row_num}', 'values': [[new_status_normalized]]})

            except Exception as e:
                logger.warning(f"Could not check status for row {row_num}: {e}")
                continue

        # --- 4. Batch update the sheet ---
        if updates_queued:
            OrdersSheet.batch_update(updates_queued, value_input_option='USER_ENTERED')
            logger.info(f"Applied {len(updates_queued)} live order status updates to the Orders sheet.")

    except Exception as e:
        logger.exception(f"An error occurred in the order status checker function: {e}")

# =====================================================================================================================
#
#                                --- END: NEW ORDER TRACKING LOGIC BASED ON DOCUMENT ---
#
# =====================================================================================================================

def check_and_update_breakdown_status():
    """
    Checks the status of price/volume setups based on the last completed candle.
    Updates the status column to "Breakdown" or clears it.
    MODIFIED: Now only triggers alerts if the suggestion text has genuinely changed.
    """
    logger.info("Checking for breakdown status and suggestions...")
    value_updates = []
    format_updates = []

    with data_lock:
        setup_details_copy = excel_3pct_setup_details.copy()
        volume_history_copy = volume_history_3pct.copy()

    if not setup_details_copy:
        return

    rev_interval_map = {v: k for k, v in CANDLE_INTERVAL_MAP_DISPLAY.items()}

    setups_to_check = [
        {'result_col': HIGHEST_UP_CANDLE_COL, 'status_col': HIGHEST_UP_CANDLE_STATUS_COL},
        {'result_col': HIGH_VOL_RESULT_COL, 'status_col': HIGH_VOL_STATUS_COL},
        {'result_col': PCT_DOWN_RESULT_COL, 'status_col': PCT_DOWN_STATUS_COL}
    ]

    try:
        all_cols = [s['result_col'] for s in setups_to_check] + [s['status_col'] for s in setups_to_check]
        all_cols.extend([SUGGESTION_COL, FULL_QTY_COL, FULL_POSITIONS_END_COL]) # Add suggestion, qty, and timestamp columns
        start_col = min(all_cols, key=col_to_num)
        end_col = max(all_cols, key=col_to_num)

        last_row = max((e['row'] for token, entries in setup_details_copy.items() for e in entries if entries), default=0)
        if last_row < START_ROW_DATA:
            return

        range_to_get = f"{start_col}{START_ROW_DATA}:{end_col}{last_row}"
        sheet_data = Dashboard.get(range_to_get, value_render_option='FORMATTED_VALUE')

    except Exception as e:
        logger.error(f"Failed to fetch setup data from Google Sheet: {e}")
        return

    for token, symbol_entries in setup_details_copy.items():
        for entry in symbol_entries:
            row = entry["row"]
            row_idx = row - START_ROW_DATA

            if row_idx < 0 or row_idx >= len(sheet_data):
                continue

            row_statuses = {}
            breakdown_details = collections.defaultdict(list)

            # First, determine the status for each of the three setups
            for setup in setups_to_check:
                result_col_letter = setup['result_col']
                status_col_letter = setup['status_col']

                result_col_idx = col_to_num(result_col_letter) - col_to_num(start_col)
                status_col_idx = col_to_num(status_col_letter) - col_to_num(start_col)

                signal_text = sheet_data[row_idx][result_col_idx] if len(sheet_data[row_idx]) > result_col_idx else ""
                current_status = sheet_data[row_idx][status_col_idx] if len(sheet_data[row_idx]) > status_col_idx else ""

                signals = re.findall(r"(\d+\.?\d*)\s*\(([^)]+)\)", str(signal_text))

                breakdown_count = 0
                if signals:
                    for low_str, timeframe_str in signals:
                        signal_low = float(low_str)
                        api_timeframe = rev_interval_map.get(timeframe_str.strip())

                        if not api_timeframe: continue
                        candle_history = volume_history_copy.get(token, {}).get(api_timeframe)
                        if not candle_history: continue

                        last_completed_candle = candle_history[-1]
                        if last_completed_candle['close'] < signal_low:
                            breakdown_count += 1
                            breakdown_details[status_col_letter].append({
                                'close': last_completed_candle['close'],
                                'low': signal_low
                            })

                new_status = "/".join(["Breakdown"] * breakdown_count) if breakdown_count > 0 else ""
                row_statuses[status_col_letter] = new_status

                if new_status != current_status.strip():
                    value_updates.append({"range": f"{status_col_letter}{row}", "values": [[new_status]]})

                bg_color = rgb_to_float((254, 112, 112)) if "Breakdown" in new_status else rgb_to_float(None)
                format_updates.append({'range': f"{status_col_letter}{row}", 'format': {'backgroundColor': bg_color}})

            new_suggestion = ""
            status_aa = row_statuses.get(HIGHEST_UP_CANDLE_STATUS_COL, "")
            status_ac = row_statuses.get(HIGH_VOL_STATUS_COL, "")
            status_ae = row_statuses.get(PCT_DOWN_STATUS_COL, "")

            exit_100_triggered = False

            if "Breakdown/Breakdown" in status_aa or "Breakdown/Breakdown" in status_ac or "Breakdown/Breakdown" in status_ae:
                exit_100_triggered = True

                breakdown_closes = []
                if "Breakdown/Breakdown" in status_aa: breakdown_closes.extend([d['close'] for d in breakdown_details[HIGHEST_UP_CANDLE_STATUS_COL]])
                if "Breakdown/Breakdown" in status_ac: breakdown_closes.extend([d['close'] for d in breakdown_details[HIGH_VOL_STATUS_COL]])
                if "Breakdown/Breakdown" in status_ae: breakdown_closes.extend([d['close'] for d in breakdown_details[PCT_DOWN_STATUS_COL]])

                exit_at_price = min(breakdown_closes) if breakdown_closes else 0

                qty_col_idx = col_to_num(FULL_QTY_COL) - col_to_num(start_col)
                qty_str = sheet_data[row_idx][qty_col_idx] if len(sheet_data[row_idx]) > qty_col_idx else "0"

                try:
                    qty = int(float(str(qty_str).replace(',', '')))
                    new_suggestion = f"Exit 100% at {exit_at_price:.2f} ({qty})"
                except (ValueError, TypeError):
                    new_suggestion = f"Exit 100% at {exit_at_price:.2f} (QTY_ERR)"


            if not exit_100_triggered:
                breakdown_columns_count = sum(1 for status in [status_aa, status_ac, status_ae] if "Breakdown" in status)

                if breakdown_columns_count >= 2:
                    breakdown_closes = []
                    if "Breakdown" in status_aa: breakdown_closes.extend([d['close'] for d in breakdown_details[HIGHEST_UP_CANDLE_STATUS_COL]])
                    if "Breakdown" in status_ac: breakdown_closes.extend([d['close'] for d in breakdown_details[HIGH_VOL_STATUS_COL]])
                    if "Breakdown" in status_ae: breakdown_closes.extend([d['close'] for d in breakdown_details[PCT_DOWN_STATUS_COL]])

                    exit_at_price = min(breakdown_closes) if breakdown_closes else 0

                    all_signal_lows = []
                    for col_letter in [HIGHEST_UP_CANDLE_COL, HIGH_VOL_RESULT_COL, PCT_DOWN_RESULT_COL]:
                        col_idx = col_to_num(col_letter) - col_to_num(start_col)
                        text = sheet_data[row_idx][col_idx] if len(sheet_data[row_idx]) > col_idx else ""
                        signals_in_cell = re.findall(r"(\d+\.?\d*)\s*\(([^)]+)\)", str(text))
                        all_signal_lows.extend([float(signal[0]) for signal in signals_in_cell])

                    trail_to_price = min(all_signal_lows) if all_signal_lows else 0

                    qty_col_idx = col_to_num(FULL_QTY_COL) - col_to_num(start_col)
                    qty_str = sheet_data[row_idx][qty_col_idx] if len(sheet_data[row_idx]) > qty_col_idx else "0"

                    try:
                        qty = int(float(str(qty_str).replace(',', '')))
                        half_qty = round(qty / 2)
                        new_suggestion = f"Exit 50% at {exit_at_price:.2f} ({half_qty}) & Trail to {trail_to_price:.2f}"
                    except (ValueError, TypeError):
                        new_suggestion = f"Exit 50% at {exit_at_price:.2f} (QTY_ERR) & Trail to {trail_to_price:.2f}"

            suggestion_col_idx = col_to_num(SUGGESTION_COL) - col_to_num(start_col)
            current_suggestion_on_sheet = sheet_data[row_idx][suggestion_col_idx] if len(sheet_data[row_idx]) > suggestion_col_idx else ""

            # Helper function to get the core instruction type ("100%", "50%", or "Blank")
            def get_suggestion_type(suggestion_str):
                if "100%" in suggestion_str:
                    return "100%"
                if "50%" in suggestion_str:
                    return "50%"
                return "Blank"

            current_suggestion_type = get_suggestion_type(current_suggestion_on_sheet)
            new_suggestion_type = get_suggestion_type(new_suggestion)

            # Only update the suggestion text if it's different
            if new_suggestion.strip() != current_suggestion_on_sheet.strip():
                 value_updates.append({"range": f"{SUGGESTION_COL}{row}", "values": [[new_suggestion]]})

            # --- START: MODIFIED SECTION ---
            # Only update the timestamp and send alert if the *type* of instruction has changed
            if new_suggestion_type != current_suggestion_type:
                logger.info(f"Suggestion TYPE CHANGE DETECTED for {entry['symbol']} at row {row}: from '{current_suggestion_type}' to '{new_suggestion_type}'")

                # Update the timestamp in column AG
                timestamp_to_write = ""
                if new_suggestion.strip():
                    timestamp_to_write = get_ist_time().strftime("%d %B, %I:%M %p")
                value_updates.append({"range": f"{FULL_POSITIONS_END_COL}{row}", "values": [[timestamp_to_write]]})

                # Send the alert
                if new_suggestion.strip(): # Only send trigger if there's a new suggestion
                    logger.info(f"Sending 'position_closed' trigger for row {row}.")
                    trigger_apps_script_alert("position_closed", row, entry['symbol'], entry['exchange'])
                    time.sleep(2) # Add a 2-second delay to pace the requests
            # --- END: MODIFIED SECTION ---


    if value_updates:
        Dashboard.batch_update(value_updates, value_input_option='USER_ENTERED')
        logger.info(f"Applied {len(value_updates)} status/suggestion value updates to Dashboard.")
    if format_updates:
        Dashboard.batch_format(format_updates)
        logger.info(f"Applied {len(format_updates)} status/suggestion format updates to Dashboard.")

# =====================================================================================================================
#
#                                         --- GOOGLE SHEET SCANNING AND UPDATING ---
#
# =====================================================================================================================

def get_cell_value(sheet_values, row, col_letter):
    """Helper to safely get a cell value from pre-fetched sheet data."""
    if not col_letter: return None
    col_idx = col_to_num(col_letter) - 1
    row_idx = row - 1
    if 0 <= row_idx < len(sheet_values) and 0 <= col_idx < len(sheet_values[row_idx]):
        return sheet_values[row_idx][col_idx]
    return None

def get_or_fetch_instrument_details(symbol_name, exchange_name, session_cache):
    """
    MODIFIED: Helper function to get instrument details (token and full name) for a given symbol
    from the pre-downloaded master instrument list.
    Returns a dictionary: {'token': '...', 'name': '...'} or None.
    """
    global instrument_master_list
    if not symbol_name or str(symbol_name).strip().upper() in ('SYMBOL', ''):
        return None

    cache_key = (symbol_name.strip().upper(), exchange_name.strip().upper())
    if cache_key in session_cache:
        return session_cache[cache_key]

    instrument_details = None
    symbol_from_sheet = symbol_name.strip().upper()
    exchange_clean = exchange_name.strip().upper()

    # Search the master list instead of making an API call
    for instrument in instrument_master_list:
        # --- FINAL FIX: Check both 'tradingsymbol' and 'symbol' to be safe ---
        if instrument.get('exch_seg') == exchange_clean:
            if instrument.get('tradingsymbol') == symbol_from_sheet or instrument.get('symbol') == symbol_from_sheet:
                instrument_details = {
                    "token": instrument.get('token'),
                    "name": instrument.get('name')
                }
                break

    if instrument_details:
        session_cache[cache_key] = instrument_details
    else:
        logger.warning(f"Could not find details for '{symbol_from_sheet}' on exchange '{exchange_clean}' in the master instrument list.")

    return instrument_details

def scan_sheet_for_all_symbols(Dashboard, ATHCache):
    """
    Unified function to scan the Google Sheet.
    This function now handles updates and deletions of tokens in the ATH Cache.
    """
    logger.info("Scanning Google Sheet for all symbols (Dashboard and Setups)...")

    local_dashboard_details = collections.defaultdict(list)
    local_setup_details = collections.defaultdict(list)
    local_3pct_setup_details = collections.defaultdict(list)
    local_dynamic_setups = collections.defaultdict(list) # <-- NEW
    all_tokens_found = set()

    scan_session_token_cache = {}
    expected_ath_cache_state = {}
    ath_cache_updates_queued = []

    try:
        all_dashboard_values = Dashboard.get_all_values()
        all_ath_cache_values = ATHCache.get_all_values()

        # --- NEW: Find dynamic setup header row ---
        dynamic_setup_start_row = None
        dynamic_setup_symbol_col = 'B'
        dynamic_setup_exchange_col = 'A'
        for i, row_data in enumerate(all_dashboard_values):
            if "Low of Highest Down candle" in row_data and "High of Highest Up candle" in row_data:
                dynamic_setup_start_row = i + 2 # Start scanning from the row below the header
                # Find the actual column letters
                for j, cell_value in enumerate(row_data):
                    if "Symbol" in cell_value:
                        dynamic_setup_symbol_col = chr(ord('A') + j)
                    if "Exch." in cell_value:
                        dynamic_setup_exchange_col = chr(ord('A') + j)
                logger.info(f"Found dynamic setup headers at row {i + 1}. Starting scan from row {dynamic_setup_start_row}.")
                break
        # ----------------------------------------

        symbol_token_map = {}
        for row_idx, row_data in enumerate(all_ath_cache_values):
            row_num = row_idx + 1
            if len(row_data) > col_to_num(ATH_CACHE_Y_COL_DASH) -1:
                token = row_data[col_to_num(ATH_CACHE_Y_COL_DASH) - 1]
                symbol = get_cell_value(all_dashboard_values, row_num, FOCUS_SYMBOL_COL)
                if token and symbol:
                    symbol_token_map[(row_num, ATH_CACHE_Y_COL_DASH)] = (symbol.strip().upper(), token)
            if len(row_data) > col_to_num(ATH_CACHE_Z_COL_DASH) -1:
                token = row_data[col_to_num(ATH_CACHE_Z_COL_DASH) - 1]
                symbol = get_cell_value(all_dashboard_values, row_num, FULL_SYMBOL_COL)
                if token and symbol:
                    symbol_token_map[(row_num, ATH_CACHE_Z_COL_DASH)] = (symbol.strip().upper(), token)

        # --- FINAL FIX: Scan both columns C and M to find the true last row ---
        last_row_focus = get_last_row_in_column(Dashboard, FOCUS_SYMBOL_COL)
        last_row_full = get_last_row_in_column(Dashboard, FULL_SYMBOL_COL)
        max_row_dashboard = max(last_row_focus, last_row_full)

        logger.info(f"Scanning Dashboard up to row {max_row_dashboard}...")

        for row in range(START_ROW_DATA, max_row_dashboard + 20):

            def process_symbol(symbol, exchange, row_num, token_col, block_details):
                symbol_clean = str(symbol).strip().upper()
                symbol_col = block_details.get('symbol_col')

                # --- FIX 1: Ignore placeholder "SYMBOL" text ---
                if not symbol or not exchange or symbol_clean == 'SYMBOL':
                    return

                # --- FIX 2: Use in-memory cache to prevent repetitive logging ---
                cache_key = (row_num, symbol_col)
                cached_symbol = scan_memory_cache.get(cache_key)

                # Log only if the symbol is new for this location or has changed
                if cached_symbol != symbol_clean:
                    logger.info(f"New or changed symbol '{symbol_clean}' found at {cache_key}. Fetching new details.")
                    scan_memory_cache[cache_key] = symbol_clean

                # Now, proceed with the original logic to get instrument details
                instrument_info = get_or_fetch_instrument_details(symbol, exchange, scan_session_token_cache)

                if instrument_info and instrument_info.get('token'):
                    token = instrument_info['token']
                    all_tokens_found.add(token)

                    # Store the full name along with other details
                    block_details['name'] = instrument_info.get('name')

                    if 'ltp_col' in block_details:
                        local_dashboard_details[token].append({'row': row_num, 'symbol': symbol, 'exchange': exchange, **block_details})
                    if 'setup_type' in block_details:
                        exchange_type_int = {'NSE': 1, 'BSE': 3}.get(str(exchange).strip().upper())
                        if exchange_type_int:
                            setup_info = {'symbol': symbol, 'row': row_num, 'exchange_type': exchange_type_int, 'name': block_details['name'], 'exchange': exchange}
                            if block_details['setup_type'] == 'ORH':
                                local_setup_details[token].append(setup_info)
                            elif block_details['setup_type'] == '3PCT':
                                local_3pct_setup_details[token].append(setup_info)
                            elif block_details['setup_type'] == 'DYNAMIC': # <-- NEW
                                local_dynamic_setups[token].append(setup_info)
                    if token_col: # Only update ATH cache for setups that use it
                        expected_ath_cache_state[(row_num, token_col)] = token

            # --- MODIFIED: Removed the hardcoded row limits ---
            exchange_focus = get_cell_value(all_dashboard_values, row, FOCUS_EXCHANGE_COL)
            symbol_focus = get_cell_value(all_dashboard_values, row, FOCUS_SYMBOL_COL)
            if exchange_focus and symbol_focus:
                process_symbol(symbol_focus, exchange_focus, row, ATH_CACHE_Y_COL_DASH, {
                    'ltp_col': FOCUS_LTP_COL, 'chg_col': FOCUS_CHG_COL, 'block_type': 'Focus List',
                    'symbol_col': FOCUS_SYMBOL_COL, 'token_cache_col': ATH_CACHE_Y_COL_DASH
                })

            exchange_full = get_cell_value(all_dashboard_values, row, FULL_EXCHANGE_COL)
            symbol_full = get_cell_value(all_dashboard_values, row, FULL_SYMBOL_COL)
            if exchange_full and symbol_full:
                process_symbol(symbol_full, exchange_full, row, ATH_CACHE_Z_COL_DASH, {
                    'ltp_col': FULL_LTP_COL, 'chg_col': '', 'block_type': 'Full Positions',
                    'symbol_col': FULL_SYMBOL_COL, 'token_cache_col': ATH_CACHE_Z_COL_DASH,
                    'price_col': FULL_PRICE_COL, 'qty_col': FULL_QTY_COL,
                    'return_amt_col': FULL_RETURN_AMT_COL, 'return_pct_col': FULL_RETURN_PCT_COL,
                    'swing_low_input_col': SWING_LOW_INPUT_COL, 'percent_from_swing_low_col': PERCENT_FROM_SWING_LOW_COL,
                    'highest_up_candle_col': HIGHEST_UP_CANDLE_COL, 'entry_date_col': FULL_ENTRY_DATE_COL,
                    'days_duration_col': FULL_DAYS_DURATION_COL
                })

            exchange_orh = get_cell_value(all_dashboard_values, row, ORH_EXCHANGE_COL)
            symbol_orh = get_cell_value(all_dashboard_values, row, ORH_SYMBOL_COL)
            if exchange_orh and symbol_orh and row <= ORH_MAX_ROW:
                process_symbol(symbol_orh, exchange_orh, row, ORH_TOKEN_COL, {'setup_type': 'ORH', 'symbol_col': ORH_SYMBOL_COL})

            exchange_3pct = get_cell_value(all_dashboard_values, row, PCT_EXCHANGE_COL_3PCT)
            symbol_3pct = get_cell_value(all_dashboard_values, row, PCT_SYMBOL_COL_3PCT)
            if exchange_3pct and symbol_3pct:
                process_symbol(symbol_3pct, exchange_3pct, row, PCT_TOKEN_COL_3PCT, {'setup_type': '3PCT', 'symbol_col': PCT_SYMBOL_COL_3PCT})

            # --- NEW: Process symbols for the dynamic setup ---
            if dynamic_setup_start_row and row >= dynamic_setup_start_row:
                exchange_dyn = get_cell_value(all_dashboard_values, row, dynamic_setup_exchange_col)
                symbol_dyn = get_cell_value(all_dashboard_values, row, dynamic_setup_symbol_col)
                if exchange_dyn and symbol_dyn:
                    # Dynamic setups don't write to the ATH Cache, so token_col is None
                    process_symbol(symbol_dyn, exchange_dyn, row, None, {'setup_type': 'DYNAMIC', 'symbol_col': dynamic_setup_symbol_col})
            # ----------------------------------------------------

        max_row_ath_cache_data = len(all_ath_cache_values) if all_ath_cache_values else 0
        rows_to_check_ath_cache = max(max_row_dashboard + 20, max_row_ath_cache_data + 1)
        token_cols_to_check = [ATH_CACHE_Y_COL_DASH, ATH_CACHE_Z_COL_DASH]

        for row_idx in range(rows_to_check_ath_cache):
            row_num = row_idx + 1
            for col_letter in token_cols_to_check:
                current_token_in_ath_cache = get_cell_value(all_ath_cache_values, row_num, col_letter)
                expected_token_for_row_col = expected_ath_cache_state.get((row_num, col_letter))

                if expected_token_for_row_col is None:
                    if current_token_in_ath_cache is not None and str(current_token_in_ath_cache).strip() != '':
                        ath_cache_updates_queued.append({'range': f"{col_letter}{row_num}", 'values': [['']]})
                        logger.info(f"Queued clearing token in ATH Cache cell {col_letter}{row_num} (was '{current_token_in_ath_cache}')")
                elif str(expected_token_for_row_col).strip() != str(current_token_in_ath_cache).strip():
                    ath_cache_updates_queued.append({'range': f"{col_letter}{row_num}", 'values': [[int(expected_token_for_row_col)]]})
                    logger.info(f"Queued updating token in ATH Cache cell {col_letter}{row_num} from '{current_token_in_ath_cache}' to '{expected_token_for_row_col}'.")

        if ath_cache_updates_queued:
            try:
                ATHCache.batch_update(ath_cache_updates_queued)
                logger.info(f"Applied {len(ath_cache_updates_queued)} batch updates to ATH Cache sheet.")
            except Exception as e:
                logger.exception(f"An error occurred during batch update to ATH Cache sheet: {e}")
        else:
            logger.info("No ATH Cache updates needed.")

    except Exception as e:
        logger.exception(f"Error during unified symbol scan and ATH Cache management: {e}")

    logger.info(f"Finished unified scan. Found {len(all_tokens_found)} unique tokens.")
    # CORRECTED: Return 5 values
    return local_dashboard_details, local_setup_details, local_3pct_setup_details, local_dynamic_setups, all_tokens_found


def update_excel_live_data():
    """
    Updates the Google Sheet with live data and the new Swing Low calculation.
    """
    global cells_to_clear_color

    with data_lock:
        dashboard_details_copy = excel_dashboard_details.copy()
        monthly_high_cache_copy = monthly_high_cache.copy()

    if not smart_ws or not smart_ws._is_connected_flag:
        logger.warning("WebSocket not connected. Skipping Google Sheet update.")
        return

    requests = []
    GREEN_COLOR, RED_COLOR = (149, 203, 186), (254, 112, 112)
    YELLOW_COLOR = (249, 203, 156) # Define yellow color for swing low
    dashboard_sheet_id = Dashboard.id

    cells_to_color_this_cycle = set()

    if cells_to_clear_color:
        for cell_a1 in cells_to_clear_color:
            col_letter = ''.join(filter(str.isalpha, cell_a1))
            row_num = int(''.join(filter(str.isdigit, cell_a1)))
            cell_range = {"sheetId": dashboard_sheet_id, "startRowIndex": row_num - 1, "endRowIndex": row_num, "startColumnIndex": col_to_num(col_letter) - 1, "endColumnIndex": col_to_num(col_letter)}
            requests.append({"repeatCell": {"range": cell_range, "cell": {"userEnteredFormat": {"backgroundColor": rgb_to_float(None)}}, "fields": "userEnteredFormat.backgroundColor"}})

    cells_to_clear_color.clear()

    input_ranges = []
    for list_of_details in dashboard_details_copy.values():
        for details in list_of_details:
            row_num = details['row']
            if details.get("symbol_col"):
                input_ranges.append(f'{details["symbol_col"]}{row_num}')

            if details.get('block_type') == "Full Positions":
                if details.get("price_col"): input_ranges.append(f'{details["price_col"]}{row_num}')
                if details.get("qty_col"): input_ranges.append(f'{details["qty_col"]}{row_num}')
                if details.get("entry_date_col"): input_ranges.append(f'{details["entry_date_col"]}{row_num}')
                if details.get("swing_low_input_col"): input_ranges.append(f'{details["swing_low_input_col"]}{row_num}')
                if details.get("block_type") == "Full Positions":
                    input_ranges.append(f'{MONTH_SORT_COL}{row_num}')

    input_data = {}
    if input_ranges:
        try:
            unique_ranges = list(set(input_ranges))
            fetched_values = Dashboard.batch_get(unique_ranges)
            fetched_map = {rng: val for rng, val in zip(unique_ranges, fetched_values)}
            for a1_notation in unique_ranges:
                val_list = fetched_map.get(a1_notation)
                input_data[a1_notation] = val_list[0][0] if val_list and val_list[0] else None
        except Exception as e:
            logger.error(f"Error fetching dashboard input data in batch: {e}")
            # --- FIX: Exit the function immediately if the data fetch fails ---
            return

    for token, list_of_details in dashboard_details_copy.items():
        current_ltp = latest_tick_data.get(token, {}).get('ltp')
        if current_ltp is None: continue

        previous_ltp = previous_ltp_data.get(token)
        ltp_cell_color = None
        if previous_ltp is not None and current_ltp != previous_ltp:
            ltp_cell_color = GREEN_COLOR if current_ltp > previous_ltp else RED_COLOR
        previous_ltp_data[token] = current_ltp

        for details in list_of_details:
            row_num = details['row']

            symbol_on_sheet_raw = input_data.get(f"{details.get('symbol_col')}{row_num}")
            symbol_on_sheet = str(symbol_on_sheet_raw).strip().upper() if symbol_on_sheet_raw else ""

            if not symbol_on_sheet:
                if details.get('block_type') == "Focus List":
                    start_col, end_col = 'D', 'J'
                elif details.get('block_type') == "Full Positions":
                    start_col, end_col = 'N', 'AG'
                else:
                    continue

                logger.info(f"Detected cleared symbol at row {row_num}. Queuing fast clear for {start_col}{row_num}:{end_col}{row_num}.")

                requests.append({
                    "repeatCell": {
                        "range": {
                            "sheetId": dashboard_sheet_id,
                            "startRowIndex": row_num - 1, "endRowIndex": row_num,
                            "startColumnIndex": col_to_num(start_col) - 1, "endColumnIndex": col_to_num(end_col)
                        },
                        "cell": {
                            "userEnteredValue": {},
                            "userEnteredFormat": { "backgroundColor": rgb_to_float(None) }
                        },
                        "fields": "userEnteredValue,userEnteredFormat.backgroundColor"
                    }
                })
                continue

            def queue_update(col_letter, value, number_format_pattern=None, bg_color='SENTINEL', is_ltp=False):
                if not col_letter: return
                cell_a1 = f"{col_letter}{row_num}"
                cell_range = {"sheetId": dashboard_sheet_id, "startRowIndex": row_num - 1, "endRowIndex": row_num, "startColumnIndex": col_to_num(col_letter) - 1, "endColumnIndex": col_to_num(col_letter)}
                cell_data, fields = {}, []
                user_entered_value = {}
                if isinstance(value, (int, float)): user_entered_value["numberValue"] = value
                else: user_entered_value["stringValue"] = str(value)
                cell_data["userEnteredValue"] = user_entered_value
                fields.append("userEnteredValue")

                user_entered_format = {}
                format_fields = []

                if bg_color != 'SENTINEL':
                    user_entered_format["backgroundColor"] = rgb_to_float(bg_color)
                    format_fields.append("backgroundColor")
                    if is_ltp and bg_color is not None:
                        cells_to_color_this_cycle.add(cell_a1)

                if number_format_pattern:
                    user_entered_format["numberFormat"] = {"type": "NUMBER" if isinstance(value, (int, float)) else "TEXT", "pattern": number_format_pattern}
                    format_fields.append("numberFormat")

                if user_entered_format:
                    cell_data["userEnteredFormat"] = user_entered_format
                    for key in format_fields:
                        fields.append(f"userEnteredFormat.{key}")

                requests.append({"updateCells": {"rows": [{"values": [cell_data]}], "fields": ",".join(fields), "range": cell_range}})

            queue_update(details.get('ltp_col'), current_ltp, "#,##0.00", bg_color=ltp_cell_color, is_ltp=True)

            if details.get('chg_col') and token in latest_quote_data:
                percentage_change = latest_quote_data[token].get('percentChange', 0.0)
                percentage_change_decimal = percentage_change / 100.0 if percentage_change is not None else 0.0
                chg_cell_color = GREEN_COLOR if percentage_change > 0 else RED_COLOR if percentage_change < 0 else None
                queue_update(details['chg_col'], percentage_change_decimal, "0.00%", bg_color=chg_cell_color)

            if details.get('block_type') == "Full Positions":
                try:
                    price_val_str = str(input_data.get(f'{details["price_col"]}{row_num}') or '0').replace(',','')
                    qty_val_str = str(input_data.get(f'{details["qty_col"]}{row_num}') or '0').replace(',','')
                    entry_date_str = input_data.get(f'{details["entry_date_col"]}{row_num}')
                    price_val = float(price_val_str) if price_val_str else 0
                    qty_val = float(qty_val_str) if qty_val_str else 0
                except (ValueError, TypeError) as e:
                    logger.warning(f"Could not parse numeric values for row {row_num}. Error: {e}"); continue

                if price_val and qty_val:
                    return_amt = (current_ltp - price_val) * qty_val
                    return_pct = (current_ltp - price_val) / price_val if price_val != 0 else 0
                    queue_update(details.get('return_amt_col'), return_amt, "#,##0.00", bg_color=(GREEN_COLOR if return_amt > 0 else RED_COLOR if return_amt < 0 else None))
                    queue_update(details.get('return_pct_col'), return_pct, "0.00%", bg_color=(GREEN_COLOR if return_pct > 0 else RED_COLOR if return_pct < 0 else None))

                swing_low_str = str(input_data.get(f'{details.get("swing_low_input_col")}{row_num}') or '').replace(',','')
                swing_low_val = None
                try:
                    if swing_low_str: swing_low_val = float(swing_low_str)
                except (ValueError, TypeError):
                    queue_update(details.get('percent_from_swing_low_col'), "Invalid Low", "@", bg_color=None)

                if swing_low_val is not None and swing_low_val > 0:
                    monthly_high = monthly_high_cache_copy.get(token)
                    if monthly_high and monthly_high > 0:
                        percent_from_high = (monthly_high - swing_low_val) / swing_low_val
                        cell_color = YELLOW_COLOR if percent_from_high >= 0.35 else None
                        queue_update(details.get('percent_from_swing_low_col'), percent_from_high, "0.00%", bg_color=cell_color)
                    else:
                        queue_update(details.get('percent_from_swing_low_col'), "No High", "@", bg_color=None)
                else:
                    queue_update(details.get('percent_from_swing_low_col'), "", "General", bg_color=None)

                month_val_str = str(input_data.get(f'{MONTH_SORT_COL}{row_num}') or '')
                try:
                    month_val = int(month_val_str)
                    cell_color = YELLOW_COLOR if month_val >= 3 else None
                    queue_update(MONTH_SORT_COL, month_val, "0", bg_color=cell_color)
                except (ValueError, TypeError):
                    queue_update(MONTH_SORT_COL, month_val_str, "@", bg_color=None)

                days_duration = ""
                if entry_date_str:
                    try:
                        entry_dt = datetime.datetime.strptime(entry_date_str, '%d-%b-%y')
                        days_duration = f"{(get_ist_time().date() - entry_dt.date()).days} Days"
                    except ValueError:
                        days_duration = "Invalid Date"
                queue_update(details.get('days_duration_col'), days_duration, "@")

    if requests:
        try:
            gsheet.batch_update({'requests': requests})
            logger.info(f"Executed {len(requests)} batch update operations on Google Sheet for dashboard.")
        except Exception as e:
            logger.exception(f"An error occurred during batch update to Google Sheet: {e}")

# =====================================================================================================================
#
#                                         --- MAIN APPLICATION LOGIC & THREADS ---
#
# =====================================================================================================================

def run_live_dashboard_updater():
    """
    This function runs in a dedicated thread to continuously update the Google Sheet
    with the latest prices, ensuring the dashboard is always live and responsive.
    """
    logger.info("Live dashboard updater thread started.")
    while True:
        try:
            update_excel_live_data()
            # MODIFIED: Changed sleep time to 0.5 seconds to reduce API calls and avoid quota errors.
            time.sleep(0.5)
        except Exception as e:
            logger.exception(f"Error in dashboard updater thread: {e}")
            time.sleep(5)

def run_quote_updater():
    """
    This function runs in a dedicated thread to periodically fetch the full quote
    (including percentChange) for all symbols using the REST API.
    """
    global latest_quote_data
    logger.info("Quote updater thread started.")
    while True:
        try:
            with data_lock:
                dashboard_details_copy = excel_dashboard_details.copy()

            focus_list_tokens = {}
            for token, details_list in dashboard_details_copy.items():
                for details in details_list:
                    if details.get('block_type') == 'Focus List':
                        exchange_name = details.get("exchange", "NSE").upper()
                        focus_list_tokens[token] = exchange_name
                        break

            if not focus_list_tokens:
                time.sleep(5)
                continue

            tokens_by_exchange = collections.defaultdict(list)
            for token, exchange in focus_list_tokens.items():
                tokens_by_exchange[exchange].append(token)

            new_quote_data = {}

            for exchange, tokens_list in tokens_by_exchange.items():
                for i in range(0, len(tokens_list), QUOTE_API_MAX_TOKENS):
                    batch_tokens = tokens_list[i:i + QUOTE_API_MAX_TOKENS]
                    payload = {"mode": "FULL", "exchangeTokens": {exchange: batch_tokens}}

                    logger.info(f"Fetching market data for {len(batch_tokens)} tokens on {exchange}...")
                    response = smart_api_obj.getMarketData(**payload)

                    if response and response.get("status") and isinstance(response.get("data"), dict):
                        fetched_data = response["data"].get("fetched", [])
                        for item in fetched_data:
                            if isinstance(item, dict):
                                token = item.get("symbolToken")
                                if token:
                                    new_quote_data[token] = {"percentChange": item.get("percentChange"), "netChange": item.get("netChange")}
                    else:
                        logger.warning(f"Could not fetch quote data for batch (Exchange: {exchange}, Tokens: {batch_tokens}). Response: {response}")

                    time.sleep(0.5)

            with data_lock:
                latest_quote_data.update(new_quote_data)

            time.sleep(3)

        except Exception as e:
            logger.exception(f"Error in quote updater thread: {e}")
            time.sleep(10)

def run_initial_setup_data_fetch():
    """
    This function now runs in a dedicated thread to fetch all historical data
    for the setups without blocking the main application startup.
    """
    logger.info("Starting background fetch for initial setup data...")

    with data_lock:
        orh_details_copy = excel_setup_details.copy()
        pct3_details_copy = excel_3pct_setup_details.copy()
        dynamic_setup_copy = excel_dynamic_setups.copy() # <-- NEW

    fetch_initial_candle_data(smart_api_obj, orh_details_copy)
    fetch_previous_day_candle_data(smart_api_obj, orh_details_copy)

    # Combine all tokens that need historical data
    all_tokens_for_history = set()
    for token, details in pct3_details_copy.items():
        if details:
            all_tokens_for_history.add((token, details[0]['exchange_type']))
    for token, details in dynamic_setup_copy.items():
        if details:
            all_tokens_for_history.add((token, details[0]['exchange_type']))

    unique_tokens_for_history = list(all_tokens_for_history)

    for interval_api in CANDLE_INTERVALS_3PCT_API:
        fetch_historical_candles_for_3pct_down(smart_api_obj, unique_tokens_for_history, interval_api)

    # After initial fetch, run the checks
    check_and_update_price_volume_setups()
    check_and_update_breakdown_status()
    check_and_update_dynamic_setups() # <-- NEW

    logger.info("Background fetch for initial setup data complete.")

def sort_full_positions():
    """
    Reads the Full Positions section, sorts it based on Column W then Column Y,
    and writes the sorted data back to the sheet, maintaining data integrity.
    """
    logger.info("Performing automatic sort of Full Positions...")
    try:
        last_row = get_last_row_in_column(Dashboard, FULL_SYMBOL_COL)
        if last_row < START_ROW_DATA:
            logger.info("No data in Full Positions to sort.")
            return

        # 1. Read all data for the section to be sorted
        range_to_sort = f"{FULL_EXCHANGE_COL}{START_ROW_DATA}:{FULL_POSITIONS_END_COL}{last_row}"
        dashboard_data = Dashboard.get(range_to_sort)

        # Read the corresponding tokens from the cache
        token_range = f"{ATH_CACHE_Z_COL_DASH}{START_ROW_DATA}:{ATH_CACHE_Z_COL_DASH}{last_row}"
        token_data = ATHCache.get(token_range)

        # 2. Combine into a list of objects for easy sorting
        combined_data = []
        month_col_index = col_to_num(MONTH_SORT_COL) - col_to_num(FULL_EXCHANGE_COL)
        swing_low_col_index = col_to_num(PERCENT_FROM_SWING_LOW_COL) - col_to_num(FULL_EXCHANGE_COL)

        for i, row_data in enumerate(dashboard_data):
            # Helper to safely convert sort keys to numbers
            def to_float(value, is_percent=False):
                try:
                    if is_percent:
                        return float(str(value).strip().replace('%', ''))
                    return float(value)
                except (ValueError, TypeError):
                    return -float('inf') # Treat errors/blanks as lowest priority

            # Safely access the data using the calculated indices
            month_val = to_float(row_data[month_col_index]) if len(row_data) > month_col_index else -float('inf')
            swing_low_pct_val = to_float(row_data[swing_low_col_index], is_percent=True) if len(row_data) > swing_low_col_index else -float('inf')

            combined_data.append({
                'dashboard_row': row_data,
                'token_row': token_data[i] if i < len(token_data) else [''],
                'sort_month': month_val,
                'sort_swing_low': swing_low_pct_val,
                'original_index': i
            })

        # 3. Perform the multi-level sort
        sorted_combined_data = sorted(combined_data, key=lambda x: (x['sort_month'], x['sort_swing_low']), reverse=True)

        # 4. Check if the order has actually changed
        if all(item['original_index'] == i for i, item in enumerate(sorted_combined_data)):
            logger.info("No change in sort order. Skipping sheet update.")
            return

        logger.info("Change in sort order detected. Updating Google Sheet.")

        # 5. Prepare the data to be written back
        sorted_dashboard_data = [item['dashboard_row'] for item in sorted_combined_data]
        sorted_token_data = [item['token_row'] for item in sorted_combined_data]

        # 6. Write the sorted data back to the sheets
        Dashboard.update(range_to_sort, sorted_dashboard_data, value_input_option='USER_ENTERED')
        ATHCache.update(token_range, sorted_token_data, value_input_option='USER_ENTERED')

        logger.info("Successfully sorted and updated Full Positions on the Google Sheet.")

    except Exception as e:
        logger.exception(f"An error occurred during the automatic sorting process: {e}")

def run_background_task_scheduler():
    """
    This function runs in a dedicated thread to handle all the slower,
    scheduled tasks like scanning the sheet for new symbols and checking for trade setups.
    """
    global subscribed_tokens, excel_dashboard_details, excel_setup_details, excel_3pct_setup_details, excel_dynamic_setups
    logger.info("Background task scheduler thread started.")

    last_checked_minute_orh, last_checked_minute_15min, last_checked_minute_30min, last_checked_minute_1hr = None, None, None, None
    last_scan_time = 0

    # --- MODIFIED: Timers for 24-hour tasks ---
    # Set to 0 to ensure they run on the first pass
    last_sort_time = 0
    last_monthly_high_fetch_time = 0

    while True:
        try:
            # --- UPDATED ORDER MANAGEMENT FLOW ---
            check_and_place_orders()
            check_and_update_order_statuses()
            # -------------------------------------

            now = get_ist_time()
            current_minute = now.minute

            # Rescan for symbol changes every 15 seconds
            if time.time() - last_scan_time > 15:
                logger.info("Rescanning Google Sheet for symbol changes...")
                new_dashboard, new_orh, new_3pct, new_dynamic, current_excel_tokens = scan_sheet_for_all_symbols(Dashboard, ATHCache)
                with data_lock:
                    excel_dashboard_details = new_dashboard
                    excel_setup_details = new_orh
                    excel_3pct_setup_details = new_3pct
                    excel_dynamic_setups = new_dynamic

                tokens_to_subscribe = current_excel_tokens - subscribed_tokens
                if tokens_to_subscribe and smart_ws and smart_ws._is_connected_flag:
                    subscribe_list_grouped = collections.defaultdict(list)
                    for token in tokens_to_subscribe:
                        with data_lock:
                            # Check all dictionaries to find the exchange type
                            if token in excel_dashboard_details and excel_dashboard_details[token]: exchange_type_num = {'NSE': 1, 'BSE': 3, 'NFO': 2}.get(excel_dashboard_details[token][0].get('exchange', 'NSE').upper(), 1)
                            elif token in excel_setup_details and excel_setup_details[token]: exchange_type_num = excel_setup_details[token][0].get('exchange_type', 1)
                            elif token in excel_3pct_setup_details and excel_3pct_setup_details[token]: exchange_type_num = excel_3pct_setup_details[token][0].get('exchange_type', 1)
                            elif token in excel_dynamic_setups and excel_dynamic_setups[token]: exchange_type_num = excel_dynamic_setups[token][0].get('exchange_type', 1)
                            else: exchange_type_num = 1
                        subscribe_list_grouped[exchange_type_num].append(token)

                    for ex_type, tokens in subscribe_list_grouped.items():
                        formatted_tokens = [{"exchangeType": ex_type, "tokens": list(tokens)}]
                        smart_ws.subscribe(f"sub_{int(time.time())}", smart_ws.QUOTE, formatted_tokens)
                        subscribed_tokens.update(tokens)
                        logger.info(f"Subscribed to {len(tokens)} new tokens on exchange type {ex_type}.")

                last_scan_time = time.time()

            # --- MODIFIED: Run these tasks once every 24 hours ---
            if time.time() - last_monthly_high_fetch_time > 86400: # 86400 seconds = 24 hours
                logger.info("Performing daily fetch for monthly highs (for Swing Low setup)...")
                with data_lock:
                    unique_tokens_3pct = list(set([(token, details[0]['exchange_type']) for token, details in excel_3pct_setup_details.items() if details]))
                if unique_tokens_3pct:
                    fetch_monthly_highs(smart_api_obj, unique_tokens_3pct)
                last_monthly_high_fetch_time = time.time()

            # --- MODIFIED: Run these tasks once every 24 hours ---
            if time.time() - last_sort_time > 86400: # 86400 seconds = 24 hours
                sort_full_positions()
                last_sort_time = time.time()

            if current_minute % 3 == 0 and current_minute != last_checked_minute_orh:
                check_and_update_orh_setup()
                last_checked_minute_orh = current_minute

            with data_lock:
                has_3pct_symbols = bool(excel_3pct_setup_details)
                has_dynamic_symbols = bool(excel_dynamic_setups)

            if has_3pct_symbols or has_dynamic_symbols:
                with data_lock:
                    all_tokens_for_history = set()
                    for token, details in excel_3pct_setup_details.items():
                        if details: all_tokens_for_history.add((token, details[0]['exchange_type']))
                    for token, details in excel_dynamic_setups.items():
                        if details: all_tokens_for_history.add((token, details[0]['exchange_type']))
                    unique_tokens_for_history = list(all_tokens_for_history)

                if current_minute % 15 == 1 and current_minute != last_checked_minute_15min:
                    fetch_historical_candles_for_3pct_down(smart_api_obj, unique_tokens_for_history, 'FIFTEEN_MINUTE')
                    check_and_update_price_volume_setups()
                    check_and_update_breakdown_status()
                    check_and_update_dynamic_setups() # <-- NEW
                    last_checked_minute_15min = current_minute

                if current_minute % 30 == 1 and current_minute != last_checked_minute_30min:
                    fetch_historical_candles_for_3pct_down(smart_api_obj, unique_tokens_for_history, 'THIRTY_MINUTE')
                    check_and_update_price_volume_setups()
                    check_and_update_breakdown_status()
                    check_and_update_dynamic_setups() # <-- NEW
                    last_checked_minute_30min = current_minute

                if current_minute == 16 and now.hour >= 10 and current_minute != last_checked_minute_1hr:
                    fetch_historical_candles_for_3pct_down(smart_api_obj, unique_tokens_for_history, 'ONE_HOUR')
                    check_and_update_price_volume_setups()
                    check_and_update_breakdown_status()
                    check_and_update_dynamic_setups() # <-- NEW
                    last_checked_minute_1hr = current_minute

            time.sleep(1)
        except Exception as e:
            logger.exception(f"Error in background scheduler thread: {e}")
            time.sleep(5)

# =====================================================================================================================
#
# --- START: MODIFIED SECTION FOR DAILY ATH CACHE UPDATE ---
#
# =====================================================================================================================

def populate_ath_cache_from_master_list(ATHCache, master_list):
    """
    MODIFIED: Filters the master instrument list for stocks and indexes,
    and populates the ATH Cache sheet with their symbols (Col A) and tokens (Col AF).
    """
    logger.info("Filtering master instrument list to populate ATH Cache...")

    symbols_to_write = []
    tokens_to_write = []

    for instrument in master_list:
        symbol = instrument.get('symbol', '')
        instrument_type = instrument.get('instrumenttype', '')

        if (symbol.endswith('-EQ') or symbol.endswith('-BE') or instrument_type == 'AMXIDX'):
            token = instrument.get('token')
            if token and symbol:
                symbols_to_write.append([symbol])
                tokens_to_write.append([token])

    if not symbols_to_write:
        logger.warning("No instruments found matching the filter criteria. ATH Cache will not be populated.")
        return

    logger.info(f"Found {len(symbols_to_write)} stocks and indexes. Updating ATH Cache sheet...")

    try:
        # Define the ranges to be updated
        symbol_range = 'A2:A'
        # --- THIS IS THE MODIFIED LINE ---
        token_range = 'AF2:AF' # Changed from 'W2:W'

        # Clear existing data in both ranges
        ATHCache.batch_clear([symbol_range, token_range])
        logger.info(f"Cleared existing data in ATH Cache ranges {symbol_range} and {token_range}.")

        # Update Column A with symbols
        ATHCache.update(range_name=symbol_range, values=symbols_to_write, value_input_option='USER_ENTERED')
        logger.info(f"Successfully populated Column A of ATH Cache with {len(symbols_to_write)} symbols.")

        # Update Column AF with tokens
        ATHCache.update(range_name=token_range, values=tokens_to_write, value_input_option='USER_ENTERED')
        logger.info(f"Successfully populated Column AF of ATH Cache with {len(tokens_to_write)} tokens.")

    except Exception as e:
        logger.exception(f"An error occurred while updating the ATH Cache sheet: {e}")

def run_daily_ath_cache_update():
    """
    MODIFIED: This function runs in a dedicated thread. It performs an initial update
    immediately upon startup, then schedules subsequent updates for 9:00 AM daily.
    """
    logger.info("Daily ATH Cache updater thread started.")

    # --- MODIFICATION: Perform the first run immediately on startup ---
    logger.info("Performing initial, one-time ATH Cache population on startup...")
    try:
        # We use the globally loaded master list for the very first run
        if instrument_master_list and ATHCache:
            populate_ath_cache_from_master_list(ATHCache, instrument_master_list)
    except Exception as e:
        logger.exception(f"An error occurred during the initial ATH cache population: {e}")

    # Set the last run date to today to prevent the scheduler from running again today
    last_run_date = get_ist_time().date()
    logger.info(f"Initial ATH Cache population complete. Next scheduled run is for tomorrow at 9:00 AM.")


    # --- This loop handles all subsequent daily runs ---
    while True:
        try:
            now = get_ist_time()
            today = now.date()

            # Check if it's 9:00 AM and if the task hasn't run today
            if now.hour == 9 and now.minute == 0 and today != last_run_date:
                logger.info(f"Scheduled time 9:00 AM reached. Starting daily ATH Cache update for {today.strftime('%Y-%m-%d')}.")

                # --- Download a fresh master list for the daily update ---
                try:
                    logger.info(f"Downloading fresh master instrument list from {INSTRUMENT_LIST_URL}...")
                    response = requests.get(INSTRUMENT_LIST_URL)
                    response.raise_for_status()
                    daily_master_list = response.json()
                    logger.info(f"Successfully downloaded {len(daily_master_list)} instruments for daily update.")

                    # --- Populate the sheet ---
                    if daily_master_list and ATHCache:
                        populate_ath_cache_from_master_list(ATHCache, daily_master_list)

                    # Mark today's run as complete
                    last_run_date = today

                except Exception as e:
                    logger.error(f"FATAL: Could not download or process the master instrument list for daily update: {e}.")

            # Wait for 60 seconds before checking the time again
            time.sleep(60)

        except Exception as e:
            logger.exception(f"An error occurred in the daily ATH cache updater thread: {e}")
            time.sleep(60) # Wait before retrying in case of an unexpected error

# =====================================================================================================================
#
# --- END: MODIFIED SECTION ---
#
# =====================================================================================================================


def start_main_application():
    """
    The primary function that initializes connections and runs the main processing loop.
    """
    global smart_api_obj, smart_ws, gsheet, Dashboard, ATHCache, OrdersSheet, subscribed_tokens
    global excel_dashboard_details, excel_setup_details, excel_3pct_setup_details, instrument_master_list, excel_dynamic_setups

    logger.info("Starting Combined Trading Dashboard and Signal Generator...")

    # --- MODIFIED: Download instrument master list at startup for immediate use by other functions ---
    try:
        logger.info(f"Performing initial download of master instrument list from {INSTRUMENT_LIST_URL}...")
        response = requests.get(INSTRUMENT_LIST_URL)
        response.raise_for_status()
        instrument_master_list = response.json()
        logger.info(f"Successfully downloaded and loaded {len(instrument_master_list)} instruments for startup.")
    except Exception as e:
        logger.error(f"FATAL: Could not download or parse the master instrument list at startup: {e}. Exiting.")
        return

    try:
        logger.info("Authenticating with Google Sheets...")
        creds = ServiceAccountCredentials.from_json_keyfile_name(JSON_KEY_FILE_PATH, SCOPE)
        client = gspread.authorize(creds)
        gsheet = client.open_by_key(GOOGLE_SHEET_ID)
        Dashboard = gsheet.worksheet(DASHBOARD_SHEET_NAME)
        ATHCache = gsheet.worksheet(ATH_CACHE_SHEET_NAME)
        OrdersSheet = gsheet.worksheet(ORDERS_SHEET_NAME)
        logger.info("Google Sheets connected successfully.")
    except Exception as e:
        logger.error(f"Error connecting to Google Sheets: {e}. Please check credentials and sheet names. Exiting.")
        return

    try:
        logger.info("Generating SmartAPI session...")
        smart_api_obj = SmartConnect(api_key=API_KEY, timeout=15)
        smart_api_obj.root = "https://apiconnect.angelbroking.com/"
        totp = pyotp.TOTP(TOTP_SECRET).now()
        data = smart_api_obj.generateSession(CLIENT_CODE, MPIN, totp=totp)
        if data and data.get('data') and data['data'].get('jwtToken'):
            auth_token = data['data']['jwtToken']
            feed_token = data['data']['feedToken']
            logger.info("SmartAPI session generated successfully!")
        else:
            logger.error(f"Failed to generate SmartAPI session. Response: {data}. Exiting.")
            return
    except Exception as e:
        logger.error(f"Error during SmartAPI session generation: {e}. Exiting.")
        return

    load_previous_day_high_cache()
    logger.info("Performing initial symbol scan...")
    new_dashboard, new_orh, new_3pct, new_dynamic, all_tokens_for_subscription = scan_sheet_for_all_symbols(Dashboard, ATHCache)
    excel_dashboard_details = new_dashboard
    excel_setup_details = new_orh
    excel_3pct_setup_details = new_3pct
    excel_dynamic_setups = new_dynamic # <-- NEW

    # --- MODIFIED: Run these tasks once at startup ---
    logger.info("Performing initial one-time fetch for monthly highs and portfolio sort...")
    unique_tokens_3pct_startup = list(set([(token, details[0]['exchange_type']) for token, details in excel_3pct_setup_details.items() if details]))
    if unique_tokens_3pct_startup:
        fetch_monthly_highs(smart_api_obj, unique_tokens_3pct_startup)
    sort_full_positions()

    try:
        logger.info("Initializing SmartAPI WebSocket...")
        smart_ws = MyWebSocketClient(auth_token, API_KEY, CLIENT_CODE, feed_token)
        websocket_thread = threading.Thread(target=smart_ws.connect, daemon=True)
        websocket_thread.start()
        time.sleep(5)
        if not smart_ws._is_connected_flag:
            logger.error("WebSocket failed to connect. Exiting.")
            return
        logger.info("SmartAPI WebSocket connected.")
    except Exception as e:
        logger.error(f"Error initializing WebSocket: {e}. Exiting.")
        return

    if all_tokens_for_subscription:
        logger.info("Subscribing to initial set of tokens for live data...")
        subscribe_list_grouped = collections.defaultdict(list)
        for token in all_tokens_for_subscription:
            # Check all dictionaries to find the exchange type
            if token in excel_dashboard_details and excel_dashboard_details[token]: exchange_type_num = {'NSE': 1, 'BSE': 3, 'NFO': 2}.get(excel_dashboard_details[token][0].get('exchange', 'NSE').upper(), 1)
            elif token in excel_setup_details and excel_setup_details[token]: exchange_type_num = excel_setup_details[token][0].get('exchange_type', 1)
            elif token in excel_3pct_setup_details and excel_3pct_setup_details[token]: exchange_type_num = excel_3pct_setup_details[token][0].get('exchange_type', 1)
            elif token in excel_dynamic_setups and excel_dynamic_setups[token]: exchange_type_num = excel_dynamic_setups[token][0].get('exchange_type', 1)
            else: exchange_type_num = 1
            subscribe_list_grouped[exchange_type_num].append(token)

        for ex_type, tokens in subscribe_list_grouped.items():
            formatted_tokens = [{"exchangeType": ex_type, "tokens": list(tokens)}]
            smart_ws.subscribe(f"sub_initial_{int(time.time())}", smart_ws.QUOTE, formatted_tokens)
            subscribed_tokens.update(tokens)
            logger.info(f"Subscribed to {len(tokens)} new tokens on exchange type {ex_type}.")

    logger.info("Starting concurrent application threads...")

    # --- ADDED: Start the new daily scheduler for the ATH Cache ---
    ath_cache_updater_thread = threading.Thread(target=run_daily_ath_cache_update, daemon=True)
    ath_cache_updater_thread.start()

    dashboard_updater_thread = threading.Thread(target=run_live_dashboard_updater, daemon=True)
    dashboard_updater_thread.start()

    quote_updater_thread = threading.Thread(target=run_quote_updater, daemon=True)
    quote_updater_thread.start()

    initial_data_fetch_thread = threading.Thread(target=run_initial_setup_data_fetch, daemon=True)
    initial_data_fetch_thread.start()

    background_scheduler_thread = threading.Thread(target=run_background_task_scheduler, daemon=True)
    background_scheduler_thread.start()

    logger.info("All systems are go! The application is now running.")

    try:
        while True:
            time.sleep(60)
            logger.info("Main thread is alive. All worker threads are running in the background.")
    except KeyboardInterrupt:
        logger.info("Script interrupted by user. Closing connections...")
    finally:
        if smart_ws:
            smart_ws.close_connection()
        logger.info("Script finished.")


# --- Threaded Logic Runner ---
def run_threaded_logic():
    """Starts the main application logic in a separate thread."""
    thread = threading.Thread(target=start_main_application)
    thread.daemon = True
    thread.start()

# --- Main Entry Point for Flask + Threaded Logic ---
if __name__ == "__main__":
    run_threaded_logic()
    # The Flask app runs in the main thread to keep the service alive for deployment platforms.
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))
