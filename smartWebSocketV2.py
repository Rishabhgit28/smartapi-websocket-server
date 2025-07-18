# =====================================================================================================================
# Combined Trading Dashboard and Signal Generator
# Merges the functionality of smartWebSocketV2.py (live dashboard) and ORH.py (trade signal detection).
# This single script provides real-time P&L tracking and simultaneously monitors for ORH and 3% Down setups.
# VERSION: Concurrent (Threaded) with REST API for Percentage Change
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

# --- Flask for Web Service Deployment ---
from flask import Flask

# --- Third-Party Library Imports ---
import gspread
import pyotp
import websocket
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
    return "✅ Combined Trading Server is running", 200

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

# Set the path to the Google credentials JSON file
if os.path.exists("/etc/secrets/creds.json"):
    JSON_KEY_FILE_PATH = "/etc/secrets/creds.json"
else:
    # Fallback for local development
    current_dir = os.path.dirname(__file__) if "__file__" in locals() else os.getcwd()
    JSON_KEY_FILE_PATH = os.path.join(current_dir, "the-money-method-ad6d7-f95331cf5cbf.json")


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

# --- Threading Lock for Data Safety ---
data_lock = threading.Lock()

# --- Data Caching and State Management Variables ---
# For Live Dashboard
latest_tick_data = collections.defaultdict(dict)
latest_quote_data = collections.defaultdict(dict) # NEW: For storing REST API quote data
excel_dashboard_details = collections.defaultdict(list)
previous_ltp_data = {}
previous_percentage_change_data = {}
cells_to_clear_color = set()

# For ORH and 3% Down Setups
excel_setup_details = collections.defaultdict(list) # For ORH symbols
excel_3pct_setup_details = collections.defaultdict(list) # For 3% Down symbols
interval_ohlc_data = collections.defaultdict(lambda: collections.defaultdict(dict))
completed_3min_candles = collections.defaultdict(list)
# MODIFIED: This now stores a large history of candles for analysis.
volume_history_3pct = collections.defaultdict(lambda: collections.defaultdict(list))
previous_day_high_cache = {}
twenty_five_day_high_cache = {} # NEW: Cache for the new "% from High" setup

# For Subscription Management
subscribed_tokens = set()

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
QUOTE_API_MAX_TOKENS = 50 # Max tokens allowed per single getMarketData API request

# For Live Dashboard
INDEX_START_ROW = 100
QUARTER_POSITIONS_START_ROW = 100
FOCUS_EXCHANGE_COL = 'B'
FOCUS_SYMBOL_COL = 'C'
FOCUS_LTP_COL = 'D'
FOCUS_CHG_COL = 'E'
ATH_CACHE_Y_COL_DASH = 'Y'
ATH_CACHE_Z_COL_DASH = 'Z'
FULL_EXCHANGE_COL = 'L'
FULL_SYMBOL_COL = 'M'
FULL_QTY_COL = 'N'
FULL_PRICE_COL = 'O'
FULL_LTP_COL = 'P'
FULL_RETURN_AMT_COL = 'Q'
FULL_RETURN_PCT_COL = 'R'
FULL_ENTRY_DATE_COL = 'T'
FULL_DAYS_DURATION_COL = 'U'
HIGHEST_UP_CANDLE_COL = 'X' 
HIGHEST_UP_CANDLE_STATUS_COL = 'Y' # NEW: Confirmation for Highest Up Candle
FULL_PERCENT_FROM_HIGH_COL = 'Z'
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
ORH_TOKEN_COL = 'Y' # This column is shared with ATH_CACHE_Y_COL_DASH
ORH_RESULT_COL = 'G'
ORH_BUY_STOP_COL = 'H'
PCT_EXCHANGE_COL_3PCT = 'L'
PCT_SYMBOL_COL_3PCT = 'M'
PCT_TOKEN_COL_3PCT = 'Z' # This column is shared with ATH_CACHE_Z_COL_DASH
PCT_DOWN_RESULT_COL = 'AA'
PCT_DOWN_STATUS_COL = 'AB'      # NEW: Confirmation for 3% Down
HIGH_VOL_RESULT_COL = 'AC'      # RENAMED for clarity
HIGH_VOL_STATUS_COL = 'AD'      # NEW: Confirmation for High Volume

CANDLE_INTERVALS_3PCT_API = ['FIFTEEN_MINUTE', 'THIRTY_MINUTE', 'ONE_HOUR']
CANDLE_INTERVAL_MAP_DISPLAY = {
    'FIFTEEN_MINUTE': '15 Min',
    'THIRTY_MINUTE': '30 Min',
    'ONE_HOUR': '1 Hour'
}

# NEW: ORH Specific Configuration
ORH_MAX_CANDLES = 5 # Number of 3-min candles to retain for ORH logic
ORH_MAX_ROW = 17    # Maximum row in Dashboard sheet for ORH setup to apply

# =====================================================================================================================
#
#                                         --- HELPER AND UTILITY FUNCTIONS ---
#
# =====================================================================================================================

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
    now = datetime.datetime.now()
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
        headers = {"Authorization": self.auth_token, "x-api-key": self.api_key, "x-client-code": self.client_code, "x-feed-token": self.feed_token}
        try:
            self._is_connected_flag = False
            update_connection_status("connecting")
            self.wsapp = websocket.WebSocketApp(self.ROOT_URI, header=headers, on_open=self._on_open, on_error=self._on_error, on_close=self._on_close, on_message=self._on_message, on_ping=self._on_ping, on_pong=self._on_pong)
            self.wsapp.run_forever(sslopt={"cert_reqs": ssl.CERT_NONE}, ping_interval=self.HEART_BEAT_INTERVAL)
        except Exception as e:
            logger.error(f"Error occurred during WebSocket connection: {e}")

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
                current_time = datetime.datetime.now()
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

    from_date = datetime.datetime.now().replace(hour=9, minute=15, second=0, microsecond=0).strftime("%Y-%m-%d %H:%M")
    to_date = datetime.datetime.now().strftime("%Y-%m-%d %H:%M")
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
                    logger.info(f"✅ Fetched {len(completed_3min_candles[token])} 3-min candles for {symbol_name} (Token: {token}).")
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

def fetch_25_day_high(smart_api_obj, symbols_to_fetch):
    """
    NEW: Fetches the highest price over the last 25 trading days for each symbol.
    """
    global twenty_five_day_high_cache
    logger.info("Fetching 25-day high for 'Full Positions'...")
    
    # Fetch data for a slightly larger window to ensure we get 25 trading days
    to_date = datetime.date.today()
    from_date = to_date - timedelta(days=40) 
    from_date_str = from_date.strftime("%Y-%m-%d %H:%M")
    to_date_str = to_date.strftime("%Y-%m-%d %H:%M")

    with data_lock:
        symbols_to_fetch_copy = symbols_to_fetch.copy()

    for token, details in symbols_to_fetch_copy.items():
        symbol_name = details['symbol']
        exchange_type = {'NSE': 1, 'BSE': 3, 'NFO': 2}.get(details.get('exchange', 'NSE').upper(), 1)
        exchange_str = {1: "NSE", 3: "BSE"}.get(exchange_type)
        if not exchange_str:
            logger.warning(f"Cannot fetch 25-day high for token {token}, unknown exchange type {exchange_type}")
            continue

        try:
            historic_param = {"exchange": exchange_str, "symboltoken": token, "interval": "ONE_DAY", "fromdate": from_date_str, "todate": to_date_str}
            response = smart_api_obj.getCandleData(historic_param)
            
            if response and response.get("status") and response.get("data"):
                # Find the highest price from the 'high' column (index 2) of the candle data
                highs = [candle[2] for candle in response["data"]]
                if highs:
                    max_high = max(highs)
                    twenty_five_day_high_cache[token] = max_high
                    logger.info(f"✅ 25-Day High for {symbol_name} (Token: {token}) is {max_high:.2f}")
                else:
                    logger.warning(f"No daily candle data found for {symbol_name} to calculate 25-day high.")
            else:
                logger.warning(f"Could not fetch 25-day high for {symbol_name}. Message: {response.get('message', 'Unknown error')}")
        except Exception as e:
            logger.error(f"Exception while fetching 25-day high for {symbol_name}: {e}")
        
        time.sleep(0.4) # Add delay to prevent rate limiting


# MODIFIED: This function now fetches data from the previous week's Monday.
def fetch_historical_candles_for_3pct_down(smart_api_obj, tokens_to_fetch, interval_api):
    """
    Fetches historical data for price/volume setups from the previous week's Monday to now.
    """
    # NEW: Calculate the start date as the Monday of the previous week.
    today = datetime.date.today()
    # today.weekday() is 0 for Monday, 6 for Sunday.
    # We go back to the previous Monday by subtracting the current weekday + 7 days.
    days_to_last_monday = today.weekday() + 7
    from_dt = datetime.datetime.combine(today - timedelta(days=days_to_last_monday), datetime.time.min)
    to_dt = datetime.datetime.now()
    
    from_date_str = from_dt.strftime("%Y-%m-%d %H:%M")
    to_date_str = to_dt.strftime("%Y-%m-%d %H:%M")
    
    logger.info(f"Fetching {interval_api} candles from {from_date_str} to {to_date_str}...")

    with data_lock:
        setup_details_copy = excel_3pct_setup_details.copy()

    for token_info in tokens_to_fetch:
        token, exchange_type = token_info[0], token_info[1]
        symbol_name = setup_details_copy.get(token, [{}])[0].get('symbol', 'Unknown')
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
                
                # Store the full history for analysis
                candle_history = []
                for c in candle_data:
                    candle_history.append({
                        'start_time': datetime.datetime.fromisoformat(c[0]), 'open': c[1], 'high': c[2], 
                        'low': c[3], 'close': c[4], 'volume': c[5] if len(c) > 5 else 0
                    })
                
                with data_lock:
                    volume_history_3pct[token][interval_api] = candle_history
                
                logger.info(f"✅ Fetched {len(candle_history)} candles for {symbol_name} ({interval_api}).")
            else:
                logger.warning(f"Fetch error for {symbol_name} ({interval_api}). Message: {response.get('message', 'Unknown error')}")
        except Exception as e:
            logger.error(f"Exception fetching data for {symbol_name} ({interval_api}): {e}")
        
        time.sleep(0.5) # Add delay between each symbol fetch to avoid rate limiting


def check_and_update_orh_setup():
    """Checks the latest completed 3-min candle for ORH setup and updates Google Sheet."""
    logger.info("🔍 Checking latest 3-min candle for ORH setup...")
    updates_queued = []
    orh_result_col_values = Dashboard.col_values(col_to_num(ORH_RESULT_COL))
    orh_buy_stop_col_values = Dashboard.col_values(col_to_num(ORH_BUY_STOP_COL))

    with data_lock:
        setup_details_copy = excel_setup_details.copy()

    for token, symbol_entries in setup_details_copy.items():
        # NEW: Filter symbol_entries to only include those within the ORH_MAX_ROW limit
        filtered_symbol_entries = [entry for entry in symbol_entries if entry['row'] <= ORH_MAX_ROW]
        
        if not filtered_symbol_entries: # If no entries are within the limit, skip this token
            continue

        candles, prev_high_entry = completed_3min_candles.get(token, []), previous_day_high_cache.get(token)
        if not candles or not prev_high_entry or not prev_high_entry.get("high"): continue

        prev_high = prev_high_entry["high"]
        latest_candle = candles[-1]
        high, low, close = latest_candle['high'], latest_candle['low'], latest_candle['close']
        orh_result, trigger_time_str, buy_stop_value = "No", "", None

        if high != low and close >= (low + 0.7 * (high - low)) and close > prev_high:
            orh_result, trigger_time_str = "Yes", latest_candle['start_time'].strftime('%H:%M')
            buy_stop_value = round(low * 0.995, 2)
            # Use filtered_symbol_entries[0]['symbol'] for logging as we know it's not empty
            logger.info(f"🚨 ORH Triggered for {filtered_symbol_entries[0]['symbol']} at {latest_candle['start_time']:%Y-%m-%d %H:%M:%S} ➤ O:{latest_candle['open']}, H:{high}, L:{low}, C:{close}, Buy Stop:{buy_stop_value}")
            
            if winsound:
                try:
                    winsound.Beep(1000, 400)
                except Exception as e:
                    logger.warning(f"🔇 Sound alert failed: {e}")

        new_value_orh_col = f"Yes({trigger_time_str})" if orh_result == "Yes" else "No"
        new_value_buy_stop_col = buy_stop_value if buy_stop_value else ""

        # Iterate over the filtered entries to apply updates
        for entry in filtered_symbol_entries: # Use filtered_symbol_entries here
            row, row_idx = entry["row"], entry["row"] - 1
            if row_idx < len(orh_result_col_values) and str(orh_result_col_values[row_idx]).strip() != new_value_orh_col.strip():
                updates_queued.append({"range": f"{ORH_RESULT_COL}{row}", "values": [[new_value_orh_col]]})
            if row_idx < len(orh_buy_stop_col_values) and str(orh_buy_stop_col_values[row_idx]).strip() != str(new_value_buy_stop_col).strip():
                updates_queued.append({"range": f"{ORH_BUY_STOP_COL}{row}", "values": [[new_value_buy_stop_col]]})

    if updates_queued:
        Dashboard.batch_update(updates_queued)
        logger.info(f"✅ Applied {len(updates_queued)} ORH updates to Dashboard.")
    else:
        logger.info("ℹ️ No ORH setup updates needed.")

# MODIFIED: This function has been significantly updated with the new logic for all setups.
def check_and_update_price_volume_setups():
    """
    Checks for 3% down, high volume, and highest up candle setups based on extended historical data.
    """
    logger.info("🔍 Checking for Price/Volume setups...")
    updates_queued = []
    
    pct_down_result_col_values = Dashboard.col_values(col_to_num(PCT_DOWN_RESULT_COL))
    high_volume_result_col_values = Dashboard.col_values(col_to_num(HIGH_VOL_RESULT_COL))
    highest_up_col_values = Dashboard.col_values(col_to_num(HIGHEST_UP_CANDLE_COL))

    with data_lock:
        setup_details_copy = excel_3pct_setup_details.copy()
        volume_history_copy = volume_history_3pct.copy()

    for token, symbol_entries in setup_details_copy.items():
        symbol_name = symbol_entries[0]['symbol']
        
        three_pct_down_candles = {}
        high_vol_candles = {}
        # NEW: Variables for Highest Up Candle setup
        highest_up_candle = None
        highest_up_candle_interval_api = None
        max_gain = -1 # Start with a negative number to ensure any gain is higher

        # --- Phase 1: Analyze historical data for all timeframes ---
        for interval_api in CANDLE_INTERVALS_3PCT_API:
            candle_history = volume_history_copy.get(token, {}).get(interval_api)

            if not candle_history:
                continue

            # --- Find representative candle for 3% Down setup ---
            triggered_3pct_candles = []
            for candle in candle_history:
                high_price, close_price = candle.get('high', 0), candle.get('close', 0)
                if high_price > 0 and (high_price - close_price) / high_price >= 0.03:
                    triggered_3pct_candles.append(candle)
            
            if triggered_3pct_candles:
                lowest_low_candle = min(triggered_3pct_candles, key=lambda c: c['low'])
                three_pct_down_candles[interval_api] = lowest_low_candle

            # --- Find representative candle for High Volume setup ---
            highest_volume_candle = max(candle_history, key=lambda c: c.get('volume', 0))
            high_vol_candles[interval_api] = highest_volume_candle

            # --- Find representative candle for Highest Up Candle setup ---
            for candle in candle_history:
                open_price, close_price = candle.get('open', 0), candle.get('close', 0)
                if open_price > 0:
                    gain = (close_price - open_price) / open_price
                    if gain > max_gain:
                        max_gain = gain
                        highest_up_candle = candle
                        highest_up_candle_interval_api = interval_api
        
        # --- Phase 2: Apply conditional logic for 3% DOWN output (Column AA) ---
        final_output_3_pct = ""
        c_1h_3pct = three_pct_down_candles.get('ONE_HOUR')
        c_30m_3pct = three_pct_down_candles.get('THIRTY_MINUTE')
        c_15m_3pct = three_pct_down_candles.get('FIFTEEN_MINUTE')
        
        available_candles_3pct = [c for c in [c_1h_3pct, c_30m_3pct, c_15m_3pct] if c]

        if len(available_candles_3pct) > 0:
            lows = [c['low'] for c in available_candles_3pct]
            if len(set(lows)) == 1:
                selected_candle = c_1h_3pct if c_1h_3pct else c_30m_3pct if c_30m_3pct else c_15m_3pct
                interval_api = next(key for key, val in three_pct_down_candles.items() if val == selected_candle)
                interval_name = CANDLE_INTERVAL_MAP_DISPLAY[interval_api]
                time_str = selected_candle['start_time'].strftime('%H:%M')
                final_output_3_pct = f"{selected_candle['low']:.2f}({interval_name}, {time_str})"
            else:
                min_low, max_low = min(lows), max(lows)
                if min_low > 0 and (max_low - min_low) / min_low <= 0.01: # Using 1% threshold
                    lowest_price_candle = min(available_candles_3pct, key=lambda c: c['low'])
                    interval_api = next(key for key, val in three_pct_down_candles.items() if val == lowest_price_candle)
                    interval_name = CANDLE_INTERVAL_MAP_DISPLAY[interval_api]
                    time_str = lowest_price_candle['start_time'].strftime('%H:%M')
                    final_output_3_pct = f"{lowest_price_candle['low']:.2f}({interval_name}, {time_str})"
                else:
                    output_parts = []
                    def check_pair(c1, c2, name1, name2, threshold):
                        if c1 and c2:
                            low1, low2 = c1['low'], c2['low']
                            if min(low1, low2) > 0 and abs(low1 - low2) / min(low1, low2) > threshold:
                                time1 = c1['start_time'].strftime('%H:%M')
                                time2 = c2['start_time'].strftime('%H:%M')
                                return [f"{low1:.2f}({name1}, {time1})", f"{low2:.2f}({name2}, {time2})"]
                        return None
                    
                    pair_result = check_pair(c_1h_3pct, c_30m_3pct, '1 Hour', '30 Min', 0.01)
                    if pair_result: output_parts = pair_result
                    elif not pair_result:
                        pair_result = check_pair(c_1h_3pct, c_15m_3pct, '1 Hour', '15 Min', 0.01)
                        if pair_result: output_parts = pair_result
                    elif not pair_result:
                        pair_result = check_pair(c_30m_3pct, c_15m_3pct, '30 Min', '15 Min', 0.01)
                        if pair_result: output_parts = pair_result

                    if output_parts:
                        final_output_3_pct = ", ".join(sorted(output_parts, reverse=True))
                    else:
                        lowest_price_candle = min(available_candles_3pct, key=lambda c: c['low'])
                        interval_api = next(key for key, val in three_pct_down_candles.items() if val == lowest_price_candle)
                        interval_name = CANDLE_INTERVAL_MAP_DISPLAY[interval_api]
                        time_str = lowest_price_candle['start_time'].strftime('%H:%M')
                        final_output_3_pct = f"{lowest_price_candle['low']:.2f}({interval_name}, {time_str})"


        # --- Phase 3: Apply conditional logic for HIGH VOLUME output (Column AC) ---
        final_output_high_vol = ""
        c_1h_hv = high_vol_candles.get('ONE_HOUR')
        c_30m_hv = high_vol_candles.get('THIRTY_MINUTE')
        c_15m_hv = high_vol_candles.get('FIFTEEN_MINUTE')

        available_candles_hv = [c for c in [c_1h_hv, c_30m_hv, c_15m_hv] if c]
        
        if len(available_candles_hv) > 0:
            lows = [c['low'] for c in available_candles_hv]
            if len(set(lows)) == 1:
                selected_candle = c_1h_hv if c_1h_hv else c_30m_hv if c_30m_hv else c_15m_hv
                interval_api = next(key for key, val in high_vol_candles.items() if val == selected_candle)
                interval_name = CANDLE_INTERVAL_MAP_DISPLAY[interval_api]
                time_str = selected_candle['start_time'].strftime('%H:%M')
                final_output_high_vol = f"{selected_candle['low']:.2f}({interval_name}, {time_str})"
            else:
                min_low, max_low = min(lows), max(lows)
                if min_low > 0 and (max_low - min_low) / min_low <= 0.02: # Using 2% threshold
                    lowest_price_candle = min(available_candles_hv, key=lambda c: c['low'])
                    interval_api = next(key for key, val in high_vol_candles.items() if val == lowest_price_candle)
                    interval_name = CANDLE_INTERVAL_MAP_DISPLAY[interval_api]
                    time_str = lowest_price_candle['start_time'].strftime('%H:%M')
                    final_output_high_vol = f"{lowest_price_candle['low']:.2f}({interval_name}, {time_str})"
                else:
                    output_parts = []
                    def check_pair(c1, c2, name1, name2, threshold):
                        if c1 and c2:
                            low1, low2 = c1['low'], c2['low']
                            if min(low1, low2) > 0 and abs(low1 - low2) / min(low1, low2) > threshold:
                                time1 = c1['start_time'].strftime('%H:%M')
                                time2 = c2['start_time'].strftime('%H:%M')
                                return [f"{low1:.2f}({name1}, {time1})", f"{low2:.2f}({name2}, {time2})"]
                        return None

                    pair_result = check_pair(c_1h_hv, c_30m_hv, '1 Hour', '30 Min', 0.02)
                    if pair_result: output_parts = pair_result
                    elif not pair_result:
                        pair_result = check_pair(c_1h_hv, c_15m_hv, '1 Hour', '15 Min', 0.02)
                        if pair_result: output_parts = pair_result
                    elif not pair_result:
                        pair_result = check_pair(c_30m_hv, c_15m_hv, '30 Min', '15 Min', 0.02)
                        if pair_result: output_parts = pair_result

                    if output_parts:
                        final_output_high_vol = ", ".join(sorted(output_parts, reverse=True))
                    else:
                        lowest_price_candle = min(available_candles_hv, key=lambda c: c['low'])
                        interval_api = next(key for key, val in high_vol_candles.items() if val == lowest_price_candle)
                        interval_name = CANDLE_INTERVAL_MAP_DISPLAY[interval_api]
                        time_str = lowest_price_candle['start_time'].strftime('%H:%M')
                        final_output_high_vol = f"{lowest_price_candle['low']:.2f}({interval_name}, {time_str})"
        
        # --- Phase 4: Format output for HIGHEST UP CANDLE output (Column X) ---
        final_output_highest_up = ""
        if highest_up_candle:
            interval_name = CANDLE_INTERVAL_MAP_DISPLAY[highest_up_candle_interval_api]
            time_str = highest_up_candle['start_time'].strftime('%H:%M')
            final_output_highest_up = f"{highest_up_candle['low']:.2f}({interval_name}, {time_str})"

        # --- Phase 5: Prepare updates for the sheet ---
        for entry in symbol_entries:
            row, row_idx = entry["row"], entry["row"] - 1

            new_value_3pct = final_output_3_pct
            existing_value_3pct = pct_down_result_col_values[row_idx] if row_idx < len(pct_down_result_col_values) else ""
            if str(existing_value_3pct).strip() != new_value_3pct.strip():
                updates_queued.append({"range": f"{PCT_DOWN_RESULT_COL}{row}", "values": [[new_value_3pct]]})

            new_value_high_vol = final_output_high_vol
            existing_value_high_vol = high_volume_result_col_values[row_idx] if row_idx < len(high_volume_result_col_values) else ""
            if str(existing_value_high_vol).strip() != new_value_high_vol.strip():
                updates_queued.append({"range": f"{HIGH_VOL_RESULT_COL}{row}", "values": [[new_value_high_vol]]})

            # NEW: Update for Highest Up Candle (Column X)
            existing_highest_up = highest_up_col_values[row_idx] if row_idx < len(highest_up_col_values) else ""
            if str(existing_highest_up).strip() != final_output_highest_up.strip():
                updates_queued.append({"range": f"{HIGHEST_UP_CANDLE_COL}{row}", "values": [[final_output_highest_up]]})

    if updates_queued:
        Dashboard.batch_update(updates_queued)
        logger.info(f"✅ Applied {len(updates_queued)} Price/Volume setup updates to Dashboard.")
    else:
        logger.info("ℹ️ No Price/Volume setup updates needed.")


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

def get_or_fetch_token_for_symbol(symbol_name, exchange_name, smart_api_obj, session_cache):
    """
    Helper function to get or fetch a token for a given symbol.
    It uses a session-level cache to avoid redundant API calls within the same scan.
    """
    if not symbol_name or str(symbol_name).strip().upper() in ('SYMBOL', ''):
        return None

    # Use a consistent key for the cache
    cache_key = (symbol_name.strip().upper(), exchange_name.strip().upper())
    if cache_key in session_cache:
        logger.debug(f"Using session-cached token for {symbol_name}.")
        return session_cache[cache_key]

    # If not in session cache, always fetch from SmartAPI to ensure correctness.
    found_token = None
    symbol_clean = symbol_name.strip().upper()
    exchange_clean = exchange_name.strip().upper()
    search_term_api = "NIFTY" if symbol_clean == "NIFTY 50" else "BANKNIFTY" if symbol_clean == "BANKNIFTY" else symbol_clean
    exchange_for_search_api = "NSE" if "NIFTY" in search_term_api else exchange_clean

    for attempt in range(SCRIP_SEARCH_RETRY_ATTEMPTS):
        try:
            # Add a delay *before* each API call to prevent rate limiting.
            time.sleep(0.4)
            search_response = smart_api_obj.searchScrip(exchange_for_search_api, search_term_api)

            if search_response and search_response.get('status') and search_response.get('data'):
                for scrip_data in search_response['data']:
                    if scrip_data.get('tradingsymbol', '').strip().upper() == symbol_clean:
                        found_token = scrip_data.get('symboltoken')
                        break
            if found_token:
                logger.info(f"Successfully fetched token for {symbol_clean}: {found_token}")
                break # Token found, exit retry loop
            else:
                logger.warning(f"Could not find a token for '{symbol_clean}' in the search results (Attempt {attempt + 1}).")

        except Exception as e:
            logger.error(f"Error searching for scrip '{symbol_name}': {e}")

        time.sleep(SCRIP_SEARCH_RETRY_DELAY * (SCRIP_SEARCH_RETRY_MULTIPLIER ** attempt))

    if not found_token:
        logger.error(f"Failed to fetch token for {symbol_name} after {SCRIP_SEARCH_RETRY_ATTEMPTS} retries.")

    # If a token was found, add it to the session cache for this scan
    if found_token:
        session_cache[cache_key] = found_token

    return found_token

def scan_sheet_for_all_symbols(smart_api_obj, Dashboard, ATHCache):
    """
    Unified function to scan the Google Sheet.
    This function now handles updates and deletions of tokens in the ATH Cache.
    """
    logger.info("Scanning Google Sheet for all symbols (Dashboard and Setups)...")
    
    local_dashboard_details = collections.defaultdict(list)
    local_setup_details = collections.defaultdict(list)
    local_3pct_setup_details = collections.defaultdict(list)
    all_tokens_found = set()
    
    # Add a temporary cache for this specific scan to avoid re-fetching the same token.
    scan_session_token_cache = {}

    # Store expected state of ATH Cache based on current Dashboard symbols
    expected_ath_cache_state = {} # Key: (row, col_letter), Value: token
    ath_cache_updates_queued = [] # List of gspread batch update requests

    try:
        all_dashboard_values = Dashboard.get_all_values()
        all_ath_cache_values = ATHCache.get_all_values() # Read ATH Cache once for comparison

        # FIX: Create a map of existing tokens from the ATH cache for quick lookup.
        # Key: (row, col_letter), Value: (symbol_from_dashboard, token_from_ath_cache)
        # This helps detect if a symbol has changed.
        symbol_token_map = {}
        for row_idx, row_data in enumerate(all_ath_cache_values):
            row_num = row_idx + 1
            # Check Column Y
            if len(row_data) > col_to_num(ATH_CACHE_Y_COL_DASH) -1:
                token = row_data[col_to_num(ATH_CACHE_Y_COL_DASH) - 1]
                symbol = get_cell_value(all_dashboard_values, row_num, FOCUS_SYMBOL_COL)
                if token and symbol:
                    symbol_token_map[(row_num, ATH_CACHE_Y_COL_DASH)] = (symbol.strip().upper(), token)
            # Check Column Z
            if len(row_data) > col_to_num(ATH_CACHE_Z_COL_DASH) -1:
                token = row_data[col_to_num(ATH_CACHE_Z_COL_DASH) - 1]
                symbol = get_cell_value(all_dashboard_values, row_num, FULL_SYMBOL_COL)
                if token and symbol:
                    symbol_token_map[(row_num, ATH_CACHE_Z_COL_DASH)] = (symbol.strip().upper(), token)


        end_row_focus_list = INDEX_START_ROW - 1
        end_row_full_positions = QUARTER_POSITIONS_START_ROW - 1
        max_row_dashboard = get_last_row_in_column(Dashboard, FULL_SYMBOL_COL) # Get max row from dashboard for scanning

        # --- Phase 1: Scan Dashboard and determine expected ATH Cache state ---
        # Iterate up to a reasonable max row to catch all potential dashboard entries
        for row in range(START_ROW_DATA, max_row_dashboard + 20): 
            
            def process_symbol(symbol, exchange, row_num, token_col, block_details):
                if not symbol or not exchange:
                    return

                token = None
                # Check if the symbol has changed compared to our initial map
                map_key = (row_num, token_col)
                cached_info = symbol_token_map.get(map_key)

                if cached_info and cached_info[0] == symbol.strip().upper():
                    # Symbol is the same as before, trust the cached token
                    token = cached_info[1]
                    logger.debug(f"Using trusted token {token} for unchanged symbol {symbol} at {map_key}")
                else:
                    # Symbol is new or has changed, must fetch from API
                    if cached_info:
                        logger.info(f"Symbol changed at {map_key}: from '{cached_info[0]}' to '{symbol.strip().upper()}'. Fetching new token.")
                    else:
                        logger.info(f"New symbol '{symbol.strip().upper()}' found at {map_key}. Fetching new token.")
                    token = get_or_fetch_token_for_symbol(symbol, exchange, smart_api_obj, scan_session_token_cache)
                
                if token:
                    all_tokens_found.add(token)
                    if 'ltp_col' in block_details: # Dashboard item
                        local_dashboard_details[token].append({'row': row_num, 'symbol': symbol, 'exchange': exchange, **block_details})
                    if 'setup_type' in block_details: # Setup item
                        exchange_type_int = {'NSE': 1, 'BSE': 3}.get(str(exchange).strip().upper())
                        if exchange_type_int:
                            if block_details['setup_type'] == 'ORH':
                                local_setup_details[token].append({'symbol': symbol, 'row': row_num, 'exchange_type': exchange_type_int})
                            elif block_details['setup_type'] == '3PCT':
                                local_3pct_setup_details[token].append({'symbol': symbol, 'row': row_num, 'pct_down_col': PCT_DOWN_RESULT_COL, 'exchange_type': exchange_type_int})
                    expected_ath_cache_state[map_key] = token


            # Focus List (Column C)
            if row <= end_row_focus_list:
                exchange = get_cell_value(all_dashboard_values, row, FOCUS_EXCHANGE_COL)
                symbol = get_cell_value(all_dashboard_values, row, FOCUS_SYMBOL_COL)
                process_symbol(symbol, exchange, row, ATH_CACHE_Y_COL_DASH, {
                    'ltp_col': FOCUS_LTP_COL, 
                    'chg_col': FOCUS_CHG_COL, 
                    'block_type': 'Focus List',
                    'symbol_col': FOCUS_SYMBOL_COL,
                    'token_cache_col': ATH_CACHE_Y_COL_DASH
                })

            # Full Positions (Column M)
            if row <= end_row_full_positions:
                exchange = get_cell_value(all_dashboard_values, row, FULL_EXCHANGE_COL)
                symbol = get_cell_value(all_dashboard_values, row, FULL_SYMBOL_COL)
                process_symbol(symbol, exchange, row, ATH_CACHE_Z_COL_DASH, {
                    'ltp_col': FULL_LTP_COL, 'chg_col': '', 'block_type': 'Full Positions', 
                    'symbol_col': FULL_SYMBOL_COL,
                    'token_cache_col': ATH_CACHE_Z_COL_DASH,
                    'price_col': FULL_PRICE_COL, 'qty_col': FULL_QTY_COL, 
                    'return_amt_col': FULL_RETURN_AMT_COL, 
                    'return_pct_col': FULL_RETURN_PCT_COL,
                    'highest_up_candle_col': HIGHEST_UP_CANDLE_COL, # NEW
                    'percent_from_high_col': FULL_PERCENT_FROM_HIGH_COL, 'entry_date_col': FULL_ENTRY_DATE_COL, 
                    'days_duration_col': FULL_DAYS_DURATION_COL
                })

            # ORH Setup (Column C, token in Y)
            exchange_orh = get_cell_value(all_dashboard_values, row, ORH_EXCHANGE_COL)
            symbol_orh = get_cell_value(all_dashboard_values, row, ORH_SYMBOL_COL)
            if exchange_orh and symbol_orh and row <= ORH_MAX_ROW:
                process_symbol(symbol_orh, exchange_orh, row, ORH_TOKEN_COL, {'setup_type': 'ORH'})
            
            # 3% Down Setup (Column M, token in Z)
            exchange_3pct = get_cell_value(all_dashboard_values, row, PCT_EXCHANGE_COL_3PCT)
            symbol_3pct = get_cell_value(all_dashboard_values, row, PCT_SYMBOL_COL_3PCT)
            if exchange_3pct and symbol_3pct:
                process_symbol(symbol_3pct, exchange_3pct, row, PCT_TOKEN_COL_3PCT, {'setup_type': '3PCT'})

        # --- Phase 2: Compare and Queue ATH Cache Updates ---
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
                        ath_cache_updates_queued.append({
                            'range': f"{col_letter}{row_num}",
                            'values': [['']] 
                        })
                        logger.info(f"Queued clearing token in ATH Cache cell {col_letter}{row_num} (was '{current_token_in_ath_cache}') as symbol is no longer in Dashboard at this location.")
                elif str(expected_token_for_row_col).strip() != str(current_token_in_ath_cache).strip():
                    ath_cache_updates_queued.append({
                        'range': f"{col_letter}{row_num}",
                        'values': [[int(expected_token_for_row_col)]] 
                    })
                    logger.info(f"Queued updating token in ATH Cache cell {col_letter}{row_num} from '{current_token_in_ath_cache}' to '{expected_token_for_row_col}'.")

        if ath_cache_updates_queued:
            try:
                ATHCache.batch_update(ath_cache_updates_queued)
                logger.info(f"✅ Applied {len(ath_cache_updates_queued)} batch updates to ATH Cache sheet.")
            except Exception as e:
                logger.exception(f"An error occurred during batch update to ATH Cache sheet: {e}")
        else:
            logger.info("ℹ️ No ATH Cache updates needed.")

    except Exception as e:
        logger.exception(f"Error during unified symbol scan and ATH Cache management: {e}")

    logger.info(f"Finished unified scan. Found {len(all_tokens_found)} unique tokens.")
    return local_dashboard_details, local_setup_details, local_3pct_setup_details, all_tokens_found


def update_excel_live_data():
    """
    Updates the Google Sheet with live data for the dashboard portion, including all calculations.
    """
    global cells_to_clear_color
    
    with data_lock:
        dashboard_details_copy = excel_dashboard_details.copy()

    if not smart_ws or not smart_ws._is_connected_flag:
        logger.warning("WebSocket not connected. Skipping Google Sheet update.")
        return

    requests = []
    # MODIFIED: The red color code has been changed as per your request.
    GREEN_COLOR, RED_COLOR = (149, 203, 186), (254, 112, 112)
    dashboard_sheet_id = Dashboard.id
    ath_cache_sheet_id = ATHCache.id
    
    cells_to_color_this_cycle = set()

    # --- Part 1: Clear colors from the previous cycle ---
    if cells_to_clear_color:
        for cell_a1 in cells_to_clear_color:
            col_letter = ''.join(filter(str.isalpha, cell_a1))
            row_num = int(''.join(filter(str.isdigit, cell_a1)))
            cell_range = {"sheetId": dashboard_sheet_id, "startRowIndex": row_num - 1, "endRowIndex": row_num, "startColumnIndex": col_to_num(col_letter) - 1, "endColumnIndex": col_to_num(col_letter)}
            requests.append({"repeatCell": {"range": cell_range, "cell": {"userEnteredFormat": {"backgroundColor": rgb_to_float(None)}}, "fields": "userEnteredFormat.backgroundColor"}})
    
    cells_to_clear_color.clear()


    # --- Part 2: Fetch input data and prepare new updates ---
    input_ranges = []
    for list_of_details in dashboard_details_copy.values():
        for details in list_of_details:
            row_num = details['row']
            # Always fetch the symbol column for the clear check
            if details.get("symbol_col"): 
                input_ranges.append(f'{details["symbol_col"]}{row_num}')
            
            if details.get('block_type') == "Full Positions":
                if details.get("price_col"): input_ranges.append(f'{details["price_col"]}{row_num}')
                if details.get("qty_col"): input_ranges.append(f'{details["qty_col"]}{row_num}')
                if details.get("entry_date_col"): input_ranges.append(f'{details["entry_date_col"]}{row_num}')
                # NEW: Add confirmation columns to the input ranges to prevent unnecessary writes
                input_ranges.append(f'{HIGHEST_UP_CANDLE_COL}{row_num}')
                input_ranges.append(f'{HIGHEST_UP_CANDLE_STATUS_COL}{row_num}')
                input_ranges.append(f'{PCT_DOWN_RESULT_COL}{row_num}')
                input_ranges.append(f'{PCT_DOWN_STATUS_COL}{row_num}')
                input_ranges.append(f'{HIGH_VOL_RESULT_COL}{row_num}')
                input_ranges.append(f'{HIGH_VOL_STATUS_COL}{row_num}')
    
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

    for token, list_of_details in dashboard_details_copy.items():
        if token not in latest_tick_data:
            continue
            
        current_ltp = latest_tick_data[token].get('ltp')
        if current_ltp is None: continue

        previous_ltp = previous_ltp_data.get(token)
        ltp_cell_color = None
        if previous_ltp is not None and current_ltp != previous_ltp:
            ltp_cell_color = GREEN_COLOR if current_ltp > previous_ltp else RED_COLOR
        previous_ltp_data[token] = current_ltp

        for details in list_of_details:
            row_num = details['row']
            
            # Fast clearing logic for both "Focus List" and "Full Positions"
            symbol_in_memory = details.get('symbol', '').strip().upper()
            symbol_on_sheet_raw = input_data.get(f"{details.get('symbol_col')}{row_num}")
            symbol_on_sheet = str(symbol_on_sheet_raw).strip().upper() if symbol_on_sheet_raw else ""

            # Trigger clear if the symbol is blank OR if it has changed from what's in memory
            if not symbol_on_sheet or (symbol_on_sheet != symbol_in_memory):
                if details.get('block_type') == "Focus List":
                    start_col, end_col = 'D', 'I'
                elif details.get('block_type') == "Full Positions":
                    start_col, end_col = 'N', 'AF'
                else:
                    continue # Not a block we need to clear

                log_msg = f"Detected cleared symbol at row {row_num}." if not symbol_on_sheet else f"Detected updated symbol at row {row_num} from '{symbol_in_memory}' to '{symbol_on_sheet}'."
                logger.info(f"{log_msg} Queuing fast clear for {start_col}{row_num}:{end_col}{row_num}.")
                
                # Request 1: Clear the Dashboard data range (values and background color only)
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
                # Request 2: Clear the corresponding token in the ATH Cache sheet
                token_cache_col = details.get('token_cache_col')
                if token_cache_col:
                    logger.info(f"Also queueing fast clear for ATH Cache cell {token_cache_col}{row_num}.")
                    requests.append({
                        "repeatCell": {
                            "range": {
                                "sheetId": ath_cache_sheet_id,
                                "startRowIndex": row_num - 1, "endRowIndex": row_num,
                                "startColumnIndex": col_to_num(token_cache_col) - 1, "endColumnIndex": col_to_num(token_cache_col)
                            },
                            "cell": {}, "fields": "userEnteredValue"
                        }
                    })
                # If the symbol cell itself was completely cleared, add a request to clear it too
                if not symbol_on_sheet:
                    symbol_col = details.get('symbol_col')
                    if symbol_col:
                        requests.append({
                            "repeatCell": {
                                "range": {
                                    "sheetId": dashboard_sheet_id,
                                    "startRowIndex": row_num - 1, "endRowIndex": row_num,
                                    "startColumnIndex": col_to_num(symbol_col) - 1, "endColumnIndex": col_to_num(symbol_col)
                                },
                                "cell": {}, "fields": "userEnteredValue"
                            }
                        })
                continue # Skip all further processing for this cleared row

            def queue_update(col_letter, value, number_format_pattern=None, bg_color=None, is_ltp=False):
                if not col_letter: return
                cell_a1 = f"{col_letter}{row_num}"
                cell_range = {"sheetId": dashboard_sheet_id, "startRowIndex": row_num - 1, "endRowIndex": row_num, "startColumnIndex": col_to_num(col_letter) - 1, "endColumnIndex": col_to_num(col_letter)}
                user_entered_value = {}
                if isinstance(value, (int, float)): user_entered_value["numberValue"] = value
                else: user_entered_value["stringValue"] = str(value)
                
                requests.append({"updateCells": {"rows": [{"values": [{"userEnteredValue": user_entered_value}]}], "fields": "userEnteredValue", "range": cell_range}})
                
                user_entered_format = {}
                if bg_color:
                    user_entered_format["backgroundColor"] = rgb_to_float(bg_color)
                    if is_ltp:
                        cells_to_color_this_cycle.add(cell_a1)
                
                if number_format_pattern:
                    user_entered_format["numberFormat"] = {"type": "NUMBER" if isinstance(value, (int, float)) else "TEXT", "pattern": number_format_pattern}
                
                if user_entered_format:
                    fields = ",".join([f"userEnteredFormat.{k}" for k in user_entered_format.keys()])
                    requests.append({"repeatCell": {"range": cell_range, "cell": {"userEnteredFormat": user_entered_format}, "fields": fields}})


            queue_update(details.get('ltp_col'), current_ltp, "#,##0.00", ltp_cell_color, is_ltp=True)
            
            if details.get('chg_col') and token in latest_quote_data:
                percentage_change = latest_quote_data[token].get('percentChange', 0.0)
                percentage_change_decimal = percentage_change / 100.0 if percentage_change is not None else 0.0
                chg_cell_color = GREEN_COLOR if percentage_change > 0 else RED_COLOR if percentage_change < 0 else None
                queue_update(details['chg_col'], percentage_change_decimal, "0.00%", chg_cell_color)

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
                    queue_update(details.get('return_amt_col'), return_amt, "#,##0.00", GREEN_COLOR if return_amt > 0 else RED_COLOR if return_amt < 0 else None)
                    queue_update(details.get('return_pct_col'), return_pct, "0.00%", GREEN_COLOR if return_pct > 0 else RED_COLOR if return_pct < 0 else None)
                else:
                    queue_update(details.get('return_amt_col'), "", "General", bg_color=None)
                    queue_update(details.get('return_pct_col'), "", "General", bg_color=None)
                    for col_key in ['return_amt_col', 'return_pct_col']:
                        col_letter = details.get(col_key)
                        if col_letter:
                            cell_range = {"sheetId": dashboard_sheet_id, "startRowIndex": row_num - 1, "endRowIndex": row_num, "startColumnIndex": col_to_num(col_letter) - 1, "endColumnIndex": col_to_num(col_letter)}
                            requests.append({"repeatCell": {"range": cell_range, "cell": {"userEnteredFormat": {"backgroundColor": rgb_to_float(None)}}, "fields": "userEnteredFormat.backgroundColor"}})

                if token in twenty_five_day_high_cache:
                    high_25_day = twenty_five_day_high_cache[token]
                    if high_25_day > 0:
                        percent_from_high = (current_ltp - high_25_day) / high_25_day
                        queue_update(details.get('percent_from_high_col'), percent_from_high, "0.00%")
                else:
                    queue_update(details.get('percent_from_high_col'), "", "General")

                days_duration = ""
                if entry_date_str:
                    try:
                        days_duration = f"{(datetime.datetime.now() - datetime.datetime.strptime(entry_date_str, '%d-%b-%y')).days} Days"
                    except ValueError: days_duration = "Invalid Date"
                queue_update(details.get('days_duration_col'), days_duration, "@")

                # --- NEW Confirmation Logic with Coloring ---
                def get_confirmation_status_and_color(cell_text, ltp):
                    if not cell_text or not isinstance(cell_text, str):
                        return "", None
                    
                    prices_str = re.findall(r"(\d+\.\d+)", cell_text)
                    if not prices_str:
                        return "", None
                    
                    prices = [float(p) for p in prices_str]
                    statuses = []
                    for price in prices:
                        if ltp > price:
                            statuses.append("Above")
                        else:
                            statuses.append("Below")
                    
                    status_text = "/".join(statuses)
                    
                    # Determine color based on the first status
                    color = None
                    if status_text.startswith("Above"):
                        color = GREEN_COLOR
                    elif status_text.startswith("Below"):
                        color = RED_COLOR
                        
                    return status_text, color

                # Highest Up Candle Confirmation (X -> Y)
                x_text = input_data.get(f'{HIGHEST_UP_CANDLE_COL}{row_num}')
                y_status, y_color = get_confirmation_status_and_color(x_text, current_ltp)
                existing_y_status = input_data.get(f'{HIGHEST_UP_CANDLE_STATUS_COL}{row_num}')
                if str(y_status) != str(existing_y_status or ''):
                     queue_update(HIGHEST_UP_CANDLE_STATUS_COL, y_status, "@", bg_color=y_color)

                # 3% Down Confirmation (AA -> AB)
                aa_text = input_data.get(f'{PCT_DOWN_RESULT_COL}{row_num}')
                ab_status, ab_color = get_confirmation_status_and_color(aa_text, current_ltp)
                existing_ab_status = input_data.get(f'{PCT_DOWN_STATUS_COL}{row_num}')
                if str(ab_status) != str(existing_ab_status or ''):
                     queue_update(PCT_DOWN_STATUS_COL, ab_status, "@", bg_color=ab_color)

                # High Volume Confirmation (AC -> AD)
                ac_text = input_data.get(f'{HIGH_VOL_RESULT_COL}{row_num}')
                ad_status, ad_color = get_confirmation_status_and_color(ac_text, current_ltp)
                existing_ad_status = input_data.get(f'{HIGH_VOL_STATUS_COL}{row_num}')
                if str(ad_status) != str(existing_ad_status or ''):
                     queue_update(HIGH_VOL_STATUS_COL, ad_status, "@", bg_color=ad_color)
    
    cells_to_clear_color = cells_to_color_this_cycle

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
    logger.info("✅ Live dashboard updater thread started.")
    while True:
        try:
            update_excel_live_data()
            time.sleep(0.25)
        except Exception as e:
            logger.exception(f"Error in dashboard updater thread: {e}")
            time.sleep(5)

def run_quote_updater():
    """
    This function runs in a dedicated thread to periodically fetch the full quote
    (including percentChange) for all symbols using the REST API.
    """
    global latest_quote_data
    logger.info("✅ Quote updater thread started.")
    while True:
        try:
            with data_lock:
                # Create a safe copy of the dashboard details to identify focus list tokens
                dashboard_details_copy = excel_dashboard_details.copy()

            focus_list_tokens = {} # Using a dict to store token -> exchange mapping
            for token, details_list in dashboard_details_copy.items():
                for details in details_list:
                    if details.get('block_type') == 'Focus List':
                        exchange_name = details.get("exchange", "NSE").upper()
                        focus_list_tokens[token] = exchange_name
                        break # Found it for this token, no need to check other entries

            if not focus_list_tokens:
                time.sleep(5)
                continue

            # Group tokens by exchange for batching
            tokens_by_exchange = collections.defaultdict(list)
            for token, exchange in focus_list_tokens.items():
                tokens_by_exchange[exchange].append(token)

            new_quote_data = {}
            
            # Iterate through each exchange and batch tokens for API calls
            for exchange, tokens_list in tokens_by_exchange.items():
                # Split tokens into batches if they exceed the limit
                for i in range(0, len(tokens_list), QUOTE_API_MAX_TOKENS):
                    batch_tokens = tokens_list[i:i + QUOTE_API_MAX_TOKENS]
                    
                    payload = {
                        "mode": "FULL",
                        "exchangeTokens": {
                            exchange: batch_tokens
                        }
                    }
                    
                    logger.info(f"Fetching market data for {len(batch_tokens)} tokens on {exchange}...")
                    response = smart_api_obj.getMarketData(**payload)

                    # Process the response for the current batch
                    if response and response.get("status") and isinstance(response.get("data"), dict):
                        fetched_data = response["data"].get("fetched", [])
                        for item in fetched_data:
                            if isinstance(item, dict):
                                token = item.get("symbolToken")
                                if token:
                                    new_quote_data[token] = {
                                        "percentChange": item.get("percentChange"),
                                        "netChange": item.get("netChange"),
                                    }
                    else:
                        logger.warning(f"Could not fetch quote data for batch (Exchange: {exchange}, Tokens: {batch_tokens}). Response: {response}")
                    
                    # Add a small delay between batches to avoid hitting rate limits too quickly
                    time.sleep(0.5)

            # Safely update the global quote data dictionary with all fetched data
            with data_lock:
                latest_quote_data.update(new_quote_data)

            # Wait for a few seconds before the next major API call to avoid overall rate limiting
            time.sleep(3)

        except Exception as e:
            logger.exception(f"Error in quote updater thread: {e}")
            time.sleep(10)

def run_initial_setup_data_fetch():
    """
    This function now runs in a dedicated thread to fetch all historical data
    for the setups without blocking the main application startup.
    """
    logger.info("⚙️ Starting background fetch for initial setup data...")
    
    # FIX: Removed the is_market_hours() check to allow testing at any time.
    with data_lock:
        orh_details_copy = excel_setup_details.copy()
        pct3_details_copy = excel_3pct_setup_details.copy()
        # Get a copy of full positions for the 25-day high fetch
        full_positions_copy = {
            token: details[0] for token, details in excel_dashboard_details.items() 
            if details and details[0].get('block_type') == 'Full Positions'
        }

    # Fetch data for ORH setup
    fetch_initial_candle_data(smart_api_obj, orh_details_copy)
    fetch_previous_day_candle_data(smart_api_obj, orh_details_copy)
    
    # Fetch data for 3% Down setup
    unique_tokens_3pct = list(set([(token, details[0]['exchange_type']) for token, details in pct3_details_copy.items() if details]))
    for interval_api in CANDLE_INTERVALS_3PCT_API:
        fetch_historical_candles_for_3pct_down(smart_api_obj, unique_tokens_3pct, interval_api)
    
    # NEW: Fetch data for % from High setup
    fetch_25_day_high(smart_api_obj, full_positions_copy)

    logger.info("✅ Background fetch for initial setup data complete.")


def run_background_task_scheduler():
    """
    This function runs in a dedicated thread to handle all the slower,
    scheduled tasks like scanning the sheet for new symbols and checking for trade setups.
    """
    global subscribed_tokens, excel_dashboard_details, excel_setup_details, excel_3pct_setup_details
    logger.info("✅ Background task scheduler thread started.")
    
    last_checked_minute_orh, last_checked_minute_15min, last_checked_minute_30min, last_checked_minute_1hr, last_checked_minute_confirmation = None, None, None, None, None
    last_scan_time = 0
    
    # The initial, slow data fetch is now moved to its own thread (`run_initial_setup_data_fetch`)
    # This allows the scheduler to start immediately.

    while True:
        try:
            now = datetime.datetime.now()
            current_minute = now.minute

            # --- Periodically re-scan Google Sheet and manage subscriptions ---
            if time.time() - last_scan_time > 15: # Scan every 15 seconds
                logger.info("Rescanning Google Sheet for symbol changes...")
                
                new_dashboard, new_orh, new_3pct, current_excel_tokens = scan_sheet_for_all_symbols(smart_api_obj, Dashboard, ATHCache)
                
                with data_lock:
                    excel_dashboard_details = new_dashboard
                    excel_setup_details = new_orh
                    excel_3pct_setup_details = new_3pct
                
                # Check for new tokens that need a subscription
                tokens_to_subscribe = current_excel_tokens - subscribed_tokens
                if tokens_to_subscribe and smart_ws and smart_ws._is_connected_flag:
                    subscribe_list_grouped = collections.defaultdict(list)
                    # This check is now redundant since we subscribe immediately at startup, but good for dynamic additions
                    for token in tokens_to_subscribe:
                        with data_lock:
                            # Determine exchange type from any of the detail dicts
                            if token in excel_dashboard_details and excel_dashboard_details[token]:
                                exchange_type_num = {'NSE': 1, 'BSE': 3, 'NFO': 2}.get(excel_dashboard_details[token][0].get('exchange', 'NSE').upper(), 1)
                            elif token in excel_setup_details and excel_setup_details[token]:
                                 exchange_type_num = excel_setup_details[token][0].get('exchange_type', 1)
                            elif token in excel_3pct_setup_details and excel_3pct_setup_details[token]:
                                 exchange_type_num = excel_3pct_setup_details[token][0].get('exchange_type', 1)
                            else:
                                exchange_type_num = 1 # Default to NSE
                        
                        subscribe_list_grouped[exchange_type_num].append(token)

                    for ex_type, tokens in subscribe_list_grouped.items():
                        formatted_tokens = [{"exchangeType": ex_type, "tokens": list(tokens)}]
                        smart_ws.subscribe(f"sub_{int(time.time())}", smart_ws.QUOTE, formatted_tokens)
                        subscribed_tokens.update(tokens)
                        logger.info(f"Subscribed to {len(tokens)} new tokens on exchange type {ex_type}.")

                last_scan_time = time.time()

            # --- Run Setup Checks on Schedule ---
            # FIX: Removed the is_market_hours() check to allow testing at any time.
            if current_minute % 3 == 0 and current_minute != last_checked_minute_orh:
                check_and_update_orh_setup()
                last_checked_minute_orh = current_minute

            with data_lock:
                has_3pct_symbols = bool(excel_3pct_setup_details)
            
            # MODIFIED: This block now calls the single new function to check both 3% Down and High Volume setups.
            if has_3pct_symbols:
                with data_lock:
                    unique_tokens_3pct = list(set([(token, details[0]['exchange_type']) for token, details in excel_3pct_setup_details.items() if details]))
                
                # Check 15-minute interval
                if current_minute % 15 == 1 and current_minute != last_checked_minute_15min:
                    fetch_historical_candles_for_3pct_down(smart_api_obj, unique_tokens_3pct, 'FIFTEEN_MINUTE')
                    check_and_update_price_volume_setups()
                    last_checked_minute_15min = current_minute
                
                # Check 30-minute interval
                if current_minute % 30 == 1 and current_minute != last_checked_minute_30min:
                    fetch_historical_candles_for_3pct_down(smart_api_obj, unique_tokens_3pct, 'THIRTY_MINUTE')
                    check_and_update_price_volume_setups()
                    last_checked_minute_30min = current_minute

                # Check 1-hour interval
                if current_minute == 16 and now.hour >= 10 and current_minute != last_checked_minute_1hr:
                    fetch_historical_candles_for_3pct_down(smart_api_obj, unique_tokens_3pct, 'ONE_HOUR')
                    check_and_update_price_volume_setups()
                    last_checked_minute_1hr = current_minute
            
            time.sleep(1)
        except Exception as e:
            logger.exception(f"Error in background scheduler thread: {e}")
            time.sleep(5)


def start_main_application():
    """
    The primary function that initializes connections and runs the main processing loop.
    """
    global smart_api_obj, smart_ws, gsheet, Dashboard, ATHCache, subscribed_tokens
    global excel_dashboard_details, excel_setup_details, excel_3pct_setup_details

    logger.info("🚀 Starting Combined Trading Dashboard and Signal Generator...")

    # --- Step 1: Connect to Google Sheets ---
    try:
        logger.info("Authenticating with Google Sheets...")
        creds = ServiceAccountCredentials.from_json_keyfile_name(JSON_KEY_FILE_PATH, SCOPE)
        client = gspread.authorize(creds)
        gsheet = client.open_by_key(GOOGLE_SHEET_ID)
        Dashboard = gsheet.worksheet(DASHBOARD_SHEET_NAME)
        ATHCache = gsheet.worksheet(ATH_CACHE_SHEET_NAME)
        logger.info("✅ Google Sheets connected successfully.")
    except Exception as e:
        logger.error(f"❌ Error connecting to Google Sheets: {e}. Please check credentials and sheet names. Exiting.")
        return

    # --- Step 2: Generate SmartAPI Session ---
    try:
        logger.info("Generating SmartAPI session...")
        # MODIFIED: Added timeout parameter to SmartConnect
        smart_api_obj = SmartConnect(api_key=API_KEY, timeout=15) # Increased timeout to 15 seconds
        totp = pyotp.TOTP(TOTP_SECRET).now()
        data = smart_api_obj.generateSession(CLIENT_CODE, MPIN, totp=totp)
        if data and data.get('data') and data['data'].get('jwtToken'):
            auth_token = data['data']['jwtToken']
            feed_token = data['data']['feedToken']
            logger.info("✅ SmartAPI session generated successfully!")
        else:
            logger.error(f"❌ Failed to generate SmartAPI session. Response: {data}. Exiting.")
            return
    except Exception as e:
        logger.error(f"❌ Error during SmartAPI session generation: {e}. Exiting.")
        return

    # --- Step 3: Initial Data Fetch and Subscriptions ---
    load_previous_day_high_cache()
    logger.info("Performing initial symbol scan...")
    new_dashboard, new_orh, new_3pct, all_tokens_for_subscription = scan_sheet_for_all_symbols(smart_api_obj, Dashboard, ATHCache)
    excel_dashboard_details = new_dashboard
    excel_setup_details = new_orh
    excel_3pct_setup_details = new_3pct
    
    # --- Step 4: Initialize and Connect WebSocket ---
    try:
        logger.info("Initializing SmartAPI WebSocket...")
        smart_ws = MyWebSocketClient(auth_token, API_KEY, CLIENT_CODE, feed_token)
        websocket_thread = threading.Thread(target=smart_ws.connect, daemon=True)
        websocket_thread.start()
        time.sleep(5) # Give the websocket time to connect
        if not smart_ws._is_connected_flag:
            logger.error("❌ WebSocket failed to connect. Exiting.")
            return
        logger.info("✅ SmartAPI WebSocket connected.")
    except Exception as e:
        logger.error(f"❌ Error initializing WebSocket: {e}. Exiting.")
        return

    # --- FIX: Step 4.5: Subscribe to initial tokens immediately for a faster dashboard start ---
    if all_tokens_for_subscription:
        logger.info("Subscribing to initial set of tokens for live data...")
        subscribe_list_grouped = collections.defaultdict(list)
        for token in all_tokens_for_subscription:
            # Determine exchange type from any of the detail dicts
            if token in excel_dashboard_details and excel_dashboard_details[token]:
                exchange_type_num = {'NSE': 1, 'BSE': 3, 'NFO': 2}.get(excel_dashboard_details[token][0].get('exchange', 'NSE').upper(), 1)
            elif token in excel_setup_details and excel_setup_details[token]:
                    exchange_type_num = excel_setup_details[token][0].get('exchange_type', 1)
            elif token in excel_3pct_setup_details and excel_3pct_setup_details[token]:
                    exchange_type_num = excel_3pct_setup_details[token][0].get('exchange_type', 1)
            else:
                exchange_type_num = 1 # Default to NSE
            subscribe_list_grouped[exchange_type_num].append(token)

        for ex_type, tokens in subscribe_list_grouped.items():
            formatted_tokens = [{"exchangeType": ex_type, "tokens": list(tokens)}]
            smart_ws.subscribe(f"sub_initial_{int(time.time())}", smart_ws.QUOTE, formatted_tokens)
            subscribed_tokens.update(tokens)
            logger.info(f"✅ Subscribed to {len(tokens)} initial tokens on exchange type {ex_type}.")

    # --- Step 5: Start Concurrent Threads ---
    logger.info("Starting concurrent application threads...")
    
    dashboard_updater_thread = threading.Thread(target=run_live_dashboard_updater, daemon=True)
    dashboard_updater_thread.start()

    quote_updater_thread = threading.Thread(target=run_quote_updater, daemon=True)
    quote_updater_thread.start()

    # FIX: Start the slow, initial data fetch in its own background thread
    initial_data_fetch_thread = threading.Thread(target=run_initial_setup_data_fetch, daemon=True)
    initial_data_fetch_thread.start()

    # This scheduler now only handles recurring tasks
    background_scheduler_thread = threading.Thread(target=run_background_task_scheduler, daemon=True)
    background_scheduler_thread.start()
    
    logger.info("🚀 All systems are go! The application is now running.")
    
    # --- Step 6: Keep the Main Thread Alive ---
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
