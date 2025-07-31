import logging
# import xlwings as xw # Removed xlwings
import pyotp
import time
import json
import threading
import struct
import ssl
import websocket
import os
import sys
import collections # Import collections for defaultdict
import datetime
from datetime import timedelta # For calculating previous day's date
import winsound  # 🟢 For playing sound on ORH detection (Windows only)

import logzero # Ensure logzero is imported at the top
from logzero import logger # Import logger from logzero

# --- Google Sheets Imports ---
from oauth2client.service_account import ServiceAccountCredentials
import gspread

# --- Configure Logging ---
# Configure logzero for daily log files globally
log_folder = time.strftime("%Y-%m-%d", time.localtime())
log_folder_path = os.path.join("logs", log_folder)
os.makedirs(log_folder_path, exist_ok=True)
log_path = os.path.join(log_folder_path, "app.log")

# Use logzero.setup_default_logger to configure both file and console logging
logzero.setup_default_logger(level=logging.INFO, logfile=log_path)

# Explicitly add a StreamHandler for console output to ensure messages are always visible.
if not any(isinstance(h, logging.StreamHandler) for h in logger.handlers):
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO)
    formatter = logging.Formatter('[%(levelname)s %(asctime)s %(filename)s:%(lineno)d] %(message)s')
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)


# Import specific exception from SmartApi
from SmartApi import SmartConnect
from SmartApi.smartExceptions import DataException

# --- Global Variables for WebSocket Data & Google Sheets Interaction ---
latest_tick_data = collections.defaultdict(dict)
interval_ohlc_data = collections.defaultdict(lambda: collections.defaultdict(dict))
completed_3min_candles = collections.defaultdict(list) # Stores recent 3-min candles for ORH logic
excel_symbol_details = collections.defaultdict(list) # Stores details like {'token': [{'symbol': '...', 'row': ...}]}
subscribed_tokens = set() # To keep track of what's currently subscribed
previous_day_high_cache = {} # Global dictionary to store previous day's high data

# --- New Global Variables for 3% Down Candle Setup ---
excel_3pct_symbol_details = collections.defaultdict(list) # Stores details for symbols relevant to 3% down setup
# Stores the latest completed 15/30/60 min candles for each token and interval
# Structure: latest_completed_candles_3pct[token][interval_name] = {'open': ..., 'high': ..., 'low': ..., 'close': ..., 'start_time': ...}
latest_completed_candles_3pct = collections.defaultdict(lambda: collections.defaultdict(dict))
volume_history_3pct = collections.defaultdict(lambda: collections.defaultdict(list))


# --- Configuration Constants ---
# EXCEL_FILE_PATH = 'MarketDashboard.xlsm' # Removed for Google Sheets
EXCEL_UPDATE_INTERVAL_SECONDS = 180
ORH_CHECK_INTERVAL_SECONDS = 180
START_ROW = 5 # Starting row for scanning symbols in Google Sheet
EXCEL_RETRY_ATTEMPTS = 3
EXCEL_RETRY_DELAY = 2 # Initial delay for Google Sheets operations, not API calls
PREV_DAY_HIGH_CACHE_FILE = 'previous_day_high_cache.json' # File to store cached previous day's high

# --- Google Sheets Specific Configuration ---
# IMPORTANT: Replace with your actual Google Sheet ID
GOOGLE_SHEET_ID = '1cYBpsVKCbrYCZzrj8NAMEgUG4cXy5Q5r9BtQE1Cjmz0' 
JSON_KEY_FILE_PATH = "" # Will be set dynamically
# Path to your service account JSON key file
# For deployment, check /etc/secrets/creds.json
if os.path.exists("/etc/secrets/creds.json"):
    JSON_KEY_FILE_PATH = "/etc/secrets/creds.json"
else:
    # Fallback for local development/testing
    JSON_KEY_FILE_PATH = os.path.join(os.path.dirname(__file__), "the-money-method-ad6d7-f95331cf5cbf.json")

SCOPE = [
    "https://spreadsheets.google.com/feeds",
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive"
]


# Sheet & Column Mappings
DASHBOARD_SHEET_NAME = 'Dashboard'
ATH_CACHE_SHEET_NAME = 'ATH Cache'

# ORH Setup Mappings
EXCHANGE_COL = 'B'      # In Dashboard sheet
SYMBOL_COL = 'C'        # In Dashboard sheet
TOKEN_COL = 'Y'         # ✅ CHANGED: In ATH Cache sheet
ORH_RESULT_COL = 'G'    # In Dashboard sheet
ORH_BUY_STOP_COL = 'H'  # In Dashboard sheet

# 3% Down Candle Setup Mappings
EXCHANGE_COL_3PCT = 'M' # In Dashboard sheet
SYMBOL_COL_3PCT = 'N'   # In Dashboard sheet
TOKEN_COL_3PCT = 'Z'    # In ATH Cache sheet
PCT_DOWN_RESULT_COL = 'AC' # In Dashboard sheet
PCT_DOWN_CONFIRM_COL = 'AD'# In Dashboard sheet

# Candle Intervals for 3% Down Setup
CANDLE_INTERVALS_3PCT_API = ['FIFTEEN_MINUTE', 'THIRTY_MINUTE', 'ONE_HOUR']
CANDLE_INTERVAL_MAP_DISPLAY = {
    'FIFTEEN_MINUTE': '15 min',
    'THIRTY_MINUTE': '30 min',
    'ONE_HOUR': 'Hourly'
}
# Define a flag for testing historical data for 3% down setup
TEST_3PCT_DOWN_HISTORICAL_FETCH = False # Set to False for live market operation after testing

# --- Google Sheets Client and Worksheet Objects (Initialized later) ---
client = None
gsheet = None
Dashboard = None
ATHCache = None # ✅ NEW: Worksheet object for ATH Cache
smart_api_obj = None # Global SmartConnect object

# --- Helper Functions ---
def update_connection_status(status_message):
    """Updates a file with the current connection status."""
    try:
        if getattr(sys, 'frozen', False):
            folder_path = os.path.dirname(sys.executable)
        else:
            folder_path = os.path.dirname(os.path.abspath(__file__))

        file_path = os.path.join(folder_path, "connection_status.txt")

        with open(file_path, "w") as f:
            f.write(status_message)
    except Exception as e:
        logger.warning(f"Failed to write connection status file: {e}")

def load_previous_day_high_cache():
    """Loads the previous day's high data from a JSON cache file."""
    global previous_day_high_cache
    if getattr(sys, 'frozen', False):
        cache_dir = os.path.dirname(sys.executable)
    else:
        cache_dir = os.path.dirname(os.path.abspath(__file__))
    cache_path = os.path.join(cache_dir, PREV_DAY_HIGH_CACHE_FILE)

    if os.path.exists(cache_path):
        try:
            with open(cache_path, 'r') as f:
                previous_day_high_cache = json.load(f)
            logger.info(f"Loaded previous day high cache from {cache_path}.")
        except json.JSONDecodeError as e:
            logger.error(f"Error decoding JSON from cache file {cache_path}: {e}. Starting with empty cache.")
            previous_day_high_cache = {}
        except Exception as e:
            logger.error(f"Error loading cache file {cache_path}: {e}. Starting with empty cache.")
            previous_day_high_cache = {}
    else:
        logger.info("Previous day high cache file not found. Starting with empty cache.")
        previous_day_high_cache = {}

def save_previous_day_high_cache():
    """Saves the previous day's high data to a JSON cache file."""
    if getattr(sys, 'frozen', False):
        cache_dir = os.path.dirname(sys.executable)
    else:
        cache_dir = os.path.dirname(os.path.abspath(__file__))
    cache_path = os.path.join(cache_dir, PREV_DAY_HIGH_CACHE_FILE)

    try:
        with open(cache_path, 'w') as f:
            json.dump(previous_day_high_cache, f, indent=4)
        logger.info(f"Saved previous day high cache to {cache_path}.")
    except Exception as e:
        logger.error(f"Error saving cache file {cache_path}: {e}")

def is_market_hours():
    """
    Checks if the current time is within Indian market hours (Monday-Friday, 9:15 AM to 3:30 PM IST).
    Assumes the system's local time is IST.
    """
    now = datetime.datetime.now()
    current_time = now.time()
    current_weekday = now.weekday() # Monday is 0, Sunday is 6

    # Check if it's a weekday (Monday to Friday)
    if 0 <= current_weekday <= 4: # Monday (0) to Friday (4)
        # Check if current time is within market hours (9:15 AM to 3:30 PM)
        market_open_time = datetime.time(9, 15)
        market_close_time = datetime.time(15, 30)
        if market_open_time <= current_time <= market_close_time:
            return True
    return False

def col_letter_to_index(letter):
    """Converts an Excel column letter (e.g., 'A', 'B', 'AA') to a 1-based index."""
    index = 0
    for i, char in enumerate(reversed(letter.upper())):
        index += (ord(char) - ord('A') + 1) * (26 ** i)
    return index

def get_last_row_in_column(sheet, column_letter):
    """Finds the last row with data in a given column for a gspread worksheet."""
    try:
        column_index = col_letter_to_index(column_letter)
        # Get all values in the column
        column_values = sheet.col_values(column_index)
        # Find the index of the last non-empty string (or non-whitespace string)
        last_non_empty_index = -1
        for i, val in enumerate(column_values):
            if val and str(val).strip() != '':
                last_non_empty_index = i
        
        # gspread returns 0-indexed list, so add 1 for row number
        # If no non-empty values, return START_ROW - 1 to indicate empty or only headers
        return last_non_empty_index + 1 if last_non_empty_index != -1 else START_ROW - 1
    except Exception as e:
        logger.error(f"Error in get_last_row_in_column for sheet '{sheet.title}', column '{column_letter}': {e}")
        return START_ROW - 1

# --- SmartWebSocketV2 Class (Copied from smartWebSocketV2.py) ---
# This class handles the low-level WebSocket communication.
class SmartWebSocketV2(object):
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
    SUBSCRIPTION_MODE_MAP = {
        1: "LTP", 2: "QUOTE", 3: "SNAP_QUOTE", 4: "DEPTH"
    }
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

        if not self._sanity_check():
            logger.error("Invalid initialization parameters. Provide valid values for all the tokens.")
            raise Exception("Provide valid value for all the tokens")

    def _sanity_check(self):
        return all([self.auth_token, self.api_key, self.client_code, self.feed_token])

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
        if self.RESUBSCRIBE_FLAG:
            self.resubscribe()
        self._is_connected_flag = True
        update_connection_status("connected")
        self.on_open(wsapp)

    def _on_pong(self, wsapp, data):
        timestamp = time.time()
        formatted_timestamp = time.strftime("%d-%m-%y %H:%M:%S", time.localtime(timestamp))
        logger.info(f"In on pong function ==> {data}, Timestamp: {formatted_timestamp}")
        print(f"[INFO] In on pong function ==> {data}, Timestamp: {formatted_timestamp}")
        self.last_pong_timestamp = timestamp

    def _on_ping(self, wsapp, data):
        timestamp = time.time()
        formatted_timestamp = time.strftime("%d-%m-%y %H:%M:%S", time.localtime(timestamp))
        logger.info(f"In on ping function ==> {data}, Timestamp: {formatted_timestamp}")
        self.last_ping_timestamp = timestamp

    def subscribe(self, correlation_id, mode, token_list):
        try:
            request_data = {
                "correlationID": correlation_id,
                "action": self.SUBSCRIBE_ACTION,
                "params": {"mode": mode, "tokenList": token_list}
            }
            if self.wsapp:
                self.wsapp.send(json.dumps(request_data))
                self.RESUBSCRIBE_FLAG = True
            else:
                logger.warning("WebSocket not initialized. Subscription request deferred.")
        except Exception as e:
            logger.error(f"Error occurred during subscribe: {e}")
            raise e

    def unsubscribe(self, correlation_id, mode, token_list):
        try:
            request_data = {
                "correlationID": correlation_id,
                "action": self.UNSUBSCRIBE_ACTION,
                "params": {"mode": mode, "tokenList": token_list}
            }
            if self.wsapp:
                self.wsapp.send(json.dumps(request_data))
            else:
                logger.warning("WebSocket not initialized. Unsubscribe request deferred.")
        except Exception as e:
            logger.error(f"Error occurred during unsubscribe: {e}")
            raise e

    def resubscribe(self):
        # Implementation for resubscribing if connection drops
        pass

    def connect(self):
        headers = {
            "Authorization": self.auth_token,
            "x-api-key": self.api_key,
            "x-client-code": self.client_code,
            "x-feed-token": self.feed_token
        }
        try:
            self._is_connected_flag = False
            update_connection_status("connecting")
            self.wsapp = websocket.WebSocketApp(self.ROOT_URI, header=headers, on_open=self._on_open,
                                                on_error=self._on_error, on_close=self._on_close,
                                                on_message=self._on_message,
                                                on_ping=self._on_ping, on_pong=self._on_pong)
            self.wsapp.run_forever(sslopt={"cert_reqs": ssl.CERT_NONE}, ping_interval=self.HEART_BEAT_INTERVAL)
        except Exception as e:
            logger.error(f"Error occurred during WebSocket connection: {e}")

    def close_connection(self):
        self.RESUBSCRIBE_FLAG = False
        self.DISCONNECT_FLAG = True
        if self.wsapp:
            self.wsapp.close()
            logger.info("WebSocket connection explicitly closed.")
        self._is_connected_flag = False
        update_connection_status("disconnected")

    def _on_error(self, wsapp, error):
        logger.error(f"Internal WebSocket error: {error}")
        self.on_error(wsapp, error)

    def _on_close(self, wsapp, close_status_code, close_msg):
        logger.warning(f"WebSocket closed. Code: {close_status_code}, Message: {close_msg})")
        self._is_connected_flag = False
        update_connection_status("disconnected")
        self.on_close(wsapp, close_status_code, close_msg)

    def _parse_binary_data(self, binary_data):
        # This function parses the raw binary data from the WebSocket.
        parsed_data = {
            "subscription_mode": self._unpack_data(binary_data, 0, 1, byte_format="B")[0],
            "exchange_type": self._unpack_data(binary_data, 1, 2, byte_format="B")[0],
            "token": SmartWebSocketV2._parse_token_value(binary_data[2:27]),
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
            if chr(binary_packet[i]) == '\x00':
                return token
            token += chr(binary_packet[i])
        return token

    def on_open(self, wsapp):
        logger.info("WebSocket connection opened.")
        print("[INFO] WebSocket connection opened.")

    def on_error(self, wsapp, error_message):
        logger.error(f"WebSocket error: {error_message}")
        print(f"[ERROR] WebSocket error: {error_message}")
        self._is_connected_flag = False
        update_connection_status("disconnecting")

    def on_close(self, wsapp, close_status_code, close_msg):
        logger.warning(f"WebSocket closed. Code: {close_status_code}, Message: {close_msg})")
        print(f"[WARNING] WebSocket closed. Code: {close_status_code}, Message: {close_msg})")
        self._is_connected_flag = False
        update_connection_status("disconnected")

    def on_data(self, wsapp, data):
        """Processes incoming ticks and builds 3-minute candles."""
        token = data.get('token')
        ltp_raw = data.get('last_traded_price')

        # Scale prices (API provides them as integers)
        ltp_scaled = ltp_raw / 100.0 if isinstance(ltp_raw, (int, float)) else None
        
        if token and ltp_scaled is not None:
            latest_tick_data[token] = {
                'ltp': ltp_scaled,
                'full_data': data
            }

            # Build 3-minute candles (ORH setup)
            current_time = datetime.datetime.now()
            interval = '3min'
            candle_info = interval_ohlc_data[token][interval]
            
            minute_floor = (current_time.minute // 3) * 3
            candle_start_dt = current_time.replace(minute=minute_floor, second=0, microsecond=0)
            
            if candle_info.get('start_time') is None or candle_start_dt > candle_info['start_time']:
                if candle_info.get('start_time') is not None:
                    completed_candle = {
                        'open': candle_info['open'],
                        'high': candle_info['high'],
                        'low': candle_info['low'],
                        'close': candle_info['last_ltp'],
                        'start_time': candle_info['start_time']
                    }
                    completed_3min_candles[token].append(completed_candle)
                    
                    if len(completed_3min_candles[token]) > 10:
                        completed_3min_candles[token].pop(0)

                    log_message = f"Completed 3min candle for {token}: O={completed_candle['open']:.2f}, H={completed_candle['high']:.2f}, L={completed_candle['low']:.2f}, C={completed_candle['close']:.2f}. History size: {len(completed_3min_candles[token])}"
                    logger.info(log_message)
                    print(f"[INFO] {log_message}")

                # Initialize the new candle
                candle_info['open'] = ltp_scaled
                candle_info['high'] = ltp_scaled
                candle_info['low'] = ltp_scaled
                candle_info['start_time'] = candle_start_dt
            
            # Update current candle
            candle_info['high'] = max(candle_info.get('high', ltp_scaled), ltp_scaled)
            candle_info['low'] = min(candle_info.get('low', ltp_scaled), ltp_scaled)
            candle_info['last_ltp'] = ltp_scaled

# --- Data Fetch and Google Sheets Functions ---
def fetch_initial_candle_data(smart_api_obj, symbols_to_fetch):
    """
    Fetches historical candle data for today to pre-populate candles for ORH.
    Includes retry logic for API errors and delays between requests.
    """
    logger.info("Fetching initial historical 3-min candle data for today (ORH setup)...")
    print("[INFO] Fetching initial historical 3-min candle data for today (ORH setup)...")

    now = datetime.datetime.now()

    # Guard clause: Prevent request before 09:16 AM or outside market hours
    if not is_market_hours() or now.time() < datetime.time(9, 16):
        logger.warning("Outside market hours or before 09:16 AM. Skipping initial ORH candle fetch.")
        return

    from_date = now.replace(hour=9, minute=15, second=0, microsecond=0).strftime("%Y-%m-%d %H:%M")
    to_date = now.strftime("%Y-%m-%d %H:%M")

    MAX_RETRIES = 5
    RETRY_DELAY_SECONDS = 20

    for token, entries in symbols_to_fetch.items():
        if not entries:
            continue
        symbol_name = entries[0]['symbol']
        exchange_type = entries[0]['exchange_type']

        exchange_map = {1: "NSE", 3: "BSE"}
        exchange_str = exchange_map.get(exchange_type)
        if not exchange_str:
            logger.warning(f"Cannot fetch history for token {token}, unknown exchange type {exchange_type}")
            time.sleep(1)
            continue

        for attempt in range(MAX_RETRIES):
            try:
                historic_param = {
                    "exchange": exchange_str,
                    "symboltoken": token,
                    "interval": "THREE_MINUTE",
                    "fromdate": from_date,
                    "todate": to_date
                }
                candle_data_response = smart_api_obj.getCandleData(historic_param)

                if candle_data_response and candle_data_response.get("status") and candle_data_response.get("data"):
                    candle_data = candle_data_response["data"]
                    completed_3min_candles[token].clear()

                    for c in candle_data:
                        candle_time = datetime.datetime.fromisoformat(c[0])
                        completed_candle = {
                            'start_time': candle_time,
                            'open': c[1],
                            'high': c[2],
                            'low': c[3],
                            'close': c[4]
                        }
                        completed_3min_candles[token].append(completed_candle)

                    if len(completed_3min_candles[token]) > 10:
                        completed_3min_candles[token] = completed_3min_candles[token][-10:]

                    logger.info(f"✅ Fetched {len(completed_3min_candles[token])} 3-min candles for {symbol_name} (Token: {token}).")
                    break
                else:
                    error_message = candle_data_response.get('message', 'Unknown error')
                    logger.warning(f"Attempt {attempt + 1}/{MAX_RETRIES}: Could not fetch 3-min data for {symbol_name}. Message: {error_message}")
                    if attempt < MAX_RETRIES - 1:
                        time.sleep(RETRY_DELAY_SECONDS)
                    else:
                        logger.error(f"❌ Failed after {MAX_RETRIES} attempts: {symbol_name}")

            except Exception as e:
                logger.warning(f"Attempt {attempt + 1}/{MAX_RETRIES}: Error fetching 3-min data for {symbol_name}: {e}")
                if attempt < MAX_RETRIES - 1:
                    time.sleep(RETRY_DELAY_SECONDS)
                else:
                    logger.error(f"❌ Exception after {MAX_RETRIES} attempts: {symbol_name}")
        time.sleep(1)


def fetch_previous_day_candle_data(smart_api_obj, symbols_to_fetch):
    """
    Fetches the previous day's ONE_DAY candle data for ORH setup.
    Includes retry logic for API errors and delays between requests.
    Prioritizes fetching from local cache.
    """
    logger.info("Fetching previous day's candle data (ORH setup)...")
    print("\n" + "="*80 + "\nPREVIOUS DAY'S HIGH PRICE (ORH setup)\n" + "="*80)

    # Guard clause: Prevent request outside market hours
    if not is_market_hours():
        logger.warning("Outside market hours. Skipping previous day ORH candle fetch.")
        return

    # 🗓️ Get last valid trading day (skip weekends)
    yesterday = datetime.date.today() - timedelta(days=1)
    while yesterday.weekday() >= 5:  # 5 = Saturday, 6 = Sunday
        yesterday -= timedelta(days=1)

    # 🕒 Use full day time range for ONE_DAY interval
    from_dt = datetime.datetime.combine(yesterday, datetime.time.min)   # 00:00
    to_dt   = datetime.datetime.combine(yesterday, datetime.time.max)   # 23:59

    if from_dt >= to_dt:
        logger.warning("Invalid date range: from_date >= to_date for previous day candle. Skipping fetch.")
        return

    from_date = from_dt.strftime("%Y-%m-%d %H:%M")
    to_date = to_dt.strftime("%Y-%m-%d %H:%M")
    yesterday_str = yesterday.strftime("%Y-%m-%d")

    MAX_RETRIES = 5
    RETRY_DELAY_SECONDS = 30

    for token, entries in symbols_to_fetch.items():
        if not entries:
            continue
        symbol_name = entries[0]['symbol']
        exchange_type = entries[0]['exchange_type']

        exchange_map = {1: "NSE", 3: "BSE"}
        exchange_str = exchange_map.get(exchange_type)
        if not exchange_str:
            logger.warning(f"Cannot fetch previous day history for token {token}, unknown exchange type {exchange_type}")
            time.sleep(1)
            continue

        # ✅ Check local cache
        if token in previous_day_high_cache and previous_day_high_cache[token].get('date') == yesterday_str:
            previous_day_high = previous_day_high_cache[token]['high']
            log_message = f"Previous Day's High for {symbol_name} (Token: {token}): {previous_day_high:.2f} (from cache)"
            logger.info(log_message)
            print(f"[INFO] {log_message}")
            time.sleep(0.1)
            continue

        # 🔁 Retry logic with API
        for attempt in range(MAX_RETRIES):
            try:
                historic_param = {
                    "exchange": exchange_str,
                    "symboltoken": token,
                    "interval": "ONE_DAY",
                    "fromdate": from_date,
                    "todate": to_date
                }
                candle_data_response = smart_api_obj.getCandleData(historic_param)

                if candle_data_response and candle_data_response.get("status") and candle_data_response.get("data"):
                    candle_data = candle_data_response["data"]
                    if candle_data:
                        previous_day_high = candle_data[0][2]  # High value
                        log_message = f"Previous Day's High for {symbol_name} (Token: {token}): {previous_day_high:.2f} (fetched from API)"
                        logger.info(log_message)
                        print(f"[INFO] {log_message}")

                        # Save to cache
                        previous_day_high_cache[token] = {
                            'date': yesterday_str,
                            'high': previous_day_high
                        }
                        save_previous_day_high_cache()
                        break  # Success
                    else:
                        logger.warning(f"No previous day's candle data found for {token} ({symbol_name}).")
                        break
                else:
                    error_message = candle_data_response.get('message', 'Unknown error')
                    logger.warning(f"Attempt {attempt + 1}/{MAX_RETRIES}: Could not fetch data for {symbol_name}. Message: {error_message}")
                    if attempt < MAX_RETRIES - 1:
                        time.sleep(RETRY_DELAY_SECONDS)
                    else:
                        logger.error(f"Failed to fetch previous day's candle data for {symbol_name} after {MAX_RETRIES} attempts.")
            except Exception as e:
                logger.warning(f"Attempt {attempt + 1}/{MAX_RETRIES}: Exception while fetching data for {symbol_name}: {e}")
                if attempt < MAX_RETRIES - 1:
                    time.sleep(RETRY_DELAY_SECONDS)
                else:
                    logger.error(f"Failed due to exception after {MAX_RETRIES} attempts for {symbol_name}.")

        time.sleep(3)  # ⏱️ Crucial to avoid API rate limits (AB1004)

    print("="*80 + "\n")


def scan_excel_for_symbols_and_tokens():
    """
    Scans Google Sheet for symbols and tokens for both ORH and 3% Down setups.
    Populates excel_symbol_details (ORH) and excel_3pct_symbol_details.
    Returns a combined list of unique tokens to subscribe to.
    """
    global excel_symbol_details, excel_3pct_symbol_details, Dashboard, ATHCache
    excel_symbol_details.clear()
    excel_3pct_symbol_details.clear()
    
    all_tokens_to_subscribe = set()
    tokens_to_subscribe_list = [] # List of dicts for SmartWebSocketV2.subscribe

    logger.info("Scanning Google Sheet for symbols and tokens...")

    try:
        last_row_dashboard_orh = get_last_row_in_column(Dashboard, SYMBOL_COL)
        last_row_dashboard_3pct = get_last_row_in_column(Dashboard, SYMBOL_COL_3PCT)
        last_row_ath_cache_orh = get_last_row_in_column(ATHCache, TOKEN_COL)
        last_row_ath_cache_3pct = get_last_row_in_column(ATHCache, TOKEN_COL_3PCT)
        
        max_row_to_scan = max(last_row_dashboard_orh, last_row_dashboard_3pct, last_row_ath_cache_orh, last_row_ath_cache_3pct)
    except Exception as e:
        logger.error(f"Error determining last row in Google Sheets: {e}")
        max_row_to_scan = START_ROW -1

    if max_row_to_scan < START_ROW:
        logger.warning(f"No symbols found in Google Sheet from row {START_ROW} onwards.")
        return []

    # Pre-fetch all values from relevant columns for efficiency
    dashboard_col_b_values = Dashboard.col_values(col_letter_to_index(EXCHANGE_COL))
    dashboard_col_c_values = Dashboard.col_values(col_letter_to_index(SYMBOL_COL))
    dashboard_col_m_values = Dashboard.col_values(col_letter_to_index(EXCHANGE_COL_3PCT))
    dashboard_col_n_values = Dashboard.col_values(col_letter_to_index(SYMBOL_COL_3PCT))
    
    ath_cache_col_y_values = ATHCache.col_values(col_letter_to_index(TOKEN_COL)) # ✅ For ORH
    ath_cache_col_z_values = ATHCache.col_values(col_letter_to_index(TOKEN_COL_3PCT)) # ✅ For 3% Down

    for row_idx in range(START_ROW - 1, max_row_to_scan): # 0-indexed for lists
        row = row_idx + 1 # 1-indexed for Excel/Sheets row number

        # --- Scan for ORH symbols (Dashboard: B, C; ATH Cache: Y) ---
        try:
            orh_exchange_type_raw = dashboard_col_b_values[row_idx] if row_idx < len(dashboard_col_b_values) else None
            orh_symbol_raw = dashboard_col_c_values[row_idx] if row_idx < len(dashboard_col_c_values) else None
            orh_token_raw = ath_cache_col_y_values[row_idx] if row_idx < len(ath_cache_col_y_values) else None # ✅ CHANGED

            orh_exchange_type_str = str(orh_exchange_type_raw).strip() if orh_exchange_type_raw is not None else None
            orh_symbol = str(orh_symbol_raw).strip() if orh_symbol_raw is not None else None
            orh_token = str(int(float(orh_token_raw))).strip() if orh_token_raw is not None else None

            if orh_exchange_type_str and orh_symbol and orh_token:
                exchange_type_map = {'NSE': 1, 'BSE': 3, 'NSE_CM': 1, 'NSE_FO': 2, 'BSE_CM': 3, 'BSE_FO': 4}
                orh_exchange_type_int = exchange_type_map.get(orh_exchange_type_str.upper())

                if orh_exchange_type_int is None:
                    logger.warning(f"Skipping ORH row {row}: Unknown Exchange Type '{orh_exchange_type_str}' for Symbol '{orh_symbol}'.")
                else:
                    excel_symbol_details[orh_token].append({'symbol': orh_symbol, 'row': row, 'orh_col': ORH_RESULT_COL, 'exchange_type': orh_exchange_type_int})
                    all_tokens_to_subscribe.add((orh_token, orh_exchange_type_int))

        except (ValueError, TypeError, IndexError) as e:
            logger.debug(f"Error reading ORH Google Sheet row {row}: {e}") # Use debug as many rows might be empty

        # --- Scan for 3% Down symbols (Dashboard: M, N; ATH Cache: Z) ---
        try:
            pct_exchange_type_raw = dashboard_col_m_values[row_idx] if row_idx < len(dashboard_col_m_values) else None
            pct_symbol_raw = dashboard_col_n_values[row_idx] if row_idx < len(dashboard_col_n_values) else None
            pct_token_raw = ath_cache_col_z_values[row_idx] if row_idx < len(ath_cache_col_z_values) else None

            pct_exchange_type_str = str(pct_exchange_type_raw).strip() if pct_exchange_type_raw is not None else None
            pct_symbol = str(pct_symbol_raw).strip() if pct_symbol_raw is not None else None
            pct_token = str(int(float(pct_token_raw))).strip() if pct_token_raw is not None else None

            if pct_exchange_type_str and pct_symbol and pct_token:
                exchange_type_map = {'NSE': 1, 'BSE': 3, 'NSE_CM': 1, 'NSE_FO': 2, 'BSE_CM': 3, 'BSE_FO': 4}
                pct_exchange_type_int = exchange_type_map.get(pct_exchange_type_str.upper())

                if pct_exchange_type_int is None:
                    logger.warning(f"Skipping 3% down row {row}: Unknown Exchange Type '{pct_exchange_type_str}' for Symbol '{pct_symbol}'.")
                else:
                    excel_3pct_symbol_details[pct_token].append({'symbol': pct_symbol, 'row': row, 'pct_down_col': PCT_DOWN_RESULT_COL, 'exchange_type': pct_exchange_type_int})
                    all_tokens_to_subscribe.add((pct_token, pct_exchange_type_int))

        except (ValueError, TypeError, IndexError) as e:
            logger.debug(f"Error reading 3% down Google Sheet row {row}: {e}") # Use debug as many rows might be empty

    # Convert set of (token, exchange_type) to list of dicts for subscription
    for token, exchange_type in all_tokens_to_subscribe:
        tokens_to_subscribe_list.append({'exchangeType': exchange_type, 'tokens': [token]})
            
    logger.info(f"Scanned {len(excel_symbol_details)} ORH symbols and {len(excel_3pct_symbol_details)} 3% Down symbols.")
    return tokens_to_subscribe_list


def fetch_historical_candles_for_3pct_down(smart_api_obj, tokens_to_fetch, interval_api, from_dt, to_dt):
    """
    Fetches at least 10 historical candles for 3% down setup.
    Updates global latest_completed_candles_3pct dictionary.
    Logs last 10 candle volumes.
    """
    logger.info(f"Fetching historical {interval_api} candles for 3% down setup from {from_dt.strftime('%Y-%m-%d %H:%M')} to {to_dt.strftime('%Y-%m-%d %H:%M')}...")

    MAX_RETRIES = 3
    RETRY_DELAY_SECONDS = 15
    TARGET_CANDLES = 10

    interval_minutes_map = {"FIFTEEN_MINUTE": 15, "THIRTY_MINUTE": 30, "ONE_HOUR": 60}
    minutes_back = interval_minutes_map.get(interval_api, 15) * TARGET_CANDLES
    adjusted_from_dt = to_dt - datetime.timedelta(minutes=minutes_back)
    
    from_date_str = adjusted_from_dt.strftime("%Y-%m-%d %H:%M")
    to_date_str = to_dt.strftime("%Y-%m-%d %H:%M")

    for token_info in tokens_to_fetch:
        token = token_info[0]
        exchange_type = token_info[1]

        symbol_name = "Unknown"
        if token in excel_3pct_symbol_details:
            symbol_name = excel_3pct_symbol_details[token][0]['symbol']
        elif token in excel_symbol_details:
            symbol_name = excel_symbol_details[token][0]['symbol']

        exchange_map = {1: "NSE", 3: "BSE"}
        exchange_str = exchange_map.get(exchange_type)
        if not exchange_str:
            logger.warning(f"Cannot fetch history for token {token}, unknown exchange type {exchange_type}")
            time.sleep(1)
            continue

        candle_data = []
        fetch_attempts = 0

        while len(candle_data) < TARGET_CANDLES and fetch_attempts < 5:
            try:
                historic_param = {
                    "exchange": exchange_str,
                    "symboltoken": token,
                    "interval": interval_api,
                    "fromdate": from_date_str,
                    "todate": to_date_str
                }
                candle_data_response = smart_api_obj.getCandleData(historic_param)

                if candle_data_response and candle_data_response.get("status") and candle_data_response.get("data"):
                    candle_data = candle_data_response["data"]
                    if len(candle_data) < TARGET_CANDLES:
                        # Go further back in time
                        adjusted_from_dt -= datetime.timedelta(minutes=minutes_back)
                        from_date_str = adjusted_from_dt.strftime("%Y-%m-%d %H:%M")
                        fetch_attempts += 1
                        continue
                else:
                    error_message = candle_data_response.get('message', 'Unknown error')
                    logger.warning(f"Fetch error for {symbol_name}. Message: {error_message}")
                    break
            except Exception as e:
                logger.error(f"Exception fetching data for {symbol_name}: {e}")
                break

        if len(candle_data) >= 1:
            last_10_candles = candle_data[-TARGET_CANDLES:]
            volumes = [row[5] if len(row) > 5 else 0 for row in last_10_candles]
            logger.info(f"📈 Last 10 {CANDLE_INTERVAL_MAP_DISPLAY.get(interval_api, interval_api)} volumes for {symbol_name} ➤ {volumes}")
            print(f"[INFO] Last 10 {CANDLE_INTERVAL_MAP_DISPLAY.get(interval_api, interval_api)} volumes for {symbol_name} ➤ {volumes}")

            latest_candle_raw = candle_data[-1]
            completed_candle = {
                'start_time': datetime.datetime.fromisoformat(latest_candle_raw[0]),
                'open': latest_candle_raw[1],
                'high': latest_candle_raw[2],
                'low': latest_candle_raw[3],
                'close': latest_candle_raw[4],
                'volume': latest_candle_raw[5] if len(latest_candle_raw) > 5 else None
            }
            latest_completed_candles_3pct[token][interval_api] = completed_candle

            log_msg = (
                f"✅ Fetched latest {CANDLE_INTERVAL_MAP_DISPLAY.get(interval_api, interval_api)} candle for {symbol_name} "
                f"(Token: {token}): Open={completed_candle['open']:.2f}, High={completed_candle['high']:.2f}, "
                f"Low={completed_candle['low']:.2f}, Close={completed_candle['close']:.2f}, "
                f"Volume={completed_candle['volume'] if completed_candle['volume'] is not None else 'N/A'}"
            )
            logger.info(log_msg)
            print(log_msg)
        else:
            logger.warning(f"❌ Not enough candle data for {symbol_name} (Token: {token}) even after multiple attempts.")

        time.sleep(1)



def check_and_update_orh_setup():
    """Checks the latest completed 3-min candle for ORH setup and updates Google Sheet accordingly."""
    import winsound
    logger.info("🔍 Checking latest 3-min candle for ORH setup...")

    google_sheet_updates_queued = []

    # Pre-fetch current values for ORH_RESULT_COL and ORH_BUY_STOP_COL
    orh_result_col_values = Dashboard.col_values(col_letter_to_index(ORH_RESULT_COL))
    orh_buy_stop_col_values = Dashboard.col_values(col_letter_to_index(ORH_BUY_STOP_COL))

    for token, symbol_entries in excel_symbol_details.items():
        candles = completed_3min_candles.get(token, [])
        prev_high_entry = previous_day_high_cache.get(token)

        if not candles or not prev_high_entry:
            logger.info(f"⏩ Skipping ORH check for {token} — Missing 3-min candles or previous day high.")
            continue

        prev_high = prev_high_entry.get("high")
        if not prev_high:
            logger.info(f"⏩ Skipping ORH check for {token} — Previous day high not available.")
            continue

        latest_candle = candles[-1] if candles else None
        orh_result = "No"
        trigger_time_str = ""
        buy_stop_value = None

        if latest_candle:
            high = latest_candle['high']
            low = latest_candle['low']
            close = latest_candle['close']

            if high != low:
                bullish_threshold = low + 0.7 * (high - low)
                if close >= bullish_threshold and close > prev_high:
                    orh_result = "Yes"
                    trigger_time_str = latest_candle['start_time'].strftime('%H:%M')
                    buy_stop_value = round(low * 0.995, 2)

                    candle_time_full = latest_candle['start_time'].strftime('%Y-%m-%d %H:%M:%S')
                    log_msg = (
                        f"🚨 ORH Triggered for {symbol_entries[0]['symbol']} at {candle_time_full} ➤ "
                        f"Open: {latest_candle['open']}, High: {high}, Low: {low}, "
                        f"Close: {close}, Buy Stop: {buy_stop_value}"
                    )
                    print(log_msg)
                    logger.info(log_msg)

                    try:
                        winsound.Beep(1000, 400)
                    except Exception as e:
                        logger.warning(f"🔇 Sound alert failed: {e}")

        new_value_orh_col = f"Yes({trigger_time_str})" if orh_result == "Yes" else "No"
        new_value_buy_stop_col = buy_stop_value if buy_stop_value else ""

        for entry in symbol_entries:
            row = entry["row"]
            row_idx = row - 1 # 0-indexed for list access
            
            # ORH Result Column (G)
            current_value_orh = orh_result_col_values[row_idx] if row_idx < len(orh_result_col_values) else ""
            if str(current_value_orh).strip() != new_value_orh_col.strip():
                google_sheet_updates_queued.append({
                    "row": row,
                    "column": ORH_RESULT_COL,
                    "value": new_value_orh_col
                })
            
            # Buy Stop Column (H)
            current_value_buy_stop = orh_buy_stop_col_values[row_idx] if row_idx < len(orh_buy_stop_col_values) else ""
            if str(current_value_buy_stop).strip() != str(new_value_buy_stop_col).strip():
                google_sheet_updates_queued.append({
                    "row": row,
                    "column": ORH_BUY_STOP_COL,
                    "value": new_value_buy_stop_col
                })

    if google_sheet_updates_queued:
        update_google_sheet_cells_batch(Dashboard, google_sheet_updates_queued)
        logger.info(f"✅ Applied {len(google_sheet_updates_queued)} ORH updates to Dashboard.")
    else:
        logger.info("ℹ️ No ORH setup updates needed.")


def check_and_update_3pct_down_setup():
    """
    Checks the latest completed 15-min, 30-min, and 1-hour candles for a 3% down condition
    and updates Google Sheet accordingly.
    - Column AC: Lists triggered intervals for 3% Down.
    - Column AD: Lists 'high volume (...) for Xmin' for candles with highest volume.
    """
    logger.info("🔍 Checking for 3% Down Candle setup...")
    google_sheet_updates_queued_pct = []
    google_sheet_updates_queued_volume = []

    # Pre-fetch current values for PCT_DOWN_RESULT_COL and PCT_DOWN_CONFIRM_COL
    pct_down_result_col_values = Dashboard.col_values(col_letter_to_index(PCT_DOWN_RESULT_COL))
    pct_down_confirm_col_values = Dashboard.col_values(col_letter_to_index(PCT_DOWN_CONFIRM_COL))

    for token, symbol_entries in excel_3pct_symbol_details.items():
        symbol_name = symbol_entries[0]['symbol']
        triggered_intervals_pct = []
        triggered_intervals_volume = []

        for interval_api in CANDLE_INTERVALS_3PCT_API:
            latest_candle = latest_completed_candles_3pct.get(token, {}).get(interval_api)

            if latest_candle:
                open_price = latest_candle['open']
                close_price = latest_candle['close']
                high_price = latest_candle.get('high', 0)
                low_price = latest_candle.get('low', 0)
                volume = latest_candle.get('volume', 0)
                start_time = latest_candle.get('start_time')

                log_msg_candle_data = (
                    f"  Candle Data for {symbol_name} ({CANDLE_INTERVAL_MAP_DISPLAY.get(interval_api, interval_api)}) "
                    f"at {start_time.strftime('%H:%M:%S')} ➤ "
                    f"Open={open_price:.2f}, High={high_price:.2f}, Low={low_price:.2f}, "
                    f"Close={close_price:.2f}, Volume={volume}"
                )
                logger.info(log_msg_candle_data)
                print(f"[INFO] {log_msg_candle_data}")

                if open_price > 0:
                    percentage_drop = (open_price - close_price) / open_price
                    if percentage_drop >= 0.03:
                        triggered_intervals_pct.append(CANDLE_INTERVAL_MAP_DISPLAY.get(interval_api, interval_api))

                        log_msg = (
                            f"📉 3% Down Triggered for {symbol_name} ({CANDLE_INTERVAL_MAP_DISPLAY.get(interval_api)}) "
                            f"➤ Drop={percentage_drop:.2%}"
                        )
                        logger.info(log_msg)
                        print(f"[INFO] {log_msg}")

                        # ✅ Phase 1: Check High Volume vs. last 10
                        if isinstance(volume, (int, float)):
                            vol_list = volume_history_3pct[token][interval_api]
                            vol_list.append(volume)
                            if len(vol_list) > 10:
                                vol_list.pop(0)

                            if all(volume > v for v in vol_list[:-1]):
                                volume_string = f"high volume ({int(volume)}) for {CANDLE_INTERVAL_MAP_DISPLAY.get(interval_api)}"
                                triggered_intervals_volume.append(volume_string)

                                # ✅ Save for future confirmation (Phase 2)
                                if 'confirmed_intervals' not in latest_completed_candles_3pct[token]:
                                    latest_completed_candles_3pct[token]['confirmed_intervals'] = {}
                                latest_completed_candles_3pct[token]['confirmed_intervals'][interval_api] = {
                                    'low': low_price,
                                    'time': start_time
                                }

            else:
                logger.debug(f"No latest {CANDLE_INTERVAL_MAP_DISPLAY.get(interval_api)} candle for {symbol_name}.")

        # ✅ Update Column AC (3% Down Trigger)
        for entry in symbol_entries:
            row = entry["row"]
            row_idx = row - 1
            pct_down_col = entry["pct_down_col"]

            existing_value = pct_down_result_col_values[row_idx] if row_idx < len(pct_down_result_col_values) else ""
            new_value = ", ".join(triggered_intervals_pct) if triggered_intervals_pct else existing_value

            if str(existing_value).strip() != new_value.strip():
                google_sheet_updates_queued_pct.append({
                    "row": row,
                    "column": pct_down_col,
                    "value": new_value,
                })

        # ✅ Update Column AD (High Volume Phase 1)
        for entry in symbol_entries:
            row = entry["row"]
            row_idx = row - 1
            col_ad = PCT_DOWN_CONFIRM_COL # 'AD'
            
            existing_ad = pct_down_confirm_col_values[row_idx] if row_idx < len(pct_down_confirm_col_values) else ""

            if triggered_intervals_volume:
                combined_new = ", ".join(triggered_intervals_volume)
                # If already confirmed with "Yes for ...", preserve those parts
                parts_to_keep = [part for part in existing_ad.split(", ") if part.startswith("Yes for")]
                full_updated_value = ", ".join(parts_to_keep + triggered_intervals_volume)
                
                # Add to batch update queue
                google_sheet_updates_queued_volume.append({
                    "row": row,
                    "column": col_ad,
                    "value": full_updated_value
                })
            else:
                # If no new high volume triggers, but there are existing "Yes for" confirmations, preserve them.
                parts_to_keep = [part for part in existing_ad.split(", ") if part.startswith("Yes for")]
                if parts_to_keep and str(existing_ad).strip() != ", ".join(parts_to_keep).strip():
                     google_sheet_updates_queued_volume.append({
                        "row": row,
                        "column": col_ad,
                        "value": ", ".join(parts_to_keep)
                    })


    # ✅ Batch apply updates to Column AC
    if google_sheet_updates_queued_pct:
        update_google_sheet_cells_batch(Dashboard, google_sheet_updates_queued_pct)
        logger.info(f"✅ Applied {len(google_sheet_updates_queued_pct)} 3% Down updates to Column AC.")
    else:
        logger.info("ℹ️ No 3% Down % updates needed for Column AC.")

    # ✅ Batch apply updates to Column AD
    if google_sheet_updates_queued_volume:
        update_google_sheet_cells_batch(Dashboard, google_sheet_updates_queued_volume)
        logger.info(f"✅ Applied {len(google_sheet_updates_queued_volume)} 3% Down volume updates to Column AD.")
    else:
        logger.info("ℹ️ No 3% Down volume updates needed for Column AD.")


def confirm_high_volume_3pct_down(smart_api_obj):
    logger.info("🔄 Checking 15-min confirmation for high volume 3% down setups...")
    google_sheet_updates_queued_confirm = []

    # Pre-fetch current values for PCT_DOWN_CONFIRM_COL
    pct_down_confirm_col_values = Dashboard.col_values(col_letter_to_index(PCT_DOWN_CONFIRM_COL))

    for token, interval_dict in latest_completed_candles_3pct.items():
        if 'confirmed_intervals' not in interval_dict:
            continue

        confirmed_intervals = interval_dict['confirmed_intervals']
        confirmed_texts_for_this_token = [] # To accumulate all "Yes for Xmin" for this token

        # Get existing AD column value to merge with new confirmations
        current_ad_value = ""
        if token in excel_3pct_symbol_details:
            row = excel_3pct_symbol_details[token][0]['row']
            row_idx = row - 1
            current_ad_value = pct_down_confirm_col_values[row_idx] if row_idx < len(pct_down_confirm_col_values) else ""
        
        # Preserve existing "high volume" parts if they exist
        existing_high_volume_parts = [part for part in current_ad_value.split(", ") if part.startswith("high volume")]
        
        for interval_api, details in confirmed_intervals.items():
            candle_low = details['low']
            candle_time = details['time']
            symbol = excel_3pct_symbol_details.get(token, [{}])[0].get('symbol', 'Unknown')

            # Wait for next 15-min candle
            from_dt = candle_time + timedelta(minutes=15)
            to_dt = from_dt + timedelta(minutes=15)

            try:
                exchange_type = excel_3pct_symbol_details[token][0]['exchange_type']
                exchange_map = {1: "NSE", 3: "BSE"}
                exchange_str = exchange_map.get(exchange_type)

                historic_param = {
                    "exchange": exchange_str,
                    "symboltoken": token,
                    "interval": "FIFTEEN_MINUTE",
                    "fromdate": from_dt.strftime("%Y-%m-%d %H:%M"),
                    "todate": to_dt.strftime("%Y-%m-%d %H:%M")
                }

                response = smart_api_obj.getCandleData(historic_param)
                if response.get("status") and response.get("data"):
                    if response["data"]: # Ensure data is not empty
                        close_price = response["data"][-1][4] # Close of next 15m candle
                        if close_price < candle_low:
                            confirmed_texts_for_this_token.append(f"Yes for {CANDLE_INTERVAL_MAP_DISPLAY.get(interval_api)}")
                            logger.info(f"✅ Confirmation: {symbol} → Yes for {CANDLE_INTERVAL_MAP_DISPLAY.get(interval_api)}")
                        else:
                             logger.info(f"ℹ️ No confirmation for {symbol} ({CANDLE_INTERVAL_MAP_DISPLAY.get(interval_api)}): Close ({close_price}) not below low ({candle_low}).")
                    else:
                        logger.warning(f"No next 15-min candle data for confirmation for {symbol}.")

            except Exception as e:
                logger.warning(f"Confirmation check failed for {token} ({symbol}): {e}")
                continue
        
        # Combine existing high volume parts with new confirmations
        final_ad_value_parts = existing_high_volume_parts + confirmed_texts_for_this_token
        final_ad_value = ", ".join(sorted(list(set(final_ad_value_parts)))) # Use set to deduplicate, then sort for consistency

        # Write confirmed result to Column AD
        if token in excel_3pct_symbol_details:
            for entry in excel_3pct_symbol_details[token]:
                row = entry["row"]
                row_idx = row - 1
                current_value_ad = pct_down_confirm_col_values[row_idx] if row_idx < len(pct_down_confirm_col_values) else ""
                if str(current_value_ad).strip() != final_ad_value.strip():
                    google_sheet_updates_queued_confirm.append({
                        "row": row,
                        "column": PCT_DOWN_CONFIRM_COL,
                        "value": final_ad_value
                    })
    
    if google_sheet_updates_queued_confirm:
        update_google_sheet_cells_batch(Dashboard, google_sheet_updates_queued_confirm)
        logger.info(f"✅ Applied {len(google_sheet_updates_queued_confirm)} 3% Down confirmation updates to Column AD.")
    else:
        logger.info("ℹ️ No 3% Down confirmation updates needed.")


def update_google_sheet_cells_batch(sheet, updates):
    """Helper function to batch update multiple Google Sheet cells."""
    if not updates:
        return
    try:
        cell_list = []
        for update in updates:
            row = update["row"]
            col_letter = update["column"]
            col_index = col_letter_to_index(col_letter)
            value = update["value"]
            cell_list.append(gspread.Cell(row, col_index, value))
        
        if cell_list:
            sheet.update_cells(cell_list)
            logger.info(f"Successfully applied {len(cell_list)} batch updates to Google Sheet '{sheet.title}'.")
    except Exception as e:
        logger.exception(f"An unexpected error during batch Google Sheet update for sheet '{sheet.title}': {e}")


# --- Main Script Execution ---
def main():
    global client, gsheet, Dashboard, ATHCache, subscribed_tokens, smart_ws, smart_api_obj

    logger.info("Starting ORH and 3% Down Standalone Script...")
    print("[INFO] Starting ORH and 3% Down Standalone Script...")

    # --- CREDENTIALS ---
    API_KEY = "oNNHQHKU"
    CLIENT_CODE = "D355432"
    MPIN = "1234"
    TOTP_SECRET = "QHO5IWOISV56Z2BFTPFSRSQVRQ"
    # --- END OF CREDENTIALS ---

    if not all([API_KEY, CLIENT_CODE, MPIN, TOTP_SECRET]):
        logger.error("SmartAPI credentials not provided. Please update the script.")
        return
    
    # --- Google Sheets Authentication ---
    try:
        logger.info("Authenticating with Google Sheets...")
        creds = ServiceAccountCredentials.from_json_keyfile_name(JSON_KEY_FILE_PATH, SCOPE)
        client = gspread.authorize(creds)
        gsheet = client.open_by_key(GOOGLE_SHEET_ID)
        Dashboard = gsheet.worksheet(DASHBOARD_SHEET_NAME)
        ATHCache = gsheet.worksheet(ATH_CACHE_SHEET_NAME)
        logger.info("Google Sheets connected successfully.")
    except Exception as e:
        logger.error(f"Error connecting to Google Sheets: {e}. Ensure the JSON key file is correct, and sheet names ('{DASHBOARD_SHEET_NAME}', '{ATH_CACHE_SHEET_NAME}') are valid.")
        return

    load_previous_day_high_cache()

    try:
        logger.info("Attempting to generate SmartAPI session...")
        smart_api_obj = SmartConnect(api_key=API_KEY)
        totp = pyotp.TOTP(TOTP_SECRET).now()
        data = smart_api_obj.generateSession(CLIENT_CODE, MPIN, totp=totp)

        if data and data.get('data') and data['data'].get('jwtToken'):
            auth_token = data['data']['jwtToken']
            feed_token = data['data']['feedToken']
            logger.info("SmartAPI session generated successfully!")
        else:
            logger.error(f"Failed to generate SmartAPI session. Response: {data}")
            return

    except Exception as e:
        logger.error(f"Error during SmartAPI session generation: {e}")
        return

    try:
        # Scan Google Sheet for all symbols (ORH and 3% Down)
        all_tokens_for_subscription = scan_excel_for_symbols_and_tokens()
        
        # Initial data fetches are now conditional on market hours
        if is_market_hours():
            logger.info("Within market hours. Fetching current day's historical data for initial setup.")
            fetch_initial_candle_data(smart_api_obj, excel_symbol_details) # For ORH
            fetch_previous_day_candle_data(smart_api_obj, excel_symbol_details) # For ORH

            # Fetch latest completed candles for 3% Down setup (initial fetch)
            unique_tokens_3pct = list(set([(t['tokens'][0], t['exchangeType']) for t in all_tokens_for_subscription if t['tokens'][0] in excel_3pct_symbol_details]))
            for interval_api in CANDLE_INTERVALS_3PCT_API:
                # Fetch last few hours to ensure we get any recently completed candles at startup
                fetch_historical_candles_for_3pct_down(smart_api_obj, unique_tokens_3pct, interval_api, datetime.datetime.now() - timedelta(hours=2), datetime.datetime.now())
                time.sleep(1) # Small delay to avoid hammering API
            
        else:
            logger.info("Outside market hours. Skipping initial live data fetches.")
            # --- TESTING BLOCK FOR 3% DOWN CANDLE SETUP (DISABLED) ---
            if TEST_3PCT_DOWN_HISTORICAL_FETCH: # This will now be False
                logger.info("TESTING MODE: Fetching historical data for 3% Down setup (Yesterday 2:15 PM - 3:15 PM).")
                # Define yesterday (Friday)
                yesterday = datetime.date.today() - timedelta(days=1)
                # Adjust to find last Friday if today is Sat/Sun
                while yesterday.weekday() >= 5: # 5=Sat, 6=Sun
                    yesterday -= timedelta(days=1)
                
                from_dt_test = datetime.datetime.combine(yesterday, datetime.time(14, 15)) # 2:15 PM
                to_dt_test = datetime.datetime.combine(yesterday, datetime.time(15, 15))   # 3:15 PM

                unique_tokens_3pct = list(set([(t['tokens'][0], t['exchangeType']) for t in all_tokens_for_subscription if t['tokens'][0] in excel_3pct_symbol_details]))
                
                for interval_api in CANDLE_INTERVALS_3PCT_API:
                    fetch_historical_candles_for_3pct_down(smart_api_obj, unique_tokens_3pct, interval_api, from_dt_test, to_dt_test)
                    time.sleep(2) # Small delay between interval fetches
            # --- END TESTING BLOCK ---


    except Exception as e:
        logger.error(f"Error during initial setup: {e}. Please ensure Google Sheet is accessible.")
        return

    try:
        logger.info("Attempting to initialize SmartAPI WebSocket...")
        smart_ws = SmartWebSocketV2(auth_token, API_KEY, CLIENT_CODE, feed_token)
        websocket_thread = threading.Thread(target=smart_ws.connect, daemon=True)
        websocket_thread.start()
        time.sleep(5)
        if not smart_ws._is_connected_flag:
            logger.error("WebSocket failed to connect. Exiting.")
            return
        logger.info("SmartAPI WebSocket initialized and connection started.")
    except Exception as e:
        logger.error(f"Error initializing WebSocket: {e}")
        return

    logger.info("Entering main processing loop...")

    # Initialize last checked minutes for precise interval checks
    last_checked_minute_orh = None
    last_checked_minute_15min = None
    last_checked_minute_30min = None
    last_checked_minute_1hr = None
    last_checked_minute_confirmation = None

    try:
        while True:
            now = datetime.datetime.now()
            current_minute = now.minute
            current_time = now.time()

            # Re-scan Google Sheet periodically to pick up new symbols/tokens
            tokens_to_subscribe_list = scan_excel_for_symbols_and_tokens()
            
            # Filter for new tokens to subscribe
            new_tokens_to_subscribe = []
            for group in tokens_to_subscribe_list:
                for token in group['tokens']:
                    # Check if token is not already in the subscribed_tokens set
                    if token not in subscribed_tokens:
                        new_tokens_to_subscribe.append({'exchangeType': group['exchangeType'], 'tokens': [token]})
            
            if smart_ws._is_connected_flag and new_tokens_to_subscribe:
                smart_ws.subscribe("sub", SmartWebSocketV2.QUOTE, new_tokens_to_subscribe)
                for group in new_tokens_to_subscribe:
                    subscribed_tokens.update(group['tokens'])
                logger.info(f"Subscribed to {len(new_tokens_to_subscribe)} new token groups.")

            if is_market_hours():
                # --- ORH Setup Checks (every 3 minutes) ---
                if current_minute % 3 == 0 and current_minute != last_checked_minute_orh:
                    logger.info(f"🕒 New 3-min candle detected (ORH) at {now.strftime('%H:%M:%S')}")
                    check_and_update_orh_setup()
                    last_checked_minute_orh = current_minute

                # --- 3% Down Candle Setup Checks (Live Market) ---
                unique_tokens_3pct = list(set([(t['tokens'][0], t['exchangeType']) for t in tokens_to_subscribe_list if t['tokens'][0] in excel_3pct_symbol_details]))

                # 15-minute check: Trigger at :01, :16, :31, :46 (1 minute after candle closes at :00, :15, :30, :45)
                # Ensure it's past 9:15 AM
                if (current_minute % 15 == 1 or (current_minute == 0 and now.hour > 9)) and current_minute != last_checked_minute_15min:
                    if now.time() >= datetime.time(9, 31): # First 15-min candle closes at 9:30, check at 9:31
                        logger.info(f"Fetching latest 15-min candles for 3% Down setup at {now.strftime('%H:%M:%S')}")
                        # Fetch last 30 minutes to ensure we get the latest completed 15-min candle
                        fetch_historical_candles_for_3pct_down(smart_api_obj, unique_tokens_3pct, 'FIFTEEN_MINUTE', now - timedelta(minutes=30), now)
                        check_and_update_3pct_down_setup()

                        last_checked_minute_15min = current_minute

                # 30-minute check: Trigger at :16, :46 (1 minute after candle closes at :15, :45)
                # Ensure it's past 9:15 AM
                if (current_minute % 30 == 16) and current_minute != last_checked_minute_30min:
                    if now.time() >= datetime.time(9, 46): # First 30-min candle closes at 9:45, check at 9:46
                        logger.info(f"Fetching latest 30-min candles for 3% Down setup at {now.strftime('%H:%M:%S')}")
                        # Fetch last 60 minutes to ensure we get the latest completed 30-min candle
                        fetch_historical_candles_for_3pct_down(smart_api_obj, unique_tokens_3pct, 'THIRTY_MINUTE', now - timedelta(minutes=60), now)
                        check_and_update_3pct_down_setup()
                        last_checked_minute_30min = current_minute

                # 1-hour check: Trigger at :16 (1 minute after candle closes at :15)
                # Ensure it's past 9:15 AM
                if (current_minute == 16) and current_minute != last_checked_minute_1hr:
                    if now.time() >= datetime.time(10, 16): # First 1-hour candle closes at 10:15, check at 10:16
                        logger.info(f"Fetching latest 1-hour candles for 3% Down setup at {now.strftime('%H:%M:%S')}")
                        # Fetch last 2 hours to ensure we get the latest completed 1-hour candle
                        fetch_historical_candles_for_3pct_down(smart_api_obj, unique_tokens_3pct, 'ONE_HOUR', now - timedelta(hours=2), now)
                        check_and_update_3pct_down_setup()
                        last_checked_minute_1hr = current_minute
                # ✅ Phase 2: Check confirmation from next 15-min candle every 5 mins
                if current_minute % 5 == 2 and current_minute != last_checked_minute_confirmation:
                    logger.info(f"🔍 Running confirmation check at {now.strftime('%H:%M:%S')}")
                    confirm_high_volume_3pct_down(smart_api_obj)
                    last_checked_minute_confirmation = current_minute        

            else:
                # Outside market hours, ORH and 3% Down checks are skipped
                logger.info(f"⏸ Outside market hours — ORH and 3% Down checks skipped at {now.strftime('%H:%M:%S')}")

            time.sleep(2) # Main loop sleep

    except KeyboardInterrupt:
        logger.info("Script interrupted by user. Closing connections...")
    except Exception as e:
        logger.exception(f"An unhandled error occurred in the main loop: {e}")
    finally:
        if smart_ws:
            smart_ws.close_connection()
 
        logger.info("Script finished.")

if __name__ == "__main__":
    main()
