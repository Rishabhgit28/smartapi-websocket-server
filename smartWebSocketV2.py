# Combined Market Data and Trading Strategy Script

# --- Flask and Threading Imports ---
from flask import Flask
import threading
# Corrected: Import the 'time' module separately for time.localtime() and time.sleep()
import time
import os
import sys
# Corrected: Import datetime, timedelta, date, and time class from datetime
from datetime import datetime, timedelta, date, time as datetime_time # Alias datetime.time to avoid conflict with time module
import collections # Import collections for defaultdict

# --- SmartAPI and WebSocket Imports ---
import logging
import json
import struct
import ssl
import websocket
from SmartApi import SmartConnect
from SmartApi.smartExceptions import DataException

# --- Google Sheets Imports ---
from oauth2client.service_account import ServiceAccountCredentials
import gspread

# --- OTP Generation ---
import pyotp

# --- Platform-specific Imports ---
try:
    import winsound # For playing sound on ORH detection (Windows only)
except ImportError:
    winsound = None # Handle cases where winsound is not available (e.g., Linux, macOS)
    print("[WARNING] 'winsound' module not found. Sound alerts will be disabled.")

# --- Logging Configuration (using logzero for robust logging) ---
import logzero
from logzero import logger

# Configure logzero for daily log files globally
log_folder = time.strftime("%Y-%m-%d", time.localtime())
log_folder_path = os.path.join("logs", log_folder)
os.makedirs(log_folder_path, exist_ok=True) # Ensure the logs directory exists
log_path = os.path.join(log_folder_path, "app.log")

# Use logzero.logfile to configure the file handler with encoding
logzero.logfile(log_path, loglevel=logging.INFO, encoding='utf-8')

# Ensure console output is also configured
# Check if a StreamHandler is already present to avoid duplicates
if not any(isinstance(h, logging.StreamHandler) for h in logger.handlers):
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO)
    formatter = logging.Formatter('[%(levelname)s %(asctime)s %(filename)s:%(lineno)d] %(message)s')
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

# --- Flask App Setup ---
app = Flask(__name__)

# --- Flask Health Check Route ---
@app.route('/ping')
def ping():
    """Health check endpoint for deployment platforms."""
    return "✅ Server is running", 200

# --- GLOBAL VARIABLES ---
# WebSocket and Market Data
latest_tick_data = collections.defaultdict(dict) # Stores latest LTP and full data per token
interval_ohlc_data = collections.defaultdict(lambda: collections.defaultdict(dict)) # For 3-min ORH candles
completed_3min_candles = collections.defaultdict(list) # Stores recent completed 3-min candles for ORH logic

# Google Sheets and Symbol Details
excel_symbol_details = collections.defaultdict(list) # Stores details for ORH symbols
excel_3pct_symbol_details = collections.defaultdict(list) # Stores details for 3% down symbols
subscribed_tokens = set() # To keep track of what's currently subscribed (token, exchange_type_int)

# Caching for Previous Day High (ORH)
previous_day_high_cache = {} # Global dictionary to store previous day's high data

# 3% Down Candle Setup Specific
# Stores the latest completed 15/30/60 min candles for each token and interval
# Structure: latest_completed_candles_3pct[token][interval_name] = {'open': ..., 'high': ..., 'low': ..., 'close': ..., 'start_time': ..., 'volume': ...}
latest_completed_candles_3pct = collections.defaultdict(lambda: collections.defaultdict(dict))
volume_history_3pct = collections.defaultdict(lambda: collections.defaultdict(list)) # Stores last 10 volumes for each token/interval

# API and Google Sheets Objects (initialized in start_websocket_logic)
auth_token = None
feedToken = None
obj = None # SmartConnect object
client = None # gspread client
gsheet = None # Google Sheet workbook
Dashboard = None # Dashboard worksheet
Credential = None # Credential worksheet
smart_ws = None # SmartWebSocketV2 instance

# --- Configuration Constants ---
# SmartAPI Credentials (IMPORTANT: REPLACE WITH YOUR ACTUAL CREDENTIALS)
API_KEY = "oNNHQHKU"
CLIENT_CODE = "D355432"
MPIN = "1234"
TOTP_SECRET = "QHO5IWOISV56Z2BFTPFSRSQVRQ"

# Google Sheets Configuration
GOOGLE_SHEET_ID = "1cYBpsVKCbrYCZzrj8NAMEgUG4cXy5Q5r9BtQE1Cjmz0" # Your Google Sheet ID (Corrected)
GOOGLE_SHEET_NAME = "MarketDashboard"
DASHBOARD_SHEET_NAME = "Dashboard"
CREDENTIAL_SHEET_NAME = "Credential"

# Path to your service account JSON key file
# For deployment, check /etc/secrets/creds.json
if os.path.exists("/etc/secrets/creds.json"):
    JSON_KEY_FILE_PATH = "/etc/secrets/cres.json" # Typo corrected: creds.json
else:
    # Fallback for local development/testing
    JSON_KEY_FILE_PATH = os.path.join(os.path.dirname(__file__), "the-money-method-ad6d7-f95331cf5cbf.json")

SCOPE = [
    "https://spreadsheets.google.com/feeds",
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive"
]

# Google Sheet Row/Column Mappings
START_ROW_DATA = 5 # Starting row for data in Dashboard sheet (both ORH and Full Positions)
INDEX_START_ROW = 26
QUARTER_POSITIONS_START_ROW = 100

# ORH Setup Columns
EXCHANGE_COL_ORH = 'B' # For ORH symbols
SYMBOL_COL_ORH = 'C'   # For ORH symbols
TOKEN_COL_ORH = 'F'    # Column in Credential sheet for ORH tokens
ORH_RESULT_COL = 'G' # Column to write ORH results in Dashboard sheet
ORH_BUY_STOP_COL = 'H' # Column to write ORH Buy Stop price

# Full Positions Columns (from smartWebSocketV2.py)
FULL_EXCHANGE_COL = 'M'
FULL_SYMBOL_COL = 'N'
FULL_LTP_COL = 'P'
FULL_CRED_TOKEN_COL = 'G' # Reusing G for full positions token
FULL_PRICE_COL = 'O'
FULL_QTY_COL = 'S'
FULL_SWING_LOW_COL = 'V'
FULL_RETURN_AMT_COL = 'Q'
FULL_RETURN_PCT_COL = 'R'
FULL_MOVE_PCT_COL = 'W'
FULL_TRAILING_STOP_COL = 'Y'
FULL_PERCENT_FROM_STOP_COL = 'Z'
FULL_ENTRY_DATE_COL = 'T'
FULL_DAYS_DURATION_COL = 'AA'

# Indices Columns (from smartWebSocketV2.py)
INDEX_EXCHANGE_COL = 'B'
INDEX_SYMBOL_COL = 'C'
INDEX_LTP_COL = 'D'
INDEX_CHG_COL = 'E'
INDEX_CRED_TOKEN_COL = 'F'

# Quarter Positions Columns (from smartWebSocketV2.py)
QUARTER_EXCHANGE_COL = 'M'
QUARTER_SYMBOL_COL = 'N'
QUARTER_LTP_COL = 'P'
QUARTER_CHG_COL = 'Q'
QUARTER_CRED_TOKEN_COL = 'G'

# 3% Down Candle Setup Columns
EXCHANGE_COL_3PCT = 'M' # Exchange for 3% down symbols (same as FULL_EXCHANGE_COL)
SYMBOL_COL_3PCT = 'N'   # Symbols for 3% down setup (same as FULL_SYMBOL_COL)
TOKEN_COL_3PCT = 'G'    # Column in Credential sheet for 3% down tokens (same as FULL_CRED_TOKEN_COL)
PCT_DOWN_RESULT_COL = 'AC' # Column to write 3% down results in Dashboard sheet
PCT_DOWN_CONFIRM_COL = 'AD' # Column to write 3% down confirmation results

# Candle Intervals for 3% Down Setup
CANDLE_INTERVALS_3PCT_API = ['FIFTEEN_MINUTE', 'THIRTY_MINUTE', 'ONE_HOUR']
CANDLE_INTERVAL_MAP_DISPLAY = {
    'FIFTEEN_MINUTE': '15 min',
    'THIRTY_MINUTE': '30 min',
    'ONE_HOUR': 'Hourly'
}
# Map for converting interval API names to minutes for timedelta calculations
interval_minutes_map = {"FIFTEEN_MINUTE": 15, "THIRTY_MINUTE": 30, "ONE_HOUR": 60}


# API Call Retry Parameters
SCRIP_SEARCH_RETRY_ATTEMPTS = 5
SCRIP_SEARCH_RETRY_DELAY = 2.0
SCRIP_SEARCH_RETRY_MULTIPLIER = 1.5
HISTORICAL_DATA_RETRY_ATTEMPTS = 3
HISTORICAL_DATA_RETRY_DELAY = 1.0
HISTORICAL_DATA_RETRY_MULTIPLIER = 1.5

# Cache File
PREV_DAY_HIGH_CACHE_FILE = 'previous_day_high_cache.json'

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
    now = datetime.now()
    current_time = now.time()
    current_weekday = now.weekday() # Monday is 0, Sunday is 6

    # Check if it's a weekday (Monday to Friday)
    if 0 <= current_weekday <= 4: # Monday (0) to Friday (4)
        # Check if current time is within market hours (9:15 AM to 3:30 PM)
        market_open_time = datetime_time(9, 15) # Use aliased datetime_time
        market_close_time = datetime_time(15, 30) # Use aliased datetime_time
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
        # If no non-empty values, return START_ROW_DATA - 1 to indicate empty or only headers
        return last_non_empty_index + 1 if last_non_empty_index != -1 else START_ROW_DATA - 1
    except Exception as e:
        logger.error(f"Error in get_last_row_in_column for sheet '{sheet.title}', column '{column_letter}': {e}")
        return START_ROW_DATA - 1

def rgb_to_float(rgb_tuple):
    """Helper function to convert RGB tuple (0-255) to float (0-1) for Google Sheets API."""
    if rgb_tuple is None:
        # Explicitly return white color if no specific color is provided
        return {"red": 1.0, "green": 1.0, "blue": 1.0}
    return {
        "red": rgb_tuple[0] / 255.0,
        "green": rgb_tuple[1] / 255.0,
        "blue": rgb_tuple[2] / 255.0
    }

def create_cell_range_for_request(sheet_id, col_letter, row_num):
    """Helper to create a range for a single cell for batch update requests."""
    return {
        "sheetId": sheet_id,
        "startRowIndex": row_num - 1,
        "endRowIndex": row_num,
        "startColumnIndex": col_letter_to_index(col_letter) - 1,
        "endColumnIndex": col_letter_to_index(col_letter)
    }

# --- SmartWebSocketV2 Class (from smartWebSocketV2.py, with ORH on_data logic integrated) ---
class SmartWebSocketV2(object):
    """
    SmartAPI Web Socket version 2 for handling live market data.
    Provides methods for connection, subscription, unsubscription, and binary data parsing.
    """

    ROOT_URI = "wss://smartapisocket.angelone.in/smart-stream"
    HEART_BEAT_MESSAGE = "ping"
    HEART_BEAT_INTERVAL = 10
    LITTLE_ENDIAN_BYTE_ORDER = "<"
    RESUBSCRIBE_FLAG = False

    # Available Actions
    SUBSCRIBE_ACTION = 1
    UNSUBSCRIBE_ACTION = 0

    # Possible Subscription Mode
    LTP_MODE = 1
    QUOTE = 2
    SNAP_QUOTE = 3
    DEPTH = 4

    # Exchange Type
    NSE_CM = 1
    NSE_FO = 2
    BSE_CM = 3
    BSE_FO = 4
    MCX_FO = 5
    NCX_FO = 7
    CDE_FO = 13

    # Subscription Mode Map for logging/display
    SUBSCRIPTION_MODE_MAP = {
        1: "LTP",
        2: "QUOTE",
        3: "SNAP_QUOTE",
        4: "DEPTH"
    }

    wsapp = None
    input_request_dict = {}
    current_retry_attempt = 0

    def __init__(self, auth_token, api_key, client_code, feed_token, max_retry_attempt=1, retry_strategy=0, retry_delay=10, retry_multiplier=2, retry_duration=60):
        """
        Initializes the SmartWebSocketV2 client.

        Args:
            auth_token (str): JWT authentication token.
            api_key (str): SmartAPI application API key.
            client_code (str): User's client code.
            feed_token (str): Market data feed token.
            max_retry_attempt (int): Maximum number of reconnection attempts.
            retry_strategy (int): 0 for linear, 1 for exponential backoff.
            retry_delay (int): Initial delay in seconds before retrying.
            retry_multiplier (int): Multiplier for exponential backoff.
            retry_duration (int): Total duration in seconds for retries (not actively used in current retry logic).
        """
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
        self._is_connected_flag = False # Internal flag to track connection status

        if not self._sanity_check():
            logger.error("Invalid initialization parameters. Provide valid values for all the tokens.")
            raise Exception("Provide valid value for all the tokens")

    def _sanity_check(self):
        """Checks if all required tokens are provided."""
        return all([self.auth_token, self.api_key, self.client_code, self.feed_token])

    def _on_message(self, wsapp, message):
        """
        Callback for receiving raw messages from the WebSocket.
        Distinguishes between 'pong' messages and binary data ticks.
        """
        if message == "pong":
            self._on_pong(wsapp, message)
        else:
            try:
                parsed_message = self._parse_binary_data(message)
                self.on_data(wsapp, parsed_message) # Call the public on_data
            except Exception as e:
                logger.error(f"Error parsing or handling binary message: {e}. Raw message (first 50 bytes): {message[:50]}...")
                if hasattr(self, 'on_error'):
                    self.on_error(wsapp, f"Data parsing error: {e}")

    def _on_open(self, wsapp):
        """Callback when the WebSocket connection is successfully opened."""
        if self.RESUBSCRIBE_FLAG:
            self.resubscribe()
        self._is_connected_flag = True
        update_connection_status("connected")
        self.on_open(wsapp) # Call the public on_open

    def _on_pong(self, wsapp, data):
        """Callback when a 'pong' message is received (heartbeat response)."""
        timestamp = time.time()
        formatted_timestamp = time.strftime("%d-%m-%y %H:%M:%S", time.localtime(timestamp))
        logger.info(f"In on pong function ==> {data}, Timestamp: {formatted_timestamp}")
        self.last_pong_timestamp = timestamp

    def _on_ping(self, wsapp, data):
        """Callback when a 'ping' message is sent (heartbeat initiation)."""
        timestamp = time.time()
        formatted_timestamp = time.strftime("%d-%m-%y %H:%M:%S", time.localtime(timestamp))
        logger.info(f"In on ping function ==> {data}, Timestamp: {formatted_timestamp}")
        self.last_ping_timestamp = timestamp

    def subscribe(self, correlation_id, mode, token_list):
        """
        Subscribes to market data for specified tokens and mode.

        Args:
            correlation_id (str): A unique ID for the subscription request.
            mode (int): Subscription mode (e.g., LTP_MODE, QUOTE, SNAP_QUOTE, DEPTH).
            token_list (list): List of dictionaries, each containing 'exchangeType' and 'tokens'.
        """
        try:
            request_data = {
                "correlationID": correlation_id,
                "action": self.SUBSCRIBE_ACTION,
                "params": {
                    "mode": mode,
                    "tokenList": token_list
                }
            }

            if mode == self.DEPTH:
                for token_item in token_list:
                    if token_item.get('exchangeType') != self.NSE_CM:
                        error_message = f"Invalid ExchangeType:{token_item.get('exchangeType')} for DEPTH mode. Only NSE_CM (1) is supported."
                        logger.error(error_message)
                        raise ValueError(error_message)
                total_tokens = sum(len(token_item["tokens"]) for token_item in token_list)
                quota_limit = 50
                if total_tokens > quota_limit:
                    error_message = f"Quota exceeded: You can subscribe to a maximum of {quota_limit} tokens for DEPTH mode."
                    logger.error(error_message)
                    raise Exception(error_message)

            # Store subscription requests for resubscription
            if self.input_request_dict.get(mode) is None:
                self.input_request_dict[mode] = {}
            for token_item in token_list:
                exchange_type = token_item['exchangeType']
                tokens = set(token_item["tokens"])
                if exchange_type in self.input_request_dict[mode]:
                    self.input_request_dict[mode][exchange_type].update(tokens)
                else:
                    self.input_request_dict[mode][exchange_type] = tokens

            if self.wsapp:
                self.wsapp.send(json.dumps(request_data))
                self.RESUBSCRIBE_FLAG = True # Set flag for resubscription on reconnect
            else:
                logger.warning("WebSocket not initialized. Subscription request deferred.")

        except Exception as e:
            logger.error(f"Error occurred during subscribe: {e}")
            raise e

    def unsubscribe(self, correlation_id, mode, token_list):
        """
        Unsubscribes from market data for specified tokens and mode.

        Args:
            correlation_id (str): A unique ID for the unsubscription request.
            mode (int): Subscription mode.
            token_list (list): List of dictionaries, each containing 'exchangeType' and 'tokens'.
        """
        try:
            request_data = {
                "correlationID": correlation_id,
                "action": self.UNSUBSCRIBE_ACTION,
                "params": {
                    "mode": mode,
                    "tokenList": token_list
                }
            }
            # Remove from stored subscription requests
            if mode in self.input_request_dict:
                for token_item in token_list:
                    exchange_type = token_item['exchangeType']
                    tokens_to_remove = set(token_item['tokens'])
                    if exchange_type in self.input_request_dict[mode]:
                        self.input_request_dict[mode][exchange_type].difference_update(tokens_to_remove)
                        if not self.input_request_dict[mode][exchange_type]:
                            del self.input_request_dict[mode][exchange_type]
                if not self.input_request_dict[mode]:
                    del self.input_request_dict[mode]

            if self.wsapp:
                self.wsapp.send(json.dumps(request_data))
                self.RESUBSCRIBE_FLAG = True # Set flag for resubscription on reconnect
            else:
                logger.warning("WebSocket not initialized. Unsubscribe request deferred.")

        except Exception as e:
            logger.error(f"Error occurred during unsubscribe: {e}")
            raise e

    def resubscribe(self):
        """Resubscribes to all previously subscribed tokens (called on reconnect)."""
        try:
            if not self.wsapp:
                logger.warning("WebSocket not initialized for resubscription attempt.")
                return

            for mode, exchange_data in self.input_request_dict.items():
                token_list = []
                for exchangeType, tokens_set in exchange_data.items():
                    temp_data = {
                        'exchangeType': exchangeType,
                        'tokens': list(tokens_set)
                    }
                    token_list.append(temp_data)
                if token_list:
                    request_data = {
                        "action": self.SUBSCRIBE_ACTION,
                        "params": {
                            "mode": mode,
                            "tokenList": token_list
                        }
                    }
                    self.wsapp.send(json.dumps(request_data))
                    logger.info(f"Resubscribed to mode {mode} with {len(token_list)} token groups.")
        except Exception as e:
            logger.error(f"Error occurred during resubscribe: {e}")
            raise e

    def connect(self):
        """Establishes the WebSocket connection."""
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
        """Closes the WebSocket connection explicitly."""
        self.RESUBSCRIBE_FLAG = False
        self.DISCONNECT_FLAG = True
        if self.wsapp:
            self.wsapp.close()
            logger.info("WebSocket connection explicitly closed.")
        else:
            logger.warning("WebSocket not initialized, no active connection to close.")
        self._is_connected_flag = False
        update_connection_status("disconnected")


    def _on_error(self, wsapp, error):
        """
        Internal callback for WebSocket errors.
        Handles reconnection attempts based on retry strategy.
        """
        logger.error(f"Internal WebSocket error: {error}")
        self.RESUBSCRIBE_FLAG = True # Ensure resubscription on next successful connect

        self.on_error(wsapp, error) # Call the public on_error

        if self.current_retry_attempt < self.MAX_RETRY_ATTEMPT:
            logger.warning(f"Attempting to reconnect (Attempt {self.current_retry_attempt + 1})...")
            self.current_retry_attempt += 1
            delay = self.retry_delay
            if self.retry_strategy == 1: # Exponential backoff
                delay = self.retry_delay * (self.retry_multiplier ** (self.current_retry_attempt - 1))

            time.sleep(delay)

            try:
                if self.wsapp:
                    self.wsapp.close() # Close existing connection before reconnecting
                    time.sleep(1) # Give it a moment to truly close
                self.connect()
            except Exception as e:
                logger.error(f"Error during reconnection attempt: {e}")
                if hasattr(self, 'on_error'):
                    self.on_error(wsapp, f"Reconnect failed: {e}")
        else:
            logger.error("Max retry attempts reached. Closing connection.")
            self.close_connection()
            if hasattr(self, 'on_error'):
                self.on_error(wsapp, "Max retry attempt reached, connection closed.")

    def _on_close(self, wsapp, close_status_code, close_msg):
        """Internal callback when the WebSocket connection is closed."""
        logger.warning(f"WebSocket closed. Code: {close_status_code}, Message: {close_msg}")
        self.current_retry_attempt = 0 # Reset retry counter on clean close
        self._is_connected_flag = False
        update_connection_status("disconnected")
        self.on_close(wsapp, close_status_code, close_msg) # Call the public on_close

    def _parse_binary_data(self, binary_data):
        """
        Parses raw binary market data into a readable dictionary format.
        Handles different subscription modes (LTP, QUOTE, SNAP_QUOTE, DEPTH).
        """
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

            if parsed_data["subscription_mode"] == self.SNAP_QUOTE:
                parsed_data["last_traded_timestamp"] = self._unpack_data(binary_data, 123, 131, byte_format="q")[0]
                parsed_data["open_interest"] = self._unpack_data(binary_data, 131, 139, byte_format="q")[0]
                parsed_data["open_interest_change_percentage"] = self._unpack_data(binary_data, 139, 147, byte_format="q")[0]
                parsed_data["upper_circuit_limit"] = self._unpack_data(binary_data, 347, 355, byte_format="q")[0]
                parsed_data["lower_circuit_limit"] = self._unpack_data(binary_data, 355, 363, byte_format="q")[0]
                parsed_data["52_week_high_price"] = self._unpack_data(binary_data, 363, 371, byte_format="q")[0]
                parsed_data["52_week_low_price"] = self._unpack_data(binary_data, 371, 379, byte_format="q")[0]
                best_5_buy_and_sell_data = self._parse_best_5_buy_and_sell_data(binary_data[147:347])
                parsed_data["best_5_buy_data"] = best_5_buy_and_sell_data["best_5_buy_data"]
                parsed_data["best_5_sell_data"] = best_5_buy_and_sell_data["best_5_sell_data"]

            if parsed_data["subscription_mode"] == self.DEPTH:
                parsed_data.pop("sequence_number", None)
                parsed_data.pop("last_traded_price", None)
                parsed_data.pop("subscription_mode_val", None)
                parsed_data["packet_received_time"]=self._unpack_data(binary_data, 35, 43, byte_format="q")[0]
                depth_data_start_index = 43
                depth_20_data = self._parse_depth_20_buy_and_sell_data(binary_data[depth_data_start_index:])
                parsed_data["depth_20_buy_data"] = depth_20_data["depth_20_buy_data"]
                parsed_data["depth_20_sell_data"] = depth_20_data["depth_20_sell_data"]

            return parsed_data
        except Exception as e:
            logger.error(f"Error occurred during binary data parsing: {e}. Data: {binary_data}")
            raise e

    def _unpack_data(self, binary_data, start, end, byte_format="I"):
        """Helper to unpack binary data using struct."""
        return struct.unpack(self.LITTLE_ENDIAN_BYTE_ORDER + byte_format, binary_data[start:end])

    @staticmethod
    def _parse_token_value(binary_packet):
        """Parses the null-terminated token string from binary data."""
        token = ""
        for i in range(len(binary_packet)):
            if chr(binary_packet[i]) == '\x00':
                return token
            token += chr(binary_packet[i])
        return token

    def _parse_best_5_buy_and_sell_data(self, binary_data):
        """Parses best 5 buy/sell depth data for SNAP_QUOTE."""
        def split_packets(binary_packets):
            packets = []
            i = 0
            while i < len(binary_packets):
                packets.append(binary_packets[i: i + 20])
                i += 20
            return packets

        best_5_buy_sell_packets = split_packets(binary_data)
        best_5_buy_data = []
        best_5_sell_data = []

        for packet in best_5_buy_sell_packets:
            each_data = {
                "flag": self._unpack_data(packet, 0, 2, byte_format="H")[0],
                "quantity": self._unpack_data(packet, 2, 10, byte_format="q")[0],
                "price": self._unpack_data(packet, 10, 18, byte_format="q")[0],
                "no of orders": self._unpack_data(packet, 18, 20, byte_format="H")[0]
            }

            if each_data["flag"] == 0:
                best_5_buy_data.append(each_data)
            else:
                best_5_sell_data.append(each_data)
        return {
            "best_5_buy_data": best_5_buy_data,
            "best_5_sell_data": best_5_sell_data
        }

    def _parse_depth_20_buy_and_sell_data(self, binary_data):
        """Parses depth 20 buy/sell data for DEPTH mode."""
        depth_20_buy_data = []
        depth_20_sell_data = []

        for i in range(20):
            buy_start_idx = i * 10
            sell_start_idx = 200 + i * 10

            buy_packet_data = {
                "quantity": self._unpack_data(binary_data, buy_start_idx, buy_start_idx + 4, byte_format="i")[0],
                "price": self._unpack_data(binary_data, buy_start_idx + 4, buy_start_idx + 8, byte_format="i")[0],
                "num_of_orders": self._unpack_data(binary_data, buy_start_idx + 8, buy_start_idx + 10, byte_format="h")[0],
            }

            sell_packet_data = {
                "quantity": self._unpack_data(binary_data, sell_start_idx, sell_start_idx + 4, byte_format="i")[0],
                "price": self._unpack_data(binary_data, sell_start_idx + 4, sell_start_idx + 8, byte_format="i")[0],
                "num_of_orders": self._unpack_data(binary_data, sell_start_idx + 8, sell_start_idx + 10, byte_format="h")[0],
            }

            depth_20_buy_data.append(buy_packet_data)
            depth_20_sell_data.append(sell_packet_data)

        return {
            "depth_20_buy_data": depth_20_buy_data,
            "depth_20_sell_data": depth_20_sell_data
        }

    def on_message(self, wsapp, message):
        """Called for raw string messages (e.g., control messages like 'ping')."""
        pass

    def on_data(self, wsapp, data):
        """
        Processes incoming parsed market data ticks.
        Updates the global latest_tick_data dictionary and builds 3-minute candles for ORH.
        This method combines logic from both original scripts.
        """
        token = data.get('token')
        ltp_raw = data.get('last_traded_price')
        open_price_raw = data.get('open_price_of_the_day') # Only available in QUOTE/SNAP_QUOTE mode

        # Scale prices (API provides them as integers)
        ltp_scaled = ltp_raw / 100.0 if isinstance(ltp_raw, (int, float)) else None
        open_price_scaled = open_price_raw / 100.0 if isinstance(open_price_raw, (int, float)) else None
        
        if token and ltp_scaled is not None:
            # Update global latest_tick_data (for general LTP updates to Google Sheet)
            latest_tick_data[token] = {
                'ltp': ltp_scaled,
                'open': open_price_scaled, # Store open price if available
                'full_data': data
            }
            logger.debug(f"Updated latest_tick_data for Token {token}: LTP={ltp_scaled}, Open={open_price_scaled}")

            # Build 3-minute candles (ORH setup)
            current_time = datetime.now()
            interval = '3min'
            candle_info = interval_ohlc_data[token][interval]
            
            minute_floor = (current_time.minute // 3) * 3
            candle_start_dt = current_time.replace(minute=minute_floor, second=0, microsecond=0)
            
            if candle_info.get('start_time') is None or candle_start_dt > candle_info['start_time']:
                # A new 3-minute candle has started, or it's the first tick
                if candle_info.get('start_time') is not None:
                    # Previous candle is complete, add it to history
                    completed_candle = {
                        'open': candle_info['open'],
                        'high': candle_info['high'],
                        'low': candle_info['low'],
                        'close': candle_info['last_ltp'],
                        'start_time': candle_info['start_time']
                    }
                    completed_3min_candles[token].append(completed_candle)
                    
                    # Keep only the last 10 completed candles
                    if len(completed_3min_candles[token]) > 10:
                        completed_3min_candles[token].pop(0)

                    log_message = (
                        f"Completed 3min candle for {token}: O={completed_candle['open']:.2f}, "
                        f"H={completed_candle['high']:.2f}, L={completed_candle['low']:.2f}, "
                        f"C={completed_candle['close']:.2f}. History size: {len(completed_3min_candles[token])}"
                    )
                    logger.info(log_message)

                # Initialize the new candle with the current tick's LTP
                candle_info['open'] = ltp_scaled
                candle_info['high'] = ltp_scaled
                candle_info['low'] = ltp_scaled
                candle_info['start_time'] = candle_start_dt
            
            # Update current candle's high, low, and last_ltp
            candle_info['high'] = max(candle_info.get('high', ltp_scaled), ltp_scaled)
            candle_info['low'] = min(candle_info.get('low', ltp_scaled), ltp_scaled)
            candle_info['last_ltp'] = ltp_scaled
        else:
            logger.warning(f"Skipped updating latest_tick_data due to incomplete data for token: {token}")


    def on_control_message(self, wsapp, message):
        """Currently unused, but reserved for control messages."""
        pass

    def on_close(self, wsapp, close_status_code, close_msg):
        """Called when the WebSocket connection is closed."""
        logger.warning(f"WebSocket closed: {close_msg} (Code: {close_status_code})")

    def on_open(self, wsapp):
        """Called when the WebSocket connection is opened."""
        logger.info("WebSocket connected")

    def on_error(self, wsapp, error_message):
        """Called when a WebSocket error occurs."""
        logger.error(f"WebSocket error: {error_message}")


# --- Data Fetch and Google Sheets Functions ---

def get_exchange_type_num(exchange_name):
    """Maps exchange name string to SmartWebSocketV2 numeric constant, handling extra spaces."""
    exchange_name_clean = exchange_name.strip().upper()
    # Use the constants from the SmartWebSocketV2 class directly
    if exchange_name_clean == "NSE": return SmartWebSocketV2.NSE_CM
    elif exchange_name_clean == "NFO": return SmartWebSocketV2.NSE_FO
    elif exchange_name_clean == "BSE": return SmartWebSocketV2.BSE_CM
    elif exchange_name_clean == "MCX": return SmartWebSocketV2.MCX_FO
    elif exchange_name_clean == "CDE": return SmartWebSocketV2.CDE_FO
    logger.warning(f"Attempted to map unknown exchange type: '{exchange_name_clean}'")
    return None

def process_symbol_entry(row, exchange, symbol, ltp_col, chg_col, cred_token_col, block_type,
                         price_col, qty_col, swing_low_col, return_amt_col, return_pct_col, move_pct_col,
                         trailing_stop_col, percent_from_stop_col, entry_date_col, days_duration_col,
                         all_credential_values):
    """
    Helper function to process a single symbol entry from Google Sheet.
    Fetches/validates token and adds its details to `excel_symbol_details` or `excel_3pct_symbol_details`.
    Includes retry logic for scrip search API calls and historical data calls.
    Writes new tokens to Credential sheet individually.
    """
    global excel_symbol_details, excel_3pct_symbol_details, obj, Credential

    found_token = None
    cred_col_num = col_letter_to_index(cred_token_col)
    existing_token_in_excel = None
    if 0 <= row - 1 < len(all_credential_values) and 0 <= cred_col_num - 1 < len(all_credential_values[row - 1]):
        existing_token_in_excel = all_credential_values[row - 1][cred_col_num - 1]

    if existing_token_in_excel:
        try:
            existing_token_clean = str(int(float(existing_token_in_excel))).strip()
            found_token = existing_token_clean
            logger.debug(f"Using existing token {found_token} from Google Sheet for {symbol} in row {row} ({block_type}).")
        except ValueError:
            logger.warning(f"Invalid token '{existing_token_in_excel}' in Google Sheet for {symbol} ({block_type}). Attempting to fetch new token.")
            found_token = None

    if not found_token:
        symbol_clean = symbol.strip().upper()
        exchange_clean = exchange.strip().upper()

        search_term_api = symbol_clean
        exchange_for_search_api = exchange_clean

        if symbol_clean == "NIFTY 50":
            search_term_api = "NIFTY"
            exchange_for_search_api = "NSE"
        elif symbol_clean == "BANKNIFTY":
            search_term_api = "BANKNIFTY"
            exchange_for_search_api = "NSE"

        for attempt in range(SCRIP_SEARCH_RETRY_ATTEMPTS):
            try:
                search_response = obj.searchScrip(exchange_for_search_api, search_term_api)
                time.sleep(0.5)

                if search_response and search_response.get('status') and search_response.get('data'):
                    for scrip_data in search_response['data']:
                        scrip_trading_symbol = scrip_data.get('tradingsymbol', '').strip().upper()

                        if scrip_trading_symbol == symbol_clean:
                            found_token = scrip_data.get('symboltoken')
                            logger.debug(f"Found exact match for {symbol_clean} with trading symbol {scrip_trading_symbol}")
                            break

                        if symbol_clean == "NIFTY 50" and scrip_trading_symbol == "NIFTY":
                            found_token = scrip_data.get('symboltoken')
                            logger.debug(f"Found NIFTY token {found_token} for Nifty 50 via tradingsymbol 'NIFTY'")
                            break
                        if symbol_clean == "BANKNIFTY" and scrip_trading_symbol == "BANKNIFTY":
                            found_token = scrip_data.get('symboltoken')
                            logger.debug(f"Found BANKNIFTY token {found_token} for BANKNIFTY via tradingsymbol 'BANKNIFTY'")
                            break

                    if found_token:
                        logger.info(f"Successfully fetched token for {symbol_clean} ({block_type}): {found_token} (Attempt {attempt+1})")
                        try:
                            Credential.update_cell(row, cred_col_num, int(found_token))
                            logger.debug(f"Wrote token {found_token} to Credential sheet for {symbol_clean}.")
                            break
                        except Exception as ex_write:
                            logger.warning(f"Error writing token to Credential for {symbol_clean}. Error: {ex_write}")
                        break
                    else:
                        logger.warning(f"Could not find matching token for '{symbol_clean}' in search results ({block_type}) after search (Attempt {attempt+1}).")
                elif search_response and not search_response.get('status'):
                    error_msg = search_response.get('message', 'No message')
                    error_code = search_response.get('errorcode', 'N/A')
                    logger.error(f"API returned error for '{symbol_clean}' ({block_type}): {error_msg}. Error Code: {error_code} (Attempt {attempt+1})")
                    if "Access denied" in error_msg or "rate" in error_msg.lower():
                        pass
                    else:
                        break
                else:
                    logger.error(f"Failed to fetch scrip search data for '{symbol_clean}' ({block_type}). Response empty/malformed (Attempt {attempt+1}). Search Response: {search_response}")

            except DataException as de:
                logger.error(f"DataException during search for token for {symbol_clean} (API Search: {exchange_for_search_api}, {search_term_api}) in row {row} ({block_type}): {de}. Raw content: {de.args[0]} (Attempt {attempt+1})")
                if "Access denied" in str(de) or "rate" in str(de).lower():
                    pass
                else:
                    break
            except Exception as e:
                logger.exception(f"Unexpected error during search for {symbol_clean} in row {row} ({block_type}): {e} (Attempt {attempt+1})")

            if attempt < SCRIP_SEARCH_RETRY_ATTEMPTS - 1:
                delay_current = SCRIP_SEARCH_RETRY_DELAY * (SCRIP_SEARCH_RETRY_MULTIPLIER ** attempt)
                logger.warning(f"Retrying scrip search for {symbol_clean} in {delay_current:.1f}s (Attempt {attempt+1}/{SCRIP_SEARCH_RETRY_ATTEMPTS})...")
                time.sleep(delay_current)

        if not found_token:
            logger.error(f"Failed to fetch token for {symbol_clean} ({block_type}) after {SCRIP_SEARCH_RETRY_ATTEMPTS} attempts.")
    
    # --- Fetch Previous Day Close (PDC) using Historical Data API for "Focus List" ---
    previous_day_close = None
    if found_token and block_type == "Focus List":
        exchange_for_api = exchange.strip().upper()
        if exchange_for_api == "NFO":
            exchange_for_api = "NSE" # Historical API typically uses "NSE" for F&O too

        today = datetime.now()
        
        # Determine the 'to_date' based on the current day of the week
        current_weekday = today.weekday()

        if current_weekday == 5: # Saturday
            target_date = today - timedelta(days=2) # Friday
            to_date = (target_date - timedelta(days=1)).strftime("%Y-%m-%d %H:%M") # Thursday
            logger.info(f"Today is Saturday. Setting 'to_date' for PDC to Thursday: {to_date}")
        elif current_weekday == 6: # Sunday
            target_date = today - timedelta(days=3) # Friday
            to_date = (target_date - timedelta(days=1)).strftime("%Y-%m-%d %H:%M") # Thursday
            logger.info(f"Today is Sunday. Setting 'to_date' for PDC to Thursday: {to_date}")
        elif current_weekday == 0: # Monday
            to_date = (today - timedelta(days=3)).strftime("%Y-%m-%d %H:%M") # Friday
            logger.info(f"Today is Monday. Setting 'to_date' for PDC to Friday: {to_date}")
        else: # Tuesday, Wednesday, Thursday, Friday
            to_date = (today - timedelta(days=1)).strftime("%Y-%m-%d %H:%M") # Yesterday
            logger.info(f"Today is a weekday. Setting 'to_date' for PDC to yesterday: {to_date}")

        # Ensure from_date is always sufficiently in the past from the calculated to_date
        from_date = (datetime.strptime(to_date.split(" ")[0], "%Y-%m-%d") - timedelta(days=15)).strftime("%Y-%m-%d %H:%M")

        logger.debug(f"Fetching historical data for {symbol} (Token: {found_token}) from {from_date} to {to_date}")

        for attempt in range(HISTORICAL_DATA_RETRY_ATTEMPTS):
            try:
                historical_data_response = obj.getCandleData({
                    "exchange": exchange_for_api,
                    "symboltoken": found_token,
                    "interval": "ONE_DAY",
                    "fromdate": from_date,
                    "todate": to_date
                })
                time.sleep(0.2)

                if historical_data_response and historical_data_response.get('status') and historical_data_response.get('data'):
                    latest_candle = None
                    for candle in reversed(historical_data_response['data']):
                        if len(candle) > 4 and candle[4] is not None and candle[4] != '':
                            try:
                                latest_candle_close = float(candle[4])
                                if latest_candle_close is not None:
                                    latest_candle = candle
                                    previous_day_close = latest_candle_close
                                    break
                            except ValueError:
                                logger.warning(f"Invalid close price format for {symbol} in candle: {candle[4]}")
                                continue

                    if previous_day_close is not None:
                        logger.info(f"Successfully fetched previous day close for {symbol} (Token: {found_token}): {previous_day_close} (Attempt {attempt+1})")
                        break
                    else:
                        logger.warning(f"No valid historical candle data with a close price found for {symbol} (Token: {found_token}) in response (Attempt {attempt+1}).")
                elif historical_data_response and not historical_data_response.get('status'):
                    error_msg = historical_data_response.get('message', 'No message')
                    error_code = historical_data_response.get('errorcode', 'N/A')
                    logger.error(f"Historical data API returned error for '{symbol}' (Token: {found_token}): {error_msg}. Error Code: {error_code} (Attempt {attempt+1})")
                    if "Access denied" in error_msg or "rate" in error_msg.lower():
                        pass
                    else:
                        break
                else:
                    logger.error(f"Failed to fetch historical data for '{symbol}' (Token: {found_token}). Response empty/malformed (Attempt {attempt+1}).")

            except DataException as de:
                logger.error(f"DataException during historical data fetch for {symbol} (Token: {found_token}): {de}. Raw content: {de.args[0]} (Attempt {attempt+1})")
                if "Access denied" in str(de) or "rate" in str(de).lower():
                    pass
                else:
                    break
            except Exception as e:
                logger.exception(f"Unexpected error during historical data fetch for {symbol} (Token: {found_token}): {e} (Attempt {attempt+1})")

            if attempt < HISTORICAL_DATA_RETRY_ATTEMPTS - 1:
                delay_current = HISTORICAL_DATA_RETRY_DELAY * (HISTORICAL_DATA_RETRY_MULTIPLIER ** attempt)
                logger.warning(f"Retrying historical data fetch for {symbol} in {delay_current:.1f}s (Attempt {attempt+1}/{HISTORICAL_DATA_RETRY_ATTEMPTS})...")
                time.sleep(delay_current)
        
        if previous_day_close is None:
            logger.warning(f"Could not fetch previous day close for {symbol} (Token: {found_token}) after {HISTORICAL_DATA_RETRY_ATTEMPTS} attempts.")
    
    if found_token:
        # Add to excel_symbol_details for ORH/Full Positions/Indices/Quarter Positions
        # Add to excel_3pct_symbol_details for 3% Down
        
        # Check for duplicates before adding
        is_duplicate = False
        target_list = None
        if block_type in ["Focus List", "Full Positions", "Indices", "Quarter Positions"]:
            target_list = excel_symbol_details[found_token]
        elif block_type == "3% Down":
            target_list = excel_3pct_symbol_details[found_token]

        if target_list:
            for existing_entry in target_list:
                # A simple check for uniqueness based on row and block type
                if existing_entry['row'] == row and existing_entry['block_type'] == block_type:
                   is_duplicate = True
                   break
        
        if not is_duplicate:
            entry_details = {
                'row': row,
                'ltp_col': ltp_col,
                'chg_col': chg_col,
                'cred_col': cred_token_col,
                'exchange': exchange,
                'symbol': symbol,
                'block_type': block_type,
                'price_col': price_col,
                'qty_col': qty_col,
                'swing_low_col': swing_low_col,
                'return_amt_col': return_amt_col,
                'return_pct_col': return_pct_col,
                'move_pct_col': move_pct_col,
                'trailing_stop_col': trailing_stop_col,
                'percent_from_stop_col': percent_from_stop_col,
                'entry_date_col': entry_date_col,
                'days_duration_col': days_duration_col,
                'previous_day_close': previous_day_close # Only relevant for "Focus List"
            }
            if block_type in ["Focus List", "Full Positions", "Indices", "Quarter Positions"]:
                excel_symbol_details[found_token].append(entry_details)
            elif block_type == "3% Down":
                # For 3% Down, we only need symbol, row, pct_down_col, exchange_type
                excel_3pct_symbol_details[found_token].append({
                    'symbol': symbol,
                    'row': row,
                    'pct_down_col': PCT_DOWN_RESULT_COL,
                    'exchange_type': get_exchange_type_num(exchange),
                    'confirm_col': PCT_DOWN_CONFIRM_COL # Add this for consistency
                })
            logger.debug(f"Added {symbol} (Token: {found_token}) to relevant details dictionary. Block: {block_type}")
        else:
            # If duplicate, update the previous_day_close if it was just fetched
            if block_type == "Focus List" and previous_day_close is not None:
                for entry in excel_symbol_details[found_token]:
                    if entry['row'] == row and entry['block_type'] == block_type:
                        entry['previous_day_close'] = previous_day_close
                        logger.debug(f"Updated previous_day_close for existing entry {symbol} (Token: {found_token}) in row {row} ({block_type}).")
                        break
            logger.debug(f"Skipping duplicate add for {symbol} (Token: {found_token}) in row {row} ({block_type}).")
    else:
        logger.error(f"Token not found for {symbol} in row {row} ({block_type}). This symbol will be skipped for updates.")


def scan_excel_for_symbols_and_tokens():
    """
    Scans Google Sheet for symbols and tokens for ORH, Full Positions, Indices, Quarter Positions,
    and 3% Down setups.
    Populates `excel_symbol_details` and `excel_3pct_symbol_details`.
    Returns a combined list of unique (token, exchange_type_int) tuples to subscribe to.
    """
    global excel_symbol_details, excel_3pct_symbol_details, Dashboard, Credential

    # Store old details to identify removed symbols for clearing Google Sheet
    old_excel_symbol_details = collections.defaultdict(list, excel_symbol_details)
    old_excel_3pct_symbol_details = collections.defaultdict(list, excel_3pct_symbol_details)

    excel_symbol_details.clear() # Clear existing data
    excel_3pct_symbol_details.clear()
    
    all_tokens_to_subscribe_set = set() # Use a set to store (token, exchange_type_int) for uniqueness

    logger.info("Scanning Google Sheet for symbols and updating tokens...")

    try:
        # Determine the maximum row to scan across all relevant columns
        last_row_orh = get_last_row_in_column(Dashboard, SYMBOL_COL_ORH)
        last_row_full_pos = get_last_row_in_column(Dashboard, FULL_SYMBOL_COL)
        last_row_indices = get_last_row_in_column(Dashboard, INDEX_SYMBOL_COL)
        last_row_quarter_pos = get_last_row_in_column(Dashboard, QUARTER_SYMBOL_COL)
        last_row_3pct = get_last_row_in_column(Dashboard, SYMBOL_COL_3PCT) # Note: SYMBOL_COL_3PCT is 'N'

        max_row_to_scan = max(last_row_orh, last_row_full_pos, last_row_indices, last_row_quarter_pos, last_row_3pct)
        # Also consider the max row in Credential sheet for tokens
        max_row_credential_f = get_last_row_in_column(Credential, TOKEN_COL_ORH)
        max_row_credential_g = get_last_row_in_column(Credential, TOKEN_COL_3PCT)
        max_row_to_scan = max(max_row_to_scan, max_row_credential_f, max_row_credential_g)

    except Exception as e:
        logger.error(f"Error determining last row in Google Sheets: {e}")
        return [] # Return empty if unable to determine scan range

    if max_row_to_scan < START_ROW_DATA:
        logger.warning(f"No symbols found in Google Sheet from row {START_ROW_DATA} onwards.")
        return []

    # Pre-fetch all values from relevant columns for efficiency
    all_dashboard_values = Dashboard.get_all_values()
    all_credential_values = Credential.get_all_values()

    end_row_focus_list = INDEX_START_ROW - 1
    end_row_full_positions = QUARTER_POSITIONS_START_ROW - 1

    def get_cell_value(sheet_values, row_idx, col_letter):
        col_idx = col_letter_to_index(col_letter) - 1
        if 0 <= row_idx - 1 < len(sheet_values) and 0 <= col_idx < len(sheet_values[row_idx - 1]):
            return sheet_values[row_idx - 1][col_idx]
        return None

    logger.debug(f"Processing Focus List from row {START_ROW_DATA} to {end_row_focus_list}")
    for i in range(START_ROW_DATA, end_row_focus_list + 1):
        exchange_from_excel_raw = get_cell_value(all_dashboard_values, i, EXCHANGE_COL_ORH)
        symbol_from_excel_raw = get_cell_value(all_dashboard_values, i, SYMBOL_COL_ORH)

        exchange_from_excel = str(exchange_from_excel_raw).strip() if exchange_from_excel_raw is not None else ""
        symbol_from_excel = str(symbol_from_excel_raw).strip() if symbol_from_excel_raw is not None else ""

        if exchange_from_excel and symbol_from_excel:
            process_symbol_entry(
                row=i,
                exchange=exchange_from_excel,
                symbol=symbol_from_excel,
                ltp_col=INDEX_LTP_COL, # Focus list uses D
                chg_col=INDEX_CHG_COL, # Focus list uses E
                cred_token_col=TOKEN_COL_ORH, # Focus list uses F
                block_type="Focus List",
                price_col="", qty_col="", swing_low_col="",
                return_amt_col="", return_pct_col="", move_pct_col="",
                trailing_stop_col="", percent_from_stop_col="",
                entry_date_col="", days_duration_col="",
                all_credential_values=all_credential_values
            )
            # Add to subscription set
            token_val = get_cell_value(all_credential_values, i, TOKEN_COL_ORH)
            if token_val:
                try:
                    token_int = int(float(token_val))
                    exchange_type_int = get_exchange_type_num(exchange_from_excel)
                    if exchange_type_int is not None:
                        all_tokens_to_subscribe_set.add((str(token_int), exchange_type_int))
                except ValueError:
                    logger.warning(f"Invalid token '{token_val}' for {symbol_from_excel} in row {i}.")


    logger.debug(f"Processing Full Positions from row {START_ROW_DATA} to {end_row_full_positions}")
    for i in range(START_ROW_DATA, end_row_full_positions + 1):
        exchange_from_excel_raw = get_cell_value(all_dashboard_values, i, FULL_EXCHANGE_COL)
        symbol_from_excel_raw = get_cell_value(all_dashboard_values, i, FULL_SYMBOL_COL)

        exchange_from_excel = str(exchange_from_excel_raw).strip() if exchange_from_excel_raw is not None else ""
        symbol_from_excel = str(symbol_from_excel_raw).strip() if symbol_from_excel_raw is not None else ""

        if exchange_from_excel and symbol_from_excel:
            process_symbol_entry(
                row=i,
                exchange=exchange_from_excel,
                symbol=symbol_from_excel,
                ltp_col=FULL_LTP_COL,
                chg_col="", # No %Change for Full Positions in this sheet
                cred_token_col=FULL_CRED_TOKEN_COL,
                block_type="Full Positions",
                price_col=FULL_PRICE_COL,
                qty_col=FULL_QTY_COL,
                swing_low_col=FULL_SWING_LOW_COL,
                return_amt_col=FULL_RETURN_AMT_COL,
                return_pct_col=FULL_RETURN_PCT_COL,
                move_pct_col=FULL_MOVE_PCT_COL,
                trailing_stop_col=FULL_TRAILING_STOP_COL,
                percent_from_stop_col=FULL_PERCENT_FROM_STOP_COL,
                entry_date_col=FULL_ENTRY_DATE_COL,
                days_duration_col=FULL_DAYS_DURATION_COL,
                all_credential_values=all_credential_values
            )
            # Add to subscription set
            token_val = get_cell_value(all_credential_values, i, FULL_CRED_TOKEN_COL)
            if token_val:
                try:
                    token_int = int(float(token_val))
                    exchange_type_int = get_exchange_type_num(exchange_from_excel)
                    if exchange_type_int is not None:
                        all_tokens_to_subscribe_set.add((str(token_int), exchange_type_int))
                except ValueError:
                    logger.warning(f"Invalid token '{token_val}' for {symbol_from_excel} in row {i}.")


    logger.debug(f"Processing Indices from row {INDEX_START_ROW} to {max_row_to_scan}")
    for i in range(INDEX_START_ROW, max_row_to_scan + 1):
        exchange_from_excel_raw = get_cell_value(all_dashboard_values, i, INDEX_EXCHANGE_COL)
        symbol_from_excel_raw = get_cell_value(all_dashboard_values, i, INDEX_SYMBOL_COL)

        exchange_from_excel = str(exchange_from_excel_raw).strip() if exchange_from_excel_raw is not None else ""
        symbol_from_excel = str(symbol_from_excel_raw).strip() if symbol_from_excel_raw is not None else ""

        if exchange_from_excel and symbol_from_excel:
            process_symbol_entry(
                row=i,
                exchange=exchange_from_excel,
                symbol=symbol_from_excel,
                ltp_col=INDEX_LTP_COL,
                chg_col=INDEX_CHG_COL,
                cred_token_col=INDEX_CRED_TOKEN_COL,
                block_type="Indices",
                price_col="", qty_col="", swing_low_col="",
                return_amt_col="", return_pct_col="", move_pct_col="",
                trailing_stop_col="", percent_from_stop_col="",
                entry_date_col="", days_duration_col="",
                all_credential_values=all_credential_values
            )
            # Add to subscription set
            token_val = get_cell_value(all_credential_values, i, INDEX_CRED_TOKEN_COL)
            if token_val:
                try:
                    token_int = int(float(token_val))
                    exchange_type_int = get_exchange_type_num(exchange_from_excel)
                    if exchange_type_int is not None:
                        all_tokens_to_subscribe_set.add((str(token_int), exchange_type_int))
                except ValueError:
                    logger.warning(f"Invalid token '{token_val}' for {symbol_from_excel} in row {i}.")

    logger.debug(f"Processing Quarter Positions from row {QUARTER_POSITIONS_START_ROW} to {max_row_to_scan}")
    for i in range(QUARTER_POSITIONS_START_ROW, max_row_to_scan + 1):
        exchange_from_excel_raw = get_cell_value(all_dashboard_values, i, QUARTER_EXCHANGE_COL)
        symbol_from_excel_raw = get_cell_value(all_dashboard_values, i, QUARTER_SYMBOL_COL)

        exchange_from_excel = str(exchange_from_excel_raw).strip() if exchange_from_excel_raw is not None else ""
        symbol_from_excel = str(symbol_from_excel_raw).strip() if symbol_from_excel_raw is not None else ""

        if exchange_from_excel and symbol_from_excel:
            process_symbol_entry(
                row=i,
                exchange=exchange_from_excel,
                symbol=symbol_from_excel,
                ltp_col=QUARTER_LTP_COL,
                chg_col=QUARTER_CHG_COL,
                cred_token_col=QUARTER_CRED_TOKEN_COL,
                block_type="Quarter Positions",
                price_col="", qty_col="", swing_low_col="",
                return_amt_col="", return_pct_col="", move_pct_col="",
                trailing_stop_col="", percent_from_stop_col="",
                entry_date_col="", days_duration_col="",
                all_credential_values=all_credential_values
            )
            # Add to subscription set
            token_val = get_cell_value(all_credential_values, i, QUARTER_CRED_TOKEN_COL)
            if token_val:
                try:
                    token_int = int(float(token_val))
                    exchange_type_int = get_exchange_type_num(exchange_from_excel)
                    if exchange_type_int is not None:
                        all_tokens_to_subscribe_set.add((str(token_int), exchange_type_int))
                except ValueError:
                    logger.warning(f"Invalid token '{token_val}' for {symbol_from_excel} in row {i}.")

    logger.debug(f"Processing 3% Down symbols from row {START_ROW_DATA} to {max_row_to_scan}")
    for i in range(START_ROW_DATA, max_row_to_scan + 1):
        exchange_from_excel_raw = get_cell_value(all_dashboard_values, i, EXCHANGE_COL_3PCT)
        symbol_from_excel_raw = get_cell_value(all_dashboard_values, i, SYMBOL_COL_3PCT)

        exchange_from_excel = str(exchange_from_excel_raw).strip() if exchange_from_excel_raw is not None else ""
        symbol_from_excel = str(symbol_from_excel_raw).strip() if symbol_from_excel_raw is not None else ""

        if exchange_from_excel and symbol_from_excel:
            process_symbol_entry(
                row=i,
                exchange=exchange_from_excel,
                symbol=symbol_from_excel,
                ltp_col="", chg_col="", # Not directly used for 3% down LTP display
                cred_token_col=TOKEN_COL_3PCT,
                block_type="3% Down",
                price_col="", qty_col="", swing_low_col="",
                return_amt_col="", return_pct_col="", move_pct_col="",
                trailing_stop_col="", percent_from_stop_col="",
                entry_date_col="", days_duration_col="",
                all_credential_values=all_credential_values
            )
            # Add to subscription set
            token_val = get_cell_value(all_credential_values, i, TOKEN_COL_3PCT)
            if token_val:
                try:
                    token_int = int(float(token_val))
                    exchange_type_int = get_exchange_type_num(exchange_from_excel)
                    if exchange_type_int is not None:
                        all_tokens_to_subscribe_set.add((str(token_int), exchange_type_int))
                except ValueError:
                    logger.warning(f"Invalid token '{token_val}' for {symbol_from_excel} in row {i}.")

    logger.info(f"Scanned {len(excel_symbol_details)} ORH/General symbols and {len(excel_3pct_symbol_details)} 3% Down symbols.")

    # Identify and clear Google Sheet data for symbols that were removed by the user
    clear_requests = []
    # Check `excel_symbol_details` (ORH, Full Positions, Indices, Quarter Positions)
    for token, list_of_old_details in old_excel_symbol_details.items():
        if token not in excel_symbol_details:
            for details in list_of_old_details:
                row_num = details['row']
                # Helper to create a range for a single cell
                def create_cell_range(sheet_name, col_letter, row_num):
                    return {
                        "sheetId": gsheet.worksheet(sheet_name).id,
                        "startRowIndex": row_num - 1,
                        "endRowIndex": row_num,
                        "startColumnIndex": col_letter_to_index(col_letter) - 1,
                        "endColumnIndex": col_letter_to_index(col_letter)
                    }

                try:
                    cols_to_clear_dashboard = []
                    if details.get("ltp_col"): cols_to_clear_dashboard.append(details["ltp_col"])
                    if details.get("chg_col"): cols_to_clear_dashboard.append(details["chg_col"])
                    if details.get("return_amt_col"): cols_to_clear_dashboard.append(details["return_amt_col"])
                    if details.get("return_pct_col"): cols_to_clear_dashboard.append(details["return_pct_col"])
                    if details.get("move_pct_col"): cols_to_clear_dashboard.append(details["move_pct_col"])
                    if details.get("percent_from_stop_col"): cols_to_clear_dashboard.append(details["percent_from_stop_col"])
                    if details.get("days_duration_col"): cols_to_clear_dashboard.append(details["days_duration_col"])
                    if details.get("orh_col"): cols_to_clear_dashboard.append(details["orh_col"])
                    if details.get("orh_buy_stop_col"): cols_to_clear_dashboard.append(details["orh_buy_stop_col"])

                    for col_letter in cols_to_clear_dashboard:
                        cell_range = create_cell_range(DASHBOARD_SHEET_NAME, col_letter, row_num)
                        clear_requests.append({
                            "updateCells": {
                                "rows": [{"values": [{"userEnteredValue": {"stringValue": ""}}]}],
                                "fields": "userEnteredValue",
                                "range": cell_range
                            }
                        })
                        clear_requests.append({
                            "repeatCell": {
                                "range": cell_range,
                                "cell": {
                                    "userEnteredFormat": {
                                        "backgroundColor": rgb_to_float(None),
                                        "numberFormat": {"type": "NUMBER", "pattern": "General"}
                                    }
                                },
                                "fields": "userEnteredFormat.backgroundColor,userEnteredFormat.numberFormat"
                            }
                        })

                    if details.get("cred_col"):
                        cred_cell_range = create_cell_range(CREDENTIAL_SHEET_NAME, details["cred_col"], row_num)
                        clear_requests.append({
                            "updateCells": {
                                "rows": [{"values": [{"userEnteredValue": {"stringValue": ""}}]}],
                                "fields": "userEnteredValue",
                                "range": cred_cell_range
                            }
                        })
                    logger.info(f"Queued for clearing Google Sheet data for removed symbol: {details['symbol']} (Token: {token}, Row: {row_num}, Block: {details['block_type']})")
                except Exception as e:
                    logger.warning(f"Error preparing clear requests for removed symbol {details['symbol']} (Row: {row_num}): {e}")

    # Check `excel_3pct_symbol_details`
    for token, list_of_old_details in old_excel_3pct_symbol_details.items():
        if token not in excel_3pct_symbol_details:
            for details in list_of_old_details:
                row_num = details['row']
                def create_cell_range(sheet_name, col_letter, row_num):
                    return {
                        "sheetId": gsheet.worksheet(sheet_name).id,
                        "startRowIndex": row_num - 1,
                        "endRowIndex": row_num,
                        "startColumnIndex": col_letter_to_index(col_letter) - 1,
                        "endColumnIndex": col_letter_to_index(col_letter)
                    }
                try:
                    cols_to_clear_dashboard_3pct = []
                    if details.get("pct_down_col"): cols_to_clear_dashboard_3pct.append(details["pct_down_col"])
                    if details.get("confirm_col"): cols_to_clear_dashboard_3pct.append(details["confirm_col"])

                    for col_letter in cols_to_clear_dashboard_3pct:
                        cell_range = create_cell_range(DASHBOARD_SHEET_NAME, col_letter, row_num)
                        clear_requests.append({
                            "updateCells": {
                                "rows": [{"values": [{"userEnteredValue": {"stringValue": ""}}]}],
                                "fields": "userEnteredValue",
                                "range": cell_range
                            }
                        })
                        clear_requests.append({
                            "repeatCell": {
                                "range": cell_range,
                                "cell": {
                                    "userEnteredFormat": {
                                        "backgroundColor": rgb_to_float(None),
                                        "numberFormat": {"type": "NUMBER", "pattern": "General"}
                                    }
                                },
                                "fields": "userEnteredFormat.backgroundColor,userEnteredFormat.numberFormat"
                            }
                        })
                    logger.info(f"Queued for clearing Google Sheet data for removed 3% Down symbol: {details['symbol']} (Token: {token}, Row: {row_num})")
                except Exception as e:
                    logger.warning(f"Error preparing clear requests for removed 3% Down symbol {details['symbol']} (Row: {row_num}): {e}")


    if clear_requests:
        try:
            gsheet.batch_update({'requests': clear_requests})
            logger.info(f"Executed {len(clear_requests)} clear operations on Google Sheet.")
        except Exception as e:
            logger.error(f"Failed to clear cells in Google Sheet: {e}")

    logger.info(f"Finished scanning. Found {len(all_tokens_to_subscribe_set)} unique tokens for subscription.")
    return list(all_tokens_to_subscribe_set) # Return as list of (token, exchange_type_int) tuples


def fetch_initial_candle_data(smart_api_obj, symbols_to_fetch):
    """
    Fetches historical 3-min candle data for today to pre-populate candles for ORH.
    Includes retry logic for API errors and delays between requests.
    """
    logger.info("Fetching initial historical 3-min candle data for today (ORH setup)...")

    now = datetime.now()

    # Guard clause: Prevent request before 09:16 AM or outside market hours
    if not is_market_hours() or now.time() < datetime_time(9, 16): # Use aliased datetime_time
        logger.warning("Outside market hours or before 09:16 AM. Skipping initial ORH candle fetch.")
        return

    from_date = now.replace(hour=9, minute=15, second=0, microsecond=0).strftime("%Y-%m-%d %H:%M")
    to_date = now.strftime("%Y-%m-%d %H:%M")

    MAX_RETRIES = 5
    RETRY_DELAY_SECONDS = 20

    for token, entries in symbols_to_fetch.items():
        # Only process if this token is actually for ORH setup (i.e., in excel_symbol_details and has ORH specific columns)
        is_orh_symbol = False
        for entry in entries:
            if entry.get('orh_col'): # Check if it has an ORH result column
                is_orh_symbol = True
                break
        if not is_orh_symbol:
            continue # Skip if not an ORH symbol

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
                        candle_time = datetime.fromisoformat(c[0])
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
        time.sleep(1) # Small delay between token fetches


def fetch_previous_day_candle_data(smart_api_obj, symbols_to_fetch):
    """
    Fetches the previous day's ONE_DAY candle data for ORH setup.
    Includes retry logic for API errors and delays between requests.
    Prioritizes fetching from local cache.
    """
    logger.info("Fetching previous day's candle data (ORH setup)...")

    # Guard clause: Prevent request outside market hours
    if not is_market_hours():
        logger.warning("Outside market hours. Skipping previous day ORH candle fetch.")
        return

    # 🗓️ Get last valid trading day (skip weekends)
    yesterday = date.today()
    while yesterday.weekday() >= 5:  # 5 = Saturday, 6 = Sunday
        yesterday -= timedelta(days=1)

    # 🕒 Use full day time range for ONE_DAY interval
    # Corrected: Use datetime.combine as a method of the datetime class
    from_dt = datetime.combine(yesterday, datetime_time.min) # Use aliased datetime_time
    to_dt   = datetime.combine(yesterday, datetime_time.max) # Use aliased datetime_time

    if from_dt >= to_dt:
        logger.warning("Invalid date range: from_date >= to_date for previous day candle. Skipping fetch.")
        return

    from_date = from_dt.strftime("%Y-%m-%d %H:%M")
    to_date = to_dt.strftime("%Y-%m-%d %H:%M")
    yesterday_str = yesterday.strftime("%Y-%m-%d")

    MAX_RETRIES = 5
    RETRY_DELAY_SECONDS = 30

    for token, entries in symbols_to_fetch.items():
        # Only process if this token is actually for ORH setup (i.e., in excel_symbol_details and has ORH specific columns)
        is_orh_symbol = False
        for entry in entries:
            if entry.get('orh_col'): # Check if it has an ORH result column
                is_orh_symbol = True
                break
        if not is_orh_symbol:
            continue # Skip if not an ORH symbol

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


def fetch_historical_candles_for_3pct_down(smart_api_obj, tokens_to_fetch, interval_api, from_dt, to_dt):
    """
    Fetches at least 10 historical candles for 3% down setup.
    Updates global latest_completed_candles_3pct dictionary.
    Logs last 10 candle volumes.
    `tokens_to_fetch` is a list of (token_str, exchange_type_int) tuples.
    """
    logger.info(f"Fetching historical {interval_api} candles for 3% down setup from {from_dt.strftime('%Y-%m-%d %H:%M')} to {to_dt.strftime('%Y-%m-%d %H:%M')}...")

    MAX_RETRIES = 3
    RETRY_DELAY_SECONDS = 15
    TARGET_CANDLES = 10

    # Use the globally defined interval_minutes_map
    minutes_back = interval_minutes_map.get(interval_api, 15) * TARGET_CANDLES
    adjusted_from_dt = to_dt - timedelta(minutes=minutes_back)
    
    from_date_str = adjusted_from_dt.strftime("%Y-%m-%d %H:%M")
    to_date_str = to_dt.strftime("%Y-%m-%d %H:%M")

    for token, exchange_type in tokens_to_fetch:
        symbol_name = "Unknown"
        if token in excel_3pct_symbol_details and excel_3pct_symbol_details[token]:
            symbol_name = excel_3pct_symbol_details[token][0]['symbol']
        elif token in excel_symbol_details and excel_symbol_details[token]:
            symbol_name = excel_symbol_details[token][0]['symbol']

        exchange_map = {1: "NSE", 3: "BSE"}
        exchange_str = exchange_map.get(exchange_type)
        if not exchange_str:
            logger.warning(f"Cannot fetch history for token {token}, unknown exchange type {exchange_type}")
            time.sleep(1)
            continue

        candle_data = []
        fetch_attempts = 0

        while len(candle_data) < TARGET_CANDLES and fetch_attempts < MAX_RETRIES:
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
                        # Go further back in time if not enough candles
                        adjusted_from_dt -= timedelta(minutes=minutes_back)
                        from_date_str = adjusted_from_dt.strftime("%Y-%m-%d %H:%M")
                        fetch_attempts += 1
                        continue
                    else:
                        break # Enough candles fetched
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

            latest_candle_raw = candle_data[-1]
            completed_candle = {
                'start_time': datetime.fromisoformat(latest_candle_raw[0]),
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
        else:
            logger.warning(f"❌ Not enough candle data for {symbol_name} (Token: {token}) even after multiple attempts.")

        time.sleep(1) # Small delay between token fetches

def check_and_update_orh_setup():
    """Checks the latest completed 3-min candle for ORH setup and updates Google Sheet accordingly."""
    logger.info("🔍 Checking latest 3-min candle for ORH setup...")

    google_sheet_updates_queued = []

    # Pre-fetch current values for ORH_RESULT_COL and ORH_BUY_STOP_COL
    orh_result_col_values = Dashboard.col_values(col_letter_to_index(ORH_RESULT_COL))
    orh_buy_stop_col_values = Dashboard.col_values(col_letter_to_index(ORH_BUY_STOP_COL))

    for token, symbol_entries in excel_symbol_details.items():
        # Only process if this token is actually for ORH setup (i.e., has an ORH result column)
        is_orh_symbol = False
        for entry in symbol_entries:
            if entry.get('orh_col'): # Check if it has an ORH result column
                is_orh_symbol = True
                break
        if not is_orh_symbol:
            continue # Skip if not an ORH symbol

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

            if high != low: # Avoid division by zero if high and low are the same
                bullish_threshold = low + 0.7 * (high - low)
                if close >= bullish_threshold and close > prev_high:
                    orh_result = "Yes"
                    trigger_time_str = latest_candle['start_time'].strftime('%H:%M')
                    buy_stop_value = round(low * 0.995, 2) # 0.5% below low for buy stop

                    candle_time_full = latest_candle['start_time'].strftime('%Y-%m-%d %H:%M:%S')
                    log_msg = (
                        f"🚨 ORH Triggered for {symbol_entries[0]['symbol']} at {candle_time_full} ➤ "
                        f"Open: {latest_candle['open']}, High: {high}, Low: {low}, "
                        f"Close: {close}, Buy Stop: {buy_stop_value}"
                    )
                    logger.info(log_msg)

                    if winsound:
                        try:
                            winsound.Beep(1000, 400) # Play a sound alert
                        except Exception as e:
                            logger.warning(f"🔇 Sound alert failed: {e}")

        # Update Google Sheet for all entries associated with this token that are ORH type
        for entry in symbol_entries:
            if entry.get('orh_col'): # Ensure it's an ORH relevant entry
                row = entry["row"]
                row_idx = row - 1 # 0-indexed for list access
                
                # ORH Result Column (G)
                current_value_orh = orh_result_col_values[row_idx] if row_idx < len(orh_result_col_values) else ""
                new_value_orh_col = f"Yes({trigger_time_str})" if orh_result == "Yes" else "No"
                if str(current_value_orh).strip() != new_value_orh_col.strip():
                    google_sheet_updates_queued.append({
                        "row": row,
                        "column": ORH_RESULT_COL,
                        "value": new_value_orh_col
                    })
                
                # Buy Stop Column (H)
                current_value_buy_stop = orh_buy_stop_col_values[row_idx] if row_idx < len(orh_buy_stop_col_values) else ""
                new_value_buy_stop_col = buy_stop_value if buy_stop_value else ""
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

                if open_price > 0:
                    percentage_drop = (open_price - close_price) / open_price
                    if percentage_drop >= 0.03:
                        triggered_intervals_pct.append(CANDLE_INTERVAL_MAP_DISPLAY.get(interval_api, interval_api))

                        log_msg = (
                            f"📉 3% Down Triggered for {symbol_name} ({CANDLE_INTERVAL_MAP_DISPLAY.get(interval_api)}) "
                            f"➤ Drop={percentage_drop:.2%}"
                        )
                        logger.info(log_msg)

                        # ✅ Phase 1: Check High Volume vs. last 10
                        if isinstance(volume, (int, float)):
                            vol_list = volume_history_3pct[token][interval_api]
                            vol_list.append(volume)
                            if len(vol_list) > 10:
                                vol_list.pop(0)

                            # Check if current volume is the highest among the last 10 (excluding itself)
                            # Or if list has less than 10, it's considered high if it's the highest so far
                            if len(vol_list) == 1 or all(volume > v for v in vol_list[:-1]):
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
            new_value = ", ".join(triggered_intervals_pct) if triggered_intervals_pct else "" # If no triggers, clear the cell

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

            # Filter out old "high volume" parts and keep "Yes for" parts
            parts_to_keep_from_existing = [part for part in existing_ad.split(", ") if part.strip().startswith("Yes for")]
            
            if triggered_intervals_volume:
                # Combine existing "Yes for" parts with new "high volume" triggers
                final_ad_value_parts = parts_to_keep_from_existing + triggered_intervals_volume
                full_updated_value = ", ".join(sorted(list(set(final_ad_value_parts)))) # Deduplicate and sort
                
                google_sheet_updates_queued_volume.append({
                    "row": row,
                    "column": col_ad,
                    "value": full_updated_value
                })
            else:
                # If no new high volume triggers, but there are existing "Yes for" confirmations, preserve them.
                # If there are only "high volume" parts and no new triggers, clear them.
                if parts_to_keep_from_existing and str(existing_ad).strip() != ", ".join(parts_to_keep_from_existing).strip():
                     google_sheet_updates_queued_volume.append({
                        "row": row,
                        "column": col_ad,
                        "value": ", ".join(parts_to_keep_from_existing)
                    })
                elif not parts_to_keep_from_existing and existing_ad.strip() != "":
                    # If no "Yes for" parts and existing value is not empty, clear it
                    google_sheet_updates_queued_volume.append({
                        "row": row,
                        "column": col_ad,
                        "value": ""
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
    """
    Checks for confirmation of high volume 3% down setups by fetching the next 15-min candle.
    Updates Column AD in Google Sheet with "Yes for Xmin" if confirmed.
    """
    logger.info("🔄 Checking 15-min confirmation for high volume 3% down setups...")
    google_sheet_updates_queued_confirm = []

    # Pre-fetch current values for PCT_DOWN_CONFIRM_COL
    pct_down_confirm_col_values = Dashboard.col_values(col_letter_to_index(PCT_DOWN_CONFIRM_COL))

    for token, interval_dict in latest_completed_candles_3pct.items():
        if 'confirmed_intervals' not in interval_dict:
            continue

        confirmed_intervals_for_token = interval_dict['confirmed_intervals']
        
        # Get existing AD column value to merge with new confirmations
        current_ad_value = ""
        if token in excel_3pct_symbol_details and excel_3pct_symbol_details[token]:
            row = excel_3pct_symbol_details[token][0]['row']
            row_idx = row - 1
            current_ad_value = pct_down_confirm_col_values[row_idx] if row_idx < len(pct_down_confirm_col_values) else ""
        
        # Parse existing parts to separate "high volume" and "Yes for"
        existing_parts = [part.strip() for part in current_ad_value.split(", ") if part.strip()]
        existing_high_volume_parts = [part for part in existing_parts if part.startswith("high volume")]
        existing_yes_for_parts = [part for part in existing_parts if part.startswith("Yes for")]

        new_yes_for_parts = [] # To accumulate new "Yes for Xmin" confirmations

        for interval_api, details in list(confirmed_intervals_for_token.items()): # Iterate over a copy
            candle_low = details['low']
            candle_time = details['time']
            symbol = excel_3pct_symbol_details.get(token, [{}])[0].get('symbol', 'Unknown')

            # Determine the time for the next 15-min candle
            # The candle_time is the start_time of the high volume candle.
            # We need to check the close of the candle that *starts* after this one.
            # So, if original candle is 9:15-9:30, next candle starts at 9:30.
            # We need to fetch from 9:30 to 9:45.
            from_dt = candle_time + timedelta(minutes=interval_minutes_map.get(interval_api, 15))
            to_dt = from_dt + timedelta(minutes=15) # Check the next 15-min candle

            # Ensure we are not trying to fetch future data
            if from_dt > datetime.now():
                logger.debug(f"Skipping future confirmation check for {symbol} ({CANDLE_INTERVAL_MAP_DISPLAY.get(interval_api)}). From: {from_dt.strftime('%H:%M')}")
                continue

            try:
                exchange_type = excel_3pct_symbol_details[token][0]['exchange_type']
                exchange_map = {1: "NSE", 3: "BSE"}
                exchange_str = exchange_map.get(exchange_type)

                if not exchange_str:
                    logger.warning(f"Cannot perform confirmation check for token {token}, unknown exchange type {exchange_type}")
                    continue

                historic_param = {
                    "exchange": exchange_str,
                    "symboltoken": token,
                    "interval": "FIFTEEN_MINUTE", # Always check next 15-min candle for confirmation
                    "fromdate": from_dt.strftime("%Y-%m-%d %H:%M"),
                    "todate": to_dt.strftime("%Y-%m-%d %H:%M")
                }
                
                response = smart_api_obj.getCandleData(historic_param)
                if response.get("status") and response.get("data"):
                    if response["data"]: # Ensure data is not empty
                        # Get the close price of the *last* candle in the response (most recent)
                        close_price = response["data"][-1][4] 
                        if close_price < candle_low:
                            confirmation_text = f"Yes for {CANDLE_INTERVAL_MAP_DISPLAY.get(interval_api)}"
                            new_yes_for_parts.append(confirmation_text)
                            logger.info(f"✅ Confirmation: {symbol} → {confirmation_text}")
                            # Remove from confirmed_intervals once confirmed to avoid re-checking
                            del confirmed_intervals_for_token[interval_api]
                        else:
                             logger.info(f"ℹ️ No confirmation for {symbol} ({CANDLE_INTERVAL_MAP_DISPLAY.get(interval_api)}): Close ({close_price}) not below low ({candle_low}).")
                    else:
                        logger.warning(f"No next 15-min candle data for confirmation for {symbol} for interval {CANDLE_INTERVAL_MAP_DISPLAY.get(interval_api)}.")

            except Exception as e:
                logger.warning(f"Confirmation check failed for {token} ({symbol}, {CANDLE_INTERVAL_MAP_DISPLAY.get(interval_api)}): {e}")
                continue
        
        # Update the confirmed_intervals in the global dictionary
        latest_completed_candles_3pct[token]['confirmed_intervals'] = confirmed_intervals_for_token

        # Combine existing high volume parts with new and existing "Yes for" confirmations
        final_ad_value_parts = existing_high_volume_parts + existing_yes_for_parts + new_yes_for_parts
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


def update_excel_live_data():
    """
    Updates the Google Sheet 'Dashboard' with live LTP, %Change, Return Amount,
    Return %, and %Move data from the `latest_tick_data` cache.
    Applies cell color based on value changes (green for increase/positive, red for
    decrease/negative), and resets to normal (no fill) if the value remains unchanged for LTP and %Change.
    Applies static color (green for positive, red for negative, white for zero) for
    Return Amount, Return %, %Move, and % From Stop.
    Calculates and updates Days Duration based on Entry Date.
    Uses batch updates for efficiency.
    """
    global latest_tick_data, Dashboard, previous_ltp_data, previous_percentage_change_data, excel_symbol_details

    if not smart_ws or not smart_ws._is_connected_flag:
        logger.warning("WebSocket is not connected. Skipping Google Sheet update for this cycle.")
        return

    requests = [] # List to hold all batch update requests

    # Define colors using the specified RGB values
    GREEN_COLOR = (149, 203, 186)
    RED_COLOR = (254, 87, 87)
    # WHITE_COLOR is implicitly handled by rgb_to_float(None)

    if not excel_symbol_details:
        logger.info("excel_symbol_details is empty. No symbols to update in Google Sheet.")
        return # Exit if no symbols to update

    # --- Optimize reading of Full Positions input data ---
    # Collect all unique rows and column letters for Full Positions input data
    full_pos_input_ranges = []
    for token, list_of_details in excel_symbol_details.items():
        for details in list_of_details:
            if details['block_type'] == "Full Positions":
                row_num = details['row']
                # Add all relevant columns for this row as A1 notations
                if details["price_col"]: full_pos_input_ranges.append(f'{details["price_col"]}{row_num}')
                if details["qty_col"]: full_pos_input_ranges.append(f'{details["qty_col"]}{row_num}')
                if details["swing_low_col"]: full_pos_input_ranges.append(f'{details["swing_low_col"]}{row_num}')
                if details["trailing_stop_col"]: full_pos_input_ranges.append(f'{details["trailing_stop_col"]}{row_num}')
                if details["entry_date_col"]: full_pos_input_ranges.append(f'{details["entry_date_col"]}{row_num}')

    # Fetch all values for these ranges in one go
    full_pos_input_data = {}
    if full_pos_input_ranges:
        try:
            all_fetched_values = Dashboard.batch_get(full_pos_input_ranges)
            for i, a1_notation in enumerate(full_pos_input_ranges):
                full_pos_input_data[a1_notation] = all_fetched_values[i][0][0] if all_fetched_values[i] and all_fetched_values[i][0] else None
            logger.debug(f"Fetched {len(full_pos_input_data)} Full Positions input cells in batch.")
        except Exception as e:
            logger.error(f"Error fetching Full Positions input data in batch: {e}. Proceeding with individual fetches if needed.")
            full_pos_input_data = {}


    for token, list_of_details in excel_symbol_details.items():
        if token in latest_tick_data:
            ltp_info = latest_tick_data[token]
            current_ltp = ltp_info['ltp']

            previous_ltp = previous_ltp_data.get(token)

            # Get previous_day_close from excel_symbol_details
            previous_day_close = None
            # Assuming all entries for a token have the same previous_day_close for a given day
            if list_of_details:
                # Find the first entry that has 'previous_day_close' (typically 'Focus List')
                for detail_entry in list_of_details:
                    if detail_entry.get('previous_day_close') is not None:
                        previous_day_close = detail_entry.get('previous_day_close')
                        break
                symbol_name = list_of_details[0].get('symbol', token) # Get symbol name for logging


            current_percentage_change = None
            # Only calculate percentage change using PDC if it's for Focus List and PDC is available
            # Note: A token might appear in multiple blocks (e.g., Focus List and Full Positions)
            # The %Change calculation is specific to Focus List.
            is_focus_list_token = any(d['block_type'] == "Focus List" for d in list_of_details)

            if is_focus_list_token:
                if previous_day_close is not None and previous_day_close != 0 and current_ltp is not None and current_ltp != 0:
                    current_percentage_change = (current_ltp - previous_day_close) / current_ltp
                    logger.debug(f"PDC Calc for {symbol_name} (Token: {token}): LTP={current_ltp}, PDC={previous_day_close}")
                    logger.debug(f"PDC Calc for {symbol_name} (Token: {token}): (LTP - PDC) / LTP = {current_percentage_change}")
                    logger.info(f"Calculated %Change for {symbol_name} (Token: {token}): {current_percentage_change:.4f}")
                else:
                    logger.warning(f"Previous day close or current LTP not available or zero for {symbol_name} (Token: {token}). Cannot calculate percentage change for Focus List.")
            # For other blocks, current_percentage_change remains None, which will clear the cell if chg_col exists


            ltp_cell_color = None
            if previous_ltp is not None:
                if current_ltp > previous_ltp:
                    ltp_cell_color = GREEN_COLOR
                elif current_ltp < previous_ltp:
                    ltp_cell_color = RED_COLOR
                else:
                    ltp_cell_color = None

            previous_ltp_data[token] = current_ltp

            chg_cell_color = None
            if current_percentage_change is not None:
                if current_percentage_change > 0:
                    chg_cell_color = GREEN_COLOR
                elif current_percentage_change < 0:
                    chg_cell_color = RED_COLOR
                else:
                    chg_cell_color = None

            previous_percentage_change_data[token] = current_percentage_change


            for details in list_of_details:
                row_num = details['row']
                ltp_col = details['ltp_col']
                chg_col = details['chg_col']

                dashboard_sheet_id = Dashboard.id

                # LTP Update (Value)
                requests.append({
                    "updateCells": {
                        "rows": [{"values": [{"userEnteredValue": {"numberValue": current_ltp}}]}],
                        "fields": "userEnteredValue",
                        "range": create_cell_range_for_request(dashboard_sheet_id, ltp_col, row_num)
                    }
                })
                # LTP Update (Color)
                requests.append({
                    "repeatCell": {
                        "range": create_cell_range_for_request(dashboard_sheet_id, ltp_col, row_num),
                        "cell": {
                            "userEnteredFormat": {
                                "backgroundColor": rgb_to_float(ltp_cell_color)
                            }
                        },
                        "fields": "userEnteredFormat.backgroundColor"
                    }
                })

                # %Change for Focus List (Value + Format + Color)
                if chg_col and details['block_type'] == "Focus List": # Only for Focus List
                    if current_percentage_change is not None:
                        requests.append({
                            "updateCells": {
                                "rows": [{"values": [{"userEnteredValue": {"numberValue": current_percentage_change}}]}],
                                "fields": "userEnteredValue",
                                "range": create_cell_range_for_request(dashboard_sheet_id, chg_col, row_num)
                            }
                        })
                        requests.append({
                            "repeatCell": {
                                "range": create_cell_range_for_request(dashboard_sheet_id, chg_col, row_num),
                                "cell": {
                                    "userEnteredFormat": {
                                        "backgroundColor": rgb_to_float(chg_cell_color),
                                        "numberFormat": {
                                            "type": "NUMBER",
                                            "pattern": "0.00%"
                                        }
                                    }
                                },
                                "fields": "userEnteredFormat.backgroundColor,userEnteredFormat.numberFormat"
                            }
                        })
                        logger.debug(f"Queued for Google Sheet update: Token {token} (Row {row_num}, {details['block_type']}), LTP: {current_ltp}, %Chg: {current_percentage_change}")
                    else:
                        # If percentage change cannot be calculated for Focus List, clear the cell
                        cell_range = create_cell_range_for_request(dashboard_sheet_id, chg_col, row_num)
                        requests.append({
                            "updateCells": {
                                "rows": [{"values": [{"userEnteredValue": {"stringValue": ""}}]}],
                                "fields": "userEnteredValue",
                                "range": cell_range
                            }
                        })
                        requests.append({
                            "repeatCell": {
                                "range": cell_range,
                                "cell": {
                                    "userEnteredFormat": {
                                        "backgroundColor": rgb_to_float(None),
                                        "numberFormat": {"type": "NUMBER", "pattern": "General"}
                                    }
                                },
                                "fields": "userEnteredFormat.backgroundColor,userEnteredFormat.numberFormat"
                            }
                        })
                        logger.debug(f"Cleared %Change for Token {token} (Row {row_num}, {details['block_type']}) due to missing PDC.")
                elif chg_col and details['block_type'] != "Focus List": # For other blocks with chg_col, clear it
                     cell_range = create_cell_range_for_request(dashboard_sheet_id, chg_col, row_num)
                     requests.append({
                         "updateCells": {
                             "rows": [{"values": [{"userEnteredValue": {"stringValue": ""}}]}],
                             "fields": "userEnteredValue",
                             "range": cell_range
                         }
                     })
                     requests.append({
                         "repeatCell": {
                             "range": cell_range,
                             "cell": {
                                 "userEnteredFormat": {
                                     "backgroundColor": rgb_to_float(None),
                                     "numberFormat": {"type": "NUMBER", "pattern": "General"}
                                 }
                             },
                             "fields": "userEnteredFormat.backgroundColor,userEnteredFormat.numberFormat"
                         }
                     })
                     logger.debug(f"Cleared %Change for Token {token} (Row {row_num}, {details['block_type']}) as PDC is only for Focus List.")


                # Full Positions specific calculations and coloring
                if details['block_type'] == "Full Positions":
                    price_val = None
                    qty_val = None
                    swing_low_val = None
                    trailing_stop_val = None
                    entry_date_str = None

                    # Try to get from batch fetched data first
                    price_a1 = f'{details["price_col"]}{row_num}'
                    qty_a1 = f'{details["qty_col"]}{row_num}'
                    swing_low_a1 = f'{details["swing_low_col"]}{row_num}'
                    trailing_stop_a1 = f'{details["trailing_stop_col"]}{row_num}'
                    entry_date_a1 = f'{details["entry_date_col"]}{row_num}'

                    if price_a1 in full_pos_input_data:
                        price_val = full_pos_input_data[price_a1]
                    if qty_a1 in full_pos_input_data:
                        qty_val = full_pos_input_data[qty_a1]
                    if swing_low_a1 in full_pos_input_data:
                        swing_low_val = full_pos_input_data[swing_low_a1]
                    if trailing_stop_a1 in full_pos_input_data:
                        trailing_stop_val = full_pos_input_data[trailing_stop_a1]
                    if entry_date_a1 in full_pos_input_data:
                        entry_date_str = full_pos_input_data[entry_date_a1]


                    # Convert to float, handle empty strings
                    try:
                        price_val = float(price_val) if price_val else None
                        qty_val = float(qty_val) if qty_val else None
                        swing_low_val = float(swing_low_val) if swing_low_val else None
                        trailing_stop_val = float(trailing_stop_val) if trailing_stop_val else None
                    except ValueError as ve:
                        logger.warning(f"Could not convert input values to float for token {token} at row {row_num}: {ve}. Ensure cells contain valid numbers.")
                        price_val, qty_val, swing_low_val, trailing_stop_val = None, None, None, None


                    return_amt = None
                    return_pct = None
                    move_pct = None
                    percent_from_stop = None
                    days_duration = None

                    # Return Amount and Return Percentage
                    if isinstance(price_val, (int, float)) and isinstance(qty_val, (int, float)) and price_val != 0:
                        return_amt = (current_ltp - price_val) * qty_val
                        return_pct = (current_ltp - price_val) / price_val

                        current_return_amt_color = None
                        if return_amt is not None:
                            if return_amt > 0: current_return_amt_color = GREEN_COLOR
                            elif return_amt < 0: current_return_amt_color = RED_COLOR

                        current_return_pct_color = None
                        if return_pct is not None:
                            if return_pct > 0: current_return_pct_color = GREEN_COLOR
                            elif return_pct < 0: current_return_pct_color = RED_COLOR

                        # Return Amount Update
                        requests.append({
                            "updateCells": {
                                "rows": [{"values": [{"userEnteredValue": {"numberValue": return_amt}}]}],
                                "fields": "userEnteredValue",
                                "range": create_cell_range_for_request(dashboard_sheet_id, details["return_amt_col"], row_num)
                            }
                        })
                        requests.append({
                            "repeatCell": {
                                "range": create_cell_range_for_request(dashboard_sheet_id, details["return_amt_col"], row_num),
                                "cell": {
                                    "userEnteredFormat": {
                                        "backgroundColor": rgb_to_float(current_return_amt_color) if current_return_amt_color else rgb_to_float(None),
                                        "numberFormat": {
                                            "type": "NUMBER",
                                            "pattern": "#,##0.00"
                                        }
                                    }
                                },
                                "fields": "userEnteredFormat.backgroundColor,userEnteredFormat.numberFormat"
                            }
                        })

                        # Return Percentage Update
                        requests.append({
                            "updateCells": {
                                "rows": [{"values": [{"userEnteredValue": {"numberValue": return_pct}}]}],
                                "fields": "userEnteredValue",
                                "range": create_cell_range_for_request(dashboard_sheet_id, details["return_pct_col"], row_num)
                            }
                        })
                        requests.append({
                            "repeatCell": {
                                "range": create_cell_range_for_request(dashboard_sheet_id, details["return_pct_col"], row_num),
                                "cell": {
                                    "userEnteredFormat": {
                                        "backgroundColor": rgb_to_float(current_return_pct_color) if current_return_pct_color else rgb_to_float(None),
                                        "numberFormat": {
                                            "type": "NUMBER",
                                            "pattern": "0.00%"
                                        }
                                    }
                                },
                                "fields": "userEnteredFormat.backgroundColor,userEnteredFormat.numberFormat"
                            }
                        })
                    else:
                        # Clear values and colors if conditions not met
                        for col_letter in [details["return_amt_col"], details["return_pct_col"]]:
                            if col_letter:
                                cell_range = create_cell_range_for_request(dashboard_sheet_id, col_letter, row_num)
                                requests.append({
                                    "updateCells": {
                                        "rows": [{"values": [{"userEnteredValue": {"stringValue": ""}}]}],
                                        "fields": "userEnteredValue",
                                        "range": cell_range
                                    }
                                })
                                requests.append({
                                    "repeatCell": {
                                        "range": cell_range,
                                        "cell": {
                                            "userEnteredFormat": {
                                                "backgroundColor": rgb_to_float(None),
                                                "numberFormat": {"type": "NUMBER", "pattern": "General"}
                                            }
                                        },
                                        "fields": "userEnteredFormat.backgroundColor,userEnteredFormat.numberFormat"
                                    }
                                })


                    # %Move Calculation (Swing Low vs LTP)
                    if isinstance(swing_low_val, (int, float)) and swing_low_val != 0 and isinstance(current_ltp, (int, float)):
                        move_pct = (current_ltp - swing_low_val) / swing_low_val

                        current_move_pct_color = None
                        if move_pct is not None:
                            if move_pct > 0: current_move_pct_color = GREEN_COLOR
                            elif move_pct < 0: current_move_pct_color = RED_COLOR

                        # %Move Update
                        requests.append({
                            "updateCells": {
                                "rows": [{"values": [{"userEnteredValue": {"numberValue": move_pct}}]}],
                                "fields": "userEnteredValue",
                                "range": create_cell_range_for_request(dashboard_sheet_id, details["move_pct_col"], row_num)
                            }
                        })
                        requests.append({
                            "repeatCell": {
                                "range": create_cell_range_for_request(dashboard_sheet_id, details["move_pct_col"], row_num),
                                "cell": {
                                    "userEnteredFormat": {
                                        "backgroundColor": rgb_to_float(current_move_pct_color) if current_move_pct_color else rgb_to_float(None),
                                        "numberFormat": {
                                            "type": "NUMBER",
                                            "pattern": "0.00%"
                                        }
                                    }
                                },
                                "fields": "userEnteredFormat.backgroundColor,userEnteredFormat.numberFormat"
                            }
                        })
                    else:
                        # Clear value and color if conditions not met
                        if details["move_pct_col"]:
                            cell_range = create_cell_range_for_request(dashboard_sheet_id, details["move_pct_col"], row_num)
                            requests.append({
                                "updateCells": {
                                    "rows": [{"values": [{"userEnteredValue": {"stringValue": ""}}]}],
                                "fields": "userEnteredValue",
                                "range": cell_range
                            }})
                            requests.append({
                                "repeatCell": {
                                    "range": cell_range,
                                    "cell": {
                                        "userEnteredFormat": {
                                            "backgroundColor": rgb_to_float(None),
                                            "numberFormat": {"type": "NUMBER", "pattern": "General"}
                                        }
                                    },
                                    "fields": "userEnteredFormat.backgroundColor,userEnteredFormat.numberFormat"
                                }
                            })

                    # New: % From Stop Calculation (Trailing Stop vs LTP)
                    if isinstance(trailing_stop_val, (int, float)) and isinstance(current_ltp, (int, float)) and current_ltp != 0:
                        percent_from_stop = (current_ltp - trailing_stop_val) / current_ltp

                        current_percent_from_stop_color = None
                        if percent_from_stop is not None:
                            if percent_from_stop > 0: current_percent_from_stop_color = GREEN_COLOR
                            elif percent_from_stop < 0: current_percent_from_stop_color = RED_COLOR

                        # % From Stop Update
                        requests.append({
                            "updateCells": {
                                "rows": [{"values": [{"userEnteredValue": {"numberValue": percent_from_stop}}]}],
                                "fields": "userEnteredValue",
                                "range": create_cell_range_for_request(dashboard_sheet_id, details["percent_from_stop_col"], row_num)
                            }
                        })
                        requests.append({
                            "repeatCell": {
                                "range": create_cell_range_for_request(dashboard_sheet_id, details["percent_from_stop_col"], row_num),
                                "cell": {
                                    "userEnteredFormat": {
                                        "backgroundColor": rgb_to_float(current_percent_from_stop_color) if current_percent_from_stop_color else rgb_to_float(None),
                                        "numberFormat": {
                                            "type": "NUMBER",
                                            "pattern": "0.00%"
                                        }
                                    }
                                },
                                "fields": "userEnteredFormat.backgroundColor,userEnteredFormat.numberFormat"
                            }
                        })
                    else:
                        # Clear value and color if conditions not met
                        if details["percent_from_stop_col"]:
                            cell_range = create_cell_range_for_request(dashboard_sheet_id, details["percent_from_stop_col"], row_num)
                            requests.append({
                                "updateCells": {
                                    "rows": [{"values": [{"userEnteredValue": {"stringValue": ""}}]}],
                                "fields": "userEnteredValue",
                                "range": cell_range
                            }})
                            requests.append({
                                "repeatCell": {
                                    "range": cell_range,
                                    "cell": {
                                        "userEnteredFormat": {
                                            "backgroundColor": rgb_to_float(None),
                                            "numberFormat": {"type": "NUMBER", "pattern": "General"}
                                        }
                                    },
                                    "fields": "userEnteredFormat.backgroundColor,userEnteredFormat.numberFormat"
                                }
                            })

                    # NEW: Days Duration Calculation and Update
                    if details["entry_date_col"] and details["days_duration_col"]:
                        if entry_date_str:
                            try:
                                entry_date_obj = datetime.strptime(entry_date_str, "%d-%b-%y")
                                current_date_obj = datetime.now()
                                days_duration = (current_date_obj - entry_date_obj).days
                                logger.debug(f"Calculated Days Duration for {details['symbol']} (Row {row_num}): {days_duration} days.")
                            except ValueError:
                                logger.warning(f"Could not parse entry date '{entry_date_str}' for token {token} at row {row_num}. Expected DD-Mon-YY format. Clearing Days Duration cell.")
                                days_duration = None

                        if days_duration is not None:
                            formatted_days_duration = f"{days_duration} Days"
                            requests.append({
                                "updateCells": {
                                    "rows": [{"values": [{"userEnteredValue": {"stringValue": formatted_days_duration}}]}],
                                    "fields": "userEnteredValue",
                                    "range": create_cell_range_for_request(dashboard_sheet_id, details["days_duration_col"], row_num)
                                }
                            })
                            requests.append({
                                "repeatCell": {
                                    "range": create_cell_range_for_request(dashboard_sheet_id, details["days_duration_col"], row_num),
                                    "cell": {
                                        "userEnteredFormat": {
                                            "numberFormat": {
                                                "type": "TEXT",
                                                "pattern": "@"
                                            }
                                        }
                                    },
                                    "fields": "userEnteredFormat.numberFormat"
                                }
                            })
                        else:
                            cell_range = create_cell_range_for_request(dashboard_sheet_id, details["days_duration_col"], row_num)
                            requests.append({
                                "updateCells": {
                                    "rows": [{"values": [{"userEnteredValue": {"stringValue": ""}}]}],
                                    "fields": "userEnteredValue",
                                    "range": cell_range
                                }
                            })
                            requests.append({
                                "repeatCell": {
                                    "range": cell_range,
                                    "cell": {
                                        "userEnteredFormat": {
                                            "backgroundColor": rgb_to_float(None),
                                            "numberFormat": {"type": "NUMBER", "pattern": "General"}
                                        }
                                    },
                                    "fields": "userEnteredFormat.backgroundColor,userEnteredFormat.numberFormat"
                                }
                            })
                            logger.debug(f"Cleared Days Duration for Token {token} (Row {row_num}, {details['block_type']}) due to missing/invalid entry date.")


        else:
            logger.debug(f"No live data for token {token} in latest_tick_data. Skipping updates for its Google Sheet entries.")
            # If no live data, also consider reverting the color if it was previously set for all associated cells
            for details in list_of_details:
                row_num = details['row']
                dashboard_sheet_id = Dashboard.id

                # Clear colors for LTP and %Change if no live data
                for col_letter in [details['ltp_col'], details['chg_col']]:
                    if col_letter:
                        requests.append({
                            "repeatCell": {
                                "range": create_cell_range_for_request(dashboard_sheet_id, col_letter, row_num),
                                "cell": {
                                    "userEnteredFormat": {
                                        "backgroundColor": rgb_to_float(None),
                                        "numberFormat": {"type": "NUMBER", "pattern": "General"}
                                    }
                                },
                                "fields": "userEnteredFormat.backgroundColor,userEnteredFormat.numberFormat"
                            }
                        })

                # Clear colors for other full positions columns if no live data
                if details['block_type'] == "Full Positions":
                    for col_letter in [details["return_amt_col"], details["return_pct_col"], details["move_pct_col"], details["percent_from_stop_col"], details["days_duration_col"]]:
                        if col_letter:
                            requests.append({
                                "repeatCell": {
                                    "range": create_cell_range_for_request(dashboard_sheet_id, col_letter, row_num),
                                    "cell": {
                                        "userEnteredFormat": {
                                            "backgroundColor": rgb_to_float(None),
                                            "numberFormat": {"type": "NUMBER", "pattern": "General"}
                                        }
                                    },
                                    "fields": "userEnteredFormat.backgroundColor,userEnteredFormat.numberFormat"
                                }
                            })

    if requests:
        try:
            gsheet.batch_update({'requests': requests})
            logger.info(f"Executed {len(requests)} batch update operations on Google Sheet.")
        except Exception as e:
            logger.exception(f"An error occurred during batch update to Google Sheet: {e}")
    else:
        logger.info("No data to write to Google Sheet in this cycle (either no symbols or no live data yet).")


# --- Main Threaded Logic ---
def start_websocket_logic():
    """
    This function contains the main logic for SmartAPI session, WebSocket connection,
    and continuous market data processing and Google Sheet updates.
    It runs in a separate thread.
    """
    global client, gsheet, Dashboard, Credential, smart_ws, obj, auth_token, feedToken
    global subscribed_tokens, previous_ltp_data, previous_percentage_change_data # Ensure these are accessible

    logger.info("🔌 Starting SmartAPI WebSocket logic...")

    # Initialize previous_ltp_data and previous_percentage_change_data
    previous_ltp_data = {}
    previous_percentage_change_data = {}

    # --- Google Sheets Authentication ---
    try:
        logger.info("Authenticating with Google Sheets...")
        creds = ServiceAccountCredentials.from_json_keyfile_name(JSON_KEY_FILE_PATH, SCOPE)
        client = gspread.authorize(creds)
        gsheet = client.open_by_key(GOOGLE_SHEET_ID)
        Dashboard = gsheet.worksheet(DASHBOARD_SHEET_NAME)
        Credential = gsheet.worksheet(CREDENTIAL_SHEET_NAME)
        logger.info("Google Sheets connected successfully.")
    except Exception as e:
        logger.error(f"Error connecting to Google Sheets: {e}. Ensure the JSON key file is correct and the sheet ID is valid.")
        sys.exit("Exiting due to failed Google Sheets connection.")

    load_previous_day_high_cache()

    # --- SmartAPI Session Generation ---
    try:
        logger.info("Attempting to generate SmartAPI session...")
        obj = SmartConnect(api_key=API_KEY)
        totp = pyotp.TOTP(TOTP_SECRET).now()
        data = obj.generateSession(CLIENT_CODE, MPIN, totp=totp)

        if data and data.get('data') and data['data'].get('jwtToken'):
            auth_token = data['data']['jwtToken']
            feedToken = data['data']['feedToken']
            logger.info("SmartAPI session generated successfully!")
        else:
            logger.error(f"Failed to generate SmartAPI session. Response: {data}")
            sys.exit("Exiting due to failed SmartAPI session generation.")

    except Exception as e:
        logger.error(f"Error during SmartAPI session generation: {e}")
        sys.exit("Exiting due to an exception during SmartAPI session generation.")

    # --- SmartWebSocketV2 Initialization and Connection ---
    try:
        if not auth_token or not feedToken or not CLIENT_CODE or not API_KEY:
            raise ValueError("Missing authentication tokens or API key for WebSocket initialization.")

        smart_ws = SmartWebSocketV2(
            auth_token=auth_token,
            api_key=API_KEY,
            client_code=CLIENT_CODE,
            feed_token=feedToken,
            max_retry_attempt=5,
            retry_strategy=1, # Exponential backoff
            retry_delay=5,
            retry_multiplier=2
        )

        ws_thread = threading.Thread(target=smart_ws.connect, daemon=True)
        ws_thread.start()
        logger.info("SmartWebSocketV2 connection attempt initiated in a separate thread.")

        timeout = 15
        start_time = time.time()
        while not smart_ws._is_connected_flag and (time.time() - start_time < timeout):
            time.sleep(0.5)

        if not smart_ws._is_connected_flag:
            logger.error(f"WebSocket connection did not establish successfully within {timeout} seconds.")
            sys.exit("Exiting due to failed WebSocket connection.")
        else:
            logger.info("WebSocket connection successfully established.")

    except Exception as e:
        logger.exception(f"An error occurred during SmartWebSocketV2 initialization or connection: {e}")
        sys.exit("Exiting due to an exception during SmartWebSocketV2 setup.")

    # --- Initial Data Fetches (conditional on market hours) ---
    try:
        # Scan Google Sheet for all symbols (ORH, Full Positions, Indices, Quarter Positions, and 3% Down)
        all_tokens_for_subscription_tuples = scan_excel_for_symbols_and_tokens()
        
        # Convert tuples to list of dicts for subscription
        initial_tokens_to_subscribe_list = []
        for token, exchange_type in all_tokens_for_subscription_tuples:
            initial_tokens_to_subscribe_list.append({'exchangeType': exchange_type, 'tokens': [token]})
            subscribed_tokens.add((token, exchange_type)) # Add to global subscribed_tokens set

        if smart_ws._is_connected_flag and initial_tokens_to_subscribe_list:
            smart_ws.subscribe("initial_sub", SmartWebSocketV2.QUOTE, initial_tokens_to_subscribe_list)
            logger.info(f"Initial subscription sent for {len(initial_tokens_to_subscribe_list)} token groups.")
        elif not smart_ws._is_connected_flag:
            logger.warning("WebSocket not connected for initial subscription.")
        else:
            logger.info("No tokens to subscribe initially.")

        if is_market_hours():
            logger.info("Within market hours. Fetching current day's historical data for initial setup.")
            fetch_initial_candle_data(obj, excel_symbol_details) # For ORH 3-min candles
            fetch_previous_day_candle_data(obj, excel_symbol_details) # For ORH previous day high

            # Fetch latest completed candles for 3% Down setup (initial fetch)
            # Filter for unique tokens relevant to 3% down setup from the scanned list
            unique_tokens_3pct_for_fetch = list(set([(t, ex_type) for t, ex_type in all_tokens_for_subscription_tuples if t in excel_3pct_symbol_details]))
            
            for interval_api in CANDLE_INTERVALS_3PCT_API:
                # Fetch last few hours to ensure we get any recently completed candles at startup
                fetch_historical_candles_for_3pct_down(obj, unique_tokens_3pct_for_fetch, interval_api, datetime.now() - timedelta(hours=2), datetime.now())
                time.sleep(1) # Small delay to avoid hammering API
            
        else:
            logger.info("Outside market hours. Skipping initial live data fetches and historical data for strategies.")

    except Exception as e:
        logger.error(f"Error during initial setup: {e}. Please ensure Google Sheet is accessible.")
        sys.exit("Exiting due to an exception during initial setup.")


    # --- Main Processing Loop Parameters ---
    EXCEL_LTP_UPDATE_INTERVAL_SECONDS = 0.5 # For frequent LTP updates
    TOKEN_SCAN_INTERVAL_CYCLES = 60 # How often to re-scan Google Sheet (e.g., every 60 * 2s = 120 seconds)
    current_cycle = 0

    # Initialize last checked minutes for precise interval checks
    last_checked_minute_orh = -1 # Use -1 to ensure it runs on first cycle if minute is 0
    last_checked_minute_15min = -1
    last_checked_minute_30min = -1
    last_checked_minute_1hr = -1
    last_checked_minute_confirmation = -1

    logger.info("Entering main processing loop...")

    try:
        while True:
            now = datetime.now()
            current_minute = now.minute
            current_hour = now.hour
            current_time = now.time()

            logger.info(f"--- Processing data at {now.strftime('%Y-%m-%d %H:%M:%S')} (Cycle: {current_cycle + 1}) ---")
            current_cycle += 1

            # --- PART 1: Periodically re-scan Google Sheet for new/removed symbols and manage subscriptions ---
            if current_cycle % TOKEN_SCAN_INTERVAL_CYCLES == 0 or current_cycle == 1:
                logger.info("Rescanning Google Sheet for new symbols and managing subscriptions...")

                current_excel_tokens_tuples = scan_excel_for_symbols_and_tokens() # Returns list of (token, exchange_type_int)
                current_excel_tokens_set = set(current_excel_tokens_tuples)

                tokens_to_subscribe_now = current_excel_tokens_set - subscribed_tokens
                tokens_to_unsubscribe_now = subscribed_tokens - current_excel_tokens_set

                # Perform Unsubscriptions
                if tokens_to_unsubscribe_now and smart_ws._is_connected_flag:
                    unsubscribe_list_grouped = collections.defaultdict(list)
                    for token_to_unsub, ex_type_int in tokens_to_unsubscribe_now:
                        unsubscribe_list_grouped[ex_type_int].append(token_to_unsub)

                    for ex_type, tokens in unsubscribe_list_grouped.items():
                        formatted_tokens = [{"exchangeType": ex_type, "tokens": tokens}]
                        try:
                            smart_ws.unsubscribe(
                                correlation_id=f"unsub_{int(time.time())}",
                                mode=SmartWebSocketV2.QUOTE,
                                token_list=formatted_tokens
                            )
                            for token_unsub in tokens:
                                subscribed_tokens.discard((token_unsub, ex_type))
                            logger.info(f"Unsubscribed from {len(tokens)} tokens on exchange {ex_type} (removed from Google Sheet).")
                        except Exception as e:
                            logger.error(f"Error sending unsubscribe request: {e}")
                elif not smart_ws._is_connected_flag:
                    logger.warning("WebSocket not connected. Skipping unsubscription of removed symbols.")
                else:
                    logger.info("No symbols to unsubscribe in this cycle.")

                # Perform Subscriptions
                if tokens_to_subscribe_now and smart_ws._is_connected_flag:
                    subscribe_list_grouped = collections.defaultdict(list)
                    for token_to_sub, ex_type_int in tokens_to_subscribe_now:
                        subscribe_list_grouped[ex_type_int].append(token_to_sub)

                    for ex_type, tokens in subscribe_list_grouped.items():
                        formatted_tokens = [{"exchangeType": ex_type, "tokens": tokens}]
                        try:
                            smart_ws.subscribe(
                                correlation_id=f"sub_{int(time.time())}",
                                mode=SmartWebSocketV2.QUOTE,
                                token_list=formatted_tokens
                            )
                            for token_sub in tokens:
                                subscribed_tokens.add((token_sub, ex_type))
                            logger.info(f"Subscribed to {len(tokens)} new tokens on exchange {ex_type}.")
                        except Exception as e:
                            logger.error(f"Error sending subscribe request: {e}")
                elif not smart_ws._is_connected_flag:
                    logger.warning("WebSocket not connected. Cannot subscribe to new tokens.")
                else:
                    logger.info("No new tokens to subscribe to in this cycle.")

                # Ensure subscribed_tokens is always in sync with current_excel_tokens_set
                # This handles cases where a token might have been in the set but failed subscription or was manually unsubscribed
                subscribed_tokens.intersection_update(current_excel_tokens_set)


            if is_market_hours():
                # --- ORH Setup Checks (every 3 minutes) ---
                # Trigger at :00, :03, :06, ... :57
                if current_minute % 3 == 0 and current_minute != last_checked_minute_orh:
                    logger.info(f"🕒 Running ORH check for new 3-min candle at {now.strftime('%H:%M:%S')}")
                    check_and_update_orh_setup()
                    last_checked_minute_orh = current_minute

                # --- 3% Down Candle Setup Checks (Live Market) ---
                # Filter for unique tokens relevant to 3% down setup from the currently subscribed tokens
                unique_tokens_3pct_for_fetch = list(set([(t, ex_type) for t, ex_type in subscribed_tokens if t in excel_3pct_symbol_details]))

                # 15-minute candle check: Trigger 1 minute after candle closes (e.g., 9:31, 9:46, 10:01, ...)
                # First 15-min candle closes at 9:30, check at 9:31.
                # Avoid checking before 9:31 AM IST
                if (current_time >= datetime_time(9, 31)) and (current_minute % 15 == 1) and (current_minute != last_checked_minute_15min): # Use aliased datetime_time
                    logger.info(f"Fetching latest 15-min candles for 3% Down setup at {now.strftime('%H:%M:%S')}")
                    # Fetch last 30 minutes to ensure we get the latest completed 15-min candle
                    fetch_historical_candles_for_3pct_down(obj, unique_tokens_3pct_for_fetch, 'FIFTEEN_MINUTE', now - timedelta(minutes=30), now)
                    check_and_update_3pct_down_setup()
                    last_checked_minute_15min = current_minute

                # 30-minute candle check: Trigger 1 minute after candle closes (e.g., 9:46, 10:16, ...)
                # First 30-min candle closes at 9:45, check at 9:46.
                # Avoid checking before 9:46 AM IST
                if (current_time >= datetime_time(9, 46)) and (current_minute % 30 == 16) and (current_minute != last_checked_minute_30min): # Use aliased datetime_time
                    logger.info(f"Fetching latest 30-min candles for 3% Down setup at {now.strftime('%H:%M:%S')}")
                    # Fetch last 60 minutes to ensure we get the latest completed 30-min candle
                    fetch_historical_candles_for_3pct_down(obj, unique_tokens_3pct_for_fetch, 'THIRTY_MINUTE', now - timedelta(minutes=60), now)
                    check_and_update_3pct_down_setup()
                    last_checked_minute_30min = current_minute

                # 1-hour candle check: Trigger 1 minute after candle closes (e.g., 10:16, 11:16, ...)
                # First 1-hour candle closes at 10:15, check at 10:16.
                # Avoid checking before 10:16 AM IST
                if (current_time >= datetime_time(10, 16)) and (current_minute == 16) and (current_minute != last_checked_minute_1hr): # Use aliased datetime_time
                    logger.info(f"Fetching latest 1-hour candles for 3% Down setup at {now.strftime('%H:%M:%S')}")
                    # Fetch last 2 hours to ensure we get the latest completed 1-hour candle
                    fetch_historical_candles_for_3pct_down(obj, unique_tokens_3pct_for_fetch, 'ONE_HOUR', now - timedelta(hours=2), now)
                    check_and_update_3pct_down_setup()
                    last_checked_minute_1hr = current_minute
                
                # ✅ Phase 2: Check confirmation from next 15-min candle every 5 mins
                # Trigger at :02, :07, :12, ...
                if current_minute % 5 == 2 and current_minute != last_checked_minute_confirmation:
                    logger.info(f"🔍 Running confirmation check for 3% Down setup at {now.strftime('%H:%M:%S')}")
                    confirm_high_volume_3pct_down(obj)
                    last_checked_minute_confirmation = current_minute        

            else:
                # Outside market hours, ORH and 3% Down checks are skipped
                logger.info(f"⏸ Outside market hours — ORH and 3% Down checks skipped at {now.strftime('%H:%M:%S')}")

            # --- PART 2: Update Google Sheet with latest LTP from WebSocket cache ---
            update_excel_live_data()

            logger.info(f"Waiting for {EXCEL_LTP_UPDATE_INTERVAL_SECONDS} seconds before next Google Sheet refresh...")
            time.sleep(EXCEL_LTP_UPDATE_INTERVAL_SECONDS) # Main loop sleep

    except KeyboardInterrupt:
        logger.info("Script interrupted by user. Closing connections...")
    except Exception as e:
        logger.exception(f"An unhandled error occurred in the main loop: {e}")
    finally:
        if smart_ws:
            smart_ws.close_connection()
        logger.info("Script finished.")

# --- Threaded logic runner (for Flask) ---
def run_threaded_logic():
    """Starts the main WebSocket and market logic in a separate daemon thread."""
    t = threading.Thread(target=start_websocket_logic)
    t.daemon = True # Daemon threads exit when the main program exits
    t.start()

# --- Main Entry Point for Flask + Threaded Logic ---
if __name__ == "__main__":
    run_threaded_logic()
    # Flask app runs in the main thread, serving the /ping endpoint
    # The market data logic runs in the background thread
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5000)))

