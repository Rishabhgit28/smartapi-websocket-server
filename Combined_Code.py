# Combined Trading Dashboard and Signal Generator (Concurrent, Threaded Version)
import os, sys, json, time, struct, ssl, logging, threading, collections, datetime, re
from datetime import timedelta
from dateutil.relativedelta import relativedelta
from zoneinfo import ZoneInfo
from flask import Flask
import gspread, pyotp, websocket, requests, yfinance as yf, logzero
from logzero import logger
from oauth2client.service_account import ServiceAccountCredentials
try:
    import winsound
except ImportError:
    winsound = None
    logger.warning("Could not import 'winsound'. Sound alerts for ORH setups will be disabled.")
from SmartApi import SmartConnect
from SmartApi.smartExceptions import DataException

# --- Flask and Logging Configuration ---
app = Flask(__name__) # A simple web server for deployment health checks.
log_folder = time.strftime("%Y-%m-%d", time.localtime())
log_folder_path = os.path.join("logs", log_folder)
os.makedirs(log_folder_path, exist_ok=True)
log_path = os.path.join(log_folder_path, "app.log")
logzero.logfile(log_path, maxBytes=1e6, backupCount=3, encoding='utf-8') # Configure a rotating file handler.
logzero.setup_default_logger(level=logging.INFO) # Configure the default logger and set the global logging level.
if not any(isinstance(h, logging.StreamHandler) for h in logger.handlers):
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO)
    formatter = logging.Formatter('[%(levelname)s %(asctime)s %(filename)s:%(lineno)d] %(message)s')
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)
@app.route('/ping')
def ping(): # A simple route for deployment services to check if the app is alive.
    return "Combined Trading Server is running", 200

# --- Global Variables and Configuration ---
API_KEY = "oNNHQHKU" # SmartAPI Credentials
CLIENT_CODE = "D355432"
MPIN = "1234"
TOTP_SECRET = "QHO5IWOISV56Z2BFTPFSRSQVRQ"
GOOGLE_SHEET_ID = '1cYBpsVKCbrYCZzrj8NAMEgUG4cXy5Q5r9BtQE1Cjmz0' # Google Sheets Configuration
DASHBOARD_SHEET_NAME = 'Dashboard'
ATH_CACHE_SHEET_NAME = 'ATH Cache'
ORDERS_SHEET_NAME = 'Orders'
APPS_SCRIPT_WEB_APP_URL = "https://script.google.com/macros/s/AKfycby40oa_gihCiVkxooq_6_jm-yyemFlAhb5W_bshsY1BwdQFnFJpiOtYKStwGcwZL1Fy/exec" # Apps Script Web App URL for Instant Triggers
INSTRUMENT_LIST_URL = "https://margincalculator.angelbroking.com/OpenAPI_File/files/OpenAPIScripMaster.json" # Instrument master list URL
instrument_master_list = []
stock_name_cache = {} # Cache for full stock names from Yahoo Finance
if os.path.exists("/etc/secrets/creds.json"): # Set the path to the Google credentials JSON file
    JSON_KEY_FILE_PATH = "/etc/secrets/creds.json"
else:
    current_dir = os.path.dirname(__file__) if "__file__" in locals() else os.getcwd()
    JSON_KEY_FILE_PATH = os.path.join(current_dir, "the-money-method-ad6d7-6d23c192b74e.json")
SCOPE = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
smart_api_obj, smart_ws, gsheet, Dashboard, ATHCache, OrdersSheet = None, None, None, None, None, None # Global Objects (Initialized in main logic)
data_lock = threading.Lock() # Threading Lock for Data Safety
initial_data_ready = threading.Event() # Event to signal when initial data is fetched
latest_tick_data = collections.defaultdict(dict) # Data Caching for Live Dashboard
latest_quote_data = collections.defaultdict(dict)
excel_dashboard_details = collections.defaultdict(list)
previous_ltp_data = {}
previous_percentage_change_data = {}
cells_to_clear_color = set()
excel_orh_setup_details = collections.defaultdict(list) # Data Caching for ORH and 3% Down Setups
excel_3pct_setup_details = collections.defaultdict(list)
interval_ohlc_data = collections.defaultdict(lambda: collections.defaultdict(dict))
completed_5min_candles = collections.defaultdict(list)
volume_history_3pct = collections.defaultdict(lambda: collections.defaultdict(list))
support_candle_details = {} # NEW: Cache for storing precise support candle date and time.
previous_day_high_cache = {}
monthly_high_cache = {}
orh_triggered_today = set() # State Tracking: Stores tokens that have already triggered to prevent re-checking
previous_j_column_state = {}
sell_triggered_today = set()
previous_ah_column_state = {}
previous_breakdown_state = {}
subscribed_tokens = set() # For Subscription Management
scan_memory_cache = {} # In-memory cache to prevent repetitive logging
START_ROW_DATA = 5 # General Configuration Constants
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
INDEX_START_ROW = 100 # For Live Dashboard
QUARTER_POSITIONS_START_ROW = 100
FOCUS_EXCHANGE_COL, FOCUS_SYMBOL_COL, FOCUS_LTP_COL, FOCUS_CHG_COL = 'B', 'C', 'D', 'E'
ATH_CACHE_Y_COL_DASH, ATH_CACHE_Z_COL_DASH = 'AM', 'AN'
FULL_EXCHANGE_COL, FULL_SYMBOL_COL, FULL_QTY_COL, FULL_PRICE_COL, FULL_LTP_COL, FULL_RETURN_AMT_COL, FULL_RETURN_PCT_COL = 'L', 'M', 'N', 'O', 'P', 'Q', 'R'
FULL_ENTRY_DATE_COL, FULL_DAYS_DURATION_COL, MONTH_SORT_COL, SWING_LOW_INPUT_COL, PERCENT_FROM_SWING_LOW_COL = 'S', 'T', 'V', 'W', 'X'
TRAILING_STOP_INPUT_COL, TRAILING_STOP_STATUS_COL = 'Y', 'AA'
HIGHEST_UP_CANDLE_COL, HIGHEST_UP_CANDLE_STATUS_COL = 'AB', 'AC'
HIGH_VOL_RESULT_COL, HIGH_VOL_STATUS_COL = 'AD', 'AE'
PCT_DOWN_RESULT_COL, PCT_DOWN_STATUS_COL = 'AF', 'AG'
ACTION_COL, FULL_POSITIONS_END_COL = 'AH', 'AI'
INDEX_EXCHANGE_COL, INDEX_SYMBOL_COL, INDEX_LTP_COL, INDEX_CHG_COL = 'B', 'C', 'D', 'E'
QUARTER_EXCHANGE_COL, QUARTER_SYMBOL_COL, QUARTER_LTP_COL, QUARTER_CHG_COL = 'M', 'N', 'P', 'Q'
SETUP_EXCHANGE_COL, SETUP_SYMBOL_COL, SETUP_QTY_COL, SETUP_TOKEN_COL, SETUP_RESULT_COL, SETUP_STOP_COL, SETUP_LOG_COL = 'B', 'C', 'I', 'Y', 'G', 'H', 'J' # For ORH Setup
PCT_EXCHANGE_COL_3PCT, PCT_SYMBOL_COL_3PCT, PCT_TOKEN_COL_3PCT = 'L', 'M', 'Z'
CANDLE_INTERVALS_3PCT_API = ['FIFTEEN_MINUTE', 'THIRTY_MINUTE', 'ONE_HOUR']
CANDLE_INTERVAL_MAP_DISPLAY = {'FIFTEEN_MINUTE': '15 min', 'THIRTY_MINUTE': '30 min', 'ONE_HOUR': '60 min'}
SETUP_MAX_ROW = 17

# --- Helper and Utility Functions ---
def get_ist_time(): return datetime.datetime.now(ZoneInfo("Asia/Kolkata")) # Returns the current time in Indian Standard Time.
def is_alert_hours(): # NEW: Checks if the current time is within the alert-triggering window.
    now = get_ist_time()
    if now.weekday() > 4: return False # It's Saturday or Sunday
    if now.weekday() == 0 and now.time() < datetime.time(9, 15): return False # Monday before 9:15 AM
    if now.weekday() == 4 and now.time() > datetime.time(16, 0): return False # Friday after 4:00 PM
    return True # It's within the allowed time
def normalize_status(api_status): # Maps raw API status strings to a user-friendly, unified format.
    if not api_status or not isinstance(api_status, str): return 'Unknown'
    status_lower = api_status.lower()
    status_mapping = {'active': 'Pending Trigger', 'new': 'Pending Trigger', 'triggered': 'Triggered - Awaiting ID', 'cancelled': 'Cancelled', 'expired': 'Expired', 'rejected': 'Rejected', 'open': 'Pending Execution', 'pending': 'Pending Execution', 'complete': 'Completed', 'executed': 'Completed', 'not_found_in_gtt_list': 'GTT Rule Not Found', 'not_found_in_order_book': 'Order Not in Book'}
    return status_mapping.get(status_lower, api_status.title())
def get_full_name_from_yahoo(symbol, exchange): # Fetches the full company name from Yahoo Finance with a fallback mechanism and caching.
    global stock_name_cache
    if not symbol or not exchange: return symbol
    base_symbol = re.sub(r'-(EQ|BE)$', '', symbol).upper()
    if base_symbol in stock_name_cache: return stock_name_cache[base_symbol] or symbol
    tickers_to_try = []
    if exchange.upper() == 'NSE': tickers_to_try.extend([f"{base_symbol}.NS", f"{base_symbol}.BO"])
    elif exchange.upper() == 'BSE': tickers_to_try.extend([f"{base_symbol}.BO", f"{base_symbol}.NS"])
    else: tickers_to_try.append(f"{base_symbol}.NS")
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
            continue
    logger.warning(f"Could not find 'longName' for {symbol} on any exchange. Falling back to symbol.")
    stock_name_cache[base_symbol] = None
    return symbol
def trigger_apps_script_alert(alert_type, row, symbol, exchange): # MODIFIED: Sends a POST request only during alert hours.
    if not is_alert_hours():
        logger.info(f"Alert ({alert_type} on row {row}) suppressed due to off-market hours.")
        return
    def _send_request():
        try:
            if not APPS_SCRIPT_WEB_APP_URL or "PASTE YOUR URL HERE" in APPS_SCRIPT_WEB_APP_URL:
                logger.warning("Apps Script Web App URL is not configured. Skipping trigger.")
                return
            full_stock_name = get_full_name_from_yahoo(symbol, exchange)
            payload = {"alertType": alert_type, "row": row, "stockName": full_stock_name or symbol}
            logger.info(f"Triggering Apps Script for {alert_type} on row {row} with payload: {payload}")
            response = requests.post(APPS_SCRIPT_WEB_APP_URL, json=payload, timeout=60)
            if response.status_code == 200: logger.info(f"Successfully triggered Apps Script. Response: {response.text}")
            else: logger.error(f"Failed to trigger Apps Script. Status: {response.status_code}, Response: {response.text}")
        except Exception as e: logger.exception(f"An error occurred while trying to trigger the Apps Script alert: {e}")
    trigger_thread = threading.Thread(target=_send_request, daemon=True)
    trigger_thread.start()
def update_connection_status(status_message): # Updates a file with the current connection status for external monitoring.
    try:
        folder_path = os.path.dirname(sys.executable) if getattr(sys, 'frozen', False) else os.path.dirname(os.path.abspath(__file__))
        file_path = os.path.join(folder_path, "connection_status.txt")
        with open(file_path, "w") as f: f.write(status_message)
    except Exception as e: logger.warning(f"Failed to write connection status file: {e}")
def load_previous_day_high_cache(): # Loads the previous day's high data from a JSON cache file for the ORH setup.
    global previous_day_high_cache
    cache_dir = os.path.dirname(sys.executable) if getattr(sys, 'frozen', False) else os.path.dirname(os.path.abspath(__file__))
    cache_path = os.path.join(cache_dir, PREV_DAY_HIGH_CACHE_FILE)
    if os.path.exists(cache_path):
        try:
            with open(cache_path, 'r') as f: previous_day_high_cache = json.load(f)
            logger.info(f"Loaded previous day high cache from {cache_path}.")
        except Exception as e:
            logger.error(f"Error loading cache file {cache_path}: {e}. Starting with empty cache.")
            previous_day_high_cache = {}
    else:
        logger.info("Previous day high cache file not found. Starting with empty cache.")
        previous_day_high_cache = {}
def save_previous_day_high_cache(): # Saves the previous day's high data to a JSON cache file.
    cache_dir = os.path.dirname(sys.executable) if getattr(sys, 'frozen', False) else os.path.dirname(os.path.abspath(__file__))
    cache_path = os.path.join(cache_dir, PREV_DAY_HIGH_CACHE_FILE)
    try:
        with open(cache_path, 'w') as f: json.dump(previous_day_high_cache, f, indent=4)
        logger.info(f"Saved previous day high cache to {cache_path}.")
    except Exception as e: logger.error(f"Error saving cache file {cache_path}: {e}")
def col_to_num(letter): # Converts a column letter (e.g., 'A', 'B', 'AA') to a 1-based index.
    index = 0
    for char in letter.upper(): index = index * 26 + (ord(char) - ord('A') + 1)
    return index
def get_last_row_in_column(sheet, column_letter): # Finds the last row with data in a given column for a gspread worksheet.
    try:
        column_index = col_to_num(column_letter)
        column_values = sheet.col_values(column_index)
        last_non_empty_index = -1
        for i, val in enumerate(column_values):
            if val and str(val).strip() != '': last_non_empty_index = i
        return last_non_empty_index + 1 if last_non_empty_index != -1 else START_ROW_DATA - 1
    except Exception as e:
        logger.error(f"Error in get_last_row_in_column for sheet '{sheet.title}', column '{column_letter}': {e}")
        return START_ROW_DATA - 1
def rgb_to_float(rgb_tuple): # Converts an RGB tuple (0-255) to a float dictionary (0-1) for the Google Sheets API.
    if rgb_tuple is None: return {"red": 1.0, "green": 1.0, "blue": 1.0}
    return {"red": rgb_tuple[0] / 255.0, "green": rgb_tuple[1] / 255.0, "blue": rgb_tuple[2] / 255.0}

# --- SmartAPI WebSocket Client ---
class SmartWebSocketV2(object): # Handles low-level WebSocket communication with the Angel One SmartAPI.
    ROOT_URI, HEART_BEAT_MESSAGE, HEART_BEAT_INTERVAL, LITTLE_ENDIAN_BYTE_ORDER = "wss://smartapisocket.angelone.in/smart-stream", "ping", 10, "<"
    RESUBSCRIBE_FLAG, SUBSCRIBE_ACTION, UNSUBSCRIBE_ACTION = False, 1, 0
    LTP_MODE, QUOTE, SNAP_QUOTE, DEPTH = 1, 2, 3, 4
    NSE_CM, NSE_FO, BSE_CM, BSE_FO, MCX_FO, NCX_FO, CDE_FO = 1, 2, 3, 4, 5, 7, 13
    SUBSCRIPTION_MODE_MAP = {1: "LTP", 2: "QUOTE", 3: "SNAP_QUOTE", 4: "DEPTH"}
    wsapp, input_request_dict, current_retry_attempt = None, {}, 0
    def __init__(self, auth_token, api_key, client_code, feed_token, max_retry_attempt=1, retry_strategy=0, retry_delay=10, retry_multiplier=2, retry_duration=60):
        self.auth_token, self.api_key, self.client_code, self.feed_token = auth_token, api_key, client_code, feed_token
        self.DISCONNECT_FLAG, self.last_pong_timestamp = True, None
        self.MAX_RETRY_ATTEMPT, self.retry_strategy, self.retry_delay, self.retry_multiplier, self.retry_duration = max_retry_attempt, retry_strategy, retry_delay, retry_multiplier, retry_duration
        self._is_connected_flag = False
        if not all([self.auth_token, self.api_key, self.client_code, self.feed_token]): raise Exception("Provide valid value for all the tokens")
    def _on_message(self, wsapp, message):
        if message == "pong": self._on_pong(wsapp, message)
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
        if self.RESUBSCRIBE_FLAG: self.resubscribe()
        self.on_open(wsapp)
    def _on_pong(self, wsapp, data):
        self.last_pong_timestamp = time.time()
        logger.info(f"Pong received at {time.strftime('%H:%M:%S', time.localtime(self.last_pong_timestamp))}")
    def _on_ping(self, wsapp, data): logger.info("Ping sent.")
    def subscribe(self, correlation_id, mode, token_list):
        try:
            request_data = {"correlationID": correlation_id, "action": self.SUBSCRIBE_ACTION, "params": {"mode": mode, "tokenList": token_list}}
            if self.wsapp and self.wsapp.sock and self.wsapp.sock.connected:
                self.wsapp.send(json.dumps(request_data))
                self.RESUBSCRIBE_FLAG = True
            else: logger.warning("WebSocket not connected. Subscription request deferred.")
        except Exception as e: logger.error(f"Error occurred during subscribe: {e}")
    def unsubscribe(self, correlation_id, mode, token_list):
        try:
            request_data = {"correlationID": correlation_id, "action": self.UNSUBSCRIBE_ACTION, "params": {"mode": mode, "tokenList": token_list}}
            if self.wsapp and self.wsapp.sock and self.wsapp.sock.connected: self.wsapp.send(json.dumps(request_data))
            else: logger.warning("WebSocket not connected. Unsubscribe request deferred.")
        except Exception as e: logger.error(f"Error occurred during unsubscribe: {e}")
    def resubscribe(self): pass
    def connect(self): # Establishes a WebSocket connection and implements a reconnect mechanism.
        headers = {"Authorization": self.auth_token, "x-api-key": self.api_key, "x-client-code": self.client_code, "x-feed-token": self.feed_token}
        self.DISCONNECT_FLAG = False
        while not self.DISCONNECT_FLAG:
            try:
                self._is_connected_flag = False
                update_connection_status("connecting")
                logger.info("Attempting to connect WebSocket...")
                self.wsapp = websocket.WebSocketApp(self.ROOT_URI, header=headers, on_open=self._on_open, on_error=self._on_error, on_close=self._on_close, on_message=self._on_message, on_ping=self._on_ping, on_pong=self._on_pong)
                self.wsapp.run_forever(sslopt={"cert_reqs": ssl.CERT_NONE}, ping_interval=self.HEART_BEAT_INTERVAL)
            except Exception as e: logger.error(f"Error occurred during WebSocket connection: {e}")
            if not self.DISCONNECT_FLAG:
                logger.warning("WebSocket connection lost. Retrying in 5 seconds...")
                time.sleep(5)
    def close_connection(self):
        self.RESUBSCRIBE_FLAG, self.DISCONNECT_FLAG = False, True
        if self.wsapp: self.wsapp.close()
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
        parsed_data = {"subscription_mode": self._unpack_data(binary_data, 0, 1, byte_format="B")[0], "exchange_type": self._unpack_data(binary_data, 1, 2, byte_format="B")[0], "token": self._parse_token_value(binary_data[2:27]), "sequence_number": self._unpack_data(binary_data, 27, 35, byte_format="q")[0], "exchange_timestamp": self._unpack_data(binary_data, 35, 43, byte_format="q")[0], "last_traded_price": self._unpack_data(binary_data, 43, 51, byte_format="q")[0]}
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
    def _unpack_data(self, binary_data, start, end, byte_format="I"): return struct.unpack(self.LITTLE_ENDIAN_BYTE_ORDER + byte_format, binary_data[start:end])
    @staticmethod
    def _parse_token_value(binary_packet):
        token = ""
        for i in range(len(binary_packet)):
            if chr(binary_packet[i]) == '\x00': return token
            token += chr(binary_packet[i])
        return token
    def on_open(self, wsapp): pass
    def on_error(self, wsapp, error_message): pass
    def on_close(self, wsapp, close_status_code, close_msg): pass
    def on_data(self, wsapp, data): pass

class MyWebSocketClient(SmartWebSocketV2): # Custom WebSocket client that implements combined logic for the dashboard and all signal setups.
    def on_open(self, wsapp): logger.info("WebSocket connection opened."); print("[INFO] WebSocket connection opened.")
    def on_error(self, wsapp, error_message): logger.error(f"WebSocket error: {error_message}"); print(f"[ERROR] WebSocket error: {error_message}")
    def on_close(self, wsapp, close_status_code, close_msg): logger.warning(f"WebSocket closed. Code: {close_status_code}, Message: {close_msg}"); print(f"[WARNING] WebSocket closed. Code: {close_status_code}, Message: {close_msg}")
    def on_data(self, wsapp, data): # Unified data handler for dashboard updates and candle construction.
        token = data.get('token')
        if not token: return
        ltp_raw = data.get('last_traded_price')
        ltp_scaled = ltp_raw / 100.0 if isinstance(ltp_raw, (int, float)) else None
        if ltp_scaled is not None: latest_tick_data[token]['ltp'] = ltp_scaled
        if data.get("subscription_mode") in [self.QUOTE, self.SNAP_QUOTE]:
            day_low_raw = data.get('low_price_of_the_day')
            if day_low_raw is not None: latest_quote_data[token]['day_low'] = day_low_raw / 100.0
        current_time = get_ist_time()
        with data_lock: is_orh_token = token in excel_orh_setup_details
        if is_orh_token and ltp_scaled is not None: self._process_candle(token, ltp_scaled, current_time, 5, completed_5min_candles)
    def _process_candle(self, token, ltp, current_time, interval_minutes, completed_candle_storage): # Helper to process a candle and trigger event-driven checks.
        interval_key = f'{interval_minutes}min'
        candle_info = interval_ohlc_data[token][interval_key]
        minute_floor = (current_time.minute // interval_minutes) * interval_minutes
        candle_start_dt = current_time.replace(minute=minute_floor, second=0, microsecond=0)
        if candle_info.get('start_time') is None or candle_start_dt > candle_info['start_time']:
            if candle_info.get('start_time') is not None:
                completed_candle = {'open': candle_info['open'], 'high': candle_info['high'], 'low': candle_info['low'], 'close': candle_info['last_ltp'], 'start_time': candle_info['start_time']}
                completed_candle_storage[token].append(completed_candle)
                if interval_minutes == 5: threading.Thread(target=schedule_orh_check, args=(token,), daemon=True).start()
                if len(completed_candle_storage[token]) > 5: completed_candle_storage[token].pop(0)
                logger.info(f"Completed {interval_key} candle for {token}: O={completed_candle['open']:.2f}, H={completed_candle['high']:.2f}, L={completed_candle['low']:.2f}, C={completed_candle['close']:.2f}")
            candle_info.update({'open': ltp, 'high': ltp, 'low': ltp, 'start_time': candle_start_dt})
        candle_info['high'] = max(candle_info.get('high', ltp), ltp)
        candle_info['low'] = min(candle_info.get('low', ltp), ltp)
        candle_info['last_ltp'] = ltp

# --- Setup-Specific Functions (ORH & 3% DOWN) ---
def fetch_initial_candle_data_5min(smart_api_obj, symbols_to_fetch): # Fetches historical 5-min candle data for today to pre-populate candles for ORH setup.
    logger.info("Fetching initial historical 5-min candle data for today (ORH setup)...")
    now_ist = get_ist_time()
    from_date = now_ist.replace(hour=9, minute=15, second=0, microsecond=0).strftime("%Y-%m-%d %H:%M")
    to_date = now_ist.strftime("%Y-%m-%d %H:%M")
    MAX_RETRIES, RETRY_DELAY_SECONDS = 5, 20
    with data_lock: symbols_to_fetch_copy = symbols_to_fetch.copy()
    for token, entries in symbols_to_fetch_copy.items():
        if not entries: continue
        symbol_name, exchange_type = entries[0]['symbol'], entries[0]['exchange_type']
        exchange_str = {1: "NSE", 2: "NFO", 3: "BSE"}.get(exchange_type)
        if not exchange_str:
            logger.warning(f"Cannot fetch history for token {token}, unknown exchange type {exchange_type}"); time.sleep(1); continue
        for attempt in range(MAX_RETRIES):
            try:
                historic_param = {"exchange": exchange_str, "symboltoken": token, "interval": "FIVE_MINUTE", "fromdate": from_date, "todate": to_date}
                response = smart_api_obj.getCandleData(historic_param)
                if response and response.get("status") and response.get("data"):
                    completed_5min_candles[token] = [{'start_time': datetime.datetime.fromisoformat(c[0]), 'open': c[1], 'high': c[2], 'low': c[3], 'close': c[4]} for c in response["data"]]
                    if len(completed_5min_candles[token]) > 5: completed_5min_candles[token] = completed_5min_candles[token][-5:]
                    logger.info(f"Fetched {len(completed_5min_candles[token])} 5-min candles for {symbol_name} (Token: {token}).")
                    break
                else:
                    logger.warning(f"Attempt {attempt + 1}/{MAX_RETRIES}: Could not fetch 5-min data for {symbol_name}. Message: {response.get('message', 'Unknown error')}")
                    if attempt < MAX_RETRIES - 1: time.sleep(RETRY_DELAY_SECONDS)
            except Exception as e:
                logger.warning(f"Attempt {attempt + 1}/{MAX_RETRIES}: Error fetching 5-min data for {symbol_name}: {e}")
                if attempt < MAX_RETRIES - 1: time.sleep(RETRY_DELAY_SECONDS)
        time.sleep(0.4)
def fetch_previous_day_candle_data_high(smart_api_obj, symbols_to_fetch): # Fetches daily candle data for last 3 trading days and finds max high for ORH setup.
    logger.info("Fetching last 3 days' HIGH candle data (ORH setup)...")
    today = datetime.date.today()
    trading_days_found, current_day = [], today - timedelta(days=1)
    for _ in range(7):
        if len(trading_days_found) >= 3: break
        if current_day.weekday() < 5: trading_days_found.append(current_day)
        current_day -= timedelta(days=1)
    if len(trading_days_found) < 1:
        logger.warning("Could not determine any previous trading days.")
        return
    to_date, from_date = trading_days_found[0], trading_days_found[-1]
    from_date_str, to_date_str = from_date.strftime("%Y-%m-%d %H:%M"), to_date.strftime("%Y-%m-%d %H:%M")
    cache_check_date_str = to_date.strftime("%Y-%m-%d")
    MAX_RETRIES, RETRY_DELAY_SECONDS = 5, 30
    with data_lock: symbols_to_fetch_copy = symbols_to_fetch.copy()
    for token, entries in symbols_to_fetch_copy.items():
        if not entries: continue
        symbol_name, exchange_type = entries[0]['symbol'], entries[0]['exchange_type']
        if token in previous_day_high_cache and previous_day_high_cache[token].get('date') == cache_check_date_str:
            logger.info(f"Last 3 Days' High for {symbol_name} (Token: {token}): {previous_day_high_cache[token]['high']:.2f} (from cache)")
            time.sleep(0.1)
            continue
        exchange_str = {1: "NSE", 2: "NFO", 3: "BSE"}.get(exchange_type)
        if not exchange_str:
            logger.warning(f"Cannot fetch previous day history for token {token}, unknown exchange type {exchange_type}"); time.sleep(1); continue
        for attempt in range(MAX_RETRIES):
            try:
                historic_param = {"exchange": exchange_str, "symboltoken": token, "interval": "ONE_DAY", "fromdate": from_date_str, "todate": to_date_str}
                response = smart_api_obj.getCandleData(historic_param)
                if response and response.get("status") and response.get("data"):
                    if response["data"]:
                        all_highs = [candle[2] for candle in response["data"]]
                        highest_high_last_3_days = max(all_highs)
                        logger.info(f"Last 3 Days' High for {symbol_name} (Token: {token}): {highest_high_last_3_days:.2f} (fetched from API)")
                        previous_day_high_cache[token] = {'date': cache_check_date_str, 'high': highest_high_last_3_days}
                        save_previous_day_high_cache()
                        break
                    else:
                        logger.warning(f"No previous day's candle data found for {token} ({symbol_name})."); break
                else:
                    logger.warning(f"Attempt {attempt + 1}/{MAX_RETRIES}: Could not fetch data for {symbol_name}. Message: {response.get('message', 'Unknown error')}")
                    if attempt < MAX_RETRIES - 1: time.sleep(RETRY_DELAY_SECONDS)
            except Exception as e:
                logger.warning(f"Attempt {attempt + 1}/{MAX_RETRIES}: Exception while fetching data for {symbol_name}: {e}")
                if attempt < MAX_RETRIES - 1: time.sleep(RETRY_DELAY_SECONDS)
        time.sleep(0.4)
def fetch_historical_candles_for_3pct_down(smart_api_obj, tokens_to_fetch, interval_api): # Fetches historical data for price/volume setups from previous week's Monday.
    today = get_ist_time()
    if today.weekday() == 5: to_dt = today - timedelta(days=1)
    elif today.weekday() == 6: to_dt = today - timedelta(days=2)
    else: to_dt = today
    to_dt = to_dt.replace(hour=23, minute=59, second=59)
    days_to_last_monday = to_dt.weekday() + 7
    from_dt = datetime.datetime.combine(to_dt.date() - timedelta(days=days_to_last_monday), datetime.time.min)
    from_date_str, to_date_str = from_dt.strftime("%Y-%m-%d %H:%M"), to_dt.strftime("%Y-%m-%d %H:%M")
    logger.info(f"Fetching {interval_api} candles from {from_date_str} to {to_date_str}...")
    with data_lock:
        setup_details_copy = excel_3pct_setup_details.copy()
        orh_setup_copy = excel_orh_setup_details.copy()
        all_tokens_to_fetch = set(tokens_to_fetch)
        for token, details in orh_setup_copy.items():
            if details: all_tokens_to_fetch.add((token, details[0]['exchange_type']))
        tokens_to_fetch_unique = list(all_tokens_to_fetch)
    for token_info in tokens_to_fetch_unique:
        token, exchange_type = token_info[0], token_info[1]
        symbol_name = 'Unknown'
        if token in setup_details_copy and setup_details_copy[token]: symbol_name = setup_details_copy[token][0].get('symbol', 'Unknown')
        elif token in orh_setup_copy and orh_setup_copy[token]: symbol_name = orh_setup_copy[token][0].get('symbol', 'Unknown')
        exchange_str = {1: "NSE", 2: "NFO", 3: "BSE"}.get(exchange_type)
        if not exchange_str:
            logger.warning(f"Cannot fetch history for token {token}, unknown exchange type {exchange_type}"); time.sleep(1); continue
        try:
            historic_param = {"exchange": exchange_str, "symboltoken": token, "interval": interval_api, "fromdate": from_date_str, "todate": to_date_str}
            response = smart_api_obj.getCandleData(historic_param)
            if response and response.get("status") and response.get("data"):
                candle_data = response["data"]
                candle_history = []
                for c in candle_data: candle_history.append({'start_time': datetime.datetime.fromisoformat(c[0]), 'open': c[1], 'high': c[2], 'low': c[3], 'close': c[4], 'volume': c[5] if len(c) > 5 else 0})
                with data_lock: volume_history_3pct[token][interval_api] = candle_history
                logger.info(f"Fetched {len(candle_history)} candles for {symbol_name} ({interval_api}).")
            else: logger.warning(f"Fetch error for {symbol_name} ({interval_api}). Message: {response.get('message', 'Unknown error')}")
        except Exception as e: logger.error(f"Exception fetching data for {symbol_name} ({interval_api}): {e}")
        time.sleep(0.5)
def fetch_monthly_highs(smart_api_obj, tokens_to_fetch): # Fetches high of current and previous month, storing the higher value.
    global monthly_high_cache
    logger.info("Fetching monthly high data for Swing Low setup...")
    today = datetime.date.today()
    prev_month_start = (today.replace(day=1) - timedelta(days=1)).replace(day=1)
    from_date_str, to_date_str = prev_month_start.strftime("%Y-%m-%d %H:%M"), today.strftime("%Y-%m-%d %H:%M")
    with data_lock: tokens_to_fetch_copy = list(tokens_to_fetch)
    for token, exchange_type in tokens_to_fetch_copy:
        exchange_str = {1: "NSE", 2: "NFO", 3: "BSE"}.get(exchange_type)
        if not exchange_str: continue
        try:
            historic_param = {"exchange": exchange_str, "symboltoken": token, "interval": "ONE_DAY", "fromdate": from_date_str, "todate": to_date_str}
            response = smart_api_obj.getCandleData(historic_param)
            if response and response.get("status") and response.get("data"):
                data = response["data"]
                current_month_high, prev_month_high = 0, 0
                for c in data:
                    candle_date = datetime.datetime.fromisoformat(c[0]).date()
                    candle_high = c[2]
                    if candle_date.month == today.month and candle_date.year == today.year: current_month_high = max(current_month_high, candle_high)
                    elif candle_date.month == prev_month_start.month and candle_date.year == prev_month_start.year: prev_month_high = max(prev_month_high, candle_high)
                final_high = max(current_month_high, prev_month_high)
                if final_high > 0:
                    with data_lock: monthly_high_cache[token] = final_high
                    logger.info(f"Updated monthly high for token {token}: {final_high:.2f}")
            else: logger.warning(f"Could not fetch monthly data for token {token}. Response: {response.get('message', 'Unknown error')}")
        except Exception as e: logger.error(f"Exception fetching monthly data for token {token}: {e}")
        time.sleep(0.5)

# --- Hybrid ORH Logic ---
def schedule_orh_check(token): # Schedules the historical ORH check to run after a delay.
    logger.info(f"[ORH EVENT] New 5-Min candle completed for token {token}. Scheduling automatic check.")
    threading.Thread(target=find_and_process_orh_breakout, args=(token,), daemon=True).start()
def find_and_process_orh_breakout(token): # AUTOMATIC trigger: Fetches today's 5-min candles and finds the first ORH breakout.
    time.sleep(2)
    with data_lock:
        if token in orh_triggered_today: return
        setup_details_copy = excel_orh_setup_details.copy()
        symbol_entries = setup_details_copy.get(token, [])
        filtered_symbol_entries = [entry for entry in symbol_entries if entry['row'] <= SETUP_MAX_ROW]
    if not filtered_symbol_entries: return
    symbol_name_for_log, exchange_name_for_api = filtered_symbol_entries[0]['symbol'], filtered_symbol_entries[0]['exchange']
    logger.info(f"--- [ORH AUTO CHECK] For {symbol_name_for_log} (Token: {token}) ---")
    prev_high_entry = previous_day_high_cache.get(token)
    if not prev_high_entry or not prev_high_entry.get("high"): return
    prev_high = prev_high_entry["high"]
    try:
        now_ist = get_ist_time()
        from_date, to_date = now_ist.replace(hour=9, minute=15, second=0, microsecond=0).strftime("%Y-%m-%d %H:%M"), now_ist.strftime("%Y-%m-%d %H:%M")
        historic_param = {"exchange": exchange_name_for_api, "symboltoken": token, "interval": "FIVE_MINUTE", "fromdate": from_date, "todate": to_date}
        response = smart_api_obj.getCandleData(historic_param)
        if not (response and response.get("status") and response.get("data")): return
        todays_candles = [{'start_time': datetime.datetime.fromisoformat(c[0]), 'open': c[1], 'high': c[2], 'low': c[3], 'close': c[4]} for c in response["data"]]
    except Exception as e:
        logger.error(f"Error fetching historical 5-min data for {symbol_name_for_log}: {e}"); return
    breakout_candle = None
    for candle in todays_candles:
        high, low, close = candle['high'], candle['low'], candle['close']
        if close > prev_high and high != low and (close >= low + 0.7 * (high - low)):
            breakout_candle = candle
            break
    if breakout_candle:
        logger.info(f"!!! ORH AUTO TRIGGER for {symbol_name_for_log} based on breakout candle at {breakout_candle['start_time']} !!!")
        with data_lock: orh_triggered_today.add(token)
        day_low = latest_quote_data.get(token, {}).get('day_low')
        if not day_low:
            logger.warning(f"Day's low not available for {symbol_name_for_log}, falling back to candle low for SL.")
            day_low = breakout_candle['low']
        process_orh_actions(token, breakout_candle['high'], day_low, breakout_candle['start_time'])
def process_manual_orh_trigger(token, row): # MANUAL trigger: Uses current LTP and Day's Low.
    with data_lock:
        if token in orh_triggered_today: return
        orh_triggered_today.add(token)
    symbol_name = excel_orh_setup_details.get(token, [{}])[0].get('symbol', 'Unknown')
    logger.info(f"!!! ORH MANUAL TRIGGER for {symbol_name} on row {row} !!!")
    ltp = latest_tick_data.get(token, {}).get('ltp')
    day_low = latest_quote_data.get(token, {}).get('day_low')
    if not ltp or not day_low:
        logger.error(f"Could not trigger manual ORH for {symbol_name}: Missing LTP or Day's Low data.")
        with data_lock: orh_triggered_today.remove(token)
        return
    process_orh_actions(token, ltp, day_low, get_ist_time())
def process_orh_actions(token, buy_price, sl_base_price, trigger_time): # Unified function to handle all actions after an ORH trigger.
    updates_queued = []
    with data_lock:
        symbol_entries = excel_orh_setup_details.get(token, [])
        filtered_symbol_entries = [entry for entry in symbol_entries if entry['row'] <= SETUP_MAX_ROW]
    if not filtered_symbol_entries: return
    buy_stop_value, update_time_str = round(sl_base_price * 0.995, 2), trigger_time.strftime('%H:%M')
    new_g_value = f"Yes, {buy_price:.2f} ({update_time_str})"
    if buy_price > 0:
        stop_loss_pct = (buy_price - buy_stop_value) / buy_price
        buy_stop_output = f"{buy_stop_value:.2f} ({stop_loss_pct:.2%})"
    else: buy_stop_output = f"{buy_stop_value:.2f}"
    if winsound:
        try: winsound.Beep(1000, 400)
        except Exception as e: logger.warning(f"Sound alert failed: {e}")
    for entry in filtered_symbol_entries:
        row = entry["row"]
        updates_queued.extend([{"range": f"{SETUP_RESULT_COL}{row}", "values": [[new_g_value]]}, {"range": f"{SETUP_STOP_COL}{row}", "values": [[buy_stop_output]]}, {"range": f"{SETUP_LOG_COL}{row}", "values": [["Buy"]]}])
        trigger_apps_script_alert("new_trade", row, entry['symbol'], entry['exchange'])
        time.sleep(1)
        try:
            qty_str = Dashboard.acell(f"{SETUP_QTY_COL}{row}").value
            quantity = int(qty_str) if qty_str and qty_str.isdigit() else 0
            if quantity <= 0:
                logger.warning(f"Cannot create order for {entry['symbol']}, quantity is missing or zero.")
                continue
            order_trigger_price = round(buy_price * 1.005, 2)
            new_order_row = [get_ist_time().strftime("%Y-%m-%d %H:%M:%S"), entry['symbol'], entry['exchange'], "BUY", "STOPLOSS_MARKET", quantity, order_trigger_price, ""]
            OrdersSheet.append_row(new_order_row, value_input_option='USER_ENTERED')
            logger.info(f"Successfully created a pre-filled order row for {entry['symbol']}.")
        except Exception as e: logger.exception(f"Failed to create order row for {entry['symbol']}: {e}")
    if updates_queued:
        try:
            Dashboard.batch_update(updates_queued)
            logger.info(f"Applied {len(updates_queued)} ORH updates to Dashboard.")
        except Exception as e: logger.error(f"Failed to apply ORH updates to Google Sheet: {e}")

# --- START: MODIFIED Breakdown Logic ---
def get_consolidated_signals(candle_dict): # NEW: Consolidates signals and returns both a sheet-friendly string and a list of candle objects.
    available_candles = [{'candle': c, 'interval_api': interval_api} for interval_api, c in candle_dict.items() if c]
    if not available_candles: return "", []
    available_candles.sort(key=lambda x: x['candle']['low'])
    groups = []
    if available_candles:
        current_group = [available_candles[0]]
        for i in range(1, len(available_candles)):
            if available_candles[i]['candle']['low'] <= current_group[0]['candle']['low'] * 1.02: current_group.append(available_candles[i])
            else:
                groups.append(current_group)
                current_group = [available_candles[i]]
        groups.append(current_group)
    unique_signals = [max(group, key=lambda info: CANDLE_INTERVALS_3PCT_API.index(info['interval_api'])) for group in groups]
    sorted_unique_signals = sorted(unique_signals, key=lambda info: info['candle']['low'], reverse=True)
    output_parts = [f"{signal_info['candle']['low']:.2f} ({CANDLE_INTERVAL_MAP_DISPLAY[signal_info['interval_api']]})" for signal_info in sorted_unique_signals]
    final_candles = [info['candle'] for info in sorted_unique_signals]
    return ", ".join(output_parts), final_candles
def check_and_update_price_volume_setups(): # MODIFIED: Checks for setups, formats for sheet, and caches precise candle times.
    global support_candle_details
    logger.info("Checking for Price/Volume setups and caching support candle details...")
    updates_queued = []
    new_support_details = {}
    with data_lock:
        setup_details_copy = excel_3pct_setup_details.copy()
        volume_history_copy = volume_history_3pct.copy()
    for token, symbol_entries in setup_details_copy.items():
        three_pct_down_candidates, high_vol_candidates, highest_up_candidates = {}, {}, {}
        for interval_api in CANDLE_INTERVALS_3PCT_API:
            candle_history = volume_history_copy.get(token, {}).get(interval_api)
            if not candle_history: continue
            triggered_3pct = [c for c in candle_history if c.get('high', 0) > 0 and (c['high'] - c['close']) / c['high'] >= 0.03]
            if triggered_3pct: three_pct_down_candidates[interval_api] = triggered_3pct[-1]
            if any(c.get('volume', 0) > 0 for c in candle_history): high_vol_candidates[interval_api] = max(candle_history, key=lambda c: c.get('volume', 0))
            gainer_candles = [c for c in candle_history if c.get('open', 0) > 0]
            if gainer_candles: highest_up_candidates[interval_api] = max(gainer_candles, key=lambda c: (c['close'] - c['open']) / c['open'])
        
        # Get consolidated signals and precise candle objects
        pct_down_str, pct_down_candles = get_consolidated_signals(three_pct_down_candidates)
        high_vol_str, high_vol_candles = get_consolidated_signals(high_vol_candidates)
        highest_up_str, highest_up_candles = get_consolidated_signals(highest_up_candidates)

        for entry in symbol_entries:
            row = entry["row"]
            # Queue sheet updates
            updates_queued.append({"range": f"{PCT_DOWN_RESULT_COL}{row}", "values": [[pct_down_str]]})
            updates_queued.append({"range": f"{HIGH_VOL_RESULT_COL}{row}", "values": [[high_vol_str]]})
            updates_queued.append({"range": f"{HIGHEST_UP_CANDLE_COL}{row}", "values": [[highest_up_str]]})
            # Populate the new cache with precise candle data for this row
            new_support_details[(row, '3% Down')] = [{'price': c['low'], 'time': c['start_time']} for c in pct_down_candles]
            new_support_details[(row, 'High Volume')] = [{'price': c['low'], 'time': c['start_time']} for c in high_vol_candles]
            new_support_details[(row, 'Highest Up Candle')] = [{'price': c['low'], 'time': c['start_time']} for c in highest_up_candles]
    
    with data_lock: support_candle_details = new_support_details
    if updates_queued:
        Dashboard.batch_update(updates_queued, value_input_option='USER_ENTERED')
        logger.info(f"Applied {len(updates_queued)} Price/Volume setup updates to Dashboard and refreshed support cache.")
    else: logger.info("No Price/Volume setup updates were needed.")
def check_and_update_all_breakdown_statuses(): # MODIFIED: Finds the *exact* 15-min candle that first breached the support level for all setups.
    logger.info("Checking all breakdown statuses with precise candle-by-candle validation...")
    requests_queued = []
    RED_COLOR, dashboard_sheet_id = (254, 112, 112), Dashboard.id
    with data_lock:
        setup_details_copy = excel_3pct_setup_details.copy()
        volume_history_copy = volume_history_3pct.copy()
        support_details_copy = support_candle_details.copy()
    if not setup_details_copy: return
    try:
        cols_to_read = [TRAILING_STOP_INPUT_COL, TRAILING_STOP_STATUS_COL, HIGHEST_UP_CANDLE_STATUS_COL, HIGH_VOL_STATUS_COL, PCT_DOWN_STATUS_COL]
        start_col, end_col = min(cols_to_read, key=col_to_num), max(cols_to_read, key=col_to_num)
        last_row = max((e['row'] for token, entries in setup_details_copy.items() for e in entries if entries), default=0)
        if last_row < START_ROW_DATA: return
        sheet_data = Dashboard.get(f"{start_col}{START_ROW_DATA}:{end_col}{last_row}")
        def get_sheet_value(row_idx, col_letter):
            if row_idx < 0 or row_idx >= len(sheet_data): return ""
            col_idx = col_to_num(col_letter) - col_to_num(start_col)
            if col_idx < 0 or col_idx >= len(sheet_data[row_idx]): return ""
            return sheet_data[row_idx][col_idx]
    except Exception as e:
        logger.error(f"Failed to fetch breakdown data from Google Sheet: {e}"); return
    for token, symbol_entries in setup_details_copy.items():
        history_15min = volume_history_copy.get(token, {}).get('FIFTEEN_MINUTE')
        if not history_15min: continue
        for entry in symbol_entries:
            row, row_idx = entry["row"], entry["row"] - START_ROW_DATA
            setups = [{'name': 'Trailing Stop', 'input_col': TRAILING_STOP_INPUT_COL, 'status_col': TRAILING_STOP_STATUS_COL}, {'name': 'Highest Up Candle', 'status_col': HIGHEST_UP_CANDLE_STATUS_COL}, {'name': 'High Volume', 'status_col': HIGH_VOL_STATUS_COL}, {'name': '3% Down', 'status_col': PCT_DOWN_STATUS_COL}]
            for setup in setups:
                current_status = get_sheet_value(row_idx, setup['status_col'])
                first_breakdown_candle, new_status = None, ""
                if 'input_col' in setup: # Logic for manual Trailing Stop
                    trigger_price_str = get_sheet_value(row_idx, setup['input_col'])
                    try: trigger_price = float(str(trigger_price_str).replace(',', '')) if trigger_price_str else None
                    except (ValueError, TypeError): trigger_price = None
                    if trigger_price is None:
                        if current_status.strip() != "": new_status = ""
                        else: continue
                    else:
                        breakdown_candidates = [c for c in history_15min if c['close'] < trigger_price]
                        if breakdown_candidates: first_breakdown_candle = min(breakdown_candidates, key=lambda c: c['start_time'])
                else: # Logic for automated setups
                    support_levels = support_details_copy.get((row, setup['name']), [])
                    if not support_levels:
                        if current_status.strip() != "": new_status = ""
                        else: continue
                    else:
                        for level in support_levels:
                            support_price, support_time = level['price'], level['time']
                            breakdown_candidates = [c for c in history_15min if c['start_time'] > support_time and c['close'] < support_price]
                            if breakdown_candidates:
                                earliest_breakdown = min(breakdown_candidates, key=lambda c: c['start_time'])
                                if first_breakdown_candle is None or earliest_breakdown['start_time'] < first_breakdown_candle['start_time']:
                                    first_breakdown_candle = earliest_breakdown
                if first_breakdown_candle:
                    candle_close_time = first_breakdown_candle['start_time'] + timedelta(minutes=15)
                    new_status = f"Yes ({candle_close_time.strftime('%d %b, %I:%M %p')})"
                else: new_status = ""
                if new_status.strip() != current_status.strip(): # Queue updates only if status has changed
                    cell_range = {"sheetId": dashboard_sheet_id, "startRowIndex": row - 1, "endRowIndex": row, "startColumnIndex": col_to_num(setup['status_col']) - 1, "endColumnIndex": col_to_num(setup['status_col'])}
                    bg_color = RED_COLOR if new_status.startswith("Yes") else None
                    cell_data = {"userEnteredValue": {"stringValue": new_status}, "userEnteredFormat": {"backgroundColor": rgb_to_float(bg_color)}}
                    fields = "userEnteredValue,userEnteredFormat.backgroundColor"
                    requests_queued.append({"updateCells": {"rows": [{"values": [cell_data]}], "fields": fields, "range": cell_range}})
    if requests_queued:
        gsheet.batch_update({'requests': requests_queued})
        logger.info(f"Applied {len(requests_queued)} precise breakdown status updates to Dashboard.")
    else: logger.info("No precise breakdown status updates were needed.")
# --- END: MODIFIED Breakdown Logic ---

def check_and_update_breakdown_status(): # Checks for a change in breakdown status to trigger the "Sell" alert.
    global previous_breakdown_state
    logger.info("Checking for breakdown status changes to trigger alerts...")
    value_updates = []
    with data_lock: setup_details_copy = excel_3pct_setup_details.copy()
    if not setup_details_copy: return
    setups_to_check = [{'status_col': TRAILING_STOP_STATUS_COL}, {'status_col': HIGHEST_UP_CANDLE_STATUS_COL}, {'status_col': HIGH_VOL_STATUS_COL}, {'status_col': PCT_DOWN_STATUS_COL}]
    try:
        all_cols = [s['status_col'] for s in setups_to_check]
        start_col, end_col = min(all_cols, key=col_to_num), max(all_cols, key=col_to_num)
        last_row = max((e['row'] for token, entries in setup_details_copy.items() for e in entries if entries), default=0)
        if last_row < START_ROW_DATA: return
        range_to_get = f"{start_col}{START_ROW_DATA}:{end_col}{last_row}"
        sheet_data = Dashboard.get(range_to_get)
    except Exception as e:
        logger.error(f"Failed to fetch setup data from Google Sheet for alert check: {e}"); return
    for token, symbol_entries in setup_details_copy.items():
        if token in sell_triggered_today: continue
        breakdown_detected_this_cycle = False
        for entry in symbol_entries:
            row, row_idx = entry["row"], entry["row"] - START_ROW_DATA
            if row_idx < 0 or row_idx >= len(sheet_data): continue
            for setup in setups_to_check:
                status_col_letter = setup['status_col']
                status_col_idx = col_to_num(status_col_letter) - col_to_num(start_col)
                current_status = ""
                if status_col_idx < len(sheet_data[row_idx]): current_status = str(sheet_data[row_idx][status_col_idx])
                cell_key = (row, status_col_letter)
                previous_status = str(previous_breakdown_state.get(cell_key, ""))
                is_breakdown_now = "yes" in current_status.lower()
                was_breakdown_before = "yes" in previous_status.lower()
                if is_breakdown_now and not was_breakdown_before:
                    logger.info(f"!!! NEW BREAKDOWN DETECTED for {entry['symbol']} on row {row} in column {status_col_letter} !!!")
                    breakdown_detected_this_cycle = True
                previous_breakdown_state[cell_key] = current_status
                if breakdown_detected_this_cycle: break
            if breakdown_detected_this_cycle: break
        if breakdown_detected_this_cycle:
            logger.info(f"!!! AUTO SELL TRIGGER for {symbol_entries[0]['symbol']} due to new breakdown. !!!")
            with data_lock: sell_triggered_today.add(token)
            for entry in symbol_entries:
                row = entry["row"]
                value_updates.append({"range": f"{ACTION_COL}{row}", "values": [["Sell"]]})
                trigger_apps_script_alert("position_closed", row, entry['symbol'], entry['exchange'])
                time.sleep(1)
    if value_updates:
        Dashboard.batch_update(value_updates, value_input_option='USER_ENTERED')
        logger.info(f"Applied {len(value_updates)} automatic sell updates to Dashboard.")

# --- Order Tracking Logic ---
def check_and_place_orders(): # Scans the 'Orders' sheet for 'PENDING', validates, and submits them to the broker.
    try:
        if not OrdersSheet:
            logger.warning("OrdersSheet object not initialized. Skipping order placement."); return
        all_values = OrdersSheet.get_all_values()
        if len(all_values) < 3: return
        updates_to_make = []
        for idx, row_data in enumerate(all_values):
            row_num = idx + 1
            if row_num < 3: continue
            try:
                if len(row_data) < 12: continue
                trigger_status = str(row_data[11]).strip().upper()
                if trigger_status != 'PENDING': continue
                logger.info(f"Found a pending submission on row {row_num}: {row_data}")
                updates_to_make.extend([{'range': f'J{row_num}', 'values': [['PROCESSING']]}, {'range': f'L{row_num}', 'values': [['']]}])
                symbol, exchange, action, order_type = str(row_data[2]), str(row_data[3]), str(row_data[4]).upper(), str(row_data[5]).upper()
                quantity, price_or_trigger = int(row_data[6]), float(row_data[7] or 0)
                if not all([symbol, exchange, action, order_type, quantity > 0]): raise ValueError("Missing or invalid required fields (Symbol, Exchange, Action, Type, Qty)")
                token_cache, instrument_info = {}, get_or_fetch_instrument_details(symbol, exchange, token_cache)
                if not instrument_info: raise ValueError(f"Could not find token for symbol {symbol}")
                token, order_id_or_rule_id, response_data = instrument_info['token'], None, None
                if order_type == 'GTT':
                    logger.info(f"Submitting GTT order for row {row_num}...")
                    gtt_params = {"tradingsymbol": symbol, "symboltoken": token, "exchange": exchange, "transactiontype": action, "producttype": "DELIVERY", "price": price_or_trigger, "qty": quantity, "triggerprice": price_or_trigger, "disclosedqty": 0, "timeperiod": 365}
                    response_data = smart_api_obj.gttCreateRule(gtt_params)
                    if isinstance(response_data, int) and response_data > 0: order_id_or_rule_id = response_data
                    elif isinstance(response_data, dict) and response_data.get("data"):
                        data_payload = response_data["data"]
                        order_id_or_rule_id = data_payload.get("ruleid") if isinstance(data_payload, dict) else data_payload
                    if not order_id_or_rule_id:
                        error_message = response_data.get("message", "GTT creation failed.") if isinstance(response_data, dict) else "GTT creation failed."
                        logger.error(f"Full API response for failed GTT: {response_data}")
                        raise DataException(error_message)
                else:
                    logger.info(f"Submitting regular order for row {row_num}...")
                    order_params = {"variety": "NORMAL", "tradingsymbol": symbol, "symboltoken": token, "transactiontype": action, "exchange": exchange, "ordertype": order_type, "producttype": "DELIVERY", "duration": "DAY", "quantity": quantity}
                    if order_type == 'LIMIT': order_params["price"] = price_or_trigger
                    elif order_type == 'STOPLOSS_MARKET': order_params["triggerprice"], order_params["price"] = price_or_trigger, 0.0
                    else: order_params["price"] = 0.0
                    response_data = smart_api_obj.placeOrder(order_params)
                    if isinstance(response_data, dict) and response_data.get("data", {}).get("orderid"): order_id_or_rule_id = response_data["data"]["orderid"]
                    else:
                        error_message = response_data.get("message", "Order placement failed.") if isinstance(response_data, dict) else "Order placement failed."
                        logger.error(f"Full API response for failed order: {response_data}")
                        raise DataException(error_message)
                logger.info(f"Order for row {row_num} accepted by broker. ID: {order_id_or_rule_id}")
                updates_to_make.append({'range': f'J{row_num}:K{row_num}', 'values': [['ACCEPTED', str(order_id_or_rule_id)]]})
            except Exception as e:
                error_text = str(e.message) if hasattr(e, 'message') and e.message else str(e)
                logger.error(f"Failed to process order for row {row_num}: {error_text}")
                updates_to_make.append({'range': f'J{row_num}:K{row_num}', 'values': [['ERROR', error_text]]})
            time.sleep(1)
        if updates_to_make:
            OrdersSheet.batch_update(updates_to_make, value_input_option='USER_ENTERED')
            logger.info(f"Applied {len(updates_to_make)} order placement updates to the Orders sheet.")
    except Exception as e: logger.exception(f"An error occurred in the main order placement function: {e}")
def check_and_update_order_statuses(): # Efficiently queries broker for live status of all trackable orders and updates the sheet.
    try:
        if not OrdersSheet or not smart_api_obj: return
        all_order_rows = OrdersSheet.get_all_values()
        if len(all_order_rows) < 3: return
        all_gtt_rules_raw, order_book_raw = {}, {}
        try: all_gtt_rules_raw = smart_api_obj.gttLists(status=['active', 'triggered'], page=1, count=200)
        except DataException as e: logger.warning(f"Could not fetch GTT list, API returned an error or empty response: {e}. Assuming no active GTTs.")
        except Exception as e: logger.error(f"An unexpected error occurred while fetching GTT list: {e}")
        try: order_book_raw = smart_api_obj.orderBook()
        except DataException as e: logger.warning(f"Could not fetch order book, API returned an error or empty response: {e}. Assuming no open orders.")
        except Exception as e: logger.error(f"An unexpected error occurred while fetching the order book: {e}")
        gtt_status_map = {str(rule['id']): rule for rule in all_gtt_rules_raw.get('data', [])} if all_gtt_rules_raw and all_gtt_rules_raw.get('data') else {}
        order_status_map = {order['orderid']: order for order in order_book_raw.get('data', [])} if order_book_raw and order_book_raw.get('data') else {}
        updates_queued = []
        for idx, row in enumerate(all_order_rows):
            row_num = idx + 1
            if row_num < 3: continue
            try:
                if len(row) < 11: continue
                current_status_on_sheet, order_id, order_type = str(row[9]).strip().upper(), str(row[10]).strip(), str(row[5]).strip().upper()
                if current_status_on_sheet in ['COMPLETED', 'REJECTED', 'CANCELLED', 'ERROR', 'EXPIRED'] or not order_id: continue
                new_status_normalized, raw_api_status = None, None
                if order_type == 'GTT':
                    if order_id in gtt_status_map:
                        raw_api_status, new_status_normalized = gtt_status_map[order_id]['status'], normalize_status(gtt_status_map[order_id]['status'])
                        if new_status_normalized == 'Triggered - Awaiting ID':
                            logger.info(f"GTT rule {order_id} triggered. Searching for matching exchange order...")
                            gtt_rule, found_match = gtt_status_map[order_id], False
                            for live_order in order_status_map.values():
                                if live_order['tradingsymbol'] == gtt_rule['tradingsymbol'] and str(live_order['quantity']) == str(gtt_rule['qty']) and live_order['transactiontype'] == gtt_rule['transactiontype']:
                                    new_exchange_id = live_order['orderid']
                                    logger.info(f"Found matching exchange order for GTT {order_id}. New Order ID: {new_exchange_id}")
                                    updates_queued.extend([{'range': f'F{row_num}', 'values': [['GTT-LIMIT']]}, {'range': f'K{row_num}', 'values': [[new_exchange_id]]}])
                                    raw_api_status, new_status_normalized, found_match = live_order['status'], normalize_status(live_order['status']), True
                                    break
                            if not found_match: logger.warning(f"GTT {order_id} is triggered, but no matching order found in order book yet.")
                    else:
                        if current_status_on_sheet == 'ACCEPTED':
                            logger.info(f"GTT Rule {order_id} not yet in list (propagation delay). Keeping status as ACCEPTED.")
                            new_status_normalized = 'ACCEPTED'
                        else: raw_api_status, new_status_normalized = 'not_found_in_gtt_list', normalize_status('not_found_in_gtt_list')
                elif order_type in ['LIMIT', 'MARKET', 'STOPLOSS_MARKET', 'GTT-LIMIT']:
                    if order_id in order_status_map: raw_api_status, new_status_normalized = order_status_map[order_id]['status'], normalize_status(order_status_map[order_id]['status'])
                    else: raw_api_status, new_status_normalized = 'not_found_in_order_book', normalize_status('not_found_in_order_book')
                if new_status_normalized and new_status_normalized.upper() != current_status_on_sheet:
                    logger.info(f"Updating status for ID {order_id} (Row {row_num}) from '{current_status_on_sheet}' to '{new_status_normalized}'")
                    updates_queued.append({'range': f'J{row_num}', 'values': [[new_status_normalized]]})
            except Exception as e:
                logger.warning(f"Could not check status for row {row_num}: {e}"); continue
        if updates_queued:
            OrdersSheet.batch_update(updates_queued, value_input_option='USER_ENTERED')
            logger.info(f"Applied {len(updates_queued)} live order status updates to the Orders sheet.")
    except Exception as e: logger.exception(f"An error occurred in the order status checker function: {e}")
def get_cell_value(sheet_values, row, col_letter): # Helper to safely get a cell value from pre-fetched sheet data.
    if not col_letter: return None
    col_idx, row_idx = col_to_num(col_letter) - 1, row - 1
    if 0 <= row_idx < len(sheet_values) and 0 <= col_idx < len(sheet_values[row_idx]): return sheet_values[row_idx][col_idx]
    return None
def get_or_fetch_instrument_details(symbol_name, exchange_name, session_cache): # Gets instrument details from the pre-downloaded master list.
    global instrument_master_list
    if not symbol_name or str(symbol_name).strip().upper() in ('SYMBOL', ''): return None
    cache_key = (symbol_name.strip().upper(), exchange_name.strip().upper())
    if cache_key in session_cache: return session_cache[cache_key]
    instrument_details, symbol_from_sheet, exchange_clean = None, symbol_name.strip().upper(), exchange_name.strip().upper()
    for instrument in instrument_master_list:
        if instrument.get('exch_seg') == exchange_clean:
            if instrument.get('tradingsymbol') == symbol_from_sheet or instrument.get('symbol') == symbol_from_sheet:
                instrument_details = {"token": instrument.get('token'), "name": instrument.get('name')}
                break
    if instrument_details: session_cache[cache_key] = instrument_details
    else: logger.warning(f"Could not find details for '{symbol_from_sheet}' on exchange '{exchange_clean}' in the master instrument list.")
    return instrument_details
def scan_sheet_for_all_symbols(Dashboard, ATHCache): # Unified function to scan the Google Sheet and manage the ATH Cache.
    logger.info("Scanning Google Sheet for all symbols (Dashboard and Setups)...")
    local_dashboard_details, local_orh_setup_details, local_3pct_setup_details = collections.defaultdict(list), collections.defaultdict(list), collections.defaultdict(list)
    all_tokens_found, scan_session_token_cache, expected_ath_cache_state, ath_cache_updates_queued = set(), {}, {}, []
    try:
        all_dashboard_values, all_ath_cache_values = Dashboard.get_all_values(), ATHCache.get_all_values()
        symbol_token_map = {}
        for row_idx, row_data in enumerate(all_ath_cache_values):
            row_num = row_idx + 1
            if len(row_data) > col_to_num(ATH_CACHE_Y_COL_DASH) -1:
                token, symbol = row_data[col_to_num(ATH_CACHE_Y_COL_DASH) - 1], get_cell_value(all_dashboard_values, row_num, FOCUS_SYMBOL_COL)
                if token and symbol: symbol_token_map[(row_num, ATH_CACHE_Y_COL_DASH)] = (symbol.strip().upper(), token)
            if len(row_data) > col_to_num(ATH_CACHE_Z_COL_DASH) -1:
                token, symbol = row_data[col_to_num(ATH_CACHE_Z_COL_DASH) - 1], get_cell_value(all_dashboard_values, row_num, FULL_SYMBOL_COL)
                if token and symbol: symbol_token_map[(row_num, ATH_CACHE_Z_COL_DASH)] = (symbol.strip().upper(), token)
        last_row_focus, last_row_full = get_last_row_in_column(Dashboard, FOCUS_SYMBOL_COL), get_last_row_in_column(Dashboard, FULL_SYMBOL_COL)
        max_row_dashboard = max(last_row_focus, last_row_full)
        logger.info(f"Scanning Dashboard up to row {max_row_dashboard}...")
        for row in range(START_ROW_DATA, max_row_dashboard + 20):
            def process_symbol(symbol, exchange, row_num, token_col, block_details):
                symbol_clean, symbol_col = str(symbol).strip().upper(), block_details.get('symbol_col')
                if not symbol or not exchange or symbol_clean == 'SYMBOL': return
                cache_key = (row_num, symbol_col)
                if scan_memory_cache.get(cache_key) != symbol_clean:
                    logger.info(f"New or changed symbol '{symbol_clean}' found at {cache_key}. Fetching new details.")
                    scan_memory_cache[cache_key] = symbol_clean
                instrument_info = get_or_fetch_instrument_details(symbol, exchange, scan_session_token_cache)
                if instrument_info and instrument_info.get('token'):
                    token = instrument_info['token']
                    all_tokens_found.add(token)
                    block_details['name'] = instrument_info.get('name')
                    if 'ltp_col' in block_details: local_dashboard_details[token].append({'row': row_num, 'symbol': symbol, 'exchange': exchange, **block_details})
                    if 'setup_type' in block_details:
                        exchange_type_int = {'NSE': 1, 'NFO': 2, 'BSE': 3}.get(str(exchange).strip().upper())
                        if exchange_type_int:
                            setup_info = {'symbol': symbol, 'row': row_num, 'exchange_type': exchange_type_int, 'name': block_details['name'], 'exchange': exchange}
                            if block_details['setup_type'] == 'ORH': local_orh_setup_details[token].append(setup_info)
                            elif block_details['setup_type'] == '3PCT': local_3pct_setup_details[token].append(setup_info)
                    if token_col: expected_ath_cache_state[(row_num, token_col)] = token
            exchange_focus, symbol_focus = get_cell_value(all_dashboard_values, row, FOCUS_EXCHANGE_COL), get_cell_value(all_dashboard_values, row, FOCUS_SYMBOL_COL)
            if exchange_focus and symbol_focus: process_symbol(symbol_focus, exchange_focus, row, ATH_CACHE_Y_COL_DASH, {'ltp_col': FOCUS_LTP_COL, 'chg_col': FOCUS_CHG_COL, 'block_type': 'Focus List', 'symbol_col': FOCUS_SYMBOL_COL, 'token_cache_col': ATH_CACHE_Y_COL_DASH})
            exchange_full, symbol_full = get_cell_value(all_dashboard_values, row, FULL_EXCHANGE_COL), get_cell_value(all_dashboard_values, row, FULL_SYMBOL_COL)
            if exchange_full and symbol_full: process_symbol(symbol_full, exchange_full, row, ATH_CACHE_Z_COL_DASH, {'ltp_col': FULL_LTP_COL, 'chg_col': '', 'block_type': 'Full Positions', 'symbol_col': FULL_SYMBOL_COL, 'token_cache_col': ATH_CACHE_Z_COL_DASH, 'price_col': FULL_PRICE_COL, 'qty_col': FULL_QTY_COL, 'return_amt_col': FULL_RETURN_AMT_COL, 'return_pct_col': FULL_RETURN_PCT_COL, 'swing_low_input_col': SWING_LOW_INPUT_COL, 'percent_from_swing_low_col': PERCENT_FROM_SWING_LOW_COL, 'highest_up_candle_col': HIGHEST_UP_CANDLE_COL, 'entry_date_col': FULL_ENTRY_DATE_COL, 'days_duration_col': FULL_DAYS_DURATION_COL})
            exchange_setup, symbol_setup = get_cell_value(all_dashboard_values, row, SETUP_EXCHANGE_COL), get_cell_value(all_dashboard_values, row, SETUP_SYMBOL_COL)
            if exchange_setup and symbol_setup and row <= SETUP_MAX_ROW:
                if len(symbol_setup) < 15: process_symbol(symbol_setup, exchange_setup, row, SETUP_TOKEN_COL, {'setup_type': 'ORH', 'symbol_col': SETUP_SYMBOL_COL})
            exchange_3pct, symbol_3pct = get_cell_value(all_dashboard_values, row, PCT_EXCHANGE_COL_3PCT), get_cell_value(all_dashboard_values, row, PCT_SYMBOL_COL_3PCT)
            if exchange_3pct and symbol_3pct: process_symbol(symbol_3pct, exchange_3pct, row, PCT_TOKEN_COL_3PCT, {'setup_type': '3PCT', 'symbol_col': PCT_SYMBOL_COL_3PCT})
        max_row_ath_cache_data = len(all_ath_cache_values) if all_ath_cache_values else 0
        rows_to_check_ath_cache, token_cols_to_check = max(max_row_dashboard + 20, max_row_ath_cache_data + 1), [ATH_CACHE_Y_COL_DASH, ATH_CACHE_Z_COL_DASH]
        for row_idx in range(rows_to_check_ath_cache):
            row_num = row_idx + 1
            for col_letter in token_cols_to_check:
                current_token_in_ath_cache, expected_token_for_row_col = get_cell_value(all_ath_cache_values, row_num, col_letter), expected_ath_cache_state.get((row_num, col_letter))
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
            except Exception as e: logger.exception(f"An error occurred during batch update to ATH Cache sheet: {e}")
        else: logger.info("No ATH Cache updates needed.")
    except Exception as e: logger.exception(f"Error during unified symbol scan and ATH Cache management: {e}")
    logger.info(f"Finished unified scan. Found {len(all_tokens_found)} unique tokens.")
    return local_dashboard_details, local_orh_setup_details, local_3pct_setup_details, all_tokens_found
def update_excel_live_data(): # Updates the Google Sheet with live data and Swing Low calculation.
    global cells_to_clear_color
    with data_lock:
        dashboard_details_copy = excel_dashboard_details.copy()
        monthly_high_cache_copy = monthly_high_cache.copy()
    if not smart_ws or not smart_ws._is_connected_flag:
        logger.warning("WebSocket not connected. Skipping Google Sheet update."); return
    requests, cells_to_color_this_cycle = [], set()
    GREEN_COLOR, RED_COLOR, YELLOW_COLOR = (149, 203, 186), (254, 112, 112), (249, 203, 156)
    dashboard_sheet_id = Dashboard.id
    if cells_to_clear_color:
        for cell_a1 in cells_to_clear_color:
            col_letter, row_num = ''.join(filter(str.isalpha, cell_a1)), int(''.join(filter(str.isdigit, cell_a1)))
            cell_range = {"sheetId": dashboard_sheet_id, "startRowIndex": row_num - 1, "endRowIndex": row_num, "startColumnIndex": col_to_num(col_letter) - 1, "endColumnIndex": col_to_num(col_letter)}
            requests.append({"repeatCell": {"range": cell_range, "cell": {"userEnteredFormat": {"backgroundColor": rgb_to_float(None)}}, "fields": "userEnteredFormat.backgroundColor"}})
    cells_to_clear_color.clear()
    input_ranges = []
    for list_of_details in dashboard_details_copy.values():
        for details in list_of_details:
            row_num = details['row']
            if details.get("symbol_col"): input_ranges.append(f'{details["symbol_col"]}{row_num}')
            if details.get('block_type') == "Full Positions":
                if details.get("price_col"): input_ranges.append(f'{details["price_col"]}{row_num}')
                if details.get("qty_col"): input_ranges.append(f'{details["qty_col"]}{row_num}')
                if details.get("entry_date_col"): input_ranges.append(f'{details["entry_date_col"]}{row_num}')
                if details.get("swing_low_input_col"): input_ranges.append(f'{details["swing_low_input_col"]}{row_num}')
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
            logger.error(f"Error fetching dashboard input data in batch: {e}"); return
    for token, list_of_details in dashboard_details_copy.items():
        current_ltp = latest_tick_data.get(token, {}).get('ltp')
        if current_ltp is None: continue
        previous_ltp, ltp_cell_color = previous_ltp_data.get(token), None
        if previous_ltp is not None and current_ltp != previous_ltp: ltp_cell_color = GREEN_COLOR if current_ltp > previous_ltp else RED_COLOR
        previous_ltp_data[token] = current_ltp
        for details in list_of_details:
            row_num = details['row']
            symbol_on_sheet_raw = input_data.get(f"{details.get('symbol_col')}{row_num}")
            symbol_on_sheet = str(symbol_on_sheet_raw).strip().upper() if symbol_on_sheet_raw else ""
            if not symbol_on_sheet:
                if details.get('block_type') == "Focus List": start_col, end_col = 'D', 'J'
                elif details.get('block_type') == "Full Positions": start_col, end_col = 'N', 'AI'
                else: continue
                logger.info(f"Detected cleared symbol at row {row_num}. Queuing fast clear for {start_col}{row_num}:{end_col}{row_num}.")
                requests.append({"repeatCell": {"range": {"sheetId": dashboard_sheet_id, "startRowIndex": row_num - 1, "endRowIndex": row_num, "startColumnIndex": col_to_num(start_col) - 1, "endColumnIndex": col_to_num(end_col)}, "cell": {"userEnteredValue": {}, "userEnteredFormat": {"backgroundColor": rgb_to_float(None)}}, "fields": "userEnteredValue,userEnteredFormat.backgroundColor"}})
                continue
            def queue_update(col_letter, value, number_format_pattern=None, bg_color='SENTINEL', is_ltp=False):
                if not col_letter: return
                cell_a1 = f"{col_letter}{row_num}"
                cell_range = {"sheetId": dashboard_sheet_id, "startRowIndex": row_num - 1, "endRowIndex": row_num, "startColumnIndex": col_to_num(col_letter) - 1, "endColumnIndex": col_to_num(col_letter)}
                cell_data, fields, user_entered_value = {}, [], {}
                if isinstance(value, (int, float)): user_entered_value["numberValue"] = value
                else: user_entered_value["stringValue"] = str(value)
                cell_data["userEnteredValue"], fields = user_entered_value, fields + ["userEnteredValue"]
                user_entered_format, format_fields = {}, []
                if bg_color != 'SENTINEL':
                    user_entered_format["backgroundColor"], format_fields = rgb_to_float(bg_color), format_fields + ["backgroundColor"]
                    if is_ltp and bg_color is not None: cells_to_color_this_cycle.add(cell_a1)
                if number_format_pattern:
                    user_entered_format["numberFormat"], format_fields = {"type": "NUMBER" if isinstance(value, (int, float)) else "TEXT", "pattern": number_format_pattern}, format_fields + ["numberFormat"]
                if user_entered_format:
                    cell_data["userEnteredFormat"] = user_entered_format
                    for key in format_fields: fields.append(f"userEnteredFormat.{key}")
                requests.append({"updateCells": {"rows": [{"values": [cell_data]}], "fields": ",".join(fields), "range": cell_range}})
            queue_update(details.get('ltp_col'), current_ltp, "#,##0.00", bg_color=ltp_cell_color, is_ltp=True)
            if details.get('chg_col') and token in latest_quote_data:
                percentage_change = latest_quote_data[token].get('percentChange', 0.0)
                percentage_change_decimal = percentage_change / 100.0 if percentage_change is not None else 0.0
                chg_cell_color = GREEN_COLOR if percentage_change > 0 else RED_COLOR if percentage_change < 0 else None
                queue_update(details['chg_col'], percentage_change_decimal, "0.00%", bg_color=chg_cell_color)
            if details.get('block_type') == "Full Positions":
                try:
                    price_val_str, qty_val_str = str(input_data.get(f'{details["price_col"]}{row_num}') or '0').replace(',',''), str(input_data.get(f'{details["qty_col"]}{row_num}') or '0').replace(',','')
                    entry_date_str = input_data.get(f'{details["entry_date_col"]}{row_num}')
                    price_val, qty_val = float(price_val_str) if price_val_str else 0, float(qty_val_str) if qty_val_str else 0
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
                except (ValueError, TypeError): queue_update(details.get('percent_from_swing_low_col'), "Invalid Low", "@", bg_color=None)
                if swing_low_val is not None and swing_low_val > 0:
                    monthly_high = monthly_high_cache_copy.get(token)
                    if monthly_high and monthly_high > 0:
                        percent_from_high = (monthly_high - swing_low_val) / swing_low_val
                        cell_color = YELLOW_COLOR if percent_from_high >= 0.35 else None
                        queue_update(details.get('percent_from_swing_low_col'), percent_from_high, "0.00%", bg_color=cell_color)
                    else: queue_update(details.get('percent_from_swing_low_col'), "No High", "@", bg_color=None)
                else: queue_update(details.get('percent_from_swing_low_col'), "", "General", bg_color=None)
                days_duration = ""
                if entry_date_str:
                    try:
                        entry_dt = datetime.datetime.strptime(entry_date_str, '%d-%b-%y')
                        days_duration = f"{(get_ist_time().date() - entry_dt.date()).days} Days"
                    except ValueError: days_duration = "Invalid Date"
                queue_update(details.get('days_duration_col'), days_duration, "@")
    if requests:
        try:
            gsheet.batch_update({'requests': requests})
            logger.info(f"Executed {len(requests)} batch update operations on Google Sheet for dashboard.")
        except Exception as e: logger.exception(f"An error occurred during batch update to Google Sheet: {e}")

# --- Main Application Logic & Threads ---
auth_token, feed_token, websocket_thread, last_login_date = None, None, None, None
def re_authenticate_and_reconnect(): # Handles daily re-authentication and restarts the WebSocket connection.
    global smart_api_obj, smart_ws, auth_token, feed_token, websocket_thread, subscribed_tokens
    logger.info("--- Starting Daily Re-authentication Process ---")
    if smart_ws:
        logger.info("Closing existing WebSocket connection...")
        smart_ws.close_connection()
        if websocket_thread and websocket_thread.is_alive(): websocket_thread.join(timeout=5)
        logger.info("Old WebSocket connection closed.")
        smart_ws = None
    try:
        logger.info("Generating new SmartAPI session for the day...")
        totp = pyotp.TOTP(TOTP_SECRET).now()
        data = smart_api_obj.generateSession(CLIENT_CODE, MPIN, totp=totp)
        if data and data.get('data') and data['data'].get('jwtToken'):
            auth_token, feed_token = data['data']['jwtToken'], data['data']['feedToken']
            logger.info("New SmartAPI session generated successfully!")
        else:
            logger.error(f"Failed to generate new SmartAPI session. Response: {data}. Aborting reconnect."); return False
    except Exception as e:
        logger.exception(f"Exception during new session generation: {e}. Aborting reconnect."); return False
    try:
        logger.info("Initializing new WebSocket connection with fresh tokens...")
        smart_ws = MyWebSocketClient(auth_token, API_KEY, CLIENT_CODE, feed_token)
        websocket_thread = threading.Thread(target=smart_ws.connect, daemon=True)
        websocket_thread.start()
        time.sleep(5)
        if not smart_ws._is_connected_flag:
            logger.error("New WebSocket failed to connect. Live feed will not be available."); return False
        logger.info("New WebSocket connected successfully.")
    except Exception as e:
        logger.exception(f"Exception during new WebSocket initialization: {e}."); return False
    if subscribed_tokens:
        logger.info(f"Re-subscribing to {len(subscribed_tokens)} previously tracked tokens...")
        tokens_to_resubscribe = subscribed_tokens.copy()
        subscribed_tokens.clear()
        subscribe_list_grouped = collections.defaultdict(list)
        with data_lock: all_details = {**excel_dashboard_details, **excel_orh_setup_details, **excel_3pct_setup_details}
        for token in tokens_to_resubscribe:
            exchange_type_num = 1
            if token in all_details and all_details[token]:
                entry = all_details[token][0]
                if 'exchange' in entry: exchange_type_num = {'NSE': 1, 'NFO': 2, 'BSE': 3}.get(entry.get('exchange', 'NSE').upper(), 1)
                elif 'exchange_type' in entry: exchange_type_num = entry.get('exchange_type', 1)
            subscribe_list_grouped[exchange_type_num].append(token)
        for ex_type, tokens in subscribe_list_grouped.items():
            formatted_tokens = [{"exchangeType": ex_type, "tokens": list(tokens)}]
            smart_ws.subscribe(f"resub_{int(time.time())}", smart_ws.QUOTE, formatted_tokens)
            subscribed_tokens.update(tokens)
            logger.info(f"Re-subscribed to {len(tokens)} tokens on exchange type {ex_type}.")
    logger.info("--- Daily Re-authentication Process Complete ---"); return True
def run_daily_reauthentication_manager(): # Dedicated thread to run re-authentication daily before market open.
    global last_login_date
    logger.info(f"Daily re-authentication manager started. First login scheduled for tomorrow.")
    while True:
        try:
            now_ist, today_ist = get_ist_time(), get_ist_time().date()
            if (0 <= now_ist.weekday() <= 4 and now_ist.hour == 9 and now_ist.minute == 10 and today_ist != last_login_date):
                logger.info(f"Scheduled time 9:10 AM reached. Triggering re-authentication for {today_ist}.")
                if re_authenticate_and_reconnect(): last_login_date = today_ist
                else:
                    logger.error("Daily re-authentication failed. Will retry tomorrow.")
                    last_login_date = today_ist
            time.sleep(60)
        except Exception as e:
            logger.exception(f"An error occurred in the daily re-authentication manager thread: {e}")
            time.sleep(300)
def run_live_dashboard_updater(): # Dedicated thread to continuously update the Google Sheet with live prices.
    logger.info("Live dashboard updater thread started.")
    while True:
        try:
            update_excel_live_data()
            time.sleep(0.5)
        except Exception as e:
            logger.exception(f"Error in dashboard updater thread: {e}")
            time.sleep(5)
def run_quote_updater(): # Dedicated thread to periodically fetch full quote data (including percentChange).
    global latest_quote_data
    logger.info("Quote updater thread started.")
    while True:
        try:
            with data_lock: dashboard_details_copy = excel_dashboard_details.copy()
            focus_list_tokens = {}
            for token, details_list in dashboard_details_copy.items():
                for details in details_list:
                    if details.get('block_type') == 'Focus List':
                        focus_list_tokens[token] = details.get("exchange", "NSE").upper(); break
            if not focus_list_tokens:
                time.sleep(5); continue
            tokens_by_exchange, new_quote_data = collections.defaultdict(list), {}
            for token, exchange in focus_list_tokens.items(): tokens_by_exchange[exchange].append(token)
            for exchange, tokens_list in tokens_by_exchange.items():
                for i in range(0, len(tokens_list), QUOTE_API_MAX_TOKENS):
                    batch_tokens = tokens_list[i:i + QUOTE_API_MAX_TOKENS]
                    payload = {"mode": "FULL", "exchangeTokens": {exchange: batch_tokens}}
                    logger.info(f"Fetching market data for {len(batch_tokens)} tokens on {exchange}...")
                    response = smart_api_obj.getMarketData(**payload)
                    if response and response.get("status") and isinstance(response.get("data"), dict):
                        for item in response["data"].get("fetched", []):
                            if isinstance(item, dict) and item.get("symbolToken"): new_quote_data[item.get("symbolToken")] = {"percentChange": item.get("percentChange"), "netChange": item.get("netChange"), "day_low": item.get("low")}
                    else: logger.warning(f"Could not fetch quote data for batch (Exchange: {exchange}, Tokens: {batch_tokens}). Response: {response}")
                    time.sleep(0.5)
            with data_lock: latest_quote_data.update(new_quote_data)
            time.sleep(3)
        except Exception as e:
            logger.exception(f"Error in quote updater thread: {e}")
            time.sleep(10)
def run_initial_setup_data_fetch(initial_data_ready_event): # Background thread to fetch all historical data for setups at startup.
    logger.info("Starting background fetch for initial setup data...")
    try:
        with data_lock:
            orh_details_copy, pct3_details_copy = excel_orh_setup_details.copy(), excel_3pct_setup_details.copy()
        fetch_initial_candle_data_5min(smart_api_obj, orh_details_copy)
        fetch_previous_day_candle_data_high(smart_api_obj, orh_details_copy)
        all_tokens_for_history = set()
        for token, details in pct3_details_copy.items():
            if details: all_tokens_for_history.add((token, details[0]['exchange_type']))
        for token, details in orh_details_copy.items():
            if details: all_tokens_for_history.add((token, details[0]['exchange_type']))
        unique_tokens_for_history = list(all_tokens_for_history)
        for interval_api in CANDLE_INTERVALS_3PCT_API: fetch_historical_candles_for_3pct_down(smart_api_obj, unique_tokens_for_history, interval_api)
        logger.info("Initial data fetch is complete. Handing over to the scheduler for timed checks.")
    except Exception as e: logger.exception(f"An error occurred during initial data fetch: {e}")
    finally: initial_data_ready_event.set()

# --- START: New Automated Sorting and HH-LL Analysis Logic ---
def calculate_hh_ll_indicator(daily_candles):
    """
    Analyzes daily candles to find the last HH-after-LL signal on daily, weekly, and monthly timeframes.
    Returns the formatted string for Column V and the raw month count for sorting.
    """
    if not daily_candles or len(daily_candles) < 3:
        return "Not enough data", 999
    
    # 1. Helper function to find the signal
    def find_last_hh_after_ll(candle_list):
        # Scans backwards from the third-to-last candle to find the pattern
        for i in range(len(candle_list) - 3, -1, -1):
            candle_A, candle_B, candle_C = candle_list[i], candle_list[i+1], candle_list[i+2]
            is_hh = candle_C['high'] > candle_B['high']
            is_ll = candle_B['low'] < candle_A['low']
            if is_hh and is_ll:
                return candle_C['start_time'] # Return date of the HH candle
        return None

    # 2. Derive weekly and monthly candles from daily data
    weekly_candles, monthly_candles = [], []
    # Weekly aggregation
    grouped_by_week = collections.defaultdict(list)
    for c in daily_candles:
        year, week, _ = c['start_time'].isocalendar()
        grouped_by_week[(year, week)].append(c)
    
    for week_key in sorted(grouped_by_week.keys()):
        days_in_week = grouped_by_week[week_key]
        weekly_candles.append({
            'start_time': days_in_week[0]['start_time'],
            'open': days_in_week[0]['open'],
            'high': max(d['high'] for d in days_in_week),
            'low': min(d['low'] for d in days_in_week),
            'close': days_in_week[-1]['close']
        })

    # Monthly aggregation
    grouped_by_month = collections.defaultdict(list)
    for c in daily_candles:
        grouped_by_month[(c['start_time'].year, c['start_time'].month)].append(c)

    for month_key in sorted(grouped_by_month.keys()):
        days_in_month = grouped_by_month[month_key]
        monthly_candles.append({
            'start_time': days_in_month[0]['start_time'],
            'open': days_in_month[0]['open'],
            'high': max(d['high'] for d in days_in_month),
            'low': min(d['low'] for d in days_in_month),
            'close': days_in_month[-1]['close']
        })
        
    # 3. Find signals on all timeframes
    last_daily_signal_date = find_last_hh_after_ll(daily_candles)
    last_weekly_signal_date = find_last_hh_after_ll(weekly_candles)
    last_monthly_signal_date = find_last_hh_after_ll(monthly_candles)

    # 4. Calculate time elapsed and format the output string
    now = get_ist_time().replace(tzinfo=None) # Use naive datetime for comparison
    results = {}
    
    for timeframe, signal_date in [('monthly', last_monthly_signal_date), ('weekly', last_weekly_signal_date), ('daily', last_daily_signal_date)]:
        if signal_date:
            delta = relativedelta(now, signal_date)
            months = delta.years * 12 + delta.months
            remaining_days = delta.days
            weeks = remaining_days // 7
            days = remaining_days % 7
            results[timeframe] = (months, weeks, days)
        else:
            results[timeframe] = (999, 999, 999) # Use a large number if no signal found
    
    m, w, d = results['monthly'], results['weekly'], results['daily']
    
    output_str = f"{m[0]}Month, {w[1]}Weeks, {d[2]}th day"
    sort_key_months = m[0]
    
    return output_str, sort_key_months

def run_daily_hh_ll_analysis_and_sort():
    """
    Orchestrator function that fetches data for all 'Full Positions' stocks,
    runs the HH-LL analysis, and then sorts and updates the Google Sheet.
    This replaces the old sort_full_positions function.
    """
    logger.info("--- Starting Daily HH-LL Analysis and Automated Sort of Full Positions ---")
    try:
        last_row = get_last_row_in_column(Dashboard, FULL_SYMBOL_COL)
        if last_row < START_ROW_DATA:
            logger.info("No data in Full Positions to analyze and sort."); return

        # 1. Read the current state of the sheet
        range_to_read = f"{FULL_EXCHANGE_COL}{START_ROW_DATA}:{FULL_POSITIONS_END_COL}{last_row}"
        token_range_to_read = f"{ATH_CACHE_Z_COL_DASH}{START_ROW_DATA}:{ATH_CACHE_Z_COL_DASH}{last_row}"
        
        dashboard_data = Dashboard.get(range_to_read)
        token_data = ATHCache.get(token_range_to_read)
        
        # 2. Analyze each stock
        analysis_results = []
        to_date = get_ist_time().date()
        from_date = to_date - timedelta(days=365)
        from_date_str, to_date_str = from_date.strftime("%Y-%m-%d %H:%M"), to_date.strftime("%Y-%m-%d %H:%M")
        
        symbol_col_idx = col_to_num(FULL_SYMBOL_COL) - col_to_num(FULL_EXCHANGE_COL)
        exchange_col_idx = 0 # FULL_EXCHANGE_COL is the first column in our range
        
        for i, row_data in enumerate(dashboard_data):
            symbol = row_data[symbol_col_idx] if len(row_data) > symbol_col_idx else None
            exchange = row_data[exchange_col_idx] if len(row_data) > exchange_col_idx else None
            token = token_data[i][0] if i < len(token_data) and token_data[i] else None
            
            if not all([symbol, exchange, token]):
                logger.warning(f"Skipping analysis for row {START_ROW_DATA + i} due to missing data.")
                continue

            v_col_value, sort_month = "Analyzing...", 999
            try:
                historic_param = {"exchange": exchange, "symboltoken": token, "interval": "ONE_DAY", "fromdate": from_date_str, "todate": to_date_str}
                response = smart_api_obj.getCandleData(historic_param)
                if response and response.get("status") and response.get("data"):
                    daily_candles_raw = [{'start_time': datetime.datetime.fromisoformat(c[0]), 'open': c[1], 'high': c[2], 'low': c[3], 'close': c[4]} for c in response["data"]]
                    v_col_value, sort_month = calculate_hh_ll_indicator(daily_candles_raw)
                    logger.info(f"Analysis for {symbol}: {v_col_value}")
                else:
                    logger.warning(f"Could not fetch historical data for {symbol}. Response: {response.get('message', 'Unknown error')}")
                    v_col_value = "Fetch Error"
            except Exception as e:
                logger.error(f"Exception during analysis for {symbol}: {e}")
                v_col_value = "Analysis Error"
            
            # Update Column V in the original data
            v_col_idx = col_to_num(MONTH_SORT_COL) - col_to_num(FULL_EXCHANGE_COL)
            if len(row_data) > v_col_idx: row_data[v_col_idx] = v_col_value
            else: row_data.extend([''] * (v_col_idx - len(row_data) + 1)); row_data[v_col_idx] = v_col_value

            analysis_results.append({
                'dashboard_row': row_data,
                'token_row': token_data[i] if i < len(token_data) else [''],
                'sort_month': sort_month,
                'original_index': i
            })
            time.sleep(0.5) # API rate limiting
            
        # 3. Sort the results
        sorted_combined_data = sorted(analysis_results, key=lambda x: x['sort_month'])

        # Check if sorting is necessary
        if all(item['original_index'] == i for i, item in enumerate(sorted_combined_data)):
            logger.info("No change in sort order. Updating Column V values in place.")
            v_col_updates = []
            v_col_idx_abs = col_to_num(MONTH_SORT_COL)
            for i, result in enumerate(sorted_combined_data):
                 v_col_updates.append({'range': f"{MONTH_SORT_COL}{START_ROW_DATA + i}", 'values': [[result['dashboard_row'][v_col_idx_abs - col_to_num(FULL_EXCHANGE_COL)]]]})
            if v_col_updates:
                Dashboard.batch_update(v_col_updates, value_input_option='USER_ENTERED')
            return

        logger.info("Change in sort order detected. Updating entire Full Positions block.")
        sorted_dashboard_data = [item['dashboard_row'] for item in sorted_combined_data]
        sorted_token_data = [item['token_row'] for item in sorted_combined_data]

        # 4. Write the sorted data back to the sheet
        Dashboard.update(range_to_read, sorted_dashboard_data, value_input_option='USER_ENTERED')
        ATHCache.update(token_range_to_read, sorted_token_data, value_input_option='USER_ENTERED')
        logger.info("Successfully analyzed, sorted, and updated Full Positions on the Google Sheet.")

    except Exception as e:
        logger.exception(f"An error occurred during the automated HH-LL analysis and sorting process: {e}")
# --- END: New Automated Sorting and HH-LL Analysis Logic ---

def run_background_task_scheduler(initial_data_ready_event): # Main scheduler for slower tasks like sheet scanning and trade setup checks.
    global subscribed_tokens, excel_dashboard_details, excel_orh_setup_details, excel_3pct_setup_details, previous_j_column_state, previous_ah_column_state, previous_breakdown_state
    logger.info("Background task scheduler thread started.")
    logger.info("Scheduler is waiting for initial data fetch to complete...")
    initial_data_ready_event.wait()
    logger.info("Initial data is ready. Scheduler is now running.")
    last_checked_minute_15min, last_checked_minute_30min, last_checked_minute_1hr, last_scan_time, last_sort_time, last_monthly_high_fetch_time = None, None, None, 0, 0, 0
    try:
        j_values = Dashboard.get(f"{SETUP_LOG_COL}{START_ROW_DATA}:{SETUP_LOG_COL}{SETUP_MAX_ROW}")
        for i, cell in enumerate(j_values): previous_j_column_state[START_ROW_DATA + i] = cell[0] if cell else ""
        last_row_full = get_last_row_in_column(Dashboard, FULL_SYMBOL_COL)
        if last_row_full >= START_ROW_DATA:
            ah_values = Dashboard.get(f"{ACTION_COL}{START_ROW_DATA}:{ACTION_COL}{last_row_full}")
            for i, cell in enumerate(ah_values): previous_ah_column_state[START_ROW_DATA + i] = cell[0] if cell else ""
        logger.info("Initializing breakdown status state...")
        if last_row_full >= START_ROW_DATA:
            setups_to_check_init = [{'status_col': TRAILING_STOP_STATUS_COL}, {'status_col': HIGHEST_UP_CANDLE_STATUS_COL}, {'status_col': HIGH_VOL_STATUS_COL}, {'status_col': PCT_DOWN_STATUS_COL}]
            all_cols_init = [s['status_col'] for s in setups_to_check_init]
            start_col_init, end_col_init = min(all_cols_init, key=col_to_num), max(all_cols_init, key=col_to_num)
            sheet_data_init = Dashboard.get(f"{start_col_init}{START_ROW_DATA}:{end_col_init}{last_row_full}")
            for i, row_data in enumerate(sheet_data_init):
                for setup in setups_to_check_init:
                    col, col_idx = setup['status_col'], col_to_num(setup['status_col']) - col_to_num(start_col_init)
                    if col_idx < len(row_data): previous_breakdown_state[(START_ROW_DATA + i, col)] = row_data[col_idx]
        logger.info("Breakdown status state initialized.")
    except Exception as e: logger.error(f"Could not initialize column states: {e}")
    while True:
        try:
            check_and_place_orders()
            check_and_update_order_statuses()
            now, current_minute = get_ist_time(), get_ist_time().minute
            if time.time() - last_scan_time > 15:
                logger.info("Rescanning Google Sheet for symbol changes and manual triggers...")
                with data_lock: old_orh_tokens = set(excel_orh_setup_details.keys())
                new_dashboard, new_orh, new_3pct, current_excel_tokens = scan_sheet_for_all_symbols(Dashboard, ATHCache)
                added_orh_tokens = set(new_orh.keys()) - old_orh_tokens
                if added_orh_tokens:
                    logger.info(f"Detected {len(added_orh_tokens)} new ORH stock(s). Fetching 3-day high data on-the-fly...")
                    threading.Thread(target=fetch_previous_day_candle_data_high, args=(smart_api_obj, {token: new_orh[token] for token in added_orh_tokens}), daemon=True).start()
                with data_lock: excel_dashboard_details, excel_orh_setup_details, excel_3pct_setup_details = new_dashboard, new_orh, new_3pct
                try:
                    current_j_values = Dashboard.get(f"{SETUP_LOG_COL}{START_ROW_DATA}:{SETUP_LOG_COL}{SETUP_MAX_ROW}")
                    token_map_orh = {details['row']: token for token, details_list in new_orh.items() for details in details_list}
                    for i, cell in enumerate(current_j_values):
                        row_num = START_ROW_DATA + i
                        current_val, previous_val = (cell[0] if cell else ""), previous_j_column_state.get(row_num, "")
                        if current_val.strip().upper() == 'BUY' and previous_val.strip().upper() != 'BUY':
                            logger.info(f"Manual 'Buy' trigger detected on row {row_num}.")
                            if token_to_trigger := token_map_orh.get(row_num): process_manual_orh_trigger(token_to_trigger, row_num)
                            else: logger.warning(f"Could not find token for manual ORH trigger on row {row_num}.")
                        previous_j_column_state[row_num] = current_val
                    last_row_full = get_last_row_in_column(Dashboard, FULL_SYMBOL_COL)
                    if last_row_full >= START_ROW_DATA:
                        current_ah_values = Dashboard.get(f"{ACTION_COL}{START_ROW_DATA}:{ACTION_COL}{last_row_full}")
                        token_map_3pct = {details['row']: token for token, details_list in new_3pct.items() for details in details_list}
                        for i, cell in enumerate(current_ah_values):
                            row_num = START_ROW_DATA + i
                            current_val, previous_val = (cell[0] if cell else ""), previous_ah_column_state.get(row_num, "")
                            if current_val.strip().upper() == 'SELL' and previous_val.strip().upper() != 'SELL':
                                logger.info(f"Manual 'Sell' trigger detected on row {row_num}.")
                                if (token_to_trigger := token_map_3pct.get(row_num)) and token_to_trigger not in sell_triggered_today:
                                    with data_lock: sell_triggered_today.add(token_to_trigger)
                                    trigger_apps_script_alert("position_closed", row_num, new_3pct[token_to_trigger][0]['symbol'], new_3pct[token_to_trigger][0]['exchange'])
                                else: logger.warning(f"Could not find token or already triggered for manual Sell on row {row_num}.")
                            previous_ah_column_state[row_num] = current_val
                except Exception as e: logger.error(f"Error checking for manual triggers: {e}")
                tokens_to_subscribe = current_excel_tokens - subscribed_tokens
                if tokens_to_subscribe and smart_ws and smart_ws._is_connected_flag:
                    subscribe_list_grouped = collections.defaultdict(list)
                    for token in tokens_to_subscribe:
                        with data_lock:
                            exchange_type_num = 1
                            if token in excel_dashboard_details and excel_dashboard_details[token]: exchange_type_num = {'NSE': 1, 'NFO': 2, 'BSE': 3}.get(excel_dashboard_details[token][0].get('exchange', 'NSE').upper(), 1)
                            elif token in excel_orh_setup_details and excel_orh_setup_details[token]: exchange_type_num = excel_orh_setup_details[token][0].get('exchange_type', 1)
                            elif token in excel_3pct_setup_details and excel_3pct_setup_details[token]: exchange_type_num = excel_3pct_setup_details[token][0].get('exchange_type', 1)
                        subscribe_list_grouped[exchange_type_num].append(token)
                    for ex_type, tokens in subscribe_list_grouped.items():
                        formatted_tokens = [{"exchangeType": ex_type, "tokens": list(tokens)}]
                        smart_ws.subscribe(f"sub_{int(time.time())}", smart_ws.QUOTE, formatted_tokens)
                        subscribed_tokens.update(tokens)
                        logger.info(f"Subscribed to {len(tokens)} new tokens on exchange type {ex_type}.")
                last_scan_time = time.time()
            if time.time() - last_monthly_high_fetch_time > 86400:
                logger.info("Performing daily fetch for monthly highs (for Swing Low setup)...")
                with data_lock: unique_tokens_3pct = list(set([(token, details[0]['exchange_type']) for token, details in excel_3pct_setup_details.items() if details]))
                if unique_tokens_3pct: fetch_monthly_highs(smart_api_obj, unique_tokens_3pct)
                last_monthly_high_fetch_time = time.time()
            if time.time() - last_sort_time > 86400:
                run_daily_hh_ll_analysis_and_sort()
                last_sort_time = time.time()
            with data_lock: has_3pct_symbols, has_orh_symbols = bool(excel_3pct_setup_details), bool(excel_orh_setup_details)
            if has_3pct_symbols or has_orh_symbols:
                with data_lock:
                    all_tokens_for_history = set()
                    for token, details in excel_3pct_setup_details.items():
                        if details: all_tokens_for_history.add((token, details[0]['exchange_type']))
                    for token, details in excel_orh_setup_details.items():
                        if details: all_tokens_for_history.add((token, details[0]['exchange_type']))
                    unique_tokens_for_history = list(all_tokens_for_history)
                if current_minute % 15 == 1 and current_minute != last_checked_minute_15min and now.time() >= datetime.time(9, 30):
                    fetch_historical_candles_for_3pct_down(smart_api_obj, unique_tokens_for_history, 'FIFTEEN_MINUTE')
                    check_and_update_price_volume_setups(); check_and_update_all_breakdown_statuses(); check_and_update_breakdown_status()
                    last_checked_minute_15min = current_minute
                if current_minute % 30 == 1 and current_minute != last_checked_minute_30min:
                    fetch_historical_candles_for_3pct_down(smart_api_obj, unique_tokens_for_history, 'THIRTY_MINUTE')
                    check_and_update_price_volume_setups(); check_and_update_all_breakdown_statuses(); check_and_update_breakdown_status()
                    last_checked_minute_30min = current_minute
                if current_minute == 16 and now.hour >= 10 and current_minute != last_checked_minute_1hr:
                    fetch_historical_candles_for_3pct_down(smart_api_obj, unique_tokens_for_history, 'ONE_HOUR')
                    check_and_update_price_volume_setups(); check_and_update_all_breakdown_statuses(); check_and_update_breakdown_status()
                    last_checked_minute_1hr = current_minute
            time.sleep(1)
        except Exception as e:
            logger.exception(f"Error in background scheduler thread: {e}")
            time.sleep(5)

# --- Daily ATH Cache Update ---
def populate_ath_cache_from_master_list(ATHCache, master_list): # Filters master list and appends new symbols/tokens to ATH Cache sheet.
    logger.info("Checking for new instruments to append to ATH Cache...")
    try:
        existing_symbols_list = ATHCache.col_values(col_to_num('AI'))
        existing_symbols = set(existing_symbols_list[2:])
        logger.info(f"Found {len(existing_symbols)} existing symbols in the ATH Cache.")
        new_symbols_to_append, new_tokens_to_append = [], []
        for instrument in master_list:
            symbol, instrument_type = instrument.get('symbol', ''), instrument.get('instrumenttype', '')
            if symbol and symbol not in existing_symbols:
                if (symbol.endswith('-EQ') or symbol.endswith('-BE') or instrument_type == 'AMXIDX'):
                    if token := instrument.get('token'):
                        new_symbols_to_append.append([symbol])
                        new_tokens_to_append.append([token])
                        existing_symbols.add(symbol)
        if new_symbols_to_append:
            logger.info(f"Found {len(new_symbols_to_append)} new instruments to add. Appending to the sheet...")
            start_row = len(existing_symbols_list) + 1
            updates = [{'range': f'AI{start_row}', 'values': new_symbols_to_append}, {'range': f'AL{start_row}', 'values': new_tokens_to_append}]
            ATHCache.batch_update(updates, value_input_option='USER_ENTERED')
            logger.info(f"Successfully appended {len(new_symbols_to_append)} new symbols and tokens to the ATH Cache sheet.")
        else: logger.info("No new instruments found. ATH Cache is already up-to-date.")
    except Exception as e: logger.exception(f"An error occurred while updating the ATH Cache sheet: {e}")
def run_daily_ath_cache_update(): # Runs an initial update on startup, then schedules daily updates for 9:00 AM.
    logger.info("Daily ATH Cache updater thread started.")
    logger.info("Performing initial, one-time ATH Cache population on startup...")
    try:
        if instrument_master_list and ATHCache: populate_ath_cache_from_master_list(ATHCache, instrument_master_list)
    except Exception as e: logger.exception(f"An error occurred during the initial ATH cache population: {e}")
    last_run_date = get_ist_time().date()
    logger.info(f"Initial ATH Cache population complete. Next scheduled run is for tomorrow at 9:00 AM.")
    while True:
        try:
            now, today = get_ist_time(), get_ist_time().date()
            if now.hour == 9 and now.minute == 0 and today != last_run_date:
                logger.info(f"Scheduled time 9:00 AM reached. Starting daily ATH Cache update for {today.strftime('%Y-%m-%d')}.")
                try:
                    logger.info(f"Downloading fresh master instrument list from {INSTRUMENT_LIST_URL}...")
                    response = requests.get(INSTRUMENT_LIST_URL)
                    response.raise_for_status()
                    daily_master_list = response.json()
                    logger.info(f"Successfully downloaded {len(daily_master_list)} instruments for daily update.")
                    if daily_master_list and ATHCache: populate_ath_cache_from_master_list(ATHCache, daily_master_list)
                    last_run_date = today
                except Exception as e: logger.error(f"FATAL: Could not download or process the master instrument list for daily update: {e}.")
            time.sleep(60)
        except Exception as e:
            logger.exception(f"An error occurred in the daily ATH cache updater thread: {e}")
            time.sleep(60)

# --- Application Start ---
def start_main_application(): # Primary function to initialize connections and run main processing loops.
    global smart_api_obj, smart_ws, gsheet, Dashboard, ATHCache, OrdersSheet, subscribed_tokens, excel_dashboard_details, excel_orh_setup_details, excel_3pct_setup_details, instrument_master_list, auth_token, feed_token, websocket_thread, last_login_date
    logger.info("Starting Combined Trading Dashboard and Signal Generator...")
    try:
        logger.info(f"Performing initial download of master instrument list from {INSTRUMENT_LIST_URL}...")
        response = requests.get(INSTRUMENT_LIST_URL)
        response.raise_for_status()
        instrument_master_list = response.json()
        logger.info(f"Successfully downloaded and loaded {len(instrument_master_list)} instruments for startup.")
    except Exception as e:
        logger.error(f"FATAL: Could not download or parse the master instrument list at startup: {e}. Exiting."); return
    try:
        logger.info("Authenticating with Google Sheets...")
        creds = ServiceAccountCredentials.from_json_keyfile_name(JSON_KEY_FILE_PATH, SCOPE)
        client = gspread.authorize(creds)
        gsheet = client.open_by_key(GOOGLE_SHEET_ID)
        Dashboard, ATHCache, OrdersSheet = gsheet.worksheet(DASHBOARD_SHEET_NAME), gsheet.worksheet(ATH_CACHE_SHEET_NAME), gsheet.worksheet(ORDERS_SHEET_NAME)
        logger.info("Google Sheets connected successfully.")
    except Exception as e:
        logger.error(f"Error connecting to Google Sheets: {e}. Please check credentials and sheet names. Exiting."); return
    try:
        logger.info("Generating SmartAPI session...")
        smart_api_obj = SmartConnect(api_key=API_KEY, timeout=15)
        smart_api_obj.root = "https://apiconnect.angelbroking.com/"
        totp = pyotp.TOTP(TOTP_SECRET).now()
        data = smart_api_obj.generateSession(CLIENT_CODE, MPIN, totp=totp)
        if data and data.get('data') and data['data'].get('jwtToken'):
            auth_token, feed_token = data['data']['jwtToken'], data['data']['feedToken']
            last_login_date = get_ist_time().date()
            logger.info("SmartAPI session generated successfully!")
        else:
            logger.error(f"Failed to generate SmartAPI session. Response: {data}. Exiting."); return
    except Exception as e:
        logger.error(f"Error during SmartAPI session generation: {e}. Exiting."); return
    load_previous_day_high_cache()
    logger.info("Performing initial symbol scan...")
    new_dashboard, new_orh, new_3pct, all_tokens_for_subscription = scan_sheet_for_all_symbols(Dashboard, ATHCache)
    excel_dashboard_details, excel_orh_setup_details, excel_3pct_setup_details = new_dashboard, new_orh, new_3pct
    logger.info("Performing initial one-time fetch for monthly highs and portfolio sort...")
    unique_tokens_3pct_startup = list(set([(token, details[0]['exchange_type']) for token, details in excel_3pct_setup_details.items() if details]))
    if unique_tokens_3pct_startup: fetch_monthly_highs(smart_api_obj, unique_tokens_3pct_startup)
    run_daily_hh_ll_analysis_and_sort()
    try:
        logger.info("Initializing SmartAPI WebSocket...")
        smart_ws = MyWebSocketClient(auth_token, API_KEY, CLIENT_CODE, feed_token)
        websocket_thread = threading.Thread(target=smart_ws.connect, daemon=True)
        websocket_thread.start()
        time.sleep(5)
        if not smart_ws._is_connected_flag:
            logger.error("WebSocket failed to connect. Exiting."); return
        logger.info("SmartAPI WebSocket connected.")
    except Exception as e:
        logger.error(f"Error initializing WebSocket: {e}. Exiting."); return
    if all_tokens_for_subscription:
        logger.info("Subscribing to initial set of tokens for live data...")
        subscribe_list_grouped = collections.defaultdict(list)
        for token in all_tokens_for_subscription:
            exchange_type_num = 1
            if token in excel_dashboard_details and excel_dashboard_details[token]: exchange_type_num = {'NSE': 1, 'NFO': 2, 'BSE': 3}.get(excel_dashboard_details[token][0].get('exchange', 'NSE').upper(), 1)
            elif token in excel_orh_setup_details and excel_orh_setup_details[token]: exchange_type_num = excel_orh_setup_details[token][0].get('exchange_type', 1)
            elif token in excel_3pct_setup_details and excel_3pct_setup_details[token]: exchange_type_num = excel_3pct_setup_details[token][0].get('exchange_type', 1)
            subscribe_list_grouped[exchange_type_num].append(token)
        for ex_type, tokens in subscribe_list_grouped.items():
            formatted_tokens = [{"exchangeType": ex_type, "tokens": list(tokens)}]
            smart_ws.subscribe(f"sub_initial_{int(time.time())}", smart_ws.QUOTE, formatted_tokens)
            subscribed_tokens.update(tokens)
            logger.info(f"Subscribed to {len(tokens)} new tokens on exchange type {ex_type}.")
    logger.info("Starting concurrent application threads...")
    threading.Thread(target=run_daily_reauthentication_manager, daemon=True).start()
    threading.Thread(target=run_daily_ath_cache_update, daemon=True).start()
    threading.Thread(target=run_live_dashboard_updater, daemon=True).start()
    threading.Thread(target=run_quote_updater, daemon=True).start()
    threading.Thread(target=run_initial_setup_data_fetch, args=(initial_data_ready,), daemon=True).start()
    threading.Thread(target=run_background_task_scheduler, args=(initial_data_ready,), daemon=True).start()
    logger.info("All systems are go! The application is now running.")
def run_threaded_logic(): # Starts the main application logic in a separate thread.
    thread = threading.Thread(target=start_main_application, daemon=True)
    thread.start()
if __name__ == "__main__": # Main entry point for Flask + Threaded Logic.
    run_threaded_logic()
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))
