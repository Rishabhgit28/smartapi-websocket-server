import logging
import time
import json
import threading
import struct
import ssl
import websocket
import os
import sys
import collections
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import pyotp

import logzero
from logzero import logger

# --- Configure Logging ---
log_folder = time.strftime("%Y-%m-%d", time.localtime())
log_folder_path = os.path.join("logs", log_folder)
os.makedirs(log_folder_path, exist_ok=True)
log_path = os.path.join(log_folder_path, "app.log")
logzero.logfile(log_path, loglevel=logging.INFO)

logging.basicConfig(level=logging.INFO,
                    format='[%(levelname)s %(asctime)s %(filename)s:%(lineno)d] %(message)s')

# Import specific exception from SmartApi
from SmartApi import SmartConnect
from SmartApi.smartExceptions import DataException

def update_connection_status(status_message):
    """
    Updates a file with the current connection status.
    Used for external monitoring of the script's connection state.
    """
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
                self.on_data(wsapp, parsed_message)
            except Exception as e:
                logger.error(f"Error parsing or handling binary message: {e}. Raw message (first 50 bytes): {message[:50]}...")
                if hasattr(self, 'on_error'):
                    self.on_error(wsapp, f"Data parsing error: {e}")

    def _on_open(self, wsapp):
        """Callback when the WebSocket connection is successfully opened."""
        if self.RESUBSCRIBE_FLAG:
            self.resubscribe()
        self.on_open(wsapp)

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
                self.RESUBSCRIBE_FLAG = True
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
                self.RESUBSCRIBE_FLAG = True
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

    def _on_error(self, wsapp, error):
        """
        Internal callback for WebSocket errors.
        Handles reconnection attempts based on retry strategy.
        """
        logger.error(f"Internal WebSocket error: {error}")
        self.RESUBSCRIBE_FLAG = True

        if hasattr(self, 'on_error'):
            self.on_error(wsapp, error)

        if self.current_retry_attempt < self.MAX_RETRY_ATTEMPT:
            logger.warning(f"Attempting to reconnect (Attempt {self.current_retry_attempt + 1})...")
            self.current_retry_attempt += 1
            delay = self.retry_delay
            if self.retry_strategy == 1:
                delay = self.retry_delay * (self.current_retry_attempt - 1)

            time.sleep(delay)

            try:
                if self.wsapp:
                    self.wsapp.close()
                    time.sleep(1)
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
        logger.warning(f"Internal WebSocket closed. Code: {close_status_code}, Message: {close_msg}")
        self.current_retry_attempt = 0
        self.on_close(wsapp, close_status_code, close_msg)

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
        """Called for parsed binary data ticks. Users should implement their data processing here."""
        pass

    def on_control_message(self, wsapp, message):
        """Currently unused, but reserved for control messages."""
        pass

    def on_close(self, wsapp, close_status_code, close_msg):
        """Called when the WebSocket connection is closed."""
        pass

    def on_open(self, wsapp):
        """Called when the WebSocket connection is opened."""
        pass

    def on_error(self, wsapp, error):
        """Called when a WebSocket error occurs."""
        pass


class MyWebSocketClient(SmartWebSocketV2):
    """
    Extends SmartWebSocketV2 to implement specific callbacks for this application.
    Manages connection status and updates the global latest_tick_data.
    """
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._is_connected_flag = False

    def on_open(self, wsapp):
        logger.info("WebSocket connected")
        self._is_connected_flag = True
        update_connection_status("connected")

    def on_data(self, wsapp, data):
        """
        Processes incoming parsed market data ticks.
        Updates the global latest_tick_data dictionary.
        """
        token = data.get('token')
        ltp_raw = data.get('last_traded_price')
        open_price_raw = data.get('open_price_of_the_day')

        ltp_scaled = ltp_raw / 100.0 if isinstance(ltp_raw, (int, float)) else None
        open_price_scaled = open_price_raw / 100.0 if isinstance(open_price_raw, (int, float)) else None
        
        if token and ltp_scaled is not None:
            latest_tick_data[token] = {
                'ltp': ltp_scaled,
                'open': open_price_scaled,
                'full_data': data
            }
            logger.debug(f"Updated latest_tick_data for Token {token}: LTP={ltp_scaled}, Open={open_price_scaled}")
        else:
            logger.warning(f"Skipped updating latest_tick_data due to incomplete data for token: {token}")

    def on_error(self, wsapp, error_message):
        logger.error(f"WebSocket error: {error_message}")
        self._is_connected_flag = False
        update_connection_status("disconnecting")

    def on_close(self, wsapp, close_status_code, close_msg):
        logger.warning(f"WebSocket closed: {close_msg} (Code: {close_status_code})")
        self._is_connected_flag = False
        update_connection_status("disconnected")


# --- IMPORTANT: REPLACE THESE WITH YOUR ACTUAL CREDENTIALS ---
api_key = "oNNHQHKU"
client_code = "D355432"
mpin = "1234"
totp_secret = "QHO5IWOISV56Z2BFTPFSRSQVRQ"
# ---------------------------------------------------------------

# --- Google Sheets Configuration ---
JSON_KEY_FILE_PATH = r"C:\Users\Infin\Setup\Market Dashboard Shareable\the-money-method-ad6d7-f95331cf5cbf.json"
GOOGLE_SHEET_ID = "1cYBpsVKCbrYCZzrj8NAMEgUG4cXy5Q5r9BtQE1Cjmz0" # Your Google Sheet ID
GOOGLE_SHEET_NAME = "MarketDashboard"
DASHBOARD_SHEET_NAME = "Dashboard"
CREDENTIAL_SHEET_NAME = "Credential"

# Define scope for Google Sheets API
SCOPE = [
    "https://spreadsheets.google.com/feeds",
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive"
]

# Helper function to convert column letter to its 1-based index
def col_to_num(col_letter):
    num = 0
    for c in col_letter.upper():
        num = num * 26 + (ord(c) - ord('A')) + 1
    return num

# Helper function to convert RGB tuple (0-255) to float (0-1) for Google Sheets API
def rgb_to_float(rgb_tuple):
    if rgb_tuple is None:
        # Explicitly return white color if no specific color is provided
        return {"red": 1.0, "green": 1.0, "blue": 1.0} 
    return {
        "red": rgb_tuple[0] / 255.0,
        "green": rgb_tuple[1] / 255.0,
        "blue": rgb_tuple[2] / 255.0
    }

# Initialize SmartConnect for session and scrip search
obj = SmartConnect(api_key=api_key)
auth_token = None
feedToken = None

try:
    logger.info("Attempting to generate session...")
    totp = pyotp.TOTP(totp_secret).now()
    data = obj.generateSession(client_code, mpin, totp)

    if data and data.get('status'):
        auth_token = data['data']['jwtToken']
        refreshToken = data['data']['refreshToken']
        feedToken = obj.getfeedToken()
        logger.info("Session generated successfully!")
        logger.debug(f"Refresh Token: {refreshToken}")
        logger.debug(f"Feed Token: {feedToken}")
        logger.debug(f"Auth Token (JWT): {auth_token[:10]}...")
    else:
        logger.error(f"Session generation failed: {data.get('message', 'Unknown error')}. Error Code: {data.get('errorcode', 'N/A')}")
        sys.exit("Exiting due to failed session generation.")
except Exception as e:
    logger.exception(f"An error occurred during session generation: {e}")
    sys.exit("Exiting due to an exception during session generation.")


# --- Global dictionaries for WebSocket data and Google Sheet mapping ---
latest_tick_data = {}
excel_symbol_details = collections.defaultdict(list)
subscribed_tokens = set()
previous_ltp_data = {}
previous_percentage_change_data = {}


# --- Google Sheets Initialization ---
Dashboard = None
Credential = None
gsheet = None # Declare gsheet globally
client = None # Declare client globally
try:
    logger.info("Authorizing gspread client...")
    creds = ServiceAccountCredentials.from_json_keyfile_name(JSON_KEY_FILE_PATH, SCOPE)
    client = gspread.authorize(creds)
    logger.info("gspread client authorized.")

    logger.info(f"Opening Google Sheet: {GOOGLE_SHEET_NAME} (ID: {GOOGLE_SHEET_ID})...")
    gsheet = client.open_by_key(GOOGLE_SHEET_ID)
    Dashboard = gsheet.worksheet(DASHBOARD_SHEET_NAME)
    Credential = gsheet.worksheet(CREDENTIAL_SHEET_NAME)
    logger.info("Google Sheet worksheets 'Dashboard' and 'Credential' opened successfully.")
except Exception as e:
    logger.exception(f"An error occurred during Google Sheets initialization: {e}")
    sys.exit("Exiting due to an exception during Google Sheets setup.")


# --- Excel/Google Sheets Configuration Constants ---
START_ROW_DATA = 5
INDEX_START_ROW = 26
QUARTER_POSITIONS_START_ROW = 22

FOCUS_EXCHANGE_COL = 'B'
FOCUS_SYMBOL_COL = 'C'
FOCUS_LTP_COL = 'D'
FOCUS_CHG_COL = 'E'
FOCUS_CRED_TOKEN_COL = 'F'

FULL_EXCHANGE_COL = 'M'
FULL_SYMBOL_COL = 'N'
FULL_LTP_COL = 'P'
FULL_CRED_TOKEN_COL = 'G'

FULL_PRICE_COL = 'O'
FULL_QTY_COL = 'S'
FULL_SWING_LOW_COL = 'V'
FULL_RETURN_AMT_COL = 'Q'
FULL_RETURN_PCT_COL = 'R'
FULL_MOVE_PCT_COL = 'W'

INDEX_EXCHANGE_COL = 'B'
INDEX_SYMBOL_COL = 'C'
INDEX_LTP_COL = 'D'
INDEX_CHG_COL = 'E'
INDEX_CRED_TOKEN_COL = 'F'

QUARTER_EXCHANGE_COL = 'M'
QUARTER_SYMBOL_COL = 'N'
QUARTER_LTP_COL = 'P'
QUARTER_CHG_COL = 'Q'
QUARTER_CRED_TOKEN_COL = 'G'


SCRIP_SEARCH_RETRY_ATTEMPTS = 5
SCRIP_SEARCH_RETRY_DELAY = 2.0
SCRIP_SEARCH_RETRY_MULTIPLIER = 1.5


def get_last_row_in_column(sheet, column_letter):
    """
    Finds the last non-empty row in a given column for a gspread worksheet.
    """
    column_values = sheet.col_values(col_to_num(column_letter))
    for i in range(len(column_values) - 1, -1, -1):
        if column_values[i].strip() != '':
            return i + 1
    return 1


def scan_excel_for_symbols_and_tokens():
    """
    Scans the Google Sheet 'Dashboard' for symbols in 'Focus List', 'Full Positions',
    'Indices', and 'Quarter Positions' blocks. Fetches/updates tokens on 'Credential' sheet
    and populates the global `excel_symbol_details` dictionary.

    Returns:
        set: A set of all unique tokens found in Google Sheet.
    """
    logger.info("Scanning Google Sheet for symbols and updating tokens...")
    global excel_symbol_details
    
    old_excel_symbol_details = collections.defaultdict(list, excel_symbol_details)
    excel_symbol_details.clear()

    all_dashboard_values = Dashboard.get_all_values()
    all_credential_values = Credential.get_all_values()

    end_row_focus_list = INDEX_START_ROW - 1
    end_row_full_positions = QUARTER_POSITIONS_START_ROW - 1
    
    actual_last_row_indices_data = get_last_row_in_column(Dashboard, INDEX_SYMBOL_COL)
    actual_last_row_quarter_positions_data = get_last_row_in_column(Dashboard, QUARTER_SYMBOL_COL)

    def get_cell_value(sheet_values, row_idx, col_letter):
        col_idx = col_to_num(col_letter) - 1
        if 0 <= row_idx - 1 < len(sheet_values) and 0 <= col_idx < len(sheet_values[row_idx - 1]):
            return sheet_values[row_idx - 1][col_idx]
        return None

    logger.debug(f"Processing Focus List from row {START_ROW_DATA} to {end_row_focus_list}")
    for i in range(START_ROW_DATA, end_row_focus_list + 1):
        exchange_from_excel_raw = get_cell_value(all_dashboard_values, i, FOCUS_EXCHANGE_COL)
        symbol_from_excel_raw = get_cell_value(all_dashboard_values, i, FOCUS_SYMBOL_COL)
        
        exchange_from_excel = str(exchange_from_excel_raw).strip() if exchange_from_excel_raw is not None else ""
        symbol_from_excel = str(symbol_from_excel_raw).strip() if symbol_from_excel_raw is not None else ""

        if exchange_from_excel and symbol_from_excel:
            process_symbol_entry(
                row=i,
                exchange=exchange_from_excel,
                symbol=symbol_from_excel,
                ltp_col=FOCUS_LTP_COL,
                chg_col=FOCUS_CHG_COL,
                cred_token_col=FOCUS_CRED_TOKEN_COL,
                block_type="Focus List",
                price_col="", qty_col="", swing_low_col="",
                return_amt_col="", return_pct_col="", move_pct_col="",
                all_credential_values=all_credential_values
            )

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
                chg_col="",
                cred_token_col=FULL_CRED_TOKEN_COL,
                block_type="Full Positions",
                price_col=FULL_PRICE_COL,
                qty_col=FULL_QTY_COL,
                swing_low_col=FULL_SWING_LOW_COL,
                return_amt_col=FULL_RETURN_AMT_COL,
                return_pct_col=FULL_RETURN_PCT_COL,
                move_pct_col=FULL_MOVE_PCT_COL,
                all_credential_values=all_credential_values
            )

    logger.debug(f"Processing Indices from row {INDEX_START_ROW} to {actual_last_row_indices_data}")
    for i in range(INDEX_START_ROW, actual_last_row_indices_data + 1):
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
                all_credential_values=all_credential_values
            )

    logger.debug(f"Processing Quarter Positions from row {QUARTER_POSITIONS_START_ROW} to {actual_last_row_quarter_positions_data}")
    for i in range(QUARTER_POSITIONS_START_ROW, actual_last_row_quarter_positions_data + 1):
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
                all_credential_values=all_credential_values
            )
    
    logger.debug(f"excel_symbol_details content after scan: {json.dumps(excel_symbol_details, indent=2)}")
    
    current_excel_tokens = set(excel_symbol_details.keys())
    
    # Identify and clear Google Sheet data for symbols that were removed by the user
    clear_requests = []
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
                        "startColumnIndex": col_to_num(col_letter) - 1,
                        "endColumnIndex": col_to_num(col_letter)
                    }

                try:
                    # Clear values and colors for LTP, %Change, Return Amount, Return %, %Move
                    cols_to_clear_dashboard = []
                    if details["ltp_col"]: cols_to_clear_dashboard.append(details["ltp_col"])
                    if details["chg_col"]: cols_to_clear_dashboard.append(details["chg_col"])
                    if details["return_amt_col"]: cols_to_clear_dashboard.append(details["return_amt_col"])
                    if details["return_pct_col"]: cols_to_clear_dashboard.append(details["return_pct_col"])
                    if details["move_pct_col"]: cols_to_clear_dashboard.append(details["move_pct_col"])

                    for col_letter in cols_to_clear_dashboard:
                        cell_range = create_cell_range(DASHBOARD_SHEET_NAME, col_letter, row_num)
                        
                        # Clear value
                        clear_requests.append({
                            "updateCells": {
                                "rows": [{"values": [{"userEnteredValue": {"stringValue": ""}}]}],
                                "fields": "userEnteredValue",
                                "range": cell_range
                            }
                        })
                        # Clear color and number format
                        clear_requests.append({
                            "repeatCell": {
                                "range": cell_range,
                                "cell": {
                                    "userEnteredFormat": {
                                        "backgroundColor": rgb_to_float(None), # Explicitly set to white
                                        "numberFormat": {"type": "NUMBER", "pattern": "General"} # Reset to General format
                                    }
                                },
                                "fields": "userEnteredFormat.backgroundColor,userEnteredFormat.numberFormat"
                            }
                        })

                    # Clear token from Credential sheet
                    if details["cred_col"]:
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
    
    if clear_requests:
        try:
            gsheet.batch_update({'requests': clear_requests})
            logger.info(f"Executed {len(clear_requests)} clear operations on Google Sheet.")
        except Exception as e:
            logger.error(f"Failed to clear cells in Google Sheet: {e}")

    logger.info(f"Finished scanning. Found {len(current_excel_tokens)} unique tokens in Google Sheet.")
    return current_excel_tokens


def process_symbol_entry(row, exchange, symbol, ltp_col, chg_col, cred_token_col, block_type,
                         price_col, qty_col, swing_low_col, return_amt_col, return_pct_col, move_pct_col,
                         all_credential_values):
    """
    Helper function to process a single symbol entry from Google Sheet.
    Fetches/validates token and adds its details to `excel_symbol_details`.
    Includes retry logic for scrip search API calls.
    Writes new tokens to Credential sheet individually.
    """
    global excel_symbol_details

    found_token = None
    cred_col_num = col_to_num(cred_token_col)
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

    if found_token:
        is_duplicate = False
        for existing_entry in excel_symbol_details[found_token]:
            if existing_entry['row'] == row and \
               existing_entry['ltp_col'] == ltp_col and \
               existing_entry['chg_col'] == chg_col and \
               existing_entry['cred_col'] == cred_token_col and \
               existing_entry['block_type'] == block_type and \
               existing_entry['price_col'] == price_col and \
               existing_entry['qty_col'] == qty_col and \
               existing_entry['swing_low_col'] == swing_low_col and \
               existing_entry['return_amt_col'] == return_amt_col and \
               existing_entry['return_pct_col'] == return_pct_col and \
               existing_entry['move_pct_col'] == move_pct_col:
                is_duplicate = True
                break
        
        if not is_duplicate:
            excel_symbol_details[found_token].append({
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
                'move_pct_col': move_pct_col
            })
            logger.debug(f"Added {symbol} (Token: {found_token}) to excel_symbol_details. Current count for token {found_token}: {len(excel_symbol_details[found_token])}")
        else:
            logger.debug(f"Skipping duplicate add for {symbol} (Token: {found_token}) in row {row} ({block_type}).")
    else:
        logger.error(f"Token not found for {symbol} in row {row} ({block_type}). This symbol will be skipped for LTP updates, and not added to excel_symbol_details.")


def get_exchange_type_num(exchange_name):
    """Maps exchange name string to SmartWebSocketV2 numeric constant, handling extra spaces."""
    exchange_name_clean = exchange_name.strip().upper()
    if exchange_name_clean == "NSE": return smart_ws.NSE_CM
    elif exchange_name_clean == "NFO": return smart_ws.NSE_FO
    elif exchange_name_clean == "BSE": return smart_ws.BSE_CM
    elif exchange_name_clean == "MCX": return smart_ws.MCX_FO
    elif exchange_name_clean == "CDE": return smart_ws.CDE_FO
    logger.warning(f"Attempted to map unknown exchange type: '{exchange_name_clean}'")
    return None

def update_excel_live_data():
    """
    Updates the Google Sheet 'Dashboard' with live LTP, %Change, Return Amount,
    Return %, and %Move data from the `latest_tick_data` cache.
    Applies cell color based on value changes (green for increase/positive, red for
    decrease/negative), and resets to normal (no fill) if the value remains unchanged for LTP and %Change.
    Applies static color (green for positive, red for negative, white for zero) for
    Return Amount, Return %, and %Move.
    Uses batch updates for efficiency.
    """
    if not smart_ws._is_connected_flag:
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
    full_pos_input_ranges = collections.defaultdict(list) # {sheet_name: [list of a1_notations]}
    for token, list_of_details in excel_symbol_details.items():
        for details in list_of_details:
            if details['block_type'] == "Full Positions":
                row_num = details['row']
                # Add all relevant columns for this row
                if details["price_col"]: full_pos_input_ranges[DASHBOARD_SHEET_NAME].append(f'{details["price_col"]}{row_num}')
                if details["qty_col"]: full_pos_input_ranges[DASHBOARD_SHEET_NAME].append(f'{details["qty_col"]}{row_num}')
                if details["swing_low_col"]: full_pos_input_ranges[DASHBOARD_SHEET_NAME].append(f'{details["swing_low_col"]}{row_num}')
    
    # Fetch all values for these ranges in one go
    full_pos_input_data = {}
    if full_pos_input_ranges:
        try:
            # Corrected: Call batch_get on the gspread client object
            all_fetched_ranges = client.batch_get(full_pos_input_ranges[DASHBOARD_SHEET_NAME])
            for i, a1_notation in enumerate(full_pos_input_ranges[DASHBOARD_SHEET_NAME]):
                full_pos_input_data[a1_notation] = all_fetched_ranges[i][0][0] if all_fetched_ranges[i] and all_fetched_ranges[i][0] else None
            logger.debug(f"Fetched {len(full_pos_input_data)} Full Positions input cells in batch.")
        except Exception as e:
            logger.error(f"Error fetching Full Positions input data in batch: {e}. Proceeding with individual fetches if needed.")
            full_pos_input_data = {} # Clear to force individual fetches if batch fails


    # Helper to create a range for a single cell for batch update requests
    def create_cell_range_for_request(sheet_id, col_letter, row_num):
        return {
            "sheetId": sheet_id,
            "startRowIndex": row_num - 1,
            "endRowIndex": row_num,
            "startColumnIndex": col_to_num(col_letter) - 1,
            "endColumnIndex": col_to_num(col_letter)
        }


    for token, list_of_details in excel_symbol_details.items():
        if token in latest_tick_data:
            ltp_info = latest_tick_data[token]
            current_ltp = ltp_info['ltp']
            open_price = ltp_info['open']

            previous_ltp = previous_ltp_data.get(token)

            current_percentage_change = None
            if open_price is not None and open_price != 0:
                current_percentage_change = ((current_ltp - open_price) / open_price) * 100

            ltp_cell_color = None
            if previous_ltp is not None:
                if current_ltp > previous_ltp:
                    ltp_cell_color = GREEN_COLOR
                elif current_ltp < previous_ltp:
                    ltp_cell_color = RED_COLOR
                else:
                    ltp_cell_color = None # No change, explicitly set to default (white)
            
            previous_ltp_data[token] = current_ltp

            chg_cell_color = None
            if current_percentage_change is not None:
                if current_percentage_change > 0:
                    chg_cell_color = GREEN_COLOR
                elif current_percentage_change < 0:
                    chg_cell_color = RED_COLOR
                else:
                    chg_cell_color = None # No change, explicitly set to default (white)
            
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
                                "backgroundColor": rgb_to_float(ltp_cell_color) # Now returns white if None
                            }
                        },
                        "fields": "userEnteredFormat.backgroundColor"
                    }
                })

                # %Change for Focus List and Indices (Value + Format + Color)
                if chg_col and details['block_type'] not in ["Full Positions", "Quarter Positions"]:
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
                                    "backgroundColor": rgb_to_float(chg_cell_color), # Now returns white if None
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

                # Full Positions specific calculations and coloring
                if details['block_type'] == "Full Positions":
                    price_val = None
                    qty_val = None
                    swing_low_val = None

                    # Try to get from batch fetched data first
                    price_a1 = f'{details["price_col"]}{row_num}'
                    qty_a1 = f'{details["qty_col"]}{row_num}'
                    swing_low_a1 = f'{details["swing_low_col"]}{row_num}'

                    if price_a1 in full_pos_input_data:
                        price_val = full_pos_input_data[price_a1]
                    if qty_a1 in full_pos_input_data:
                        qty_val = full_pos_input_data[qty_a1]
                    if swing_low_a1 in full_pos_input_data:
                        swing_low_val = full_pos_input_data[swing_low_a1]

                    # Convert to float, handle empty strings
                    try:
                        price_val = float(price_val) if price_val else None
                        qty_val = float(qty_val) if qty_val else None
                        swing_low_val = float(swing_low_val) if swing_low_val else None
                    except ValueError as ve:
                        logger.warning(f"Could not convert input values to float for token {token} at row {row_num}: {ve}. Ensure cells contain valid numbers.")
                        price_val, qty_val, swing_low_val = None, None, None


                    return_amt = None
                    return_pct = None
                    move_pct = None

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
                                        "backgroundColor": rgb_to_float(current_return_amt_color) if current_return_amt_color else {},
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
                                        "backgroundColor": rgb_to_float(current_return_pct_color) if current_return_pct_color else {},
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
                                                "backgroundColor": rgb_to_float(None), # Explicitly set to white
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
                                        "backgroundColor": rgb_to_float(current_move_pct_color) if current_move_pct_color else {},
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
                                }
                            })
                            requests.append({
                                "repeatCell": {
                                    "range": cell_range,
                                    "cell": {
                                        "userEnteredFormat": {
                                            "backgroundColor": rgb_to_float(None), # Explicitly set to white
                                            "numberFormat": {"type": "NUMBER", "pattern": "General"}
                                        }
                                    },
                                    "fields": "userEnteredFormat.backgroundColor,userEnteredFormat.numberFormat"
                                }
                            })

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
                                        "backgroundColor": rgb_to_float(None), # Explicitly set to white
                                        "numberFormat": {"type": "NUMBER", "pattern": "General"} # Reset format
                                    }
                                },
                                "fields": "userEnteredFormat.backgroundColor,userEnteredFormat.numberFormat"
                            }
                        })
                
                # Clear colors for other full positions columns if no live data
                if details['block_type'] == "Full Positions":
                    for col_letter in [details["return_amt_col"], details["return_pct_col"], details["move_pct_col"]]:
                        if col_letter:
                            requests.append({
                                "repeatCell": {
                                    "range": create_cell_range_for_request(dashboard_sheet_id, col_letter, row_num),
                                    "cell": {
                                        "userEnteredFormat": {
                                            "backgroundColor": rgb_to_float(None), # Explicitly set to white
                                            "numberFormat": {"type": "NUMBER", "pattern": "General"} # Reset format
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


# --- Initialize and connect SmartWebSocketV2 ---
try:
    if not auth_token or not feedToken or not client_code or not api_key:
        raise ValueError("Missing authentication tokens or API key for WebSocket initialization.")

    smart_ws = MyWebSocketClient(
        auth_token=auth_token,
        api_key=api_key,
        client_code=client_code,
        feed_token=feedToken,
        max_retry_attempt=5,
        retry_strategy=1,
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


# --- Main Refresh Loop Parameters ---
EXCEL_UPDATE_INTERVAL_SECONDS = 0.5
TOKEN_SCAN_INTERVAL_CYCLES = 60
EXCEL_RETRY_ATTEMPTS = 3
EXCEL_RETRY_DELAY = 0.5
current_cycle = 0

logger.info(f"Starting live tick-by-tick dashboard update loop, updating Google Sheet every {EXCEL_UPDATE_INTERVAL_SECONDS} seconds...")

while True:
    current_time = time.strftime("%Y-%m-%d %H:%M:%S")
    logger.info(f"--- Processing data at {current_time} (Cycle: {current_cycle + 1}) ---")
    current_cycle += 1

    # --- PART 1: Periodically re-scan Google Sheet for new/removed symbols and manage subscriptions ---
    if current_cycle % TOKEN_SCAN_INTERVAL_CYCLES == 0 or current_cycle == 1:
        logger.info("Rescanning Google Sheet for new symbols and managing subscriptions...")
        
        current_excel_tokens = scan_excel_for_symbols_and_tokens()

        tokens_to_subscribe = current_excel_tokens - subscribed_tokens
        tokens_to_unsubscribe = subscribed_tokens - current_excel_tokens

        # Perform Unsubscriptions
        if tokens_to_unsubscribe and smart_ws._is_connected_flag:
            unsubscribe_list_grouped = collections.defaultdict(list)
            for token_to_unsub in tokens_to_unsubscribe:
                if token_to_unsub in excel_symbol_details and excel_symbol_details[token_to_unsub]:
                    exchange_type_name = excel_symbol_details[token_to_unsub][0]['exchange']
                    exchange_type_num = get_exchange_type_num(exchange_type_name)

                    if exchange_type_num:
                        unsubscribe_list_grouped[exchange_type_num].append(token_to_unsub)
                    else:
                        logger.warning(f"Could not determine exchange type for token {token_to_unsub} for unsubscription. Skipping.")
                else:
                    logger.warning(f"Token {token_to_unsub} not found in current excel_symbol_details for unsubscription (might be already removed or empty list).")

            for ex_type, tokens in unsubscribe_list_grouped.items():
                formatted_tokens = [{"exchangeType": ex_type, "tokens": tokens}]
                try:
                    smart_ws.unsubscribe(
                        correlation_id=f"unsub_{int(time.time())}",
                        mode=smart_ws.QUOTE,
                        token_list=formatted_tokens
                    )
                    for token in tokens:
                        subscribed_tokens.discard(token)
                    logger.info(f"Unsubscribed from {len(tokens)} tokens on exchange {ex_type} (removed from Google Sheet).")
                except Exception as e:
                    logger.error(f"Error sending unsubscribe request: {e}")
        elif not smart_ws._is_connected_flag:
            logger.warning("WebSocket not connected. Skipping unsubscription of removed symbols.")
        else:
            logger.info("No symbols to unsubscribe in this cycle.")

        # Perform Subscriptions
        if tokens_to_subscribe and smart_ws._is_connected_flag:
            subscribe_list_grouped = collections.defaultdict(list)
            for token_to_sub in tokens_to_subscribe:
                if token_to_sub in excel_symbol_details and excel_symbol_details[token_to_sub]:
                    exchange_type_name = excel_symbol_details[token_to_sub][0]['exchange']
                    exchange_type_num = get_exchange_type_num(exchange_type_name)

                    if exchange_type_num:
                        subscribe_list_grouped[exchange_type_num].append(token_to_sub)
                    else:
                        logger.warning(f"Unknown exchange type '{exchange_type_name}' for token {token_to_sub}. Cannot subscribe.")
                else:
                    logger.warning(f"Token {token_to_sub} not found in excel_symbol_details for subscription. Skipping.")


            for ex_type, tokens in subscribe_list_grouped.items():
                formatted_tokens = [{"exchangeType": ex_type, "tokens": tokens}]
                try:
                    smart_ws.subscribe(
                        correlation_id=f"sub_{int(time.time())}",
                        mode=smart_ws.QUOTE,
                        token_list=formatted_tokens
                    )
                    for token in tokens:
                        subscribed_tokens.add(token)
                    logger.info(f"Subscribed to {len(tokens)} new tokens on exchange {ex_type}.")
                except Exception as e:
                    logger.error(f"Error sending subscribe request: {e}")
        elif not smart_ws._is_connected_flag:
            logger.warning("WebSocket not connected. Cannot subscribe to new tokens.")
        else:
            logger.info("No new tokens to subscribe to in this cycle.")
        
        subscribed_tokens.intersection_update(current_excel_tokens)


    # --- PART 2: Update Google Sheet with latest LTP from WebSocket cache ---
    update_excel_live_data()

    logger.info(f"Waiting for {EXCEL_UPDATE_INTERVAL_SECONDS} seconds before next Google Sheet refresh...")
    time.sleep(EXCEL_UPDATE_INTERVAL_SECONDS)

logger.info("Script finished.")
