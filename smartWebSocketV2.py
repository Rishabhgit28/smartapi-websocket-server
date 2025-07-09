import logging
import xlwings as xw
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

import logzero # Ensure logzero is imported at the top
from logzero import logger # Import logger from logzero

# --- Configure Logging ---
# Configure logzero for daily log files globally
log_folder = time.strftime("%Y-%m-%d", time.localtime())
log_folder_path = os.path.join("logs", log_folder)
os.makedirs(log_folder_path, exist_ok=True)
log_path = os.path.join(log_folder_path, "app.log")
logzero.logfile(log_path, loglevel=logging.INFO) # Configure file logging using logzero

# Basic configuration (optional, as logzero often handles enough)
# If you want more control over console output, keep this.
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
        # Determine the base path for the status file
        if getattr(sys, 'frozen', False):
            # If running from EXE (PyInstaller)
            folder_path = os.path.dirname(sys.executable)
        else:
            # If running from script
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

    ROOT_URI = "wss://smartapisocket.angelone.in/smart-stream" # CORRECTED: Changed from https:// to wss://
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
    input_request_dict = {} # Stores active subscriptions for resubscription
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
        self.DISCONNECT_FLAG = True # Flag to control intentional disconnection
        self.last_pong_timestamp = None
        self.MAX_RETRY_ATTEMPT = max_retry_attempt
        self.retry_strategy = retry_strategy
        self.retry_delay = retry_delay
        self.retry_multiplier = retry_multiplier
        self.retry_duration = retry_duration # This parameter is not explicitly used in the retry mechanism.

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
                # All other messages are assumed to be binary market data ticks
                parsed_message = self._parse_binary_data(message)
                self.on_data(wsapp, parsed_message) # Call the user's on_data handler
            except Exception as e:
                logger.error(f"Error parsing or handling binary message: {e}. Raw message (first 50 bytes): {message[:50]}...")
                if hasattr(self, 'on_error'):
                    self.on_error(wsapp, f"Data parsing error: {e}")

    def _on_open(self, wsapp):
        """Callback when the WebSocket connection is successfully opened."""
        if self.RESUBSCRIBE_FLAG:
            self.resubscribe() # Resubscribe to previous tokens if reconnecting
        self.on_open(wsapp) # Call user's on_open handler

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

            # Depth mode specific validation
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

            # Update internal tracking for resubscription
            if self.input_request_dict.get(mode) is None:
                self.input_request_dict[mode] = {}
            for token_item in token_list:
                exchange_type = token_item['exchangeType']
                tokens = set(token_item["tokens"]) # Use set for efficient add/remove
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
            # Remove from input_request_dict for resubscription tracking
            if mode in self.input_request_dict:
                for token_item in token_list:
                    exchange_type = token_item['exchangeType']
                    tokens_to_remove = set(token_item['tokens'])
                    if exchange_type in self.input_request_dict[mode]:
                        # Remove specified tokens from the set
                        self.input_request_dict[mode][exchange_type].difference_update(tokens_to_remove)
                        if not self.input_request_dict[mode][exchange_type]: # If no tokens left for this exchange type
                            del self.input_request_dict[mode][exchange_type]
                if not self.input_request_dict[mode]: # If no exchange types left for this mode
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
                    # Convert set back to list for sending
                    temp_data = {
                        'exchangeType': exchangeType,
                        'tokens': list(tokens_set)
                    }
                    token_list.append(temp_data)
                if token_list: # Only send if there are tokens to resubscribe
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
        self.RESUBSCRIBE_FLAG = False # Do not resubscribe if intentionally closed
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
        self.RESUBSCRIBE_FLAG = True # Mark for resubscription attempt on next connect

        # Call the user's on_error callback
        if hasattr(self, 'on_error'):
            self.on_error(wsapp, error)

        if self.current_retry_attempt < self.MAX_RETRY_ATTEMPT:
            logger.warning(f"Attempting to reconnect (Attempt {self.current_retry_attempt + 1})...")
            self.current_retry_attempt += 1
            delay = self.retry_delay
            if self.retry_strategy == 1: # Exponential backoff
                delay = self.retry_delay * (self.current_retry_attempt - 1)

            time.sleep(delay)

            try:
                if self.wsapp:
                    self.wsapp.close() # Ensure previous connection is truly closed
                    time.sleep(1) # Give a moment for socket to properly close
                self.connect() # Attempt to reconnect
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
        self.current_retry_attempt = 0 # Reset retry count on close
        self.on_close(wsapp, close_status_code, close_msg) # Pass all args to user's on_close

    def _parse_binary_data(self, binary_data):
        """
        Parses raw binary market data into a readable dictionary format.
        Handles different subscription modes (LTP, QUOTE, SNAP_QUOTE, DEPTH).
        """
        # Common fields for all modes
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

            # Fields specific to QUOTE and SNAP_QUOTE modes
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

            # Fields specific to SNAP_QUOTE mode
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
                parsed_data["best_5_sell_data"] = best_5_buy_and_sell_data["best_5_sell_data"] # Corrected from best_5_buy_data

            # Fields specific to DEPTH mode
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

            if each_data["flag"] == 0: # 0 indicates Buy, 1 indicates Sell
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
            sell_start_idx = 200 + i * 10 # 20 packets * 10 bytes/packet = 200 bytes offset

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

    # Placeholder methods for users to override for custom logic
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

# --- Custom WebSocket Callback Handler Class ---
class MyWebSocketClient(SmartWebSocketV2):
    """
    Extends SmartWebSocketV2 to implement specific callbacks for this application.
    Manages connection status and updates the global latest_tick_data.
    """
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._is_connected_flag = False # Custom flag to track connection status

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

        # Scale prices if they are integers (assuming 2 decimal places precision)
        # For Nifty/Banknifty indices, the LTP might come directly, or require specific scaling.
        # Generally, equity/futures prices from SmartAPI are often in paisa (multiplied by 100).
        # We'll assume the /100.0 scaling is consistent for all. If indices appear off, this can be
        # a point to investigate (e.g., check API docs for index data format).
        ltp_scaled = ltp_raw / 100.0 if isinstance(ltp_raw, (int, float)) else None
        open_price_scaled = open_price_raw / 100.0 if isinstance(open_price_raw, (int, float)) else None
        
        if token and ltp_scaled is not None:
            latest_tick_data[token] = {
                'ltp': ltp_scaled,
                'open': open_price_scaled,
                'full_data': data # Store full data for potential future use
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
api_key = "oNNHQHKU"  # Replace with your actual API key
client_code = "D355432"  # Replace with your actual Client Code
mpin = "1234"  # YOU MUST REPLACE THIS WITH YOUR REAL MPIN
totp_secret = "QHO5IWOISV56Z2BFTPFSRSQVRQ"  # Replace with your actual TOTP secret
# ---------------------------------------------------------------

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
        logger.debug(f"Auth Token (JWT): {auth_token[:10]}...") # Log only first 10 chars for security
    else:
        logger.error(f"Session generation failed: {data.get('message', 'Unknown error')}. Error Code: {data.get('errorcode', 'N/A')}")
        sys.exit("Exiting due to failed session generation.") # Use sys.exit to terminate script
except Exception as e:
    logger.exception(f"An error occurred during session generation: {e}")
    sys.exit("Exiting due to an exception during session generation.") # Use sys.exit to terminate script


# --- Global dictionaries for WebSocket data and Excel mapping ---
latest_tick_data = {} # Key: token (string), Value: {'ltp': float, 'open': float, 'full_data': dict}
# Stores details for each symbol found in Excel, indexed by token.
# Each token maps to a LIST of detail dictionaries, allowing for multiple instances of the same token in Excel.
# {'token': [{'row': int, 'ltp_col': str, 'chg_col': str, 'cred_col': str, 'exchange': str, 'symbol': str, 'block_type': str}, ...]}
excel_symbol_details = collections.defaultdict(list) # Use defaultdict to simplify appending
subscribed_tokens = set() # Set of tokens currently subscribed via WebSocket
previous_ltp_data = {} # Stores the LTP from the *previous* update cycle for color comparison
previous_percentage_change_data = {} # Stores the previous percentage change for color comparison
# Removed previous_return_amt_data, previous_return_pct_data, previous_move_pct_data
# as their coloring will now be static based on current value.


# --- Excel Automation Initialization ---
try:
    wb = xw.Book("MarketDashboard.xlsm")
    Dashboard = wb.sheets('Dashboard')
    Credential = wb.sheets('Credential')
    logger.info("Excel workbook 'MarketDashboard.xlsm' opened successfully.")
except Exception as e:
    logger.exception(f"An error occurred during Excel workbook opening: {e}")
    sys.exit("Exiting due to an exception during Excel setup.")


# --- Excel Configuration Constants ---
START_ROW_DATA = 5 # Data for both blocks starts from row 5
INDEX_START_ROW = 26 # Indices block starts from row 26 (as per screenshot)
QUARTER_POSITIONS_START_ROW = 22 # Based on your screenshot, Quarter Positions start here
# Assuming Quarter Positions block also ends at some row before other content or sheet ends.
# We'll use get_last_row_in_column to find the end of data within its symbol column.

# Focus List Columns (Based on your latest instruction to use D and E)
FOCUS_EXCHANGE_COL = 'B'
FOCUS_SYMBOL_COL = 'C'
FOCUS_LTP_COL = 'D' 
FOCUS_CHG_COL = 'E' 
FOCUS_CRED_TOKEN_COL = 'F' # On Credential sheet

# Full Positions Columns
FULL_EXCHANGE_COL = 'M'
FULL_SYMBOL_COL = 'N'
FULL_LTP_COL = 'P' # Full Position LTP is now in Column P
FULL_CRED_TOKEN_COL = 'G' # On Credential sheet

# NEW: Full Positions additional columns as per VBA
FULL_PRICE_COL = 'O'
FULL_QTY_COL = 'S'
FULL_SWING_LOW_COL = 'V'
FULL_RETURN_AMT_COL = 'Q'
FULL_RETURN_PCT_COL = 'R'
FULL_MOVE_PCT_COL = 'W'


# Indices Block Columns (as per screenshot from previous turns)
INDEX_EXCHANGE_COL = 'B'
INDEX_SYMBOL_COL = 'C'
INDEX_LTP_COL = 'D'
INDEX_CHG_COL = 'E'
INDEX_CRED_TOKEN_COL = 'F' # Assuming indices tokens also go into column F on Credential sheet

# Quarter Positions Columns (NEW)
QUARTER_EXCHANGE_COL = 'M' # Assuming same as Full Positions
QUARTER_SYMBOL_COL = 'N' # Assuming same as Full Positions
QUARTER_LTP_COL = 'P' # Assuming 'Price' column is for LTP
QUARTER_CHG_COL = 'Q' # Assuming 'Return %' column is for %Change
QUARTER_CRED_TOKEN_COL = 'G' # Assuming same as Full Positions token column on Credential


# --- Scrip Search Retry Parameters ---
SCRIP_SEARCH_RETRY_ATTEMPTS = 5
SCRIP_SEARCH_RETRY_DELAY = 2.0 # Initial delay for search retries (seconds)
SCRIP_SEARCH_RETRY_MULTIPLIER = 1.5 # Multiplier for exponential backoff


def get_last_row_in_column(sheet, column):
    """
    Finds the last non-empty row in a given column.
    This is robust for finding the actual end of data within a column.
    """
    return sheet.range(f'{column}' + str(sheet.cells.last_cell.row)).end('up').row


def scan_excel_for_symbols_and_tokens():
    """
    Scans the Excel 'Dashboard' sheet for symbols in 'Focus List', 'Full Positions',
    'Indices', and 'Quarter Positions' blocks. Fetches/updates tokens on 'Credential' sheet
    and populates the global `excel_symbol_details` dictionary.

    Returns:
        set: A set of all unique tokens found in Excel.
    """
    logger.info("Scanning Excel for symbols and updating tokens...")
    global excel_symbol_details
    
    # Store old details to identify removed symbols for cleanup
    old_excel_symbol_details = collections.defaultdict(list, excel_symbol_details)
    excel_symbol_details.clear() # Reset for current scan

    # Determine scan range for each block
    end_row_focus_list = INDEX_START_ROW - 1 
    # For Full Positions, we scan from START_ROW_DATA up to the row before Quarter Positions.
    end_row_full_positions = QUARTER_POSITIONS_START_ROW - 1 
    
    # For the indices block, we scan from its start row to the actual last populated row in its symbol column.
    actual_last_row_indices_data = get_last_row_in_column(Dashboard, INDEX_SYMBOL_COL)

    # For Quarter Positions block, we scan from its start row to the actual last populated row in its symbol column.
    actual_last_row_quarter_positions_data = get_last_row_in_column(Dashboard, QUARTER_SYMBOL_COL)


    # Process Focus List
    logger.debug(f"Processing Focus List from row {START_ROW_DATA} to {end_row_focus_list}")
    for i in range(START_ROW_DATA, end_row_focus_list + 1):
        exchange_from_excel_raw = Dashboard.range(f'{FOCUS_EXCHANGE_COL}{i}').value
        symbol_from_excel_raw = Dashboard.range(f'{FOCUS_SYMBOL_COL}{i}').value
        
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
                price_col="", qty_col="", swing_low_col="", # Not applicable for Focus List
                return_amt_col="", return_pct_col="", move_pct_col=""
            )

    # Process Full Positions
    logger.debug(f"Processing Full Positions from row {START_ROW_DATA} to {end_row_full_positions}")
    for i in range(START_ROW_DATA, end_row_full_positions + 1):
        exchange_from_excel_raw = Dashboard.range(f'{FULL_EXCHANGE_COL}{i}').value
        symbol_from_excel_raw = Dashboard.range(f'{FULL_SYMBOL_COL}{i}').value

        exchange_from_excel = str(exchange_from_excel_raw).strip() if exchange_from_excel_raw is not None else ""
        symbol_from_excel = str(symbol_from_excel_raw).strip() if symbol_from_excel_raw is not None else ""

        if exchange_from_excel and symbol_from_excel:
            process_symbol_entry(
                row=i,
                exchange=exchange_from_excel,
                symbol=symbol_from_excel,
                ltp_col=FULL_LTP_COL, # P column
                chg_col="", # No dedicated %Chg column for Full Positions from API data
                cred_token_col=FULL_CRED_TOKEN_COL,
                block_type="Full Positions",
                price_col=FULL_PRICE_COL, # O column
                qty_col=FULL_QTY_COL,     # S column
                swing_low_col=FULL_SWING_LOW_COL, # V column
                return_amt_col=FULL_RETURN_AMT_COL, # Q column
                return_pct_col=FULL_RETURN_PCT_COL, # R column
                move_pct_col=FULL_MOVE_PCT_COL # W column
            )

    # Process Indices Block
    logger.debug(f"Processing Indices from row {INDEX_START_ROW} to {actual_last_row_indices_data}")
    for i in range(INDEX_START_ROW, actual_last_row_indices_data + 1):
        exchange_from_excel_raw = Dashboard.range(f'{INDEX_EXCHANGE_COL}{i}').value
        symbol_from_excel_raw = Dashboard.range(f'{INDEX_SYMBOL_COL}{i}').value

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
                price_col="", qty_col="", swing_low_col="", # Not applicable for Indices
                return_amt_col="", return_pct_col="", move_pct_col=""
            )

    # Process Quarter Positions Block (NEW)
    logger.debug(f"Processing Quarter Positions from row {QUARTER_POSITIONS_START_ROW} to {actual_last_row_quarter_positions_data}")
    for i in range(QUARTER_POSITIONS_START_ROW, actual_last_row_quarter_positions_data + 1):
        exchange_from_excel_raw = Dashboard.range(f'{QUARTER_EXCHANGE_COL}{i}').value
        symbol_from_excel_raw = Dashboard.range(f'{QUARTER_SYMBOL_COL}{i}').value

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
                price_col="", qty_col="", swing_low_col="", # Not applicable for Quarter Positions direct calcs here
                return_amt_col="", return_pct_col="", move_pct_col=""
            )
    
    # After processing all rows, check the contents of excel_symbol_details
    logger.debug(f"excel_symbol_details content after scan: {json.dumps(excel_symbol_details, indent=2)}")
    
    current_excel_tokens = set(excel_symbol_details.keys())
    
    # Identify and clear Excel data for symbols that were removed by the user
    for token, list_of_old_details in old_excel_symbol_details.items():
        if token not in excel_symbol_details:
            for details in list_of_old_details:
                row_num = details['row']
                try:
                    if details["ltp_col"]:
                        Dashboard.range(f'{details["ltp_col"]}{row_num}').value = ""
                        # Clear color when symbol is removed
                        Dashboard.range(f'{details["ltp_col"]}{row_num}').color = None 
                    if details["chg_col"]: 
                        Dashboard.range(f'{details["chg_col"]}{row_num}').value = ""
                        # Clear color for %change when symbol is removed
                        Dashboard.range(f'{details["chg_col"]}{row_num}').color = None 
                    
                    # NEW: Clear colors for return and move columns if symbol is removed
                    if details["return_amt_col"]:
                        Dashboard.range(f'{details["return_amt_col"]}{row_num}').value = ""
                        Dashboard.range(f'{details["return_amt_col"]}{row_num}').color = None
                    if details["return_pct_col"]:
                        Dashboard.range(f'{details["return_pct_col"]}{row_num}').value = ""
                        Dashboard.range(f'{details["return_pct_col"]}{row_num}').color = None
                    if details["move_pct_col"]:
                        Dashboard.range(f'{details["move_pct_col"]}{row_num}').value = ""
                        Dashboard.range(f'{details["move_pct_col"]}{row_num}').color = None

                    if details["cred_token_col"]:
                        Credential.range(f'{details["cred_token_col"]}{row_num}').value = "" # Corrected 'cred_col' to 'cred_token_col' based on previous context.
                    logger.info(f"Cleared Excel data for removed symbol: {details['symbol']} (Token: {token}, Row: {row_num}, Block: {details['block_type']})")
                except Exception as e:
                    logger.warning(f"Error clearing Excel cells for removed symbol {details['symbol']} (Row: {row_num}): {e}")

    try:
        wb.save()
        logger.info("Excel workbook saved after token updates.")
    except Exception as e:
        logger.error(f"Failed to save Excel workbook after token updates: {e}")

    logger.info(f"Finished scanning. Found {len(current_excel_tokens)} unique tokens in Excel.")
    return current_excel_tokens


def process_symbol_entry(row, exchange, symbol, ltp_col, chg_col, cred_token_col, block_type,
                         price_col, qty_col, swing_low_col, return_amt_col, return_pct_col, move_pct_col):
    """
    Helper function to process a single symbol entry from Excel.
    Fetches/validates token and adds its details to `excel_symbol_details`.
    Includes retry logic for scrip search API calls.
    Writes new tokens to Credential sheet individually.
    """
    global excel_symbol_details # Explicitly declare global to modify it

    found_token = None
    existing_token_in_excel = Credential.range(f'{cred_token_col}{row}').value

    if existing_token_in_excel:
        try:
            existing_token_clean = str(int(existing_token_in_excel)).strip()
            found_token = existing_token_clean
            logger.debug(f"Using existing token {found_token} from Excel for {symbol} in row {row} ({block_type}).")
        except ValueError:
            logger.warning(f"Invalid token '{existing_token_in_excel}' in Excel for {symbol} ({block_type}). Attempting to fetch new token.")
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
                time.sleep(0.5) # Small fixed delay after each API call to be gentle

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
                        # Write to Credential sheet individually
                        for write_attempt in range(EXCEL_RETRY_ATTEMPTS):
                            try:
                                Credential.range(f'{cred_token_col}{row}').value = int(found_token)
                                logger.debug(f"Wrote token {found_token} to Credential sheet for {symbol_clean}.")
                                break
                            except Exception as ex_write:
                                logger.warning(f"Attempt {write_attempt+1}/{EXCEL_RETRY_ATTEMPTS}: Error writing token to Credential for {symbol_clean}. Retrying. Error: {ex_write}")
                                time.sleep(EXCEL_RETRY_DELAY)
                        else:
                            logger.error(f"Failed to write token for {symbol_clean} ({block_type}) to Credential after multiple attempts. Token: {found_token}")
                        break # Token found and written, exit retry loop
                    else:
                        logger.warning(f"Could not find matching token for '{symbol_clean}' in search results ({block_type}) after search (Attempt {attempt+1}).")
                elif search_response and not search_response.get('status'):
                    error_msg = search_response.get('message', 'No message')
                    error_code = search_response.get('errorcode', 'N/A')
                    logger.error(f"API returned error for '{symbol_clean}' ({block_type}): {error_msg}. Error Code: {error_code} (Attempt {attempt+1})")
                    if "Access denied" in error_msg or "rate" in error_msg.lower():
                        pass 
                    else:
                        break # Non-retryable error, break from retry loop
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
        
        if not found_token: # Log after all retries
            logger.error(f"Failed to fetch token for {symbol_clean} ({block_type}) after {SCRIP_SEARCH_RETRY_ATTEMPTS} attempts.")

    # Always add details to our global mapping IF a token was found/valid.
    # This prevents duplicate entries in excel_symbol_details if the scan runs multiple times
    # and a token was already correctly processed in a previous scan.
    if found_token:
        # Check if this specific entry (token + row + ltp_col) already exists in excel_symbol_details
        is_duplicate = False
        for existing_entry in excel_symbol_details[found_token]:
            # A "duplicate" means the exact same Excel cell mapping for this token is already recorded
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
    exchange_name_clean = exchange_name.strip().upper() # Ensure no leading/trailing spaces
    if exchange_name_clean == "NSE": return smart_ws.NSE_CM
    elif exchange_name_clean == "NFO": return smart_ws.NSE_FO
    elif exchange_name_clean == "BSE": return smart_ws.BSE_CM
    elif exchange_name_clean == "MCX": return smart_ws.MCX_FO
    elif exchange_name_clean == "CDE": return smart_ws.CDE_FO
    # Add other exchanges if needed
    logger.warning(f"Attempted to map unknown exchange type: '{exchange_name_clean}'")
    return None

def update_excel_live_data():
    """
    Updates the Excel 'Dashboard' sheet with live LTP, %Change, Return Amount,
    Return %, and %Move data from the `latest_tick_data` cache.
    Applies cell color based on value changes (green for increase/positive, red for
    decrease/negative), and resets to normal (no fill) if the value remains unchanged for LTP and %Change.
    Applies static color (green for positive, red for negative, white for zero) for
    Return Amount, Return %, and %Move.
    Writes directly to individual cells for precision and formatting.
    """
    if not smart_ws._is_connected_flag:
        logger.warning("WebSocket is not connected. Skipping Excel update for this cycle.")
        return

    try:
        # Disable screen updating for faster Excel writes
        for attempt in range(EXCEL_RETRY_ATTEMPTS):
            try:
                wb.app.screen_updating = False
                break
            except Exception as ex:
                logger.warning(f"Attempt {attempt+1}/{EXCEL_RETRY_ATTEMPTS}: Could not set screen_updating to False. Retrying in {EXCEL_RETRY_DELAY}s. Error: {ex}")
                time.sleep(EXCEL_RETRY_DELAY)
        else:
            logger.error("Failed to set screen_updating to False after multiple attempts. Continuing without this optimization.")

        # Prepare lists for updates and colors
        updates_to_perform = []
        colors_to_apply = [] 

        # Define colors using the specified RGB values
        GREEN_COLOR = (149, 203, 186)
        RED_COLOR = (254, 87, 87)

        # Iterate through tokens that are currently in our Excel mapping
        if not excel_symbol_details:
            logger.info("excel_symbol_details is empty. No symbols to update in Excel.")

        for token, list_of_details in excel_symbol_details.items():
            if token in latest_tick_data:
                ltp_info = latest_tick_data[token]
                current_ltp = ltp_info['ltp']
                open_price = ltp_info['open']

                # Get previous LTP for comparison (for flashing effect)
                previous_ltp = previous_ltp_data.get(token)

                current_percentage_change = None
                if open_price is not None and open_price != 0:
                    current_percentage_change = ((current_ltp - open_price) / open_price) * 100

                # Determine LTP cell color based on LTP change (flashing)
                ltp_cell_color = None 
                if previous_ltp is not None:
                    if current_ltp > previous_ltp:
                        ltp_cell_color = GREEN_COLOR
                    elif current_ltp < previous_ltp:
                        ltp_cell_color = RED_COLOR
                    else: 
                        ltp_cell_color = None 
                
                previous_ltp_data[token] = current_ltp # Update previous LTP for next cycle

                # Determine %Change cell color based on its value (flashing)
                # This still flashes if the *value* of %change crosses zero or changes sign.
                # If you only want it to flash on actual non-zero *change* from previous tick,
                # you'd reintroduce previous_percentage_change_data and compare.
                chg_cell_color = None 
                if current_percentage_change is not None:
                    if current_percentage_change > 0:
                        chg_cell_color = GREEN_COLOR
                    elif current_percentage_change < 0:
                        chg_cell_color = RED_COLOR
                    else: 
                        chg_cell_color = None 
                
                previous_percentage_change_data[token] = current_percentage_change # Update for next cycle


                # Update all instances of this token in Excel
                for details in list_of_details:
                    row_num = details['row']
                    ltp_col = details['ltp_col']
                    chg_col = details['chg_col'] 

                    ltp_cell_range = f'{ltp_col}{row_num}'
                    updates_to_perform.append((ltp_cell_range, current_ltp))
                    colors_to_apply.append((ltp_cell_range, ltp_cell_color)) # Apply LTP color

                    # Handle %Change for Focus List and Indices
                    if chg_col and details['block_type'] not in ["Full Positions", "Quarter Positions"]: 
                        chg_cell_range = f'{chg_col}{row_num}'
                        updates_to_perform.append((chg_cell_range, current_percentage_change)) 
                        colors_to_apply.append((chg_cell_range, chg_cell_color)) # Apply %Change color
                        Dashboard.range(chg_cell_range).number_format = "0.00%" # Apply format
                        logger.debug(f"Queued for Excel update: Token {token} (Row {row_num}, {details['block_type']}), LTP: {current_ltp}, %Chg: {current_percentage_change}")

                    # NEW: Handle Full Positions specific calculations and coloring (STATIC COLORS)
                    if details['block_type'] == "Full Positions":
                        price_val = Dashboard.range(f'{details["price_col"]}{row_num}').value
                        qty_val = Dashboard.range(f'{details["qty_col"]}{row_num}').value
                        swing_low_val = Dashboard.range(f'{details["swing_low_col"]}{row_num}').value

                        return_amt = None
                        return_pct = None
                        move_pct = None

                        if isinstance(price_val, (int, float)) and isinstance(qty_val, (int, float)) and price_val != 0:
                            return_amt = (current_ltp - price_val) * qty_val
                            return_pct = (current_ltp - price_val) / price_val

                            # Determine Return Amount color (STATIC)
                            current_return_amt_color = None
                            if return_amt is not None:
                                if return_amt > 0: current_return_amt_color = GREEN_COLOR
                                elif return_amt < 0: current_return_amt_color = RED_COLOR
                                else: current_return_amt_color = None 

                            # Determine Return Percentage color (STATIC)
                            current_return_pct_color = None
                            if return_pct is not None:
                                if return_pct > 0: current_return_pct_color = GREEN_COLOR
                                elif return_pct < 0: current_return_pct_color = RED_COLOR
                                else: current_return_pct_color = None 


                            # Add to updates and colors lists
                            updates_to_perform.append((f'{details["return_amt_col"]}{row_num}', return_amt))
                            colors_to_apply.append((f'{details["return_amt_col"]}{row_num}', current_return_amt_color))
                            Dashboard.range(f'{details["return_amt_col"]}{row_num}').number_format = "#,##0.00" # Match VBA format

                            updates_to_perform.append((f'{details["return_pct_col"]}{row_num}', return_pct))
                            colors_to_apply.append((f'{details["return_pct_col"]}{row_num}', current_return_pct_color))
                            Dashboard.range(f'{details["return_pct_col"]}{row_num}').number_format = "0.00%" # Match VBA format

                        else:
                            # Clear values and colors if conditions not met
                            updates_to_perform.append((f'{details["return_amt_col"]}{row_num}', ""))
                            colors_to_apply.append((f'{details["return_amt_col"]}{row_num}', None))
                            updates_to_perform.append((f'{details["return_pct_col"]}{row_num}', ""))
                            colors_to_apply.append((f'{details["return_pct_col"]}{row_num}', None))


                        # %Move Calculation (Swing Low vs LTP) (STATIC COLORS)
                        if isinstance(swing_low_val, (int, float)) and swing_low_val != 0 and isinstance(current_ltp, (int, float)):
                            move_pct = (current_ltp - swing_low_val) / swing_low_val

                            # Determine %Move color (STATIC)
                            current_move_pct_color = None
                            if move_pct is not None:
                                if move_pct > 0: current_move_pct_color = GREEN_COLOR
                                elif move_pct < 0: current_move_pct_color = RED_COLOR
                                else: current_move_pct_color = None

                            updates_to_perform.append((f'{details["move_pct_col"]}{row_num}', move_pct))
                            colors_to_apply.append((f'{details["move_pct_col"]}{row_num}', current_move_pct_color))
                            Dashboard.range(f'{details["move_pct_col"]}{row_num}').number_format = "0.00%" # Match VBA format

                        else:
                            updates_to_perform.append((f'{details["move_pct_col"]}{row_num}', ""))
                            colors_to_apply.append((f'{details["move_pct_col"]}{row_num}', None))

            else:
                logger.debug(f"No live data for token {token} in latest_tick_data. Skipping updates for its Excel entries.")
                # If no live data, also consider reverting the color if it was previously set for all associated cells
                for details in list_of_details:
                    row_num = details['row']
                    ltp_col = details['ltp_col']
                    chg_col = details['chg_col']
                    return_amt_col = details['return_amt_col']
                    return_pct_col = details['return_pct_col']
                    move_pct_col = details['move_pct_col']

                    ltp_cell_range = f'{ltp_col}{row_num}'
                    if ltp_cell_range not in [upd[0] for upd in updates_to_perform]: 
                         colors_to_apply.append((ltp_cell_range, None))
                    
                    if chg_col and details['block_type'] not in ["Full Positions", "Quarter Positions"]:
                        chg_cell_range = f'{chg_col}{row_num}'
                        if chg_cell_range not in [upd[0] for upd in updates_to_perform]:
                            colors_to_apply.append((chg_cell_range, None))
                    
                    # Clear colors for other full positions columns if no live data
                    if details['block_type'] == "Full Positions":
                        for col in [return_amt_col, return_pct_col, move_pct_col]:
                            if col and f'{col}{row_num}' not in [upd[0] for upd in updates_to_perform]:
                                colors_to_apply.append((f'{col}{row_num}', None))


        # Perform the actual cell updates (cell by cell)
        if updates_to_perform or colors_to_apply:
            for attempt in range(EXCEL_RETRY_ATTEMPTS):
                try:
                    for cell_range, value in updates_to_perform:
                        Dashboard.range(cell_range).value = value
                    for cell_range, color_tuple in colors_to_apply: # Apply colors
                        Dashboard.range(cell_range).color = color_tuple
                    logger.info(f"Updated LTP/Chg/Returns/Move for {len(updates_to_perform)} cells and applied colors to {len(colors_to_apply)} cells in Excel.")
                    break
                except Exception as ex_write:
                    logger.warning(f"Attempt {attempt+1}/{EXCEL_RETRY_ATTEMPTS}: Error writing to Excel. Retrying in {EXCEL_RETRY_DELAY}s. Error: {ex_write}")
                    time.sleep(EXCEL_RETRY_DELAY)
            else:
                logger.error("Failed to write to Excel after multiple attempts. Manual update might be needed.")
        else:
            logger.info("No data to write to Excel in this cycle (either no symbols or no live data yet).")

    except Exception as e:
        logger.exception(f"An unexpected error occurred in the Excel update phase: {e}")
    finally:
        # Ensure screen updating is re-enabled
        for attempt in range(EXCEL_RETRY_ATTEMPTS):
            try:
                wb.app.screen_updating = True
                break
            except Exception as ex:
                logger.warning(f"Attempt {attempt+1}/{EXCEL_RETRY_ATTEMPTS}: Could not set screen_updating to True. Excel screen might remain off. Error: {ex}")
                time.sleep(EXCEL_RETRY_DELAY)
        else:
            logger.error("Failed to set screen_updating to True after multiple attempts. Excel screen might remain off.")


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
        retry_strategy=1, # Exponential backoff
        retry_delay=5,
        retry_multiplier=2
    )

    ws_thread = threading.Thread(target=smart_ws.connect, daemon=True)
    ws_thread.start()
    logger.info("SmartWebSocketV2 connection attempt initiated in a separate thread.")

    timeout = 15 # Increased timeout for connection establishment
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
EXCEL_UPDATE_INTERVAL_SECONDS = 0.5 # How often to update Excel from cache
TOKEN_SCAN_INTERVAL_CYCLES = 60     # How often to re-scan Excel for new/removed symbols (e.g., every 30 seconds)
EXCEL_RETRY_ATTEMPTS = 3
EXCEL_RETRY_DELAY = 0.5
current_cycle = 0

logger.info(f"Starting live tick-by-tick dashboard update loop, updating Excel every {EXCEL_UPDATE_INTERVAL_SECONDS} seconds...")

while True:
    current_time = time.strftime("%Y-%m-%d %H:%M:%S")
    logger.info(f"--- Processing data at {current_time} (Cycle: {current_cycle + 1}) ---")
    current_cycle += 1

    # --- PART 1: Periodically re-scan Excel for new/removed symbols and manage subscriptions ---
    if current_cycle % TOKEN_SCAN_INTERVAL_CYCLES == 0 or current_cycle == 1:
        logger.info("Rescanning Excel for new symbols and managing subscriptions...")
        
        # Get all tokens currently present in Excel
        current_excel_tokens = scan_excel_for_symbols_and_tokens()

        # Determine tokens to subscribe and unsubscribe
        tokens_to_subscribe = current_excel_tokens - subscribed_tokens
        tokens_to_unsubscribe = subscribed_tokens - current_excel_tokens

        # Perform Unsubscriptions
        if tokens_to_unsubscribe and smart_ws._is_connected_flag:
            unsubscribe_list_grouped = collections.defaultdict(list)
            for token_to_unsub in tokens_to_unsubscribe:
                # When unsubscribing, ensure that excel_symbol_details contains the token
                # before attempting to access its details to get the exchange type.
                # This prevents errors if a token was added to subscribed_tokens
                # but then removed from excel_symbol_details before the next scan.
                if token_to_unsub in excel_symbol_details and excel_symbol_details[token_to_unsub]:
                    exchange_type_name = excel_symbol_details[token_to_unsub][0]['exchange']
                    exchange_type_num = get_exchange_type_num(exchange_type_name) # Use the cleaned function

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
                        subscribed_tokens.discard(token) # Remove from our tracking set
                    logger.info(f"Unsubscribed from {len(tokens)} tokens on exchange {ex_type} (removed from Excel).")
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
                    exchange_type_name = excel_symbol_details[token_to_sub][0]['exchange'] # Pick first entry for exchange
                    exchange_type_num = get_exchange_type_num(exchange_type_name) # Use the cleaned function

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
                        subscribed_tokens.add(token) # Add to our tracking set
                    logger.info(f"Subscribed to {len(tokens)} new tokens on exchange {ex_type}.")
                except Exception as e:
                    logger.error(f"Error sending subscribe request: {e}")
        elif not smart_ws._is_connected_flag:
            logger.warning("WebSocket not connected. Cannot subscribe to new tokens.")
        else:
            logger.info("No new tokens to subscribe to in this cycle.")
        
        # At the end of the scan, ensure subscribed_tokens reflects only what's currently active in Excel
        subscribed_tokens.intersection_update(current_excel_tokens)


    # --- PART 2: Update Excel with latest LTP from WebSocket cache ---
    update_excel_live_data()

    # Wait for the specified interval before the next Excel refresh...
    logger.info(f"Waiting for {EXCEL_UPDATE_INTERVAL_SECONDS} seconds before next Excel refresh...")
    time.sleep(EXCEL_UPDATE_INTERVAL_SECONDS)

logger.info("Script finished.")
