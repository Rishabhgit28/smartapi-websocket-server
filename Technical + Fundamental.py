import yfinance as yf
import os
from datetime import datetime, time, timedelta, date
from concurrent.futures import ThreadPoolExecutor, as_completed
import locale
import math
import subprocess
import json
import sys
import pandas as pd
import time # Added for gspread rate limiting considerations
import numpy as np # Added for NaN handling

# GSpread specific imports
from oauth2client.service_account import ServiceAccountCredentials
import gspread

# Selenium imports (from combined_runner.py)
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver import ActionChains


# === Global Configuration and Paths ===
# IMPORTANT: Replace these placeholder values with your actual Google Sheet ID and sheet names.
# The GOOGLE_SHEET_ID is found in your Google Sheet's URL.
GOOGLE_SHEET_ID = "1cYBpsVKCbrYCZzrj8NAMEgUG4cXy5Q5r9BtQE1Cjmz0"
ATH_CACHE_SHEET_NAME = "ATH Cache"
PRICE_SCAN_SHEET_NAME = "Price Scan" # This sheet is used by both parts of the script

# Determine the path for the service account JSON key file
# This logic checks for a secret file in a specific path first, then falls back to local.
if os.path.exists("/etc/secrets/creds.json"):
    JSON_KEY_FILE_PATH = "/etc/secrets/creds.json"
else:
    # Ensure this JSON file is in the same directory as your script, or provide its full path.
    JSON_KEY_FILE_PATH = os.path.join(os.path.dirname(__file__), "the-money-method-ad6d7-f95331cf5cbf.json")

# Define the necessary OAuth2 scopes for Google Sheets and Drive access
SCOPE = [
    "https://spreadsheets.google.com/feeds",
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive"
]

# --- Helper function for resource paths (from combined_runner.py) ---
def resource_path(relative_path):
    """
    Determines the correct path for resources, especially when running as a PyInstaller executable.
    """
    if hasattr(sys, "_MEIPASS"):
        return os.path.join(sys._MEIPASS, relative_path)
    else:
        return os.path.join(os.path.dirname(sys.argv[0]), relative_path)

# Paths for external scripts and data (from combined_runner.py)
JS_SCRIPT = resource_path("moneycontrol_scraper.js")
JSON_FILE = resource_path("earnings.json")
NODE_PATH = resource_path("nodejs-portable/node.exe")
NODE_MODULES = resource_path("nodejs-portable/node_modules")


# === Google Sheets Authentication (Performed once for both parts) ===
try:
    creds = ServiceAccountCredentials.from_json_keyfile_name(JSON_KEY_FILE_PATH, SCOPE)
    client = gspread.authorize(creds)
    gsheet = client.open_by_key(GOOGLE_SHEET_ID)

    # Assign worksheets for ATH+Price Scan part
    stock_sheet = gsheet.worksheet(ATH_CACHE_SHEET_NAME) # Used for loading initial symbols
    cache_sheet = gsheet.worksheet(ATH_CACHE_SHEET_NAME) # Used for ATH updates and all scan results
    scan_sheet = gsheet.worksheet(PRICE_SCAN_SHEET_NAME) # For filtered output and combined_runner updates

    print("✅ Successfully connected to Google Sheets.")
except Exception as e:
    print(f"❌ Error connecting to Google Sheets: {e}")
    print("Please ensure your GOOGLE_SHEET_ID is correct and the service account JSON key file is valid and accessible.")
    sys.exit() # Exit if connection fails, as the script cannot proceed without sheet access


# === Functions from ATH+Price Scan.py ===

# Set Indian locale-style grouping manually for comma formatting
# This function is used for Market Cap formatting, which is a string output.
# This function is no longer used for the Price Scan sheet output,
# but is kept here as it was part of the original ATH+Price Scan.py functions.
def format_indian_currency(value):
    try:
        # Convert to integer first to handle potential floats before formatting
        s = f"{int(value):,}"
        # This custom logic attempts to replicate Indian number grouping (lakhs, crores)
        # It's a simplified version and might not be perfectly accurate for all cases.
        parts = s.replace(",", "_").split("_") # Temporary replace to split by thousands
        if len(parts) <= 3: # Up to 9,99,999
            return ",".join(parts)
        elif len(parts) == 4: # Crores (e.g., 1,23,45,678)
            return f"{parts[0]},{parts[1]},{parts[2]},{parts[3]}"
        else: # More complex numbers, fall back to default comma or extend logic
            # This part might need further refinement for very large numbers
            return f"{parts[0]},{''.join(parts[1:3])},{','.join(parts[3:])}"
    except Exception as e:
        return str(value)


# === Functions from combined_runner.py ===

def map_period_to_range(p):
    """
    Maps a period string (e.g., "Q1 FY24-25") to a human-readable date range.
    """
    if not p or "FY" not in p:
        return ""
    qtr, fy = p.split(" ")
    fy_start, fy_end = fy.replace("FY", "").split("-")
    return {
        "Q1": f"Apr {fy_start} to Jun {fy_start}",
        "Q2": f"Jul {fy_start} to Sep {fy_start}",
        "Q3": f"Oct {fy_start} to Dec {fy_start}",
        "Q4": f"Jan {fy_end} to Mar {fy_end}",
    }.get(qtr, "")

def safe_click(locator, by=By.CSS_SELECTOR, timeout=10, driver_instance=None):
    """
    Safely clicks an element after waiting for it to be clickable and scrolling into view.
    Accepts a driver_instance to ensure the correct driver is used.
    """
    driver = driver_instance if driver_instance else globals().get('driver')
    if not driver:
        raise ValueError("Selenium WebDriver instance not provided or found in global scope.")

    el = WebDriverWait(driver, timeout).until(EC.element_to_be_clickable((by, locator)))
    driver.execute_script("arguments[0].scrollIntoView({block:'center'});", el)
    driver.execute_script("arguments[0].click();", el)
    return el

def close_popup_if_exists(driver_instance=None):
    """
    Attempts to close common pop-up elements that might obstruct interactions.
    Accepts a driver_instance to ensure the correct driver is used.
    """
    driver = driver_instance if driver_instance else globals().get('driver')
    if not driver:
        return # Cannot close popup without a driver

    for sel in (".ad-close-button", ".close-button", '[aria-label="close"]'):
        try:
            driver.find_element(By.CSS_SELECTOR, sel).click()
            break
        except:
            pass

def search_and_open_company(name, driver_instance=None, wait_instance=None):
    """
    Navigates to Tickertape, searches for a company, and opens its profile page.
    Accepts driver_instance and wait_instance.
    """
    driver = driver_instance if driver_instance else globals().get('driver')
    wait = wait_instance if wait_instance else globals().get('wait')
    if not driver or not wait:
        raise ValueError("Selenium WebDriver or WebDriverWait instance not provided or found.")

    driver.get("https://www.tickertape.in/")
    close_popup_if_exists(driver_instance=driver)
    try:
        search = wait.until(EC.element_to_be_clickable((By.ID, "search-stock-input")))
        ActionChains(driver).click(search).send_keys(name).pause(1.5)\
            .send_keys(Keys.ARROW_DOWN).send_keys(Keys.ENTER).perform()
        time.sleep(3) # Give time for page to load after search
        close_popup_if_exists(driver_instance=driver)
        # Wait for a key element on the financials section to ensure page is loaded
        wait.until(EC.presence_of_element_located((By.XPATH, "//a[contains(@href,'#financials')]")))
        return True
    except Exception as e:
        return False

def select_quarterly(driver_instance=None, wait_instance=None):
    """
    Selects the quarterly view for financial data on Tickertape.
    Accepts driver_instance and wait_instance.
    """
    driver = driver_instance if driver_instance else globals().get('driver')
    wait = wait_instance if wait_instance else globals().get('wait')
    if not driver or not wait:
        return # Cannot select quarterly without driver/wait

    try:
        safe_click("span.suggestion-toggle-btn", driver_instance=driver) # Click the toggle button for period selection
        radio = wait.until(EC.presence_of_element_located((By.XPATH, "//input[@value='interim']")))
        driver.execute_script("arguments[0].click();", radio) # Click the interim (quarterly) radio button
        time.sleep(0.5) # Short pause for UI update
    except:
        pass # If elements not found, assume quarterly is already selected or not applicable

def scroll_table_right(driver_instance=None, wait_instance=None):
    """
    Scrolls the financial table horizontally to reveal the latest quarters.
    Accepts driver_instance and wait_instance.
    """
    driver = driver_instance if driver_instance else globals().get('driver')
    wait = wait_instance if wait_instance else globals().get('wait')
    if not driver or not wait:
        return # Cannot scroll without driver/wait

    try:
        table = wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "div.financials-table-root")))
        time.sleep(1.2) # Wait for table to render
        driver.execute_script("arguments[0].scrollLeft = arguments[0].scrollWidth", table) # Scroll to the far right
        time.sleep(0.8) # Wait for scroll to complete
    except:
        pass

def get_quarters(driver_instance=None):
    """
    Extracts the last three quarter names from the financial table header.
    Accepts a driver_instance.
    """
    driver = driver_instance if driver_instance else globals().get('driver')
    if not driver:
        return []

    try:
        # Select table headers with class 'period-text' within the income statement table
        ths = driver.find_elements(By.CSS_SELECTOR, 'table[data-statement-type="income"] thead th.period-text')
        return [th.text.strip() for th in ths if th.text.strip()][-3:] # Get last 3 non-empty quarter texts
    except:
        return []

def get_row_by_data_key(data_key, driver_instance=None):
    """
    Retrieves the last three data points for a specific financial row (e.g., Revenue, PBT).
    Accepts a driver_instance.
    """
    driver = driver_instance if driver_instance else globals().get('driver')
    if not driver:
        return []

    try:
        # Find the table row using its 'data-row' attribute
        row = driver.find_element(By.CSS_SELECTOR, f'tr[data-row="{data_key}"]')
        # Get all table data cells (td) in that row, skipping the first (label)
        tds = row.find_elements(By.CSS_SELECTOR, "td")[1:]
        return [td.text.strip() for td in tds if td.text.strip()][-3:] # Get last 3 non-empty values
    except:
        return []

def calculate_growth(old, new):
    """
    Calculates percentage growth between two values. Handles division by zero.
    """
    try:
        old = float(old)
        new = float(new)
        if old == 0:
            return float('inf') if new > 0 else float('-inf') # Handle division by zero
        return ((new - old) / abs(old)) * 100
    except:
        return 0 # Return 0 if calculation fails


# ==============================================================================
# === PART 1: ATH + Price Scan Logic (from ATH+Price Scan.py) ===
# ==============================================================================
print("\n--- Starting ATH + Price Scan Logic ---")

# === Load symbols from ATH Cache (Column A) ===
# Reads symbols from column A, starting from A2, expanding downwards.
# In gspread, col_values(1) gets all values from column A. We slice [1:] to skip the header.
try:
    symbols_raw = stock_sheet.col_values(1)[1:] # Get values from column A, starting from A2
except Exception as e:
    print(f"❌ Error reading symbols from ATH Cache sheet: {e}")
    symbols_raw = []

# --- REVISED SYMBOL PROCESSING FOR NSE AND BSE ---
# These lists are for the initial yfinance.download() in the ATH update section.
# They will contain both .NS and .BO versions if the raw symbol has no suffix.
all_nse_symbols_to_download = []
all_bse_symbols_to_download = []

# This set will store unique full symbols (e.g., RELIANCE.NS, RELIANCE.BO)
# that will be processed in subsequent steps like LTP, SMA, etc.
all_unique_symbols_for_processing = set()

for s_raw in symbols_raw:
    if not isinstance(s_raw, str) or not s_raw.strip():
        continue
    clean_s = s_raw.strip().upper()

    if clean_s.endswith(".NS"):
        all_nse_symbols_to_download.append(clean_s)
        all_unique_symbols_for_processing.add(clean_s)
    elif clean_s.endswith(".BO"):
        all_bse_symbols_to_download.append(clean_s)
        all_unique_symbols_for_processing.add(clean_s)
    else:
        # If no suffix, assume it could be either NSE or BSE for initial download
        nse_version = clean_s + ".NS"
        bse_version = clean_s + ".BO"
        all_nse_symbols_to_download.append(nse_version)
        all_bse_symbols_to_download.append(bse_version)

        # Add both versions to the unique set for subsequent processing
        all_unique_symbols_for_processing.add(nse_version)
        all_unique_symbols_for_processing.add(bse_version)

# Remove duplicates from the download lists
nse_symbols = list(set(all_nse_symbols_to_download))
bse_symbols = list(set(all_bse_symbols_to_download))

# The 'symbols' list for subsequent processing (LTP, SMA, etc.)
# This list contains all possible .NS and .BO versions that were derived from symbols_raw.
symbols = list(all_unique_symbols_for_processing)
# --- END REVISED SYMBOL PROCESSING ---


# === Check if ATH update is allowed ===
# Determines if the All-Time High (ATH) cache update should run based on time.
# This prevents heavy data downloads during market hours.
now = datetime.now()
weekday = now.weekday() # Monday is 0, Sunday is 6
current_time = now.time()

ath_allowed = (
    (weekday == 4 and current_time >= time(16, 0)) or # Friday after 4 PM
    (weekday == 5) or                                  # Saturday
    (weekday == 6) or                                  # Sunday
    (weekday == 0 and current_time <= time(8, 0))      # Monday before 8 AM
)

# === ATH Cache Update (Columns B and C of ATH Cache sheet) ===
# This section updates the All-Time High (ATH) and its last update date
# for the symbols listed in column A of the 'ATH Cache' sheet.
if ath_allowed:
    print("📈 Running ATH Cache Update...")

    today_str = now.strftime("%d-%b-%Y")
    updates = 0

    # ✅ Step 1: Global check on Column C (Last Updated - where the date is actually written)
    try:
        date_values_raw = cache_sheet.col_values(3)[1:] # Column C is index 3 (1-based)
        date_values = date_values_raw if date_values_raw else []
    except Exception as e:
        print(f"❌ Error reading last update dates from ATH Cache sheet (Column C): {e}")
        date_values = []

    # Normalize all dates and add robust error handling for parsing
    normalized_dates = []
    for val in date_values:
        try:
            if isinstance(val, (float, int)): # gspread might return numbers for dates (Excel serial date)
                # Convert Excel serial date to datetime object. Excel's epoch is 1899-12-30.
                d = datetime.fromordinal(datetime(1899, 12, 30).toordinal() + int(val)).date()
            elif isinstance(val, str) and val.strip(): # Ensure it's a non-empty string
                # Try parsing common date formats
                try:
                    d = datetime.strptime(val.strip(), "%d-%b-%Y").date()
                except ValueError:
                    try:
                        d = datetime.strptime(val.strip(), "%Y-%m-%d").date()
                    except ValueError:
                        print(f"⚠️ Could not parse date value '{val}' from Column C. Skipping.")
                        continue # Skip this value if it cannot be parsed
            elif isinstance(val, datetime):
                d = val.date()
            else:
                continue # Skip non-string, non-numeric, non-datetime values
            normalized_dates.append(d)
        except Exception as e:
            print(f"⚠️ Unexpected error parsing date value '{val}' from Column C: {e}. Skipping.")
            continue

    latest_update = max(normalized_dates) if normalized_dates else None

    # Check the actual condition for skipping
    if latest_update and (now.date() - latest_update).days < 2:
        print(f"⏩ Skipping ATH Cache update: Already updated on {latest_update.strftime('%d-%b-%Y')} (less than 2 days ago).")
    else:
        # ✅ Step 2: Download data if update is required
        data_nse = {}
        data_bse = {}

        if nse_symbols:
            try:
                print("📦 Downloading NSE data (bulk)...")
                data_nse = yf.download(
                    tickers=" ".join(nse_symbols),
                    period="max",
                    interval="1d",
                    group_by='ticker',
                    auto_adjust=True,
                    threads=True,
                    progress=True
                )
            except Exception as e:
                print(f"❌ NSE bulk download failed: {e}")
                if "ConnectionError" in str(e):
                    print("   (Likely a network issue or yfinance server problem for NSE symbols.)")
                elif "YFInvalidPeriodError" in str(e):
                    print("   (One or more NSE symbols might have an invalid 'max' period.)")


        if bse_symbols:
            try:
                print("📦 Downloading BSE data (bulk)...")
                data_bse = yf.download(
                    tickers=" ".join(bse_symbols),
                    period="max",
                    interval="1d",
                    group_by='ticker',
                    auto_adjust=True,
                    threads=True,
                    progress=True
                )
            except Exception as e:
                print(f"❌ BSE bulk download failed: {e}")
                if "ConnectionError" in str(e):
                    print("   (Likely a network issue or yfinance server problem for BSE symbols.)")
                elif "YFInvalidPeriodError" in str(e):
                    print("   (One or more BSE symbols might have an invalid 'max' period.)")

        # Prepare batch update list for ATH and Date columns (B and C)
        updates_to_sheet = []
        # Get current ATH and Date values from the sheet to compare
        try:
            current_ath_values = cache_sheet.col_values(2)[1:] # Column B
            current_date_values = cache_sheet.col_values(3)[1:] # Column C
        except Exception as e:
            print(f"❌ Error reading current ATH/Date values: {e}")
            current_ath_values = []
            current_date_values = []

        # Iterate over the original symbols_raw to match sheet rows correctly
        for i, base_symbol_raw in enumerate(symbols_raw):
            try:
                clean_symbol = base_symbol_raw.strip().upper()
                if not clean_symbol:
                    continue

                full_symbol_nse = clean_symbol + ".NS"
                full_symbol_bse = clean_symbol + ".BO"

                df_nse = data_nse.get(full_symbol_nse)
                df_bse = data_bse.get(full_symbol_bse)

                # Choose the DataFrame with more rows for ATH
                df_use = None
                if df_bse is not None and not df_bse.empty and (df_nse is None or len(df_bse) > len(df_nse)):
                    df_use = df_bse
                elif df_nse is not None and not df_nse.empty:
                    df_use = df_nse

                if df_use is None or df_use.empty:
                    continue

                if len(df_use) < 100: # Log if skipped due to insufficient data
                    continue

                new_ath = round(df_use["High"].max(), 2)

                # Validate new_ath before writing to sheet
                if not math.isfinite(new_ath):
                    new_ath_to_write = None # gspread can handle None (empty cell)
                else:
                    new_ath_to_write = new_ath

                # Row number in Google Sheet (1-based index)
                gs_row_num = i + 2 # A2 is the first data row, so index i=0 corresponds to row 2

                # Get previous ATH from the fetched current_ath_values list
                prev_ath_str = current_ath_values[i] if i < len(current_ath_values) else None
                prev_ath = None
                if prev_ath_str:
                    try:
                        prev_ath = float(prev_ath_str)
                    except ValueError:
                        pass # prev_ath remains None

                # Get previous date from the fetched current_date_values list
                prev_date_str = current_date_values[i] if i < len(current_date_values) else None

                # Check if ATH needs update
                # Only update if new_ath_to_write is valid AND (previous ATH was invalid/different OR date is old)
                if new_ath_to_write is not None:
                    if (not isinstance(prev_ath, (float, int)) or round(prev_ath, 2) != new_ath_to_write) or prev_date_str != today_str:
                        updates_to_sheet.append({
                            'range': f'B{gs_row_num}:C{gs_row_num}', # Update B and C for this row
                            'values': [[new_ath_to_write, today_str]]
                        })
                        updates += 1
                else:
                    pass


            except Exception as e:
                print(f"❌ Error processing {clean_symbol} for ATH update: {e}")

        # Perform batch update for ATH Cache sheet
        if updates_to_sheet:
            try:
                cache_sheet.batch_update(updates_to_sheet, value_input_option='USER_ENTERED')
                print(f"✅ ATH Cache update complete: {updates} record(s) updated.")
            except Exception as e:
                print(f"❌ Error performing batch update for ATH Cache: {e}")
        else:
            print("ℹ️ No ATH records needed updating in ATH Cache.")
else:
    print(f"🕒 Skipping ATH Cache update (not in allowed time range or already updated recently). Current time: {now.strftime('%H:%M:%S')}, Weekday: {weekday} (0=Mon).")


# === Price Scan - Fetching LTPs ===
print("⚡ Fetching LTPs for price scan...")

# Load ATH data from ATH Cache sheet (A:B) for calculations
try:
    ath_data_raw = cache_sheet.get_all_values()
    # Skip header row and ensure rows have at least 2 columns (Symbol, ATH)
    ath_data = [row for row in ath_data_raw[1:] if len(row) >= 2 and row[0].strip()]
except Exception as e:
    print(f"❌ Error reading ATH data for LTP calculation: {e}")
    ath_data = []

# Create a map for quick ATH lookup: {symbol.NS: ATH_value}
ath_map = {}
# Iterate through the symbols_raw to build the ath_map correctly based on the sheet's content
for row_idx, row in enumerate(ath_data):
    try:
        # The symbol from the sheet (e.g., '20MICRONS')
        base_symbol_from_sheet = row[0].strip().upper()
        ath_value = float(row[1]) # Convert ATH value to float

        if math.isfinite(ath_value): # Only add finite ATH values to map
            # Add both .NS and .BO versions to the map if the base symbol was used
            if not base_symbol_from_sheet.endswith(".NS") and not base_symbol_from_sheet.endswith(".BO"):
                ath_map[base_symbol_from_sheet + ".NS"] = ath_value
                ath_map[base_symbol_from_sheet + ".BO"] = ath_value
            else:
                ath_map[base_symbol_from_sheet] = ath_value
        else:
            pass
    except (ValueError, IndexError) as e:
        continue # Skip malformed rows

# Function to fetch LTP for a single symbol
def fetch_ltp(symbol):
    try:
        # Use fast_info for quicker access to last_price
        ltp = yf.Ticker(symbol).fast_info["last_price"]
        return symbol, round(ltp, 2)
    except Exception as e:
        return symbol, None # Return None if LTP cannot be fetched

# Use ThreadPoolExecutor for concurrent LTP fetching
ltp_map = {}
# MODIFIED: Filter symbols to only include those ending with .NS for LTP fetching
relevant_symbols_for_ltp = [s for s in symbols if s.endswith(".NS") and s in ath_map]

if relevant_symbols_for_ltp:
    with ThreadPoolExecutor(max_workers=40) as executor:
        futures = {executor.submit(fetch_ltp, sym): sym for sym in relevant_symbols_for_ltp}
        for f in as_completed(futures):
            sym, ltp = f.result()
            if ltp is not None:
                ltp_map[sym] = ltp
else:
    print("ℹ️ No relevant symbols found for LTP fetching.")
print(f"✅ LTP data fetched for {len(ltp_map)} records.")

# === Build Scanned Symbols List and Initial Scan Data ===
# This list will contain only the symbols that pass the initial drop_pct filter.
scanned_symbols = []
# This list will store the raw data for symbols, LTP, ATH, and % from ATH
# for symbols that pass the initial filter.
initial_scan_data = []

# Iterate through the original symbols_raw to ensure we match the sheet's input order for output
for s_raw in symbols_raw:
    clean_s = s_raw.strip().upper()

    # Prioritize NSE data for LTP and ATH for the scan filter
    nse_sym = clean_s + ".NS"

    ltp = ltp_map.get(nse_sym) # Only try to get LTP from NSE version

    # For ATH, we still want the best available (could be from .BO if it had more history)
    # So, we should check both .NS and .BO for ATH, then use that for the filter.
    # The `ath_map` already contains the best ATH, so we should use that.
    ath_from_map_nse = ath_map.get(clean_s + ".NS")
    ath_from_map_bse = ath_map.get(clean_s + ".BO")
    ath = None
    if ath_from_map_nse is not None and math.isfinite(ath_from_map_nse):
        ath = ath_from_map_nse
    elif ath_from_map_bse is not None and math.isfinite(ath_from_map_bse):
        ath = ath_from_map_bse


    if ltp is not None and ath is not None and ath != 0:
        drop_pct = round(100 - (ltp / ath * 100), 2)
        # Only include symbols where drop from ATH is 30% or less
        if drop_pct <= 30:
            base_sym_display = clean_s # Keep original display for output
            # Add the NSE version to scanned_symbols for subsequent calculations
            scanned_symbols.append(nse_sym)
            initial_scan_data.append([f"NSE:{base_sym_display},", ltp, ath, drop_pct])

print(f"✅ Initial price scan filtered {len(scanned_symbols)} records.")

# === SMA Logic (200 or 50) ===
# Fetches Simple Moving Averages (SMA) for the scanned symbols.
print("📊 Fetching SMA (200 or 50)...")

def fetch_sma(symbol):
    try:
        # Download 250 days of history to ensure enough data for 200-day SMA
        df = yf.Ticker(symbol).history(period="250d")
        if df.empty or 'Close' not in df.columns:
            return symbol, None

        if len(df) >= 200:
            sma = round(df["Close"][-200:].mean(), 2) # Calculate 200-day SMA
            return symbol, sma
        elif len(df) >= 50:
            sma = round(df["Close"][-50:].mean(), 2)  # Calculate 50-day SMA if 200 not available
            return symbol, sma
        else:
            return symbol, None
    except Exception as e:
        return symbol, None # Return None if data cannot be fetched or calculated

sma_map = {}
if scanned_symbols:
    with ThreadPoolExecutor(max_workers=30) as executor:
        futures = {executor.submit(fetch_sma, sym): sym for sym in scanned_symbols}
        for f in as_completed(futures):
            sym, sma = f.result()
            if sma is not None:
                sma_map[sym] = sma
else:
    print("ℹ️ No symbols to fetch SMA for.")
print(f"✅ SMA data fetched for {len(sma_map)} records.")

# === PMHigh Logic (Previous Month High / Previous-Previous Month High) ===
# Calculates dates for the previous two months to check high prices.
today = date.today()
first_day_this_month = today.replace(day=1)
last_day_prev_month = first_day_this_month - timedelta(days=1)
first_day_prev_month = last_day_prev_month.replace(day=1)
last_day_prev2_month = first_day_prev_month - timedelta(days=1)
first_day_prev2_month = last_day_prev2_month.replace(day=1)

pm_start = first_day_prev_month.strftime('%Y-%m-%d')
pm_end = last_day_prev_month.strftime('%Y-%m-%d')
pp_start = first_day_prev2_month.strftime('%Y-%m-%d')
pp_end = last_day_prev2_month.strftime('%Y-%m-%d')

print("🔎 PMHigh/Prev2PMHigh logic...")
def fetch_pm_logic(symbol):
    try:
        # Historical data for last 3 months to cover previous two full months + current month for breakout check
        df_full = yf.Ticker(symbol).history(period="3mo")
        if df_full.empty or 'Close' not in df_full.columns:
            return symbol, "No"

        df_full = df_full[~df_full['Close'].isna()]

        # Filter previous and prev-prev month ranges
        df_pm = df_full.loc[pm_start:pm_end]
        df_pp = df_full.loc[pp_start:pp_end]

        pm_high = df_pm["High"].max() if not df_pm.empty else None
        pp_high = df_pp["High"].max() if not df_pp.empty else None

        # Check for breakout in current month (after prev month end)
        current_month_start_date = last_day_prev_month + timedelta(days=1)
        # Ensure index is datetime for comparison
        df_current = df_full[df_full.index.date >= current_month_start_date]

        breakout_date = None
        breakout_reason = "No"

        # Check for PM High breakout first
        if pm_high is not None:
            # Find the first date where Close price is greater than PM High in the current month
            breakout_rows = df_current[df_current["Close"] > pm_high]
            if not breakout_rows.empty:
                breakout_date = breakout_rows.index[0].date()
                breakout_reason = f"Yes ({breakout_date.strftime('%d-%b-%Y')})"
                return symbol, breakout_reason

        # Check for PP High breakout if PM High wasn't broken or didn't exist
        if pp_high is not None:
            # Find the first date where Close price is greater than PP High in the current month
            breakout_rows = df_current[df_current["Close"] > pp_high]
            if not breakout_rows.empty:
                breakout_date = breakout_rows.index[0].date()
                breakout_reason = f"Yes ({breakout_date.strftime('%d-%b-%Y')})"
                return symbol, breakout_reason

        return symbol, breakout_reason
    except Exception as e:
        return symbol, "No" # Return "No" on error

pm_flags = {}
if scanned_symbols:
    with ThreadPoolExecutor(max_workers=30) as executor:
        futures = {executor.submit(fetch_pm_logic, sym): sym for sym in scanned_symbols}
        for f in as_completed(futures):
            sym, flag = f.result()
            pm_flags[sym] = flag
else:
    print("ℹ️ No symbols to fetch PMHigh logic for.")
print(f"✅ PMHigh logic complete for {len(pm_flags)} records.")

# === Weekly Inside Bar (WIB) ===
# Checks for consecutive weekly inside bars over the last few weeks based on custom definition.
print("📉 Weekly Inside Bar check (Consecutive count - Custom Logic)...")

def check_wib(symbol):
    try:
        # Fetch weekly data directly from yfinance
        df_weekly = yf.Ticker(symbol).history(period="3mo", interval="1wk")
        if df_weekly.empty or 'High' not in df_weekly.columns or 'Low' not in df_weekly.columns or 'Close' not in df_weekly.columns:
            return symbol, 0 # Return 0 if no data or essential columns are missing

        # Determine if the current day is Saturday (5) or Sunday (6)
        current_day_of_week = datetime.now().weekday() # Monday is 0, Sunday is 6

        # --- Conditional exclusion of current (incomplete) week ---
        # If it's Monday-Friday, drop the last (potentially incomplete) week.
        # If it's Saturday/Sunday, the last week is considered complete (Friday's close).
        if current_day_of_week <= 4: # Monday (0) to Friday (4)
            # Check if the last entry in df_weekly is from the current week
            # This is a heuristic; yfinance weekly data usually ends on Friday.
            # If the last entry's date is within the last 7 days of today, assume it's incomplete.
            if not df_weekly.empty and (datetime.now().date() - df_weekly.index[-1].date()).days < 7:
                df_weekly = df_weekly.iloc[:-1] # Drop the last (potentially incomplete) week
        # Else (Saturday/Sunday), the last week is considered complete and is kept.

        # Need at least two full weeks to check for the first pair (Week 1 vs Week 2)
        if len(df_weekly) < 2:
            return symbol, 0

        consecutive_valid_pairs = 0
        # Iterate backward from the most recent completed week (index -1, representing Week 1)
        # We need up to 6 weeks to form 5 pairs (Week 1 vs 2, Week 2 vs 3, ..., Week 5 vs 6)
        # The loop range should go from len-1 down to 1 (inclusive of 1, because i-1 is used)
        # And we stop after checking 5 pairs, meaning when consecutive_valid_pairs reaches 5.
        for i in range(len(df_weekly) - 1, 0, -1): # Loop from most recent full week backwards
            current_week = df_weekly.iloc[i]
            previous_week = df_weekly.iloc[i-1]

            # Custom Inside Bar Conditions:
            # 1. Current High <= 102% of Previous High
            is_high_condition = (current_week['High'] <= previous_week['High'] * 1.02)

            # 2. Current Close >= Previous Low
            is_close_condition = (current_week['Close'] >= previous_week['Low'])

            # A pair is valid if BOTH High Condition AND Close Condition are met.
            # Low breach of current candle is explicitly allowed and thus not checked here.
            is_valid_inside_pair = is_high_condition and is_close_condition

            if is_valid_inside_pair:
                consecutive_valid_pairs += 1
                if consecutive_valid_pairs >= 5: # Stop at a maximum of 5 consecutive days/pairs
                    break
            else:
                break # Stop counting if any pair fails the conditions

        return symbol, consecutive_valid_pairs
    except Exception as e:
        return symbol, 0 # Return 0 on any error

wib_flags = {}
if scanned_symbols:
    with ThreadPoolExecutor(max_workers=30) as executor:
        futures = {executor.submit(check_wib, sym): sym for sym in scanned_symbols}
        for f in as_completed(futures):
            sym, count = f.result()
            wib_flags[sym] = count # Store the numerical count
else:
    print("ℹ️ No symbols to check WIB for.")
print(f"✅ WIB check complete for {len(wib_flags)} records.")

# === Daily Inside Bar (DIB) ===
# Checks for consecutive daily inside bars over the last few days based on the new definition.
print("📉 Daily Inside Bar check (5-day consecutive count - New Logic)...")

def check_dib(symbol):
    try:
        # Get recent daily historical data (enough for 6 days + buffer for 5 pairs)
        df_daily = yf.Ticker(symbol).history(period="1mo") # Fetch ~1 month of data
        if df_daily.empty or 'High' not in df_daily.columns or 'Low' not in df_daily.columns or 'Close' not in df_daily.columns:
            return symbol, 0 # Return 0 if no data or essential columns are missing

        df_daily = df_daily[~df_daily['Close'].isna()] # Remove rows with NaN Close prices

        # Drop the current (incomplete) day if it exists as the last row.
        # This ensures we only use finalized, completed daily candles.
        if not df_daily.empty and df_daily.index[-1].date() == datetime.now().date():
            df_daily = df_daily.iloc[:-1] # Drop the last (potentially incomplete) day

        if len(df_daily) < 2: # Need at least two full days to check for an inside bar
            return symbol, 0

        consecutive_inside_bars = 0
        # Iterate backward from the most recent completed day (index -1, representing Day 1)
        # We need up to 6 days to form 5 pairs (Day 1 vs 2, Day 2 vs 3, ..., Day 5 vs 6)
        # The loop range should go from len-1 down to 1 (inclusive of 1, because i-1 is used)
        # And we stop after checking 5 pairs, meaning when consecutive_inside_bars reaches 5.
        for i in range(len(df_daily) - 1, 0, -1): # Loop from most recent full day backwards
            current_day = df_daily.iloc[i]
            previous_day = df_daily.iloc[i-1]

            # NEW Inside Bar Condition for DIB (same as refined WIB logic):
            # 1. Current High <= 102% of Previous High
            is_high_condition = (current_day['High'] <= previous_day['High'] * 1.02)

            # 2. Current Close >= Previous Low
            is_close_condition = (current_day['Close'] >= previous_day['Low'])

            # A pair is valid if BOTH High Condition AND Close Condition are met.
            # Low breach of current candle is explicitly allowed and thus not checked here.
            is_valid_inside_pair = is_high_condition and is_close_condition

            if is_valid_inside_pair:
                consecutive_inside_bars += 1
                if consecutive_inside_bars >= 5: # Stop at a maximum of 5 consecutive days/pairs
                    break
            else:
                break # Stop counting if a non-inside bar is encountered

        return symbol, consecutive_inside_bars
    except Exception as e:
        return symbol, 0 # Return 0 on any error

dib_flags = {}
if scanned_symbols:
    with ThreadPoolExecutor(max_workers=30) as executor:
        futures = {executor.submit(check_dib, sym): sym for sym in scanned_symbols}
        for f in as_completed(futures):
            sym, count = f.result()
            dib_flags[sym] = count # Store the numerical count
else:
    print("ℹ️ No symbols to check DIB for.")
print(f"✅ DIB check complete for {len(dib_flags)} records. ✅ All scan steps finished.")

# === Consolidate and Write All Scan Results to ATH Cache (G:O) ===
print("📝 Consolidating and writing all scan results to ATH Cache sheet (G:O)...")

# Clear previous scan results in ATH Cache (columns G to P)
try:
    cache_sheet.batch_clear(['G2:P1000'])
    print("✅ Cleared previous scan results in ATH Cache (G:P).")
except Exception as e:
    print(f"❌ Error clearing ATH Cache sheet range G:P: {e}")

final_output_rows_ath_cache = []
# Iterate through the original symbols_raw to ensure output matches input order
for s_raw in symbols_raw:
    clean_s = s_raw.strip().upper()

    # Define the NSE and BSE versions for lookups
    nse_sym = clean_s + ".NS"
    bse_sym = clean_s + ".BO"

    # For LTP, we only want NSE data, as per the user's request
    ltp = ltp_map.get(nse_sym)

    # For ATH, we want the best available ATH, which could be from either NSE or BSE
    ath_from_map_nse = ath_map.get(nse_sym)
    ath_from_map_bse = ath_map.get(bse_sym)
    ath = None
    if ath_from_map_nse is not None and math.isfinite(ath_from_map_nse):
        ath = ath_from_map_nse
    elif ath_from_map_bse is not None and math.isfinite(ath_from_map_bse):
        ath = ath_from_map_bse

    # For SMA, PM_Flag, WIB, DIB, these maps will primarily contain NSE data
    # because `scanned_symbols` is now built from NSE-only LTPs.
    sma = sma_map.get(nse_sym)
    pm_flag = pm_flags.get(nse_sym, "")
    wib_count = wib_flags.get(nse_sym, 0)
    dib_count = dib_flags.get(nse_sym, 0)

    # Calculate ltp_gt_sma_flag
    ltp_gt_sma_flag = "No"
    if ltp is not None and sma is not None:
        if ltp > sma:
            ltp_gt_sma_flag = "Yes"

    # Only proceed if we found valid LTP and ATH for the NSE version (or best ATH available)
    if ltp is not None and ath is not None and ath != 0:
        drop_pct = round(100 - (ltp / ath * 100), 2)
    else:
        # If no valid data found for this symbol, append empty/default values
        drop_pct = None
        ltp = None
        ath = None
        sma = None
        pm_flag = ""
        wib_count = 0
        dib_count = 0
        ltp_gt_sma_flag = "No" # Ensure it's reset if no valid data

    # Determine the display symbol for the sheet
    display_symbol = f"NSE:{clean_s}," # Default to NSE

    # --- RE-APPLYING THE FILTER FOR ATH CACHE SHEET (G:O) ---
    # Only append the row if drop_pct is valid and within 30%
    if drop_pct is not None and drop_pct <= 30:
        # Construct the row for Google Sheet (ATH Cache sheet)
        # Order: Symbol, LTP, ATH, % from ATH, SMA, LTP > SMA, PMHigh, WIB, DIB
        row_data = [
            display_symbol,      # G - Symbol (display original symbol with detected exchange)
            ltp,                 # H
            ath,                 # I
            drop_pct,            # J
            sma,                 # K (This will be None if SMA not found)
            ltp_gt_sma_flag,     # L
            pm_flag,             # M
            wib_count,           # N (Now a number)
            dib_count            # O (Now a number)
        ]
        final_output_rows_ath_cache.append(row_data)

# Write all consolidated data to ATH Cache sheet starting from G2
if final_output_rows_ath_cache:
    try:
        start_cell = 'G2'
        end_col = 'O'
        end_row = len(final_output_rows_ath_cache) + 1 # +1 for header row (G2 is first data row)
        range_to_update = f"{start_cell}:{end_col}{end_row}"

        cache_sheet.update(values=final_output_rows_ath_cache, range_name=range_to_update, value_input_option='USER_ENTERED')

        # Apply number formatting to relevant columns (H, I, J, K)
        # H: LTP, I: ATH, J: % from ATH, K: SMA
        num_rows_written_ath_cache = len(final_output_rows_ath_cache)
        if num_rows_written_ath_cache > 0:
            cache_sheet.format(f"H2:K{1 + num_rows_written_ath_cache}", {'numberFormat': {'type': 'NUMBER', 'pattern': '0.00'}})

        print(f"✅ All scan results written to ATH Cache sheet with {len(final_output_rows_ath_cache)} records.")
    except Exception as e:
        print(f"❌ Error writing consolidated data to ATH Cache sheet: {e}")
else:
    print("ℹ️ No records met the scan criteria to be written to ATH Cache.")


# === Filter and Write to Price Scan Sheet (B4:G4) ===
# Modified to remove Market Cap column and adjust range to B:G
print("📊 Filtering data and writing to Price Scan sheet (B4:G4)...")

# Clear previous contents in Price Scan sheet (B4 to G and down)
try:
    scan_sheet.batch_clear(['B4:G1000']) # Changed from H1000 to G1000
    print("✅ Cleared previous contents in Price Scan sheet (B:G).")
except Exception as e:
    print(f"❌ Error clearing Price Scan sheet range B:G: {e}")


filtered_scan_results = []

# Read all relevant data from ATH Cache sheet (G:O)
try:
    all_scan_data_from_ath_cache_raw = cache_sheet.get_all_values()
    # Assuming header is in row 1, data starts from row 2.
    # We need columns G to O, which are indices 6 to 14 (0-based)
    # Filter out empty rows and ensure enough columns
    all_scan_data_from_ath_cache = [
        row for row in all_scan_data_from_ath_cache_raw[1:]
        if len(row) > 14 and row[6].strip() # Check if G column is not empty
    ]
except Exception as e:
    print(f"❌ Error reading all scan data from ATH Cache sheet: {e}")
    all_scan_data_from_ath_cache = []


if all_scan_data_from_ath_cache:
    for row_data_slice in all_scan_data_from_ath_cache:
        # Ensure the row has enough elements for all expected columns (G to O is 9 elements)
        if len(row_data_slice) < 9:
            continue # Skip malformed rows

        # Extract relevant values (0-based index from the sliced row_data_slice)
        # G is index 6, H is 7, J is 9, L is 11, M is 12, N is 13, O is 14.
        symbol_g         = row_data_slice[6]       # G - Symbol
        ltp_h            = row_data_slice[7]       # H - LTP
        pct_from_ath_str = row_data_slice[9]       # J - % from ATH (as string)
        ltp_gt_sma_str   = row_data_slice[11]      # L - LTP > SMA (as string)
        pm_high_col_str  = row_data_slice[12]      # M - PM High breakout with date (as string)
        wib_count_str    = row_data_slice[13]      # N - WIB (as string)
        dib_count_str    = row_data_slice[14]      # O - DIB (as string)


        # Convert string values to appropriate types for filtering
        pct_from_ath = None
        try:
            pct_from_ath = float(pct_from_ath_str)
        except ValueError:
            pass # pct_from_ath remains None

        wib_count = 0
        try:
            wib_count = int(wib_count_str)
        except ValueError:
            pass

        dib_count = 0
        try:
            dib_count = int(dib_count_str)
        except ValueError:
            pass

        # === Filtering Logic ===
        is_pct_ath_valid = isinstance(pct_from_ath, (int, float)) and pct_from_ath <= 30
        is_ltp_gt_sma = isinstance(ltp_gt_sma_str, str) and ltp_gt_sma_str.strip().upper() == "YES"
        is_pm_breakout = isinstance(pm_high_col_str, str) and pm_high_col_str.strip().upper().startswith("YES")

        if is_pct_ath_valid and is_ltp_gt_sma and is_pm_breakout:
            # ✅ Extract breakout date from "Yes (01-Jul-2025)"
            breakout_date = ""
            try:
                # Split by the first '(' and then remove the trailing ')'
                breakout_date = pm_high_col_str.strip().split("(", 1)[1].rstrip(")")
            except IndexError:
                breakout_date = "" # Handle cases where '(' is not found

            # Removed Market Cap (D) from the filtered_row structure
            filtered_row = [
                symbol_g,       # B - Symbol
                ltp_h,          # C - LTP
                breakout_date,  # D - Breakout Date (shifted from E)
                pct_from_ath,   # E - % From ATH (shifted from F)
                wib_count,      # F - WIB (shifted from G)
                dib_count       # G - DIB (shifted from H)
            ]
            filtered_scan_results.append(filtered_row)

# Write to Price Scan sheet
if filtered_scan_results:
    try:
        # Update range B4 to G (last row of filtered results)
        start_cell = 'B4'
        end_col = 'G' # Changed from H to G
        end_row = len(filtered_scan_results) + 3 # +3 because B4 is the first data row
        range_to_update = f"{start_cell}:{end_col}{end_row}"

        scan_sheet.update(values=filtered_scan_results, range_name=range_to_update, value_input_option='USER_ENTERED')

        num_filtered_rows_written = len(filtered_scan_results)
        # Apply number formatting to LTP (C) and % From ATH (E, shifted from F)
        if num_filtered_rows_written > 0:
            scan_sheet.format(f"C4:C{3 + num_filtered_rows_written}", {'numberFormat': {'type': 'NUMBER', 'pattern': '0.00'}}) # LTP
            scan_sheet.format(f"E4:E{3 + num_filtered_rows_written}", {'numberFormat': {'type': 'NUMBER', 'pattern': '0.00'}}) # % From ATH (shifted from F)

        print(f"✅ Filtered results written to Price Scan sheet with {num_filtered_rows_written} records.")

        # Removed the call to update_market_cap_in_price_scan() as per user's request.

    except Exception as e:
        print(f"❌ Error writing filtered results to Price Scan sheet: {e}")
else:
    print("ℹ️ No records met the filtering criteria for Price Scan sheet.")


# ==============================================================================
# === PART 2: Combined Runner Logic (from combined_runner.py) ===
# ==============================================================================
print("\n--- Starting Combined Runner Logic ---")

# Set environment variable for Node.js modules
env = os.environ.copy()
env["NODE_PATH"] = NODE_MODULES

print("🚀 Running moneycontrol_scraper.js...")
# Execute the Node.js scraper script
result = subprocess.run(
    [NODE_PATH, JS_SCRIPT],
    cwd=os.path.dirname(JS_SCRIPT),
    capture_output=True,
    text=True,
    encoding='utf-8',
    env=env
)

if result.returncode != 0:
    print("❌ JS script failed:\n", result.stderr)
    sys.exit()

print("✅ JS script finished.")

# Check if the JSON output file exists
if not os.path.exists(JSON_FILE):
    print("❌ earnings.json not found.")
    sys.exit()

# Load data from the JSON file
with open(JSON_FILE, "r", encoding="utf-8") as f:
    data = json.load(f)

if not data:
    print("⚠️ earnings.json is empty.")
    sys.exit()

# Convert JSON data to a pandas DataFrame
df = pd.DataFrame(data)

# IMPORTANT FIX: Apply the period mapping to create the 'period_text' column
# BEFORE filtering the DataFrame based on growth criteria.
# This ensures 'period_text' always exists in the DataFrame structure.
df["period_text"] = df["period"].apply(map_period_to_range)


# Handle non-numeric values like '--' by replacing them with NaN before conversion
# Then use pd.to_numeric with errors='coerce' to turn invalid parsing into NaN
df["rev%"] = pd.to_numeric(df["revGrowth"].str.replace('%', '').str.replace(',', '').replace('--', np.nan), errors='coerce')
df["net%"] = pd.to_numeric(df["netGrowth"].str.replace('%', '').str.replace(',', '').replace('--', np.nan), errors='coerce')

# Filter DataFrame based on growth criteria, dropping rows where conversion resulted in NaN
df = df[(df["rev%"] > 30) & (df["net%"] > 30)].dropna(subset=["rev%", "net%"]).reset_index(drop=True)


# Read existing company names from Column K (11th column) of the Price Scan sheet
existing_names = set()
# gspread's col_values is 1-indexed for column number
# It returns a list of all values in the column. We need to start from row 4 (index 3).
all_col_k_values = scan_sheet.col_values(11) # Using scan_sheet consistently
for val in all_col_k_values[3:]: # Slice from index 3 to get values from row 4 onwards
    if val:
        existing_names.add(str(val).strip())

# Filter for new companies not already in the Google Sheet
df_new = df[~df["name"].isin(existing_names)].reset_index(drop=True)

if df_new.empty:
    print("📭 No new companies to add.")
else:
    count_new = len(df_new)
    print(f"Adding {count_new} new companies to '{PRICE_SCAN_SHEET_NAME}' sheet.")

    # --- Prepare data for batch update (Part 1: K, O-S) ---
    # To simulate the Excel 'insert and shift down' behavior, we'll read existing data
    # from row 4 onwards, prepend the new data, and then update the entire range.

    # Get all existing values from the sheet
    # This is necessary to get the full context for prepending
    existing_data_full = scan_sheet.get_all_values() # Using scan_sheet consistently

    # Determine the actual last row with data in column K
    last_row_k_idx = len(existing_data_full) # current last row index (0-based)
    if last_row_k_idx < 3: # If less than 4 rows exist, ensure we start from row 4 (index 3)
        last_row_k_idx = 3

    # Slice existing data from row 4 (index 3) for columns K to S (indices 10 to 18)
    # If the sheet is empty or has fewer than 4 rows, this slice will be empty, which is fine.
    existing_relevant_data = []
    if len(existing_data_full) > 3: # Only slice if there are enough rows
        existing_relevant_data = [row[10:19] for row in existing_data_full[3:]] # K to S, from row 4 onwards

    # Prepare the new data to be inserted (columns K, O, P, Q, R, S)
    new_data_for_sheet = []
    for _, row_df in df_new.iterrows():
        # Create a list representing the row from K to S (9 columns)
        # Fill with empty strings initially, then populate specific indices
        new_row_values = [""] * 9
        new_row_values[0] = row_df["name"]         # K (index 0 of K-S block)
        new_row_values[4] = row_df["period_text"]  # O (index 4 of K-S block)
        new_row_values[5] = row_df["revGrowth"]    # P (index 5 of K-S block)
        new_row_values[6] = row_df["netGrowth"]    # Q (index 6 of K-S block)

        # --- FIX START: Robustly Format Revenue and Net Profit with " Cr." ---
        # Convert to string, remove commas, strip whitespace, and replace '--' with empty string
        cleaned_revenue_val = str(row_df["revenue"]).replace(',', '').strip().replace('--', '')
        cleaned_net_val = str(row_df["net"]).replace(',', '').strip().replace('--', '')

        # Attempt conversion and formatting
        if cleaned_revenue_val: # Check if not empty after cleaning
            try:
                new_row_values[7] = f"{float(cleaned_revenue_val):.2f} Cr." # R
            except ValueError:
                new_row_values[7] = cleaned_revenue_val # Fallback to original cleaned string if conversion fails
        else:
            new_row_values[7] = '' # Set to empty if no valid number

        if cleaned_net_val: # Check if not empty after cleaning
            try:
                new_row_values[8] = f"{float(cleaned_net_val):.2f} Cr." # S
            except ValueError:
                new_row_values[8] = cleaned_net_val # Fallback to original cleaned string if conversion fails
        else:
            new_row_values[8] = '' # Set to empty if no valid number
        # --- FIX END ---

        new_data_for_sheet.append(new_row_values)

    # Combine new data with existing data that was originally from row 4 onwards
    combined_data_to_update = new_data_for_sheet + existing_relevant_data

    # Determine the range for the batch update
    end_row_for_update = 3 + len(combined_data_to_update) # gspread ranges are 1-indexed
    range_to_update = f"K4:S{end_row_for_update}"

    # Perform the batch update using named arguments to avoid DeprecationWarning
    scan_sheet.update(values=combined_data_to_update, range_name=range_to_update) # Using scan_sheet consistently
    print(f"✅ Successfully added {count_new} new companies to '{PRICE_SCAN_SHEET_NAME}' sheet.")


# ============================ PART 2: TICKERTAPE FETCHER + GOOGLE SHEETS (L–N) ============================

start_row_tickertape = 4 # Starting row for processing in Google Sheet

# Configure Chrome options for Selenium
options = Options()
options.add_argument("--headless=new") # Run Chrome in headless mode
options.add_argument("--disable-gpu")
options.add_argument("--window-size=1920,1080")
options.add_argument("--log-level=3") # Suppress verbose logging

driver = None # Initialize driver to None
try:
    driver = webdriver.Chrome(options=options)
    wait = WebDriverWait(driver, 15) # WebDriverWait for explicit waits

    # List to store updates for batch processing in Google Sheets
    batch_updates = []

    # Iterate through rows in the Google Sheet, starting from row 4
    # We'll fetch all relevant data once to avoid repeated API calls inside the loop
    # Get values from Column K (stock symbols) and Columns L, M, N (existing data)
    all_sheet_values = scan_sheet.get_all_values() # Using scan_sheet consistently

    # Process up to 100 rows starting from start_row_tickertape (4)
    # Adjust for 0-indexed list: row 4 is index 3
    for r_idx in range(start_row_tickertape - 1, min(start_row_tickertape - 1 + 100, len(all_sheet_values))):
        current_row_num = r_idx + 1 # Convert 0-indexed list index back to 1-indexed row number

        row_data = all_sheet_values[r_idx]

        # Column K is index 10, L is 11, M is 12, N is 13 (0-indexed)
        stock = row_data[10] if len(row_data) > 10 else None
        col_l_val = row_data[11] if len(row_data) > 11 else None
        col_m_val = row_data[12] if len(row_data) > 12 else None
        col_n_val = row_data[13] if len(row_data) > 13 else None

        # Skip if no stock symbol or if L, M, N are already populated
        if not stock or (col_l_val and col_m_val and col_n_val):
            continue

        print(f"\n🔍 Processing {stock} (Row {current_row_num})")
        if not search_and_open_company(stock, driver_instance=driver, wait_instance=wait):
            print("❌ Could not open Tickertape page.")
            continue

        try:
            # Click on the Financials tab
            safe_click("//a[contains(@href,'#financials')]", by=By.XPATH, driver_instance=driver)
        except Exception as e:
            print(f"❌ Financials tab not found for {stock}: {e}")
            continue

        select_quarterly(driver_instance=driver, wait_instance=wait) # Select quarterly view
        scroll_table_right(driver_instance=driver, wait_instance=wait) # Scroll table to reveal latest data

        quarters = get_quarters(driver_instance=driver)
        rev_vals = get_row_by_data_key("qIncTrev", driver_instance=driver) # Total Revenue
        pbt_vals = get_row_by_data_key("qIncPbt", driver_instance=driver)   # Profit Before Tax

        print("📊 Quarters:", quarters)
        print("📊 Revenue:", rev_vals)
        print("📊 PBT    :", pbt_vals)

        # Ensure enough data points are available for growth calculation
        if len(quarters) < 3 or len(rev_vals) < 3 or len(pbt_vals) < 3:
            print("⚠️ Incomplete data. Skipping...")
            continue

        try:
            # Extract and clean values for the last two available quarters
            rev_sep = float(rev_vals[0].replace(',', ''))
            rev_dec = float(rev_vals[1].replace(',', ''))
            pbt_sep = float(pbt_vals[0].replace(',', ''))
            pbt_dec = float(pbt_vals[1].replace(',', ''))

            # Calculate growth percentages
            rev_growth = calculate_growth(rev_sep, rev_dec)
            pbt_growth = calculate_growth(pbt_sep, pbt_dec)

            # Format percentages
            rev_pct = f"{rev_growth:+.2f}%"
            pbt_pct = f"{pbt_growth:+.2f}%"

            # Create period text for the growth calculation
            from_q = quarters[0][:3].capitalize() + " " + quarters[0][-2:]
            to_q   = quarters[1][:3].capitalize() + " " + quarters[1][-2:]
            period = f"{from_q} to {to_q}"

            print(f"📈 Revenue: {rev_pct}, PBT: {pbt_pct}")

            # Add this row's updates to the batch list
            batch_updates.append({
                'range': f'L{current_row_num}:N{current_row_num}',
                'values': [[period, rev_pct, pbt_pct]]
            })

        except Exception as e:
            print(f"❌ Error processing data for {stock}: {e}")
            continue

finally:
    if driver:
        driver.quit() # Ensure the browser is closed
    # Perform a single batch update for all collected Tickertape data
    if batch_updates:
        scan_sheet.batch_update(batch_updates) # Using scan_sheet consistently
        print(f"\n✅ Successfully updated {len(batch_updates)} rows in Google Sheet with Tickertape data.")
    print("\n✅ Done – browser closed.")

print("\n--- Combined execution process finished. ---")
