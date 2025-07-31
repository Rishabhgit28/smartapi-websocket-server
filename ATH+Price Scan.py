import yfinance as yf
import os
from datetime import datetime, time, timedelta, date
from concurrent.futures import ThreadPoolExecutor, as_completed
import locale
import math # Import the math module for isfinite()

# --- Google Sheets Imports ---
from oauth2client.service_account import ServiceAccountCredentials
import gspread

# === Google Sheets Setup ===
# IMPORTANT: Replace these placeholder values with your actual Google Sheet ID and sheet names.
# The GOOGLE_SHEET_ID is found in your Google Sheet's URL.
GOOGLE_SHEET_ID = "1cYBpsVKCbrYCZzrj8NAMEgUG4cXy5Q5r9BtQE1Cjmz0"
ATH_CACHE_SHEET_NAME = "ATH Cache"
PRICE_SCAN_SHEET_NAME = "Price Scan"

# These might not be used in this specific script but are included as per your provided setup.
# If not used, you can remove their initialization later.
DASHBOARD_SHEET_NAME = "Dashboard" # Placeholder, replace if used
CREDENTIAL_SHEET_NAME = "Credential" # Placeholder, replace if used

# Determine the path for the service account JSON key file
# This logic checks for a secret file in a specific path first, then falls back to local.
if os.path.exists("/etc/secrets/creds.json"):
    JSON_KEY_FILE_PATH = "/etc/secrets/creds.json"
else:
    # Ensure this JSON file is in the same directory as your script, or provide its full path.
    # Replace "the-money-method-ad6d7-f95331cf5cbf.json" with your actual service account key filename.
    JSON_KEY_FILE_PATH = os.path.join(os.path.dirname(__file__), "the-money-method-ad6d7-f95331cf5cbf.json")

# Define the necessary OAuth2 scopes for Google Sheets and Drive access
SCOPE = [
    "https://spreadsheets.google.com/feeds",
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive"
]

# Authenticate with Google Sheets using the service account credentials
try:
    creds = ServiceAccountCredentials.from_json_keyfile_name(JSON_KEY_FILE_PATH, SCOPE)
    client = gspread.authorize(creds)
    gsheet = client.open_by_key(GOOGLE_SHEET_ID)

    # Assign worksheets
    # stock_sheet and cache_sheet refer to the same Google Sheet worksheet instance
    stock_sheet = gsheet.worksheet(ATH_CACHE_SHEET_NAME) # Used for loading initial symbols
    cache_sheet = gsheet.worksheet(ATH_CACHE_SHEET_NAME) # Used for ATH updates and all scan results
    scan_sheet = gsheet.worksheet(PRICE_SCAN_SHEET_NAME) # For filtered output

    # Optional: Initialize Dashboard and Credential sheets if they are part of your workflow
    # Dashboard = gsheet.worksheet(DASHBOARD_SHEET_NAME)
    # Credential = gsheet.worksheet(CREDENTIAL_SHEET_NAME)

    print("✅ Successfully connected to Google Sheets.")
except Exception as e:
    print(f"❌ Error connecting to Google Sheets: {e}")
    print("Please ensure your GOOGLE_SHEET_ID is correct and the service account JSON key file is valid and accessible.")
    exit() # Exit if connection fails, as the script cannot proceed without sheet access

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
    (weekday == 0 and current_time <= time(8, 0))      # Monday before 8 AM (Corrected from 1 to 0 for Monday)
)

# --- TEMPORARY OVERRIDE FOR TESTING ---
# REMOVED: ath_allowed = True (This was removed in the previous iteration to test date skipping)
# --- END TEMPORARY OVERRIDE ---


# === ATH Cache Update (Columns B and C of ATH Cache sheet) ===
# This section updates the All-Time High (ATH) and its last update date
# for the symbols listed in column A of the 'ATH Cache' sheet.
if ath_allowed:
    print("📈 Running ATH Cache Update...")

    today_str = now.strftime("%d-%b-%Y")
    updates = 0

    # ✅ Step 1: Global check on Column C (Last Updated - where the date is actually written)
    # FIX: Changed from column 4 (D) to column 3 (C) for date check
    try:
        date_values_raw = cache_sheet.col_values(3)[1:] # Column C is index 3 (1-based)
        # print(f"DEBUG: Raw date values from Column C: {date_values_raw}") # DEBUG PRINT
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

    # print(f"DEBUG: Normalized dates: {normalized_dates}") # DEBUG PRINT
    latest_update = max(normalized_dates) if normalized_dates else None
    # print(f"DEBUG: latest_update: {latest_update}, now.date(): {now.date()}, Days difference: {(now.date() - latest_update).days if latest_update else 'N/A'}") # DEBUG PRINT

    # Check the actual condition for skipping
    if latest_update and (now.date() - latest_update).days < 2:
        print(f"⏩ Skipping ATH Cache update: Already updated on {latest_update.strftime('%d-%b-%Y')} (less than 2 days ago).")
    else:
        # ✅ Step 2: Download data if update is required
        # nse_symbols and bse_symbols are now correctly populated by the new logic above
        # print(f"DEBUG: NSE Symbols for download: {nse_symbols}") # DEBUG PRINT
        # print(f"DEBUG: BSE Symbols for download: {bse_symbols}") # DEBUG PRINT

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
                    progress=True # Re-enabled progress bar for yf.download()
                )
            except Exception as e:
                print(f"❌ NSE bulk download failed: {e}")
                # Log specific errors for individual symbols if possible, or general failure
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
                    progress=True # Re-enabled progress bar for yf.download()
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
                    # print(f"DEBUG: ATH - Skipping {clean_symbol}: No data found after download for either NSE or BSE version.")
                    continue

                if len(df_use) < 100: # Log if skipped due to insufficient data
                    # print(f"DEBUG: ATH - Skipping {clean_symbol}: Insufficient data ({len(df_use)} days) for ATH calculation (needs at least 100).")
                    continue

                new_ath = round(df_use["High"].max(), 2)

                # Validate new_ath before writing to sheet
                if not math.isfinite(new_ath):
                    # print(f"DEBUG: ATH - Invalid (NaN/Inf) ATH value for {clean_symbol}: {new_ath}. Setting to None for sheet.")
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
                    # print(f"DEBUG: ATH - Not updating {clean_symbol} in sheet as new_ath is invalid.")
                    pass


            except Exception as e:
                print(f"❌ Error processing {clean_symbol} for ATH update: {e}")

        # Perform batch update for ATH Cache sheet
        if updates_to_sheet:
            try:
                # DeprecationWarning fix: Pass values first, then range_name as named argument
                cache_sheet.batch_update(updates_to_sheet, value_input_option='USER_ENTERED')
                print(f"✅ ATH Cache update complete: {updates} record(s) updated.")
            except Exception as e:
                print(f"❌ Error performing batch update for ATH Cache: {e}")
        else:
            print("ℹ️ No ATH records needed updating in ATH Cache.")
else:
    print(f"🕒 Skipping ATH Cache update (not in allowed time range or already updated recently). Current time: {now.strftime('%H:%M:%S')}, Weekday: {weekday} (0=Mon).")


# === Price Scan - Fetching LTPs ===
# This section fetches Last Traded Prices (LTPs) for the scanned symbols.
print("⚡ Fetching LTPs for price scan...")

# Load ATH data from ATH Cache sheet (A:B) for calculations
# gspread.get_all_values() fetches all data as a list of lists (rows)
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
            # This ensures that if the sheet has 'RELIANCE', both RELIANCE.NS and RELIANCE.BO can be looked up.
            if not base_symbol_from_sheet.endswith(".NS") and not base_symbol_from_sheet.endswith(".BO"):
                ath_map[base_symbol_from_sheet + ".NS"] = ath_value
                ath_map[base_symbol_from_sheet + ".BO"] = ath_value
            else:
                ath_map[base_symbol_from_sheet] = ath_value
        else:
            # print(f"DEBUG: ATH Map - Skipping non-finite ATH for {base_symbol_from_sheet}: {row[1]}")
            pass
    except (ValueError, IndexError) as e:
        # print(f"DEBUG: ATH Map - Skipping malformed row {row_idx+2} (symbol: {row[0] if len(row)>0 else 'N/A'}): {e}")
        continue # Skip malformed rows

# Function to fetch LTP for a single symbol
def fetch_ltp(symbol):
    try:
        # Use fast_info for quicker access to last_price
        ltp = yf.Ticker(symbol).fast_info["last_price"]
        return symbol, round(ltp, 2)
    except Exception as e:
        # print(f"Warning: Could not fetch LTP for {symbol}: {e}") # Too verbose
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
    ath = ath_map.get(nse_sym) # Get ATH from NSE version (even if BSE had more data, for consistency in scan)

    # If the original symbol in the sheet had a suffix, use that specific one for lookup
    # This part is a bit tricky, as 'symbols' now has both. We need to ensure we use the correct one.
    # The previous logic for `ltp` and `ath` directly above handles this by explicitly looking for `nse_sym`.
    # If the original `clean_s` was `RELIANCE.BO`, then `nse_sym` would be `RELIANCE.BO.NS`, which won't exist.
    # Let's refine this to ensure we use the *correct* full symbol from the `symbols` list for lookup.

    # Find the actual full symbol that was processed and has data
    actual_full_symbol = None
    if nse_sym in ltp_map: # Check if NSE version has LTP data
        actual_full_symbol = nse_sym
    elif clean_s.endswith(".BO") and (clean_s in ltp_map): # If it was explicitly a .BO symbol and has LTP (though we restricted LTP to .NS only)
        # This branch will likely not be hit for LTP, but keep for ATH if needed
        actual_full_symbol = clean_s
    elif not clean_s.endswith(".NS") and not clean_s.endswith(".BO"):
        # If it was a raw symbol, and we only fetched NSE LTP, then it must be the NSE version
        actual_full_symbol = nse_sym
    
    if actual_full_symbol:
        ltp = ltp_map.get(actual_full_symbol)
        # For ATH, we still want the best available (could be from .BO if it had more history)
        # So, we should check both .NS and .BO for ATH, then use that for the filter.
        # This is where the initial ATH update's `df_use` selection comes into play.
        # The `ath_map` already contains the best ATH, so we should use that.
        ath_from_map_nse = ath_map.get(clean_s + ".NS")
        ath_from_map_bse = ath_map.get(clean_s + ".BO")
        ath = ath_from_map_nse if ath_from_map_nse is not None else ath_from_map_bse


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
        # 'progress' argument removed from history() as it's not supported by your yfinance version
        df = yf.Ticker(symbol).history(period="250d")
        if df.empty or 'Close' not in df.columns:
            # print(f"DEBUG: SMA - No data or 'Close' column for {symbol}. len(df): {len(df)}")
            return symbol, None

        if len(df) >= 200:
            sma = round(df["Close"][-200:].mean(), 2) # Calculate 200-day SMA
            # print(f"DEBUG: SMA - {symbol}: 200-day SMA calculated: {sma}") # Suppress for cleaner output
            return symbol, sma
        elif len(df) >= 50:
            sma = round(df["Close"][-50:].mean(), 2)  # Calculate 50-day SMA if 200 not available
            # print(f"DEBUG: SMA - {symbol}: 50-day SMA calculated: {sma}") # Suppress for cleaner output
            return symbol, sma
        else:
            # print(f"DEBUG: SMA - Not enough data for {symbol} (need 50 or 200 days). Current data points: {len(df)}.")
            return symbol, None
    except Exception as e:
        # print(f"DEBUG: SMA - Error fetching SMA for {symbol}: {e}") # Detailed error message
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
        # 'progress' argument removed from history() as it's not supported by your yfinance version
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
        # print(f"❌ Error in PM logic for {symbol}: {e}") # Too verbose
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
        # 'progress' argument removed from history() as it's not supported by your yfinance version
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
        # print(f"Error checking WIB for {symbol}: {e}") # Too verbose
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
        # 'progress' argument removed from history() as it's not supported by your yfinance version
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
        # print(f"Error checking DIB for {symbol}: {e}") # Too verbose
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
# gspread's clear() clears the entire sheet or a specified range.
# We need to clear a specific range.
try:
    # Clear a generous range to ensure all previous data is gone.
    # Assuming data starts from row 2 (after header)
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
    # This aligns with the initial ATH update logic that fetches both and picks the best.
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
        # gspread update takes a list of lists for rows
        # The range is G2 to O (last row of data)
        start_cell = 'G2'
        end_col = 'O'
        end_row = len(final_output_rows_ath_cache) + 1 # +1 for header row (G2 is first data row)
        range_to_update = f"{start_cell}:{end_col}{end_row}"

        # DeprecationWarning fix: Pass values first, then range_name as named argument
        cache_sheet.update(values=final_output_rows_ath_cache, range_name=range_to_update, value_input_option='USER_ENTERED')

        # Apply number formatting to relevant columns (H, I, J, K)
        # H: LTP, I: ATH, J: % from ATH, K: SMA
        num_rows_written_ath_cache = len(final_output_rows_ath_cache)
        if num_rows_written_ath_cache > 0:
            # Formatting for H, I, J, K
            cache_sheet.format(f"H2:K{1 + num_rows_written_ath_cache}", {'numberFormat': {'type': 'NUMBER', 'pattern': '0.00'}})

        # FIX: Use len(final_output_rows_ath_cache) instead of undefined num_filtered_rows_written
        print(f"✅ All scan results written to ATH Cache sheet with {len(final_output_rows_ath_cache)} records.")
    except Exception as e:
        print(f"❌ Error writing consolidated data to ATH Cache sheet: {e}")
else:
    print("ℹ️ No records met the scan criteria to be written to ATH Cache.")


# Set Indian locale-style grouping manually for comma formatting
# This function is used for Market Cap formatting, which is a string output.
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
        # print(f"Warning: Error formatting currency {value}: {e}") # Debugging
        return str(value)

def update_market_cap_in_price_scan():
    print("🏦 Updating Market Cap in 'Price Scan' sheet (Column D)...")

    # Get symbols from Price Scan sheet (Column B), starting from B4
    try:
        symbol_range_raw = scan_sheet.col_values(2)[3:] # Column B, starting from row 4 (index 3)
        symbol_range = [s for s in symbol_range_raw if s.strip()] # Filter out empty strings
    except Exception as e:
        print(f"❌ Error reading symbols from Price Scan sheet for Market Cap update: {e}")
        symbol_range = []
        
    if not symbol_range:
        print("⚠️ No symbols found in Price Scan sheet for Market Cap update.")
        return

    def fetch_cap(symbol_raw):
        try:
            if not isinstance(symbol_raw, str):
                return ""

            # Clean symbol: remove "NSE:" and trailing comma
            clean_symbol = symbol_raw.replace("NSE:", "").replace(",", "").strip()
            yf_symbol = clean_symbol + ".NS" # Assuming .NS for market cap lookup

            ticker = yf.Ticker(yf_symbol)
            info = ticker.info # Using full .info (returns accurate value)
            mcap_raw = info.get("marketCap")

            if mcap_raw and isinstance(mcap_raw, (int, float)):
                cap_cr = round(mcap_raw / 1e7)  # Convert to Crores (1 Crore = 10^7)
                return f"₹ {format_indian_currency(cap_cr)} Cr."
            else:
                return ""
        except Exception as e:
            # print(f"⚠️ Error fetching market cap for {symbol_raw}: {e}") # Too verbose
            return ""

    market_caps = []
    if symbol_range:
        with ThreadPoolExecutor(max_workers=5) as executor:
            futures = {executor.submit(fetch_cap, sym): sym for sym in symbol_range}
            for f in as_completed(futures):
                market_caps.append(f.result())

    if market_caps:
        try:
            # Update Column D, starting from D4
            # gspread update expects a list of lists, where each inner list is a row.
            # Since we are updating a single column, each inner list will have one element.
            market_caps_for_update = [[cap] for cap in market_caps]
            scan_sheet.update(values=market_caps_for_update, range_name=f"D4:D{3 + len(market_caps)}", value_input_option='USER_ENTERED')
            print(f"✅ Market Cap updated for {len(market_caps)} stocks in Column D.")
        except Exception as e:
            print(f"❌ Error writing Market Cap to Price Scan sheet: {e}")
    else:
        print("ℹ️ No market caps to update.")


# === Filter and Write to Price Scan Sheet (B4:H4) ===
print("📊 Filtering data and writing to Price Scan sheet (B4:H4)...")

# Clear previous contents in Price Scan sheet (B4 to H and down)
try:
    scan_sheet.batch_clear(['B4:H1000']) # Clear a generous range
    print("✅ Cleared previous contents in Price Scan sheet (B:H).")
except Exception as e:
    print(f"❌ Error clearing Price Scan sheet range B:H: {e}")


filtered_scan_results = []

# Read all relevant data from ATH Cache sheet (G:O)
# gspread.get_all_values() gets everything, then we slice it.
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
        # Note: These are 0-based indices *from the original full row*, not the sliced one.
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

            filtered_row = [
                symbol_g,       # B - Symbol
                ltp_h,          # C - LTP
                "",             # D - Market Cap (placeholder, will be updated later)
                breakout_date,  # E - Breakout Date
                pct_from_ath,   # F - % From ATH
                wib_count,      # G - WIB
                dib_count       # H - DIB
            ]
            filtered_scan_results.append(filtered_row)

# Write to Price Scan sheet
if filtered_scan_results:
    try:
        # Update range B4 to H (last row of filtered results)
        start_cell = 'B4'
        end_col = 'H'
        end_row = len(filtered_scan_results) + 3 # +3 because B4 is the first data row
        range_to_update = f"{start_cell}:{end_col}{end_row}"

        # DeprecationWarning fix: Pass values first, then range_name as named argument
        scan_sheet.update(values=filtered_scan_results, range_name=range_to_update, value_input_option='USER_ENTERED')

        num_filtered_rows_written = len(filtered_scan_results)
        # Apply number formatting to LTP (C) and % From ATH (F)
        if num_filtered_rows_written > 0:
            scan_sheet.format(f"C4:C{3 + num_filtered_rows_written}", {'numberFormat': {'type': 'NUMBER', 'pattern': '0.00'}}) # LTP
            scan_sheet.format(f"F4:F{3 + num_filtered_rows_written}", {'numberFormat': {'type': 'NUMBER', 'pattern': '0.00'}}) # % From ATH

        print(f"✅ Filtered results written to Price Scan sheet with {num_filtered_rows_written} records.")

        # ✅ Call the Market Cap updater separately after writing symbols
        update_market_cap_in_price_scan()

    except Exception as e:
        print(f"❌ Error writing filtered results to Price Scan sheet: {e}")
else:
    print("ℹ️ No records met the filtering criteria for Price Scan sheet.")

