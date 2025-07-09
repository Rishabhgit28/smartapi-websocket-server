import yfinance as yf
import os
import xlwings as xw
from datetime import datetime, time, timedelta, date
from concurrent.futures import ThreadPoolExecutor, as_completed
import locale

# === Setup Excel ===
script_dir = os.path.dirname(os.path.abspath(__file__))
wb_path = os.path.join(script_dir, "MarketDashboard.xlsm")
# Connect to the Excel workbook
wb = xw.Book(wb_path)

# Assign sheets - now all scan results will go to 'ATH Cache'
stock_sheet = wb.sheets["ATH Cache"] # Used for loading initial symbols
cache_sheet = wb.sheets["ATH Cache"] # Used for ATH updates and all scan results
scan_sheet = wb.sheets["Price Scan"] # Re-introducing for filtered output

# === Load symbols from ATH Cache (Column A) ===
# Reads symbols from column A, starting from A2, expanding downwards.
symbols_raw = stock_sheet.range("A2").expand("down").value
# Cleans and standardizes symbols: strips whitespace, converts to uppercase,
# and appends '.NS' for Indian National Stock Exchange if not already present,
# or if not ending with '.BO' for Bombay Stock Exchange.
symbols = [s.strip().upper() for s in symbols_raw if isinstance(s, str) and s.strip()]
symbols = [s + ".NS" if not s.endswith(".NS") and not s.endswith(".BO") else s for s in symbols]

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
    (weekday == 1 and current_time <= time(10, 0))      # Monday before 8 AM
)

# === ATH Cache Update (Columns B and C of ATH Cache sheet) ===
# This section updates the All-Time High (ATH) and its last update date
# for the symbols listed in column A of the 'ATH Cache' sheet.
if ath_allowed:
    print("📈 Running ATH Cache Update...")

    today_str = now.strftime("%d-%b-%Y")
    updates = 0
    marketcap_updates = 0

    # ✅ Step 1: Global check on Column D (Last Updated, now shifted to D)
    raw_date_range = cache_sheet.range("D2").expand("down")
    date_values = raw_date_range.value if raw_date_range.value else []

    # Normalize all dates
    normalized_dates = []
    for val in (date_values if isinstance(date_values, list) else [date_values]):
        try:
            if isinstance(val, (float, int)):
                d = datetime.fromordinal(datetime(1900, 1, 1).toordinal() + int(val) - 2).date()
            elif isinstance(val, str):
                d = datetime.strptime(val.strip(), "%d-%b-%Y").date()
            elif isinstance(val, datetime):
                d = val.date()
            else:
                continue
            normalized_dates.append(d)
        except:
            continue

    latest_update = max(normalized_dates) if normalized_dates else None

    if latest_update and (now.date() - latest_update).days < 2:
        print(f"⏩ Skipping ATH Cache update: Already updated on {latest_update.strftime('%d-%b-%Y')}")
    else:
        # ✅ Step 2: Download data if update is required
        nse_symbols = [s + ".NS" for s in symbols_raw if isinstance(s, str) and s.strip()]
        bse_symbols = [s + ".BO" for s in symbols_raw if isinstance(s, str) and s.strip()]

        try:
            print("📦 Downloading NSE data (bulk)...")
            data_nse = yf.download(
                tickers=" ".join(nse_symbols),
                period="max",
                interval="1d",
                group_by='ticker',
                auto_adjust=True,
                threads=True
            )
        except Exception as e:
            print(f"❌ NSE download failed: {e}")
            data_nse = {}

        try:
            print("📦 Downloading BSE data (bulk)...")
            data_bse = yf.download(
                tickers=" ".join(bse_symbols),
                period="max",
                interval="1d",
                group_by='ticker',
                auto_adjust=True,
                threads=True
            )
        except Exception as e:
            print(f"❌ BSE download failed: {e}")
            data_bse = {}

        for i, base_symbol in enumerate(symbols_raw, 1):
            try:
                clean_symbol = base_symbol.strip().upper()
                if not clean_symbol:
                    continue

                print(f"🔍 ({i}/{len(symbols_raw)}) {clean_symbol}...")

                symbol_nse = clean_symbol + ".NS"
                symbol_bse = clean_symbol + ".BO"

                df_nse = data_nse.get(symbol_nse)
                df_bse = data_bse.get(symbol_bse)

                # Choose the DataFrame with more rows
                df_use = None
                if df_bse is not None and not df_bse.empty and (df_nse is None or len(df_bse) > len(df_nse)):
                    df_use = df_bse
                elif df_nse is not None and not df_nse.empty:
                    df_use = df_nse

                if df_use is None or df_use.empty or len(df_use) < 100:
                    print(f"⚠️ Skipping {clean_symbol}: insufficient or missing data.")
                    continue

                new_ath = round(df_use["High"].max(), 2)

                row_num = i + 1
                prev_ath_cell = cache_sheet.range(f"B{row_num}")
                date_cell = cache_sheet.range(f"C{row_num}")
                prev_ath = prev_ath_cell.value

                if not isinstance(prev_ath, (float, int)) or round(prev_ath, 2) != new_ath:
                    prev_ath_cell.value = new_ath
                    date_cell.value = today_str
                    updates += 1

            except Exception as e:
                print(f"❌ Error processing {clean_symbol}: {e}")

        print(f"✅ ATH Cache update complete: {updates} record(s) updated.")
else:
    print("🕒 Skipping ATH Cache update (not in allowed time range).")

# === Price Scan - Fetching LTPs ===
# This section fetches Last Traded Prices (LTPs) for all symbols.
print("⚡ Fetching LTPs for price scan...")
# Load ATH data from ATH Cache sheet (A:B) for calculations
ath_data = cache_sheet.range("A2").expand("table").value or []
# Create a map for quick ATH lookup: {symbol.NS: ATH_value}
ath_map = {row[0] + ".NS": row[1] for row in ath_data if isinstance(row[0], str) and isinstance(row[1], (float, int))}

# Function to fetch LTP for a single symbol
def fetch_ltp(symbol):
    try:
        ltp = yf.Ticker(symbol).fast_info["last_price"]
        return symbol, round(ltp, 2)
    except:
        return symbol, None # Return None if LTP cannot be fetched

# Use ThreadPoolExecutor for concurrent LTP fetching
ltp_map = {}
with ThreadPoolExecutor(max_workers=40) as executor:
    futures = {executor.submit(fetch_ltp, sym): sym for sym in symbols}
    for f in as_completed(futures):
        sym, ltp = f.result()
        if ltp:
            ltp_map[sym] = ltp

# === Build Scanned Symbols List and Initial Scan Data ===
# This list will contain only the symbols that pass the initial drop_pct filter.
scanned_symbols = []
# This list will store the raw data for symbols, LTP, ATH, and % from ATH
# for symbols that pass the initial filter.
initial_scan_data = []

for sym in symbols:
    ltp = ltp_map.get(sym)
    ath = ath_map.get(sym)
    if ltp and ath:
        drop_pct = round(100 - (ltp / ath * 100), 2)
        # Only include symbols where drop from ATH is 30% or less
        if drop_pct <= 30:
            base_sym = sym.replace(".NS", "").replace(".BO", "")
            scanned_symbols.append(sym) # Add full symbol to list for further processing
            initial_scan_data.append([f"NSE:{base_sym},", ltp, ath, drop_pct])

print(f"✅ Initial price scan filtered {len(scanned_symbols)} records.")

# === SMA Logic (200 or 50) ===
# Fetches Simple Moving Averages (SMA) for the scanned symbols.
print("📊 Fetching SMA (200 or 50)...")

def fetch_sma(symbol):
    try:
        # Download 250 days of history to ensure enough data for 200-day SMA
        df = yf.Ticker(symbol).history(period="250d")
        if len(df) >= 200:
            sma = round(df["Close"][-200:].mean(), 2) # Calculate 200-day SMA
            return symbol, sma
        elif len(df) >= 50:
            sma = round(df["Close"][-50:].mean(), 2)  # Calculate 50-day SMA if 200 not available
            return symbol, sma
    except:
        pass # Silently fail and return None if data cannot be fetched or calculated
    return symbol, None

sma_map = {}
with ThreadPoolExecutor(max_workers=30) as executor:
    futures = {executor.submit(fetch_sma, sym): sym for sym in scanned_symbols}
    for f in as_completed(futures):
        sym, sma = f.result()
        if sma is not None:
            sma_map[sym] = sma
print("✅ SMA data fetched.")

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
        # Historical data for last 2 months + current month for breakout check
        df_full = yf.Ticker(symbol).history(period="3mo")
        df_full = df_full[~df_full['Close'].isna()]

        # Filter previous and prev-prev month ranges
        df_pm = df_full.loc[pm_start:pm_end]
        df_pp = df_full.loc[pp_start:pp_end]

        pm_high = df_pm["High"].max() if not df_pm.empty else None
        pp_high = df_pp["High"].max() if not df_pp.empty else None

        # Check for breakout in current month (after prev month end)
        current_month_start = last_day_prev_month + timedelta(days=1)
        df_current = df_full[df_full.index.date >= current_month_start]

        breakout_date = None
        breakout_reason = ""

        # Check for PM High breakout first
        if pm_high:
            breakout_rows = df_current[df_current["Close"] > pm_high]
            if not breakout_rows.empty:
                breakout_date = breakout_rows.index[0].date()
                breakout_reason = f"Yes ({breakout_date.strftime('%d-%b-%Y')})"
                return symbol, breakout_reason

        # Check for PP High breakout if PM High wasn't broken
        if pp_high:
            breakout_rows = df_current[df_current["Close"] > pp_high]
            if not breakout_rows.empty:
                breakout_date = breakout_rows.index[0].date()
                breakout_reason = f"Yes ({breakout_date.strftime('%d-%b-%Y')})"
                return symbol, breakout_reason

        return symbol, "No"
    except Exception as e:
        print(f"❌ Error in PM logic for {symbol}: {e}")
        return symbol, "No" # Return "No" on error

pm_flags = {}
with ThreadPoolExecutor(max_workers=30) as executor:
    futures = {executor.submit(fetch_pm_logic, sym): sym for sym in scanned_symbols}
    for f in as_completed(futures):
        sym, flag = f.result()
        pm_flags[sym] = flag
print("✅ PMHigh logic complete.")

# === Weekly Inside Bar (WIB) ===
# Checks for consecutive weekly inside bars over the last few weeks based on custom definition.
print("📉 Weekly Inside Bar check (Consecutive count - Custom Logic)...")

def check_wib(symbol):
    try:
        # Fetch weekly data directly from yfinance
        # "3mo" period should be sufficient to get at least 6 completed weeks (5 pairs)
        df_weekly = yf.Ticker(symbol).history(period="3mo", interval="1wk")
        if df_weekly.empty:
            return symbol, 0 # Return 0 if no data

        # Determine if the current day is Saturday (5) or Sunday (6)
        current_day_of_week = datetime.now().weekday() # Monday is 0, Sunday is 6

        # --- Conditional exclusion of current (incomplete) week ---
        # If it's Monday-Friday, drop the last (potentially incomplete) week.
        # If it's Saturday/Sunday, the last week is considered complete (Friday's close).
        if current_day_of_week <= 4: # Monday (0) to Friday (4)
            if not df_weekly.empty and df_weekly.index[-1].date() >= (datetime.now() - timedelta(days=7)).date():
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
                if consecutive_valid_pairs >= 5: # Changed from 4 to 5 to check up to 5 consecutive pairs
                    break
            else:
                break # Stop counting if any pair fails the conditions

        return symbol, consecutive_valid_pairs
    except Exception as e:
        print(f"Error checking WIB for {symbol}: {e}")
        return symbol, 0 # Return 0 on any error

wib_flags = {}
with ThreadPoolExecutor(max_workers=30) as executor:
    futures = {executor.submit(check_wib, sym): sym for sym in scanned_symbols}
    for f in as_completed(futures):
        sym, count = f.result()
        wib_flags[sym] = count # Store the numerical count
print("✅ WIB check complete.")

# === Daily Inside Bar (DIB) ===
# Checks for consecutive daily inside bars over the last few days based on the new definition.
print("📉 Daily Inside Bar check (5-day consecutive count - New Logic)...")

def check_dib(symbol):
    try:
        # Get recent daily historical data (enough for 6 days + buffer for 5 pairs)
        # "1mo" period should be sufficient to get at least 6 completed daily candles (5 pairs)
        df_daily = yf.Ticker(symbol).history(period="1mo") # Fetch ~1 month of data
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
        print(f"Error checking DIB for {symbol}: {e}")
        return symbol, 0 # Return 0 on any error

dib_flags = {}
with ThreadPoolExecutor(max_workers=30) as executor:
    futures = {executor.submit(check_dib, sym): sym for sym in scanned_symbols}
    for f in as_completed(futures):
        sym, count = f.result()
        dib_flags[sym] = count # Store the numerical count
print("✅ DIB check complete. ✅ All scan steps finished.")

# === Consolidate and Write All Scan Results to ATH Cache (G:O) ===
print("📝 Consolidating and writing all scan results to ATH Cache sheet (G:O)...")

# Clear previous scan results in ATH Cache (columns G to P)
# This ensures old data is removed before new data is written.
# Clearing a generous range to ensure all previous data is gone.
cache_sheet.range("G2:P1000").clear_contents()

final_output_rows_ath_cache = []
for sym in scanned_symbols:
    # Retrieve all calculated values for the current symbol
    base_sym = sym.replace(".NS", "").replace(".BO", "")
    ltp = ltp_map.get(sym)
    ath = ath_map.get(sym)
    
    # Recalculate drop_pct as initial_scan_data might not be directly accessible here
    # or for consistency.
    drop_pct = round(100 - (ltp / ath * 100), 2) if ltp and ath else None

    sma = sma_map.get(sym)
    pm_flag = pm_flags.get(sym, "")
    wib_count = wib_flags.get(sym, 0) # Get the numerical count for WIB
    dib_count = dib_flags.get(sym, 0) # Get the numerical count for DIB (UPDATED)

    # Calculate LTP > SMA flag
    ltp_gt_sma_flag = ""
    if isinstance(ltp, (int, float)) and isinstance(sma, (int, float)):
        ltp_gt_sma_flag = "Yes" if ltp > sma else "No"

    # Construct the row for Excel (ATH Cache sheet)
    # Order: Symbol, LTP, ATH, % from ATH, SMA, LTP > SMA, PMHigh, WIB, DIB
    row_data = [
        f"NSE:{base_sym},", # G
        ltp,                 # H
        ath,                 # I
        drop_pct,            # J
        sma,                 # K
        ltp_gt_sma_flag,     # L
        pm_flag,             # M
        wib_count,           # N (Now a number)
        dib_count            # O (Now a number)
    ]
    final_output_rows_ath_cache.append(row_data)

# Write all consolidated data to ATH Cache sheet starting from G2
if final_output_rows_ath_cache:
    cache_sheet.range("G2").value = final_output_rows_ath_cache
    # Apply number formatting to relevant columns (H, I, J, K)
    # H: LTP, I: ATH, J: % from ATH, K: SMA
    num_rows_written_ath_cache = len(final_output_rows_ath_cache)
    cache_sheet.range(f"H2:K{1 + num_rows_written_ath_cache}").number_format = "0.00"
    print(f"✅ All scan results written to ATH Cache sheet with {num_rows_written_ath_cache} records.")
else:
    print("ℹ️ No records met the scan criteria to be written to ATH Cache.")


# Set Indian locale-style grouping manually for comma formatting
def format_indian_currency(value):
    try:
        s = f"{int(value):,}"
        s = s.replace(",", "_")  # temporary replace
        parts = s.split("_")
        if len(parts) <= 3:
            return ",".join(parts)
        return f"{parts[0]},{''.join(parts[1:3])},{','.join(parts[3:])}"
    except:
        return str(value)

def update_market_cap_in_price_scan():
    print("🏦 Updating Market Cap in 'Price Scan' sheet (Column D)...")

    symbol_range = scan_sheet.range("B4").expand("down").value
    if not symbol_range or not isinstance(symbol_range, list):
        print("⚠️ No symbols found in Price Scan sheet.")
        return

    def fetch_cap(symbol_raw):
        try:
            if not isinstance(symbol_raw, str):
                return ""

            clean_symbol = symbol_raw.replace("NSE:", "").replace(",", "").strip()
            yf_symbol = clean_symbol + ".NS"

            ticker = yf.Ticker(yf_symbol)
            info = ticker.info  # ✅ Using full .info (returns accurate value)
            mcap_raw = info.get("marketCap")

            if mcap_raw and isinstance(mcap_raw, (int, float)):
                cap_cr = round(mcap_raw / 1e7)  # Convert to Cr
                return f"₹ {format_indian_currency(cap_cr)} Cr."
            else:
                return ""
        except Exception as e:
            print(f"⚠️ Error fetching market cap for {symbol_raw}: {e}")
            return ""

    market_caps = []
    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = {executor.submit(fetch_cap, sym): sym for sym in symbol_range}
        for f in as_completed(futures):
            market_caps.append(f.result())

    scan_sheet.range("D4").options(transpose=True).value = market_caps
    print(f"✅ Market Cap updated for {len(market_caps)} stocks in Column D.")


# === Filter and Write to Price Scan Sheet (B4:H4) ===
print("📊 Filtering data and writing to Price Scan sheet (B4:H4)...")

# Clear previous contents in Price Scan sheet (B4 to H and down)
scan_sheet.range("B4:H1000").clear_contents()

filtered_scan_results = []

# Read from G2, expanding to the last row and column O
all_scan_data_from_ath_cache = cache_sheet.range("G2").expand("right").expand("down").value

if all_scan_data_from_ath_cache:
    for row in all_scan_data_from_ath_cache:
        if len(row) < 9:
            continue  # Skip malformed rows

        # Extract relevant values
        symbol_g     = row[0]       # G - Symbol
        ltp_h        = row[1]       # H - LTP
        pct_from_ath = row[3]       # J - % from ATH
        ltp_gt_sma   = row[5]       # L - LTP > SMA
        pm_high_col  = row[6]       # M - PM High breakout with date
        wib_count    = row[7]       # N - WIB
        dib_count    = row[8]       # O - DIB

        # === Filtering Logic ===
        is_pct_ath_valid = isinstance(pct_from_ath, (int, float)) and pct_from_ath <= 30
        is_ltp_gt_sma = isinstance(ltp_gt_sma, str) and ltp_gt_sma.strip().upper() == "YES"
        is_pm_breakout = isinstance(pm_high_col, str) and pm_high_col.strip().upper().startswith("YES")

        if is_pct_ath_valid and is_ltp_gt_sma and is_pm_breakout:
            # ✅ Extract breakout date from "Yes (01-Jul-2025)"
            breakout_date = ""
            try:
                breakout_date = pm_high_col.strip().split("(", 1)[1].rstrip(")")
            except:
                breakout_date = ""

            filtered_row = [
                symbol_g,       # B - Symbol
                ltp_h,          # C - LTP
                "",             # D - Market Cap (placeholder)
                breakout_date,  # E - Breakout Date
                pct_from_ath,   # F - % From ATH
                wib_count,      # G - WIB
                dib_count       # H - DIB
            ]
            filtered_scan_results.append(filtered_row)

# Write to Price Scan sheet
if filtered_scan_results:
    scan_sheet.range("B4").value = filtered_scan_results
    num_filtered_rows_written = len(filtered_scan_results)
    scan_sheet.range(f"C4:C{3 + num_filtered_rows_written}").number_format = "0.00"  # LTP
    scan_sheet.range(f"F4:F{3 + num_filtered_rows_written}").number_format = "0.00"  # % From ATH
    print(f"✅ Filtered results written to Price Scan sheet with {num_filtered_rows_written} records.")
    
    # ✅ Call the Market Cap updater separately after writing symbols
    update_market_cap_in_price_scan()

else:
    print("ℹ️ No records met the filtering criteria for Price Scan sheet.")
