# ============================ PART 1: MONEYCONTROL + GOOGLE SHEETS (K, O–S) ============================
import subprocess
import json
import os
import sys
import pandas as pd
import time # Added for gspread rate limiting considerations
import numpy as np # Added for NaN handling

# GSpread specific imports
from oauth2client.service_account import ServiceAccountCredentials
import gspread

# --- GSpread Configuration ---
GOOGLE_SHEET_ID = "1cYBpsVKCbrYCZzrj8NAMEgUG4cXy5Q5r9BtQE1Cjmz0"
DASHBOARD_SHEET_NAME = "Price Scan" # This matches the original SHEET_NAME

def resource_path(relative_path):
    """
    Determines the correct path for resources, especially when running as a PyInstaller executable.
    """
    if hasattr(sys, "_MEIPASS"):
        return os.path.join(sys._MEIPASS, relative_path)
    else:
        return os.path.join(os.path.dirname(sys.argv[0]), relative_path)

# Paths for external scripts and data
JS_SCRIPT = resource_path("moneycontrol_scraper.js")
JSON_FILE = resource_path("earnings.json")
NODE_PATH = resource_path("nodejs-portable/node.exe")
NODE_MODULES = resource_path("nodejs-portable/node_modules")

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

# Handle non-numeric values like '--' by replacing them with NaN before conversion
# Then use pd.to_numeric with errors='coerce' to turn invalid parsing into NaN
df["rev%"] = pd.to_numeric(df["revGrowth"].str.replace('%', '').str.replace(',', '').replace('--', np.nan), errors='coerce')
df["net%"] = pd.to_numeric(df["netGrowth"].str.replace('%', '').str.replace(',', '').replace('--', np.nan), errors='coerce')

# Filter DataFrame based on growth criteria, dropping rows where conversion resulted in NaN
df = df[(df["rev%"] > 30) & (df["net%"] > 30)].dropna(subset=["rev%", "net%"]).reset_index(drop=True)


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

# Apply the period mapping to create a new column
df["period_text"] = df["period"].apply(map_period_to_range)

# --- GSpread Authentication and Sheet Opening ---
print("📂 Connecting to Google Sheet...")
try:
    # Path to your service account JSON key file
    # This assumes the key file is either in /etc/secrets/creds.json (for Render)
    # or in the same directory as the script (for local execution)
    if os.path.exists("/etc/secrets/creds.json"):
        JSON_KEY_FILE_PATH = "/etc/secrets/creds.json"
    else:
        JSON_KEY_FILE_PATH = resource_path("the-money-method-ad6d7-f95331cf5cbf.json")

    # Define the scopes for Google Sheets and Drive API access
    SCOPE = [
        "https://spreadsheets.google.com/feeds",
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive"
    ]

    # Authorize gspread using the service account credentials
    creds = ServiceAccountCredentials.from_json_keyfile_name(JSON_KEY_FILE_PATH, SCOPE)
    client = gspread.authorize(creds)

    # Open the Google Sheet by its ID
    gsheet = client.open_by_key(GOOGLE_SHEET_ID)
    # Select the specific worksheet by name
    sheet = gsheet.worksheet(DASHBOARD_SHEET_NAME)
    print(f"📘 Using Google Sheet: '{gsheet.title}' (Sheet: '{sheet.title}')")

except Exception as e:
    print(f"❌ Error connecting to Google Sheet: {e}")
    sys.exit()

# Read existing company names from Column K (11th column)
existing_names = set()
# gspread's col_values is 1-indexed for column number
# It returns a list of all values in the column. We need to start from row 4 (index 3).
all_col_k_values = sheet.col_values(11)
for val in all_col_k_values[3:]: # Slice from index 3 to get values from row 4 onwards
    if val:
        existing_names.add(str(val).strip())

# Filter for new companies not already in the Google Sheet
df_new = df[~df["name"].isin(existing_names)].reset_index(drop=True)

if df_new.empty:
    print("📭 No new companies to add.")
else:
    count_new = len(df_new)
    print(f"Adding {count_new} new companies to '{DASHBOARD_SHEET_NAME}' sheet.")

    # --- Prepare data for batch update (Part 1: K, O-S) ---
    # To simulate the Excel 'insert and shift down' behavior, we'll read existing data
    # from row 4 onwards, prepend the new data, and then update the entire range.

    # Get all existing values from the sheet
    # This is necessary to get the full context for prepending
    existing_data_full = sheet.get_all_values()

    # Determine the actual last row with data in column K
    # This helps in defining the range for existing data
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
    # The update will start from row 4 (K4) and extend downwards.
    # The number of rows to update will be the length of combined_data_to_update.
    end_row_for_update = 3 + len(combined_data_to_update) # gspread ranges are 1-indexed
    range_to_update = f"K4:S{end_row_for_update}"

    # Perform the batch update using named arguments to avoid DeprecationWarning
    sheet.update(values=combined_data_to_update, range_name=range_to_update)
    print(f"✅ Successfully added {count_new} new companies to '{DASHBOARD_SHEET_NAME}' sheet.")

    # Note: gspread does not directly support cell formatting like Excel's NumberFormat, Font, ColorIndex.
    # These lines are removed as they are Excel-specific.
    # sheet.Range(f"P4:Q{end_row}").NumberFormat = "0%"
    # reset_range = sheet.Range(f"K4:S{end_row}")
    # reset_range.Font.Bold = False
    # reset_range.Interior.ColorIndex = 0
    # reset_range.Borders.LineStyle = -4142

# ============================ PART 2: TICKERTAPE FETCHER + GOOGLE SHEETS (L–N) ============================

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver import ActionChains

start_row = 4 # Starting row for processing in Google Sheet

# Configure Chrome options for Selenium
options = Options()
options.add_argument("--headless=new") # Run Chrome in headless mode
options.add_argument("--disable-gpu")
options.add_argument("--window-size=1920,1080")
options.add_argument("--log-level=3") # Suppress verbose logging
driver = webdriver.Chrome(options=options)
wait = WebDriverWait(driver, 15) # WebDriverWait for explicit waits

def safe_click(locator, by=By.CSS_SELECTOR, timeout=10):
    """
    Safely clicks an element after waiting for it to be clickable and scrolling into view.
    """
    el = WebDriverWait(driver, timeout).until(EC.element_to_be_clickable((by, locator)))
    driver.execute_script("arguments[0].scrollIntoView({block:'center'});", el)
    driver.execute_script("arguments[0].click();", el)
    return el

def close_popup_if_exists():
    """
    Attempts to close common pop-up elements that might obstruct interactions.
    """
    for sel in (".ad-close-button", ".close-button", '[aria-label="close"]'):
        try:
            driver.find_element(By.CSS_SELECTOR, sel).click()
            # print(f"Closed popup with selector: {sel}") # For debugging
            break
        except:
            pass

def search_and_open_company(name):
    """
    Navigates to Tickertape, searches for a company, and opens its profile page.
    """
    driver.get("https://www.tickertape.in/")
    close_popup_if_exists()
    try:
        search = wait.until(EC.element_to_be_clickable((By.ID, "search-stock-input")))
        ActionChains(driver).click(search).send_keys(name).pause(1.5)\
            .send_keys(Keys.ARROW_DOWN).send_keys(Keys.ENTER).perform()
        time.sleep(3) # Give time for page to load after search
        close_popup_if_exists()
        # Wait for a key element on the financials section to ensure page is loaded
        wait.until(EC.presence_of_element_located((By.XPATH, "//a[contains(@href,'#financials')]")))
        return True
    except Exception as e:
        # print(f"Error in search_and_open_company for {name}: {e}") # For debugging
        return False

def select_quarterly():
    """
    Selects the quarterly view for financial data on Tickertape.
    """
    try:
        safe_click("span.suggestion-toggle-btn") # Click the toggle button for period selection
        radio = wait.until(EC.presence_of_element_located((By.XPATH, "//input[@value='interim']")))
        driver.execute_script("arguments[0].click();", radio) # Click the interim (quarterly) radio button
        time.sleep(0.5) # Short pause for UI update
    except:
        pass # If elements not found, assume quarterly is already selected or not applicable

def scroll_table_right():
    """
    Scrolls the financial table horizontally to reveal the latest quarters.
    """
    try:
        table = wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "div.financials-table-root")))
        time.sleep(1.2) # Wait for table to render
        driver.execute_script("arguments[0].scrollLeft = arguments[0].scrollWidth", table) # Scroll to the far right
        time.sleep(0.8) # Wait for scroll to complete
    except:
        pass

def get_quarters():
    """
    Extracts the last three quarter names from the financial table header.
    """
    try:
        # Select table headers with class 'period-text' within the income statement table
        ths = driver.find_elements(By.CSS_SELECTOR, 'table[data-statement-type="income"] thead th.period-text')
        return [th.text.strip() for th in ths if th.text.strip()][-3:] # Get last 3 non-empty quarter texts
    except:
        return []

def get_row_by_data_key(data_key):
    """
    Retrieves the last three data points for a specific financial row (e.g., Revenue, PBT).
    """
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

# List to store updates for batch processing in Google Sheets
batch_updates = []

try:
    # Iterate through rows in the Google Sheet, starting from row 4
    # We'll fetch all relevant data once to avoid repeated API calls inside the loop
    # Get values from Column K (stock symbols) and Columns L, M, N (existing data)
    # We need to fetch enough rows to cover the 100-row range.
    all_sheet_values = sheet.get_all_values() # Get all values from the sheet

    # Process up to 100 rows starting from start_row (4)
    # Adjust for 0-indexed list: row 4 is index 3
    for r_idx in range(start_row - 1, min(start_row - 1 + 100, len(all_sheet_values))):
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
        if not search_and_open_company(stock):
            print("❌ Could not open Tickertape page.")
            continue

        try:
            # Click on the Financials tab
            safe_click("//a[contains(@href,'#financials')]", by=By.XPATH)
        except Exception as e:
            print(f"❌ Financials tab not found for {stock}: {e}")
            continue

        select_quarterly() # Select quarterly view
        scroll_table_right() # Scroll table to reveal latest data

        quarters = get_quarters()
        rev_vals = get_row_by_data_key("qIncTrev") # Total Revenue
        pbt_vals = get_row_by_data_key("qIncPbt")   # Profit Before Tax

        print("📊 Quarters:", quarters)
        print("📊 Revenue:", rev_vals)
        print("📊 PBT    :", pbt_vals)

        # Ensure enough data points are available for growth calculation
        if len(quarters) < 3 or len(rev_vals) < 3 or len(pbt_vals) < 3:
            print("⚠️ Incomplete data. Skipping...")
            continue

        try:
            # Extract and clean values for the last two available quarters
            # rev_vals[0] is the oldest of the last three, rev_vals[1] is the middle one
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
            # gspread batch_update expects a list of dictionaries, each with 'range' and 'values'
            batch_updates.append({
                'range': f'L{current_row_num}:N{current_row_num}',
                'values': [[period, rev_pct, pbt_pct]]
            })

        except Exception as e:
            print(f"❌ Error processing data for {stock}: {e}")
            continue

finally:
    driver.quit() # Ensure the browser is closed
    # Perform a single batch update for all collected Tickertape data
    if batch_updates:
        sheet.batch_update(batch_updates)
        print(f"\n✅ Successfully updated {len(batch_updates)} rows in Google Sheet with Tickertape data.")
    print("\n✅ Done – browser closed.")
