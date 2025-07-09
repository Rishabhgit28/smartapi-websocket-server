# ============================ PART 1: MONEYCONTROL + EXCEL (K, O–S) ============================
import subprocess
import json
import win32com.client as win32
import pandas as pd
import os
import sys

def resource_path(relative_path):
    if hasattr(sys, "_MEIPASS"):
        return os.path.join(sys._MEIPASS, relative_path)
    else:
        return os.path.join(os.path.dirname(sys.argv[0]), relative_path)

JS_SCRIPT  = resource_path("moneycontrol_scraper.js")
JSON_FILE  = resource_path("earnings.json")
SHEET_NAME = "Price Scan"
NODE_PATH = resource_path("nodejs-portable/node.exe")
NODE_MODULES = resource_path("nodejs-portable/node_modules")

env = os.environ.copy()
env["NODE_PATH"] = NODE_MODULES

print("🚀 Running moneycontrol_scraper.js...")
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

if not os.path.exists(JSON_FILE):
    print("❌ earnings.json not found.")
    sys.exit()

with open(JSON_FILE, "r", encoding="utf-8") as f:
    data = json.load(f)

if not data:
    print("⚠️ earnings.json is empty.")
    sys.exit()

df = pd.DataFrame(data)
df["rev%"] = df["revGrowth"].str.replace('%', '').str.replace(',', '').astype(float)
df["net%"] = df["netGrowth"].str.replace('%', '').str.replace(',', '').astype(float)
df = df[(df["rev%"] > 30) & (df["net%"] > 30)].reset_index(drop=True)

def map_period_to_range(p):
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

df["period_text"] = df["period"].apply(map_period_to_range)

print("📂 Connecting to Excel...")
xl = win32.Dispatch("Excel.Application")

if xl.Workbooks.Count == 0:
    print("❌ No Excel workbook is open.")
    sys.exit()

wb = None
for book in xl.Workbooks:
    if "marketdashboard.xlsm" in book.Name.lower():
        wb = book
        break

if not wb:
    print("❌ Workbook 'MarketDashboard.xlsm' is not open.")
    sys.exit()

print(f"📘 Using active workbook: {wb.Name}")
try:
    sheet = wb.Sheets(SHEET_NAME)
except Exception:
    print(f"❌ Sheet '{SHEET_NAME}' not found.")
    sys.exit()

existing_names = set()
row = 4
while True:
    val = sheet.Cells(row, 11).Value  # Column K (Symbol)
    if not val:
        break
    existing_names.add(str(val).strip())
    row += 1

df_new = df[~df["name"].isin(existing_names)].reset_index(drop=True)

if df_new.empty:
    print("📭 No new companies to add.")
else:
    count_new = len(df_new)
    last_row = sheet.Cells(sheet.Rows.Count, 11).End(-4162).Row
    if last_row >= 4:
        sheet.Range(f"K4:S{last_row}").Insert(Shift=-4121)

    for i, row in df_new.iterrows():
        r = 4 + i
        sheet.Cells(r, 11).Value = row["name"]         # K (Symbol)
        sheet.Cells(r, 15).Value = row["period_text"]  # O
        sheet.Cells(r, 16).Value = row["revGrowth"]    # P
        sheet.Cells(r, 17).Value = row["netGrowth"]    # Q
        sheet.Cells(r, 18).Value = row["revenue"]      # R
        sheet.Cells(r, 19).Value = row["net"]          # S

    end_row = 3 + count_new
    sheet.Range(f"P4:Q{end_row}").NumberFormat = "0%"
    reset_range = sheet.Range(f"K4:S{end_row}")
    reset_range.Font.Bold = False
    reset_range.Interior.ColorIndex = 0
    reset_range.Borders.LineStyle = -4142

    print(f"✅ Added {count_new} new companies to '{SHEET_NAME}' sheet.")

# ============================ PART 2: TICKERTAPE FETCHER (L–N) ============================

import time
import xlwings as xw
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver import ActionChains

wb = xw.Book("MarketDashboard.xlsm")
sheet = wb.sheets["Price Scan"]
start_row = 4

options = Options()
options.add_argument("--headless=new")
options.add_argument("--disable-gpu")
options.add_argument("--window-size=1920,1080")
options.add_argument("--log-level=3")
driver = webdriver.Chrome(options=options)
wait = WebDriverWait(driver, 15)

def safe_click(locator, by=By.CSS_SELECTOR, timeout=10):
    el = WebDriverWait(driver, timeout).until(EC.element_to_be_clickable((by, locator)))
    driver.execute_script("arguments[0].scrollIntoView({block:'center'});", el)
    driver.execute_script("arguments[0].click();", el)
    return el

def close_popup_if_exists():
    for sel in (".ad-close-button", ".close-button", '[aria-label="close"]'):
        try:
            driver.find_element(By.CSS_SELECTOR, sel).click()
            break
        except:
            pass

def search_and_open_company(name):
    driver.get("https://www.tickertape.in/")
    close_popup_if_exists()
    try:
        search = wait.until(EC.element_to_be_clickable((By.ID, "search-stock-input")))
        ActionChains(driver).click(search).send_keys(name).pause(1.5)\
            .send_keys(Keys.ARROW_DOWN).send_keys(Keys.ENTER).perform()
        time.sleep(3)
        close_popup_if_exists()
        wait.until(EC.presence_of_element_located((By.XPATH, "//a[contains(@href,'#financials')]")))
        return True
    except:
        return False

def select_quarterly():
    try:
        safe_click("span.suggestion-toggle-btn")
        radio = wait.until(EC.presence_of_element_located((By.XPATH, "//input[@value='interim']")))
        driver.execute_script("arguments[0].click();", radio)
        time.sleep(0.5)
    except:
        pass

def scroll_table_right():
    try:
        table = wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "div.financials-table-root")))
        time.sleep(1.2)
        driver.execute_script("arguments[0].scrollLeft = arguments[0].scrollWidth", table)
        time.sleep(0.8)
    except:
        pass

def get_quarters():
    try:
        ths = driver.find_elements(By.CSS_SELECTOR, 'table[data-statement-type="income"] thead th.period-text')
        return [th.text.strip() for th in ths if th.text.strip()][-3:]
    except:
        return []

def get_row_by_data_key(data_key):
    try:
        row = driver.find_element(By.CSS_SELECTOR, f'tr[data-row="{data_key}"]')
        tds = row.find_elements(By.CSS_SELECTOR, "td")[1:]
        return [td.text.strip() for td in tds if td.text.strip()][-3:]
    except:
        return []

def calculate_growth(old, new):
    try:
        old = float(old)
        new = float(new)
        if old == 0:
            return float('inf') if new > 0 else float('-inf')
        return ((new - old) / abs(old)) * 100
    except:
        return 0

try:
    for r in range(start_row, start_row + 100):
        stock = sheet[f"K{r}"].value
        if not stock or (sheet[f"L{r}"].value and sheet[f"M{r}"].value and sheet[f"N{r}"].value):
            continue

        print(f"\n🔍 {stock}")
        if not search_and_open_company(stock):
            print("❌ Could not open page.")
            continue

        try:
            safe_click("//a[contains(@href,'#financials')]", by=By.XPATH)
        except:
            print("❌ Financials tab not found.")
            continue

        select_quarterly()
        scroll_table_right()

        quarters = get_quarters()
        rev_vals = get_row_by_data_key("qIncTrev")
        pbt_vals = get_row_by_data_key("qIncPbt")

        print("📊 Quarters:", quarters)
        print("📊 Revenue:", rev_vals)
        print("📊 PBT    :", pbt_vals)

        if len(quarters) < 3 or len(rev_vals) < 3 or len(pbt_vals) < 3:
            print("⚠️ Incomplete data. Skipping...")
            continue

        try:
            rev_sep = float(rev_vals[0].replace(',', ''))
            rev_dec = float(rev_vals[1].replace(',', ''))
            pbt_sep = float(pbt_vals[0].replace(',', ''))
            pbt_dec = float(pbt_vals[1].replace(',', ''))

            rev_growth = calculate_growth(rev_sep, rev_dec)
            pbt_growth = calculate_growth(pbt_sep, pbt_dec)

            rev_pct = f"{rev_growth:+.2f}%"
            pbt_pct = f"{pbt_growth:+.2f}%"

            from_q = quarters[0][:3].capitalize() + " " + quarters[0][-2:]
            to_q   = quarters[1][:3].capitalize() + " " + quarters[1][-2:]
            period = f"{from_q} to {to_q}"

            print(f"📈 Revenue: {rev_pct}, PBT: {pbt_pct}")
            sheet[f"L{r}"].value = period
            sheet[f"M{r}"].value = rev_pct
            sheet[f"N{r}"].value = pbt_pct
            wb.save()

        except Exception as e:
            print("❌ Error:", e)
            continue

finally:
    driver.quit()
    print("\n✅ Done – browser closed.")
