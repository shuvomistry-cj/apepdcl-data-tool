import json
import time
import pandas as pd
import os
import sys
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException, WebDriverException
from selenium.webdriver.chrome.service import Service as ChromeService
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.support.ui import Select
import requests
import signal
import threading

# ---------------------- CONFIGURATION ----------------------
INPUT_FILE = "/Users/sankarrao/Desktop/VSK/INPUT/vsk002.xlsx"
OUTPUT_FILE = "/Users/sankarrao/Desktop/VSK/OUTPUT/vsk002_output.xlsx"
FAILED_FILE = "/Users/sankarrao/Desktop/VSK/FAILED/vsk002_failed.json"
STATUS_FILE = "/Users/sankarrao/Desktop/VSK/STATUS/vsk002_status.json"
URL = "https://www.apeasternpower.com/viewBillDetailsMain"
CHECK_INTERNET_URL = "http://www.google.com"
MAX_RETRIES = 2
RETRY_DELAY = 5

# ---------------------- GLOBAL STATE ----------------------
should_pause = False
should_stop = False
scraper_thread = None

def signal_handler(sig, frame):
    global should_stop
    print("\n🛑 Received interrupt signal. Stopping gracefully...")
    should_stop = True
    sys.exit(0)

signal.signal(signal.SIGINT, signal_handler)

# ✅ New helper to reinitialize driver
def get_new_driver():
    HEADLESS_MODE = True
    options = webdriver.ChromeOptions()
    if HEADLESS_MODE:
        options.add_argument("--headless")
        options.add_argument("--disable-gpu")
        options.add_argument("--window-size=1920,1080")
    return webdriver.Chrome(service=ChromeService(ChromeDriverManager().install()), options=options)

def check_internet_connection():
    try:
        requests.get(CHECK_INTERNET_URL, timeout=5)
        return True
    except requests.ConnectionError:
        return False

def wait_for_internet():
    print("🌐 Waiting for internet connection...")
    while not check_internet_connection():
        time.sleep(5)
    print("🌐 Internet connection restored")

def load_status():
    if os.path.exists(STATUS_FILE) and os.path.getsize(STATUS_FILE) > 0:
        try:
            with open(STATUS_FILE, "r") as f:
                return json.load(f)
        except Exception as e:
            print(f"⚠ Couldn't read status file: {e}")
    return {"last_processed": 0, "total_processed": 0}

def save_status(last_processed, total_processed):
    with open(STATUS_FILE, "w") as f:
        json.dump({"last_processed": last_processed, "total_processed": total_processed}, f)

def load_existing_data():
    if os.path.exists(OUTPUT_FILE) and os.path.getsize(OUTPUT_FILE) > 0:
        try:
            existing_df = pd.read_excel(OUTPUT_FILE, engine='openpyxl')
            print(f"✅ Loaded existing data from {OUTPUT_FILE}")
            if "CID" not in existing_df.columns:
                existing_df["CID"] = None
        except Exception as e:
            print(f"⚠ Couldn't read existing Excel file: {e}")
            existing_df = pd.DataFrame(columns=["CID"])
    else:
        existing_df = pd.DataFrame(columns=["CID"])
    
    if os.path.exists(FAILED_FILE) and os.path.getsize(FAILED_FILE) > 0:
        try:
            with open(FAILED_FILE, "r") as f:
                existing_failed = set(json.load(f))
        except Exception as e:
            print(f"⚠ Couldn't read failed JSON file: {e}")
            existing_failed = set()
    else:
        existing_failed = set()
    
    return existing_df, existing_failed

def save_data(output_data, not_scraped):
    try:
        data_list = []
        for cid, months in output_data.items():
            for month, amount in months.items():
                data_list.append({'CID': cid, 'Month': month, 'Amount': amount})

        temp_df = pd.DataFrame(data_list)
        pivot_df = temp_df.pivot(index='CID', columns='Month', values='Amount').reset_index()
        sorted_columns = ['CID'] + sorted([col for col in pivot_df.columns if col != 'CID'], reverse=True)
        pivot_df = pivot_df[sorted_columns]
        pivot_df.to_excel(OUTPUT_FILE, index=False, engine="openpyxl")
        print(f"\n✅ Pivoted data saved to {OUTPUT_FILE}")

        if not_scraped:
            if os.path.exists(FAILED_FILE) and os.path.getsize(FAILED_FILE) > 0:
                try:
                    with open(FAILED_FILE, "r") as f:
                        existing_failed = set(json.load(f))
                    not_scraped = list(set(not_scraped).union(existing_failed))
                except Exception as e:
                    print(f"⚠ Couldn't read failed JSON file: {e}")

            with open(FAILED_FILE, "w") as f:
                json.dump(list(not_scraped), f, indent=4)
            print(f"⚠ Failed CIDs saved to {FAILED_FILE}")

    except Exception as e:
        print(f"❌ Error saving data: {str(e)}")

def check_pause():
    global should_pause
    if should_pause:
        print("\n⏸ Scraping paused. Press '3' to resume or '4' to stop")
        while should_pause:
            time.sleep(1)
            if should_stop:
                print("🛑 Stopping as requested during pause")
                return True
        print("▶ Resuming scraping...")
    return False

def process_cid(driver, cid):
    retries = 0
    while retries < MAX_RETRIES:
        try:
            if not check_internet_connection():
                wait_for_internet()

            # ✅ Check driver session before use
            try:
                driver.title
            except WebDriverException:
                print("⚠ Browser session lost. Re-initializing driver...")
                driver.quit()
                driver = get_new_driver()

            driver.get(URL)
            time.sleep(2)

            WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.ID, 'ltscno')))
            driver.find_element(By.ID, 'ltscno').send_keys(cid)

            WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.ID, 'Billquestion')))
            captcha_text = driver.execute_script("return document.getElementById('Billquestion').innerText;").strip()
            driver.find_element(By.ID, 'Billans').send_keys(captcha_text)
            driver.find_element(By.ID, 'Billsignin').click()
            time.sleep(2)

            try:
                alert = driver.switch_to.alert
                alert_text = alert.text
                alert.accept()
                raise Exception(f"CAPTCHA validation failed: {alert_text}")
            except:
                pass

            try:
                WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.ID, "historyDivbtn")))
                driver.execute_script("window.scrollBy(0, 280)")
                time.sleep(2)
                driver.find_element(By.ID, "historyDivbtn").click()
            except TimeoutException:
                raise Exception("CAPTCHA failed or no history button")

            WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.ID, "consumptionData")))

            target_years = ["2025-2026"]
            transactions = {}
            for year_value in target_years:
                try:
                    select_year = Select(driver.find_element(By.ID, "year"))
                    select_year.select_by_value(year_value)

                    view_button = driver.find_element(By.ID, "Billsignin")
                    driver.execute_script("arguments[0].click();", view_button)

                    WebDriverWait(driver, 20).until(EC.presence_of_element_located((By.ID, "consumptionData")))

                    table = driver.find_element(By.ID, "consumptionData")
                    rows = table.find_elements(By.TAG_NAME, "tr")[1:]

                    for row in rows:
                        cells = row.find_elements(By.TAG_NAME, "td")
                        if len(cells) < 3:
                            continue
                        bill_month = cells[1].text.strip()
                        amount_cell = cells[3]
                        try:
                            input_field = amount_cell.find_element(By.TAG_NAME, "input")
                            amount_text = input_field.get_attribute("value").strip()
                        except NoSuchElementException:
                            amount_text = amount_cell.text.strip()
                        try:
                            amount = float(amount_text.replace(",", "")) if amount_text.replace(",", "").replace(".", "").isdigit() else 0
                        except:
                            amount = 0
                        transactions[bill_month] = amount
                except Exception:
                    pass
            return transactions
        except Exception as e:
            retries += 1
            if retries < MAX_RETRIES:
                time.sleep(RETRY_DELAY)
            else:
                raise e

def print_progress(current, total, success, failed):
    bar_length = 25
    filled_length = int(bar_length * current // total)
    bar = '█' * filled_length + '░' * (bar_length - filled_length)
    sys.stdout.write(f"\rProgress: [{bar}]  {current}/{total} | ✅ {success} | ❌ {failed}")
    sys.stdout.flush()

def scraping_worker():
    global should_pause, should_stop
    driver = get_new_driver()  # ✅ use new helper
    try:
        df = pd.read_excel(INPUT_FILE, header=None, engine='openpyxl')
        cid_list = df[0].astype(str).tolist()
        existing_df, existing_failed = load_existing_data()
        status = load_status()
        output_data = {}
        if not existing_df.empty:
            for _, row in existing_df.iterrows():
                cid = row['CID']
                if cid not in output_data:
                    output_data[cid] = {}
                for col in row.index:
                    if col != 'CID' and pd.notna(row[col]):
                        output_data[cid][col] = row[col]
        not_scraped = set(existing_failed)
        total = len(cid_list)
        success_count = status.get("total_processed", 0)
        failed_count = len(not_scraped)
        start_index = status.get("last_processed", 0)

        for index in range(start_index, total):
            if should_stop:
                print("\n🛑 Stopping as requested")
                break
            if check_pause():
                should_stop = True
                break
            cid = cid_list[index]
            if cid in output_data or cid in not_scraped:
                continue
            try:
                cid_data = process_cid(driver, cid)
                output_data[cid] = cid_data
                success_count += 1
            except Exception:
                not_scraped.add(cid)
                failed_count += 1
            save_status(index + 1, success_count)
            print_progress(index + 1, total, success_count, failed_count)
            if (index + 1) % 10 == 0:
                save_data(output_data, not_scraped)

        print_progress(total, total, success_count, failed_count)
        print("\n✅ Scraping completed!")
        save_data(output_data, not_scraped)

    except Exception as e:
        print(f"❌ Scraping failed with error: {str(e)}")
    finally:
        if driver:
            driver.quit()
            print("\n🚪 Browser closed")

def start_scraping():
    global should_pause, should_stop, scraper_thread
    should_pause = False
    should_stop = False
    if scraper_thread and scraper_thread.is_alive():
        print("⚠ Scraping is already running")
        return
    scraper_thread = threading.Thread(target=scraping_worker)
    scraper_thread.start()
    print("🚀 Scraping started")

def pause_scraping():
    global should_pause
    if not scraper_thread or not scraper_thread.is_alive():
        print("⚠ No active scraping to pause")
        return
    should_pause = True
    print("⏸ Pause requested. Will pause after current CID completes.")

def resume_scraping():
    global should_pause
    if not should_pause:
        print("⚠ Scraping is not paused")
        return
    should_pause = False
    print("▶ Resuming scraping...")

def stop_scraping():
    global should_stop
    if not scraper_thread or not scraper_thread.is_alive():
        print("⚠ No active scraping to stop")
        return
    should_stop = True
    print("🛑 Stop requested. Will stop after current CID completes.")

def show_status():
    if not scraper_thread:
        print("🛑 No scraping session exists")
        return
    if not scraper_thread.is_alive():
        print("🛑 No active scraping running")
        return
    if should_pause:
        print("⏸ Scraping is currently paused")
    elif should_stop:
        print("🛑 Scraping is stopping...")
    else:
        print("▶ Scraping is running")

if __name__ == "__main__":
    print("Scraping Control Options:")
    print("1. Start scraping")
    print("2. Pause scraping")
    print("3. Resume scraping")
    print("4. Stop scraping")
    print("5. Check status")
    print("6. Exit")
    
    while True:
        choice = input("\nEnter your choice (1-6): ")
        if choice == "1":
            start_scraping()
        elif choice == "2":
            pause_scraping()
        elif choice == "3":
            resume_scraping()
        elif choice == "4":
            stop_scraping()
        elif choice == "5":
            show_status()
        elif choice == "6":
            stop_scraping()
            print("👋 Exiting...")
            break
        else:
            print("Invalid choice. Please try again.")
