import csv
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
import os
import logging
from datetime import datetime

# ============================================================
# LOGGING SETUP
# ============================================================

LOG_DIR = "logs"
os.makedirs(LOG_DIR, exist_ok=True)

log_file = os.path.join(
    LOG_DIR,
    f"autopartsearch_extract_part_links_{datetime.now().strftime('%Y%m%d')}.log"
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[
        logging.FileHandler(log_file, encoding="utf8"),
        logging.StreamHandler()
    ]
)

CHECKPOINT_DIR = "checkpoints"
os.makedirs(CHECKPOINT_DIR, exist_ok=True)

CHECKPOINT_FILE = os.path.join(
    CHECKPOINT_DIR,
    f"checkpoint_{datetime.now().strftime('%Y%m%d')}.txt"
)

def save_checkpoint(year, make):
    with open(CHECKPOINT_FILE, "w", encoding="utf8") as f:
        f.write(f"{year}|{make}")

def load_checkpoint():
    if not os.path.exists(CHECKPOINT_FILE):
        return None, None
    with open(CHECKPOINT_FILE, "r", encoding="utf8") as f:
        content = f.read().strip()
        if "|" not in content:
            return None, None
        return content.split("|", 1)

# ============================================================
# SELECT2 CLICK HELPER
# ============================================================
def select2_click(driver, container_css, visible_text):
    control = WebDriverWait(driver, 20).until(
        EC.element_to_be_clickable((By.CSS_SELECTOR, container_css))
    )
    control.click()

    option = WebDriverWait(driver, 20).until(
        EC.element_to_be_clickable(
            (By.XPATH,
             f"//li[contains(@class,'select2-results__option') and normalize-space(text())='{visible_text}']")
        )
    )
    option.click()


# ============================================================
# WAIT FOR PART TYPES
# ============================================================
def wait_for_parts_to_load(driver, timeout=20):
    def loaded(driver):
        select_el = driver.find_element(By.CSS_SELECTOR, "select#afmkt-parttype")
        opts = select_el.find_elements(By.TAG_NAME, "option")

        if not opts:
            return False

        txt = opts[0].text.strip().lower()
        if txt in ("loading...", "loading"):
            return False

        return True

    WebDriverWait(driver, timeout).until(loaded)


# ============================================================
# EXTRACT PART TYPES
# ============================================================
def get_part_types(driver):
    wait_for_parts_to_load(driver)

    select_el = driver.find_element(By.CSS_SELECTOR, "select#afmkt-parttype")
    opts = select_el.find_elements(By.TAG_NAME, "option")

    parts = []
    for opt in opts:
        value = opt.get_attribute("value")
        name = opt.text.strip()
        if not value:
            continue
        parts.append((name, value))

    return parts


# ============================================================
# GET OPTIONS FOR YEAR/MAKE/MODEL
# ============================================================
def get_select_options(driver, css_selector):
    select_el = WebDriverWait(driver, 20).until(
        EC.presence_of_element_located((By.CSS_SELECTOR, css_selector))
    )
    opts = select_el.find_elements(By.TAG_NAME, "option")
    return [opt.text.strip() for opt in opts if opt.get_attribute("value")]


# ============================================================
# MAIN SCRAPER 
# ============================================================
def main():

    logging.info("Starting AutoPartSearch scraper")

    run_ts = datetime.now().strftime("%Y%m%d%H%M%S")
    logging.info(f"Run timestamp: {run_ts}")

    last_year, last_make = load_checkpoint()
    logging.info(f"Checkpoint loaded. Last year {last_year}, last make {last_make}")
   
    MAX_LINKS = None
    collected_links = 0

    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()))
    driver.get("https://autopartsearch.com/")
    driver.maximize_window()

    years = get_select_options(driver, "select#afmkt-year")
    years = [y for y in years if int(y) >= 2010]
    logging.info(f"Found {len(years)} years.")

    csv_name = (
        f"autopartsearch_all_links_{run_ts}.csv"
        if MAX_LINKS is None
        else f"autopartsearch_test_{MAX_LINKS}_{run_ts}.csv"
    )
    out = open(csv_name, "w", newline="", encoding="utf8")

    writer = csv.writer(out)
    writer.writerow([
        "run_timestamp",
        "year",
        "make",
        "model",
        "part_name",
        "part_slug",
        "url",
        "part_count"
    ])

    for year in years:
        if last_year and int(year) < int(last_year):
            logging.info(f"Skipping year {year}, already processed")
            continue

        logging.info(f"\n=== YEAR: {year} ===")
        select2_click(driver, "span#select2-afmkt-year-container", year)

        makes = get_select_options(driver, "select#afmkt-make")
        logging.info(f"Found {len(makes)} makes.")

        for make in makes:
            if last_year == year and last_make and make.lower() <= last_make.lower():
                logging.info(f"Skipping make {make}, already processed")
                continue
            logging.info(f"\n--- MAKE: {make} ---")
            select2_click(driver, "span#select2-afmkt-make-container", make)

            models = get_select_options(driver, "select#afmkt-model")
            logging.info(f"Found {len(models)} models.")

            for model in models:
                logging.info(f"Model: {model}")
                select2_click(driver, "span#select2-afmkt-model-container", model)

                try:
                    parts = get_part_types(driver)
                except Exception as e:
                    logging.error(f"Part dropdown failed for {year} {make} {model}. Error: {e}")
                    writer.writerow([
                        run_ts,
                        year,
                        make,
                        model,
                        "",
                        "",
                        "",
                        0
                    ])
                    out.flush()
                    continue

                part_count = len(parts)
                logging.info(f"Parts found: {part_count}")

                # Zero part model → write one row
                if part_count == 0:
                    writer.writerow([
                        run_ts,
                        year,
                        make,
                        model,
                        "",
                        "",
                        "",
                        0
                    ])
                    out.flush()
                    continue

                # Otherwise write one row per part
                for part_name, part_slug in parts:
                    url = f"https://www.autopartsearch.com/catalog-6/vehicle/{make}/{year}/{model}/{part_slug}"

                    writer.writerow([
                        run_ts,
                        year,
                        make,
                        model,
                        part_name,
                        part_slug,
                        url,
                        part_count
                    ])

                    collected_links += 1

                    logging.info(f"[{collected_links}] {url}")

                    if MAX_LINKS is not None and collected_links >= MAX_LINKS:
                        logging.info(f"\n=== Reached {MAX_LINKS} real links. Stopping. ===")
                        out.flush()
                        out.close()
                        driver.quit()
                        return

                out.flush()

            save_checkpoint(year, make)
            logging.info(f"Checkpoint saved at year {year}, make {make}")

   
    logging.info(f"Total links collected: {collected_links}")
    logging.info("Scraper finished successfully")

    out.close()
    driver.quit()

    if os.path.exists(CHECKPOINT_FILE):
        os.remove(CHECKPOINT_FILE)
        logging.info("Checkpoint cleared after successful completion")

if __name__ == "__main__":
    main()



