import csv
import os
import logging
from datetime import datetime
from multiprocessing import Pool, current_process

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager

# ============================================================
# GLOBAL CONFIG
# ============================================================

BASE_URL = "https://autopartsearch.com/"
MIN_YEAR = 2006
MAX_YEAR = 2013
NUM_WORKERS = 5

LOG_DIR = "logs"
OUT_DIR = "output"

os.makedirs(LOG_DIR, exist_ok=True)
os.makedirs(OUT_DIR, exist_ok=True)

RUN_TS = datetime.now().strftime("%Y%m%d%H%M%S")

# ============================================================
# LOGGING SETUP PER PROCESS
# ============================================================

def setup_logger(year):
    logger = logging.getLogger(f"scraper_{year}")
    logger.setLevel(logging.INFO)
    logger.handlers.clear()

    log_file = os.path.join(
        LOG_DIR,
        f"autopartsearch_{year}_{RUN_TS}.log"
    )

    formatter = logging.Formatter(
        "%(asctime)s | %(levelname)s | %(message)s"
    )

    fh = logging.FileHandler(log_file, encoding="utf8")
    fh.setFormatter(formatter)

    sh = logging.StreamHandler()
    sh.setFormatter(formatter)

    logger.addHandler(fh)
    logger.addHandler(sh)

    return logger

# ============================================================
# SELENIUM HELPERS
# ============================================================

def select2_click(driver, container_css, visible_text):
    control = WebDriverWait(driver, 20).until(
        EC.element_to_be_clickable((By.CSS_SELECTOR, container_css))
    )
    control.click()

    option = WebDriverWait(driver, 20).until(
        EC.element_to_be_clickable(
            (
                By.XPATH,
                f"//li[contains(@class,'select2-results__option') and normalize-space(text())='{visible_text}']"
            )
        )
    )
    option.click()

def wait_for_parts_to_load(driver, timeout=20):
    def loaded(driver):
        select_el = driver.find_element(By.CSS_SELECTOR, "select#afmkt-parttype")
        opts = select_el.find_elements(By.TAG_NAME, "option")
        if not opts:
            return False
        txt = opts[0].text.strip().lower()
        return txt not in ("loading", "loading...")
    WebDriverWait(driver, timeout).until(loaded)

def get_part_types(driver):
    wait_for_parts_to_load(driver)
    select_el = driver.find_element(By.CSS_SELECTOR, "select#afmkt-parttype")
    opts = select_el.find_elements(By.TAG_NAME, "option")

    parts = []
    for opt in opts:
        value = opt.get_attribute("value")
        name = opt.text.strip()
        if value:
            parts.append((name, value))
    return parts

def get_select_options(driver, css_selector):
    select_el = WebDriverWait(driver, 20).until(
        EC.presence_of_element_located((By.CSS_SELECTOR, css_selector))
    )
    opts = select_el.find_elements(By.TAG_NAME, "option")
    return [opt.text.strip() for opt in opts if opt.get_attribute("value")]

# ============================================================
# WORKER FUNCTION
# ============================================================

def scrape_year(year):
    logger = setup_logger(year)
    logger.info(f"Starting scrape for year {year}")

    csv_path = os.path.join(
        OUT_DIR,
        f"autopartsearch_{year}_{RUN_TS}.csv"
    )

    driver = webdriver.Chrome(
        service=Service(ChromeDriverManager().install())
    )

    try:
        driver.get(BASE_URL)
        driver.maximize_window()

        with open(csv_path, "w", newline="", encoding="utf8") as out:
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

            select2_click(driver, "span#select2-afmkt-year-container", year)

            makes = get_select_options(driver, "select#afmkt-make")
            logger.info(f"{year} makes found: {len(makes)}")

            for make in makes:
                logger.info(f"{year} make {make}")
                select2_click(driver, "span#select2-afmkt-make-container", make)

                models = get_select_options(driver, "select#afmkt-model")
                logger.info(f"{year} {make} models {len(models)}")

                for model in models:
                    select2_click(driver, "span#select2-afmkt-model-container", model)

                    try:
                        parts = get_part_types(driver)
                    except Exception as e:
                        logger.error(f"Parts failed {year} {make} {model} {e}")
                        writer.writerow([RUN_TS, year, make, model, "", "", "", 0])
                        out.flush()
                        continue

                    part_count = len(parts)
                    if part_count == 0:
                        writer.writerow([RUN_TS, year, make, model, "", "", "", 0])
                        out.flush()
                        continue

                    for part_name, part_slug in parts:
                        url = f"https://www.autopartsearch.com/catalog-6/vehicle/{make}/{year}/{model}/{part_slug}"
                        writer.writerow([
                            RUN_TS,
                            year,
                            make,
                            model,
                            part_name,
                            part_slug,
                            url,
                            part_count
                        ])

                out.flush()

        logger.info(f"Completed scrape for year {year}")

    finally:
        driver.quit()
        logger.info(f"Browser closed for year {year}")

# ============================================================
# PARENT PROCESS
# ============================================================

def main():
    print("Starting multiprocessing AutoPartSearch scrape")

    temp_driver = webdriver.Chrome(
        service=Service(ChromeDriverManager().install())
    )
    temp_driver.get(BASE_URL)

    years = get_select_options(temp_driver, "select#afmkt-year")
    temp_driver.quit()

    years = [y for y in years if int(y) >= MIN_YEAR and int(y) <= MAX_YEAR]
    # sort newest to oldest
    years = sorted(years, reverse=True)
    
    print(f"Years queued: {len(years)}")

    with Pool(processes=NUM_WORKERS) as pool:
        pool.map(scrape_year, years)

    print("All workers finished")

if __name__ == "__main__":
    main()
