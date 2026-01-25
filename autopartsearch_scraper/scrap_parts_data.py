from bs4 import BeautifulSoup
import re
import requests
import json
import os
import logging
import logging.handlers
from datetime import datetime
import csv
import multiprocessing as mp
import random
import time

# ============================================================
# GLOBAL RUN CONFIG
# ============================================================

RUN_TS = datetime.now().strftime("%Y%m%d%H%M%S")
RUN_DATE = datetime.now().strftime("%Y%m%d")

LOG_DIR = "logs"
RUN_ROOT = os.path.join("output", f"parts_scrape_{RUN_DATE}")
FINAL_DIR = os.path.join(RUN_ROOT, "final")
TEMP_DIR = os.path.join(RUN_ROOT, "temp")

os.makedirs(FINAL_DIR, exist_ok=True)
os.makedirs(TEMP_DIR, exist_ok=True)
os.makedirs(LOG_DIR, exist_ok=True)

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/121.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 Chrome/120.0 Safari/537.36"
]

PROXY_HOST = "geo.iproyal.com:12321"
PROXY_AUTH = "DewbRx43TyL9c0VL:Pm9YlpYW09eOGRsj_country-us"

PROXIES = {
    "http": f"http://{PROXY_AUTH}@{PROXY_HOST}",
    "https": f"http://{PROXY_AUTH}@{PROXY_HOST}"
}

USE_PROXY = True

def create_session():
    session = requests.Session()

    if USE_PROXY:
        session.proxies.update(PROXIES)
        logger.info("Proxy enabled for session")
    else:
        logger.info("Proxy disabled for session")

    session.headers.update({
        "Accept": "text/html",
        "Accept-Language": "en-US,en;q=0.9"
    })

    return session

# ============================================================
# LOGGING
# ============================================================

WORKER_LOG_QUEUE = None

def setup_logger():
    logger = logging.getLogger("autopartsearch_scraper")
    logger.setLevel(logging.INFO)
    logger.handlers.clear()

    log_file = os.path.join(
        LOG_DIR,
        f"autopartsearch_run_{RUN_TS}.log"
    )

    file_handler = logging.FileHandler(log_file, encoding="utf8")
    stream_handler = logging.StreamHandler()

    formatter = logging.Formatter(
        "%(asctime)s | %(processName)s | %(levelname)s | %(message)s"
    )

    file_handler.setFormatter(formatter)
    stream_handler.setFormatter(formatter)

    logger.addHandler(file_handler)
    logger.addHandler(stream_handler)

    return logger

def log_listener(queue):
    logger = setup_logger()
    while True:
        record = queue.get()
        if record is None:
            break
        logger.handle(record)

def setup_worker_logger(queue):
    global WORKER_LOG_QUEUE
    WORKER_LOG_QUEUE = queue

    logger = logging.getLogger("autopartsearch_scraper")
    logger.setLevel(logging.INFO)
    logger.handlers.clear()
    logger.addHandler(logging.handlers.QueueHandler(queue))
    return logger

logger = setup_logger()
logger.info("Starting AutoPartSearch scrape")

# ============================================================
# CSV LOADER
# ============================================================

def load_catalog_urls(csv_path):
    urls = []
    with open(csv_path, newline="", encoding="utf8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            url = row.get("url")
            if url:
                urls.append({
                    "year": row.get("year"),
                    "make": row.get("manufacturer"),
                    "model": row.get("model_name"),
                    "part_name": row.get("part_name"),
                    "part_slug": row.get("part_slug"),
                    "url": url
                })
    return urls

# ============================================================
# PARSING HELPERS
# ============================================================

def parse_address(lines):
    city = state = phone = None

    if len(lines) >= 3:
        m = re.match(r"(.+),\s*(\w\w)", lines[2])
        if m:
            city = m.group(1).strip()
            state = m.group(2).strip()

    if lines and "(" in lines[-1]:
        phone = lines[-1].strip()

    return city, state, phone

# ============================================================
# OLD LAYOUT PARSER
# ============================================================

def parse_old_layout(soup, interchange, yard_distances, application_meta):
    parts = []

    for item in soup.select("form.list-item"):
        pn = item.select_one("a[title*='Engine Assembly'], a[href*='itemdetail']")
        part_name = pn.get_text(strip=True) if pn else None
        detail_url = pn["href"] if pn else None

        price_tag = item.select_one(".buy-panel-sell-price")
        price = price_tag.get_text(strip=True).replace("$", "").strip() if price_tag else None

        seller_tag = item.select_one(".item-company-address strong")
        seller = seller_tag.get_text(strip=True) if seller_tag else None

        address_block = item.select_one(".item-company-address")
        address_lines = address_block.get_text("\n", strip=True).split("\n") if address_block else []
        seller_city, seller_state, seller_phone = parse_address(address_lines)

        tds = item.select("td")
        mileage = tds[2].get_text(strip=True) if len(tds) > 2 else None
        grade = tds[3].get_text(strip=True) if len(tds) > 3 else None

        condition_description = {"A": "Very Good", "B": "Good", "C": "Fair"}.get(grade)

        vin_tag = item.select_one("td b")
        vin = vin_tag.get_text(strip=True).replace("Vin:", "") if vin_tag else None

        position = None
        color = None

        if len(tds) >= 5:
            info_texts = [t.strip() for t in tds[4].stripped_strings]
            for t in info_texts:
                if t in ["Left", "Right", "Front", "Rear"]:
                    position = t
                elif t.isupper() and len(t) >= 3 and t not in ["VIN", "SHOW", "INFO"]:
                    color = t

        stock_tag = item.select_one(".stockno-link")
        stock_no = stock_tag.get_text(strip=True) if stock_tag else None

        img_tag = item.select_one("td img")
        thumbnail = img_tag["src"] if img_tag else None

        script = item.find("script")
        images = re.findall(r'"src":"(.*?)"', script.string) if script and script.string else []
        image_count = len(images)

        yard_id = None
        if thumbnail:
            m = re.search(r"/([a-zA-Z0-9]{4})/images/", thumbnail)
            yard_id = m.group(1).upper() if m else None

        distance_miles = yard_distances.get(yard_id)

        info_link = item.find("a", attrs={"id": "tool-tip"}) or item.find("a", string=lambda s: s and "Show Info" in s)
        show_info = info_link.get("data-original-title") if info_link else None

        parts.append({
            "run_timestamp": RUN_TS,
            "application_text": application_meta["application_text"] if application_meta else None,
            "application_id": application_meta["application_id"] if application_meta else None,
            "application_url": application_meta["application_url"] if application_meta else None,
            "part_name": part_name,
            "detail_url": detail_url,
            "price": price,
            "seller": seller,
            "seller_city": seller_city,
            "seller_state": seller_state,
            "seller_phone": seller_phone,
            "address": address_lines,
            "mileage": mileage,
            "grade": grade,
            "condition_description": condition_description,
            "vin": vin,
            "stock_no": stock_no,
            "position": position,
            "color": color,
            "show_info": show_info,
            "thumbnail": thumbnail,
            "yard_id": yard_id,
            "distance_miles": distance_miles,
            "interchange": interchange,
            "images": images,
            "image_count": image_count,
        })

    return parts

# ============================================================
# NEW LAYOUT PARSER
# ============================================================

def parse_new_layout(soup, interchange, yard_distances, application_meta):
    parts = []
    rows = soup.select("table.table.table-bordered tbody tr")
    if not rows:
        return parts

    for row in rows:
        tds = row.select("td")
        if len(tds) < 5:
            continue

        pn = tds[1].select_one("a[href*='itemdetail']")
        part_name = pn.get_text(strip=True) if pn else None
        detail_url = pn["href"] if pn else None

        price_tag = tds[1].select_one(".buy-panel-sell-price") or tds[0].select_one(".buy-panel-sell-price")
        price = price_tag.get_text(strip=True).replace("$", "") if price_tag else None

        mileage = tds[2].get_text(strip=True)
        grade = tds[3].get_text(strip=True)
        condition_description = {"A": "Very Good", "B": "Good", "C": "Fair"}.get(grade)

        info_td = tds[4]
        texts = [t.strip() for t in info_td.stripped_strings]

        vin = position = color = None
        for t in texts:
            if t.startswith("Vin:"):
                vin = t.replace("Vin:", "").strip()
            elif t in ["Left", "Right", "Front", "Rear"]:
                position = t
            elif t.isupper() and len(t) >= 3 and t not in ["VIN", "SHOW", "INFO"]:
                color = t

        stock_tag = info_td.select_one(".stockno-link")
        stock_no = stock_tag.get_text(strip=True) if stock_tag else None

        img = row.select_one("img")
        thumbnail = img["src"] if img else None
        images = [thumbnail] if thumbnail else []

        yard_id = None
        if thumbnail:
            m = re.search(r"//.*?/(.*?)/inventory/", thumbnail)
            yard_id = m.group(1).upper() if m else None

        distance_miles = yard_distances.get(yard_id)

        seller_tag = soup.select_one(".item-company-address strong")
        seller = seller_tag.get_text(strip=True) if seller_tag else None

        address_block = soup.select_one(".item-company-address")
        address_lines = address_block.get_text("\n", strip=True).split("\n") if address_block else []
        seller_city, seller_state, seller_phone = parse_address(address_lines)

        info_link = info_td.find("a", attrs={"id": "tool-tip"})
        show_info = info_link.get("data-original-title") if info_link else None

        parts.append({
            "run_timestamp": RUN_TS,
            "application_text": application_meta["application_text"] if application_meta else None,
            "application_id": application_meta["application_id"] if application_meta else None,
            "application_url": application_meta["application_url"] if application_meta else None,
            "part_name": part_name,
            "detail_url": detail_url,
            "price": price,
            "seller": seller,
            "seller_city": seller_city,
            "seller_state": seller_state,
            "seller_phone": seller_phone,
            "address": address_lines,
            "mileage": mileage,
            "grade": grade,
            "condition_description": condition_description,
            "vin": vin,
            "stock_no": stock_no,
            "position": position,
            "color": color,
            "show_info": show_info,
            "thumbnail": thumbnail,
            "yard_id": yard_id,
            "distance_miles": distance_miles,
            "interchange": interchange,
            "images": images,
            "image_count": len(images),
        })

    return parts

# ============================================================
# SCRAPING CORE
# ============================================================

def fetch_page(url, session, timeout, max_retries=3):
    headers = {
        "User-Agent": random.choice(USER_AGENTS)
    }

    for attempt in range(1, max_retries + 1):
        try:
            response = session.get(
                url,
                headers=headers,
                timeout=timeout
            )
            response.raise_for_status()

            # time.sleep(random.uniform(2.5, 5.5))
            return response.text, len(response.content)

        except requests.exceptions.Timeout as e:
            logger.warning(
                f"Timeout attempt {attempt} | {url} | {type(e).__name__}: {e}"
            )
            time.sleep(2 * attempt)

        except requests.exceptions.ConnectionError as e:
            logger.warning(
                f"ConnectionError attempt {attempt} | {url} | {type(e).__name__}: {e}"
            )
            time.sleep(2 * attempt)

        except requests.exceptions.HTTPError as e:
            status = e.response.status_code if e.response else "unknown"
            logger.warning(
                f"HTTPError attempt {attempt} | {url} | status={status}"
            )
            return None, 0

        except Exception as e:
            logger.warning(
                f"UnexpectedError attempt {attempt} | {url} | {type(e).__name__}: {e}"
            )
            time.sleep(2 * attempt)

    return None, 0

def scrape_autopartsearch(response_text, application_meta):
    soup = BeautifulSoup(response_text, "html.parser")

    interchange = None
    app_facet = soup.select_one("#applications-facet .panel-body")
    if app_facet:
        lbl = app_facet.select_one("label.checkbox")
        if lbl:
            interchange = re.sub(r"\(\d+\)$", "", lbl.get_text(strip=True))

    yard_distances = {}
    for li in soup.select("#yard-facet li label"):
        raw = li.get_text(" ", strip=True)
        m = re.search(r"\((\d+)\s*mi\.\)", raw)
        a = li.select_one("a")
        if m and a and "yard=" in a["href"]:
            yard_distances[a["href"].split("yard=")[1].upper()] = m.group(1)

    if soup.select("form.list-item"):
        return parse_old_layout(soup, interchange, yard_distances, application_meta)

    if soup.select("table.table.table-bordered tbody tr"):
        return parse_new_layout(soup, interchange, yard_distances, application_meta)

    return []

def scrape_all_pages(base_url, application_meta, session, timeout=10, max_pages=1000):
    all_parts = []
    page = 1

    pages_scraped = 0
    total_bytes = 0

    while page <= max_pages:
        page_url = base_url if page == 1 else f"{base_url}&currentpage={page}"
        logger.info(f"Fetching page {page}: {page_url}")

        html, page_size = fetch_page(page_url, session, timeout)
        if not html:
            break

        parts = scrape_autopartsearch(html, application_meta)
        logger.info(f"Found {len(parts)} parts on page {page} | size={page_size} bytes")

        if not parts:
            break

        all_parts.extend(parts)
        pages_scraped += 1
        total_bytes += page_size
        page += 1

    return {
        "parts": all_parts,
        "pages_scraped": pages_scraped,
        "total_bytes": total_bytes,
        "avg_page_size": int(total_bytes / pages_scraped) if pages_scraped else 0
    }

def get_applications(base_url, session):
    html, _ = fetch_page(base_url, session, 10)
    if not html:
        return []

    soup = BeautifulSoup(html, "html.parser")
    apps = []

    for a in soup.select("#applications-facet a.name"):
        href = a.get("href")
        if href and "application=" in href:
            apps.append({
                "application_text": a.get_text(strip=True),
                "application_id": href.split("application=")[1],
                "application_url": href
            })

    return apps

def scrape_with_applications(base_url, session):
    applications = get_applications(base_url, session)

    all_parts = []
    total_pages = 0
    total_bytes = 0

    if applications:
        for app in applications:
            logger.info(f"Scraping application {app['application_id']}")
            result = scrape_all_pages(app["application_url"], app, session)

            all_parts.extend(result["parts"])
            total_pages += result["pages_scraped"]
            total_bytes += result["total_bytes"]
    else:
        result = scrape_all_pages(base_url, None, session)
        all_parts.extend(result["parts"])
        total_pages += result["pages_scraped"]
        total_bytes += result["total_bytes"]

    return {
        "parts": all_parts,
        "pages_scraped": total_pages,
        "total_bytes": total_bytes,
        "avg_page_size": int(total_bytes / total_pages) if total_pages else 0
    }

# ============================================================
# MULTIPROCESS WORKER
# ============================================================

def scrape_record_worker(rec):
    global logger
    logger = logging.getLogger("autopartsearch_scraper")
    session = create_session()

    try:
        base_name = f"{rec['make']}_{rec['year']}_{rec['model']}_{rec['part_slug']}"
        base_name = re.sub(r"[^a-zA-Z0-9_]", "_", base_name)
        temp_path = os.path.join(TEMP_DIR, f"{base_name}.json")

        if os.path.exists(temp_path):
            with open(temp_path, "r", encoding="utf8") as f:
                return json.load(f)

        result = scrape_with_applications(rec["url"], session)

        logger.info(
            f"Completed {base_name} | pages={result['pages_scraped']} | bytes={result['total_bytes']}"
        )
        parts = result["parts"]

        for p in parts:
            p.update({
                "source_year": rec["year"],
                "source_make": rec["make"],
                "source_model": rec["model"],
                "source_part_name": rec["part_name"],
                "source_part_slug": rec["part_slug"],
                "source_url": rec["url"],
            })

        with open(temp_path, "w", encoding="utf8") as f:
            json.dump(
                {
                    "parts": parts,
                    "pages_scraped": result["pages_scraped"],
                    "total_bytes": result["total_bytes"]
                },
                f,
                indent=2
            )

        return {
            "parts": parts,
            "pages_scraped": result["pages_scraped"],
            "total_bytes": result["total_bytes"]
        }

    except Exception as e:
        logger.exception(
            f"Worker failure | {type(e).__name__}: {e}"
        )
        return {
            "parts": [],
            "pages_scraped": 0,
            "total_bytes": 0
        }

# ============================================================
# MULTIPROCESS ENTRY
# ============================================================

def scrape_from_csv(csv_path, workers=4):
    records = load_catalog_urls(csv_path)

    sample_size = min(500, len(records))
    records = random.sample(records, sample_size)

    filtered = []
    for r in records:
        if (r.get("part_name") or "").lower() in ["engine assembly"] or "transmission" in (r.get("part_name") or "").lower():
            filtered.append(r)

    log_queue = mp.Queue()
    listener = mp.Process(target=log_listener, args=(log_queue,))
    listener.start()

    all_parts = []
    total_pages = 0
    total_bytes = 0

    with mp.Pool(
        processes=workers,
        initializer=setup_worker_logger,
        initargs=(log_queue,)
    ) as pool:
        for result in pool.imap_unordered(scrape_record_worker, filtered):
            all_parts.extend(result["parts"])
            total_pages += result["pages_scraped"]
            total_bytes += result["total_bytes"]

    log_queue.put(None)
    listener.join()

    return {
        "parts": all_parts,
        "total_pages": total_pages,
        "total_bytes": total_bytes
    }


# ============================================================
# RUN
# ============================================================

if __name__ == "__main__":
    mp.freeze_support()

    CSV_PATH = "output/parts_data.csv"

    result = scrape_from_csv(CSV_PATH, workers=4)
    all_parts = result["parts"]
    total_pages = result["total_pages"]
    total_bytes = result["total_bytes"]

    logger.info(f"TOTAL pages scraped: {total_pages}")
    logger.info(f"TOTAL bytes transferred: {total_bytes}")
    logger.info(
        f"AVERAGE page size: {int(total_bytes / total_pages) if total_pages else 0} bytes"
    )


    final_path = os.path.join(FINAL_DIR, f"parts_data_{RUN_DATE}.json")
    with open(final_path, "w", encoding="utf8") as f:
        json.dump(all_parts, f, indent=2)

    logger.info(f"Saved final parts file to {final_path}")
