from bs4 import BeautifulSoup
import re
import requests
import json
import os
import logging
import logging.handlers
from datetime import datetime
import csv
import asyncio
import aiohttp
from aiohttp.client_exceptions import ClientConnectorError
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

def get_aiohttp_session():
    headers = {
        "Accept": "text/html",
        "Accept-Language": "en-US,en;q=0.9"
    }

    timeout = aiohttp.ClientTimeout(total=30)

    if USE_PROXY:
        return aiohttp.ClientSession(
            headers=headers,
            timeout=timeout
        )
    else:
        return aiohttp.ClientSession(
            headers=headers,
            timeout=timeout
        )

# ============================================================
# LOGGING
# ============================================================

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

logger = setup_logger()
logger.info("Starting AutoPartSearch scrape")

# ============================================================
# CSV LOADER
# ============================================================

def load_catalog_urls(csv_path):
    urls = []
    seen_urls = set()

    with open(csv_path, newline="", encoding="utf8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            # Skip rows without a valid link
            if row.get("link_found", "").lower() != "true":
                continue

            url = row.get("url")
            if not url:
                continue

            # Deduplicate on URL
            if url in seen_urls:
                continue

            seen_urls.add(url)

            urls.append({
                "year": row.get("year"),
                "make": row.get("manufacturer"),
                "model": row.get("model_name"),
                "part_name": row.get("part_name"),
                "part_slug": row.get("part_slug"),
                "url": url,
                "ic_description": row.get("ic_description"),
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

def normalize_text(s):
    if not s:
        return ""
    s = s.lower()
    s = re.sub(r"\(\d+\)$", "", s)
    s = re.sub(r"\s+", " ", s)
    return s.strip()

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

SEM = asyncio.Semaphore(15)

async def fetch_page(url, session, timeout=15, max_retries=3):
    headers = {
        "User-Agent": random.choice(USER_AGENTS)
    }

    proxy = PROXIES["http"] if USE_PROXY else None

    for attempt in range(1, max_retries + 1):
        try:
            async with SEM:
                async with session.get(
                    url,
                    headers=headers,
                    proxy=proxy,
                    timeout=timeout
                ) as response:
                    response.raise_for_status()
                    text = await response.text()
                    size = len(text.encode("utf8"))
                    return text, size

        except asyncio.TimeoutError:
            logger.warning(f"Timeout attempt {attempt} | {url}")
            await asyncio.sleep(2 * attempt)

        except ClientConnectorError as e:
            logger.warning(f"Connection error attempt {attempt} | {url} | {e}")
            await asyncio.sleep(2 * attempt)

        except aiohttp.ClientError as e:
            logger.warning(f"HTTP error attempt {attempt} | {url} | {e}")
            await asyncio.sleep(2 * attempt)

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

async def scrape_all_pages(
        base_url,
        application_meta,
        session,
        record_idx,
        total_records,
        timeout=10,
        max_pages=1000
    ):

    all_parts = []
    page = 1

    pages_scraped = 0
    total_bytes = 0

    while page <= max_pages:
        page_url = base_url if page == 1 else f"{base_url}&currentpage={page}"
        logger.info(
            f"Record {record_idx} of {total_records} | "
            f"Fetching page {page} | {page_url}"
        )

        html, page_size = await fetch_page(page_url, session, timeout)
        if not html:
            break

        parts = scrape_autopartsearch(html, application_meta)
        logger.info(
            f"Record {record_idx} of {total_records} | "
            f"Page {page} returned {len(parts)} parts | size={page_size} bytes"
        )

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

async def get_applications(base_url, session):
    html, _ = await fetch_page(base_url, session, 10)
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

async def scrape_with_applications(base_url, session, record_idx, total_records, ic_description):
    applications = await get_applications(base_url, session)

    all_parts = []
    total_pages = 0
    total_bytes = 0

    if applications and ic_description:
        target = normalize_text(ic_description)

        matched_apps = [
            app for app in applications
            if normalize_text(app["application_text"]) == target
        ]

        if not matched_apps:
            logger.warning(
                f"Record {record_idx} of {total_records} | "
                f"No application matched ic_description | {ic_description}"
            )
            return {
                "parts": [],
                "pages_scraped": 0,
                "total_bytes": 0,
                "avg_page_size": 0
            }

        applications = matched_apps

        for app in applications:
            logger.info(
                f"Record {record_idx} of {total_records} | "
                f"Scraping application {app['application_id']}"
            )

            logger.info(
                f"Application details | "
                f"id={app['application_id']} | "
                f"text={app['application_text']} | "
                f"url={app['application_url']}"
            )
            
            result = await scrape_all_pages(app["application_url"], app, session, record_idx, total_records)

            all_parts.extend(result["parts"])
            total_pages += result["pages_scraped"]
            total_bytes += result["total_bytes"]
    else:
        result = await scrape_all_pages(base_url, None, session, record_idx, total_records)
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
# ASYNC WORKER
# ============================================================

async def scrape_record(rec, record_idx, total_records, session):
    try:
        start_ts = time.perf_counter()
        logger.info(
            f"Starting record {record_idx} of {total_records} | "
            f"{rec['make']} {rec['year']} {rec['model']} {rec['part_slug']}"
        )
        base_name = f"{rec['make']}_{rec['year']}_{rec['model']}_{rec['part_slug']}"
        base_name = re.sub(r"[^a-zA-Z0-9_]", "_", base_name)
        temp_path = os.path.join(TEMP_DIR, f"{base_name}.json")

        if os.path.exists(temp_path):
            with open(temp_path, "r", encoding="utf8") as f:
                return json.load(f)

        result = await scrape_with_applications(
            rec["url"],
            session,
            record_idx,
            total_records,
            rec.get("ic_description")
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

        elapsed_sec = round(time.perf_counter() - start_ts, 2)
        result["record_runtime_seconds"] = elapsed_sec

        with open(temp_path, "w", encoding="utf8") as f:
            json.dump(result, f, indent=2)

        logger.info(
            f"Finished record {record_idx} of {total_records} | "
            f"pages={result['pages_scraped']} | "
            f"bytes={result['total_bytes']} | "
            f"time={elapsed_sec}s | "
            f"seconds_per_page={round(elapsed_sec / result['pages_scraped'], 2) if result['pages_scraped'] else 0}"
        )

        return result

    except Exception as e:
        logger.exception(f"Worker failure | {e}")
        return {"parts": [], "pages_scraped": 0, "total_bytes": 0}

# ============================================================
# ASYNC ENTRY
# ============================================================

async def scrape_from_csv(csv_path):
    records = load_catalog_urls(csv_path)

    # sample_size = min(2, len(records))
    # records = random.sample(records, sample_size)

    filtered = []
    for r in records:
        name = (r.get("part_name") or "").lower()
        if name == "engine assembly" or "transmission" in name:
            filtered.append(r)

    total_records = len(filtered)

    logger.info(f"Total URLs to scrap: {total_records}")

    async with get_aiohttp_session() as session:
        tasks = [
            scrape_record(r, idx + 1, total_records, session)
            for idx, r in enumerate(filtered)
        ]
        tasks = tasks[0:2]
        results = await asyncio.gather(*tasks)

    all_parts = []
    total_pages = 0
    total_bytes = 0

    for r in results:
        all_parts.extend(r["parts"])
        total_pages += r["pages_scraped"]
        total_bytes += r["total_bytes"]

    return {
        "parts": all_parts,
        "total_pages": total_pages,
        "total_bytes": total_bytes
    }

# ============================================================
# RUN
# ============================================================

if __name__ == "__main__":

    program_start_ts = time.perf_counter()

    CSV_PATH = "output/ic_parts_data_combined_with_links_from_autopartsearch.csv"

    result = asyncio.run(scrape_from_csv(CSV_PATH))

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

    program_elapsed_sec = round(time.perf_counter() - program_start_ts, 2)
    logger.info(f"TOTAL runtime seconds: {program_elapsed_sec}")

    logger.info(f"Saved final parts file to {final_path}")
