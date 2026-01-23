from bs4 import BeautifulSoup
import re
import requests
import json 
import os
import logging
from datetime import datetime
import csv

# ============================================================
# GLOBAL RUN CONFIG
# ============================================================

# BASE_URL = "https://www.autopartsearch.com/catalog-6/vehicle/TOYOTA/2010/HIGHLANDER/engine-assembly"

RUN_TS = datetime.now().strftime("%Y%m%d%H%M%S")

LOG_DIR = "logs"

RUN_DATE = datetime.now().strftime("%Y%m%d")

RUN_ROOT = os.path.join("output", f"parts_scrape_{RUN_DATE}")
FINAL_DIR = os.path.join(RUN_ROOT, "final")
TEMP_DIR = os.path.join(RUN_ROOT, "temp")

os.makedirs(FINAL_DIR, exist_ok=True)
os.makedirs(TEMP_DIR, exist_ok=True)
os.makedirs(LOG_DIR, exist_ok=True)

def setup_logger():
    logger = logging.getLogger("autopartsearch_scraper")
    logger.setLevel(logging.INFO)
    logger.handlers.clear()

    log_file = os.path.join(
        LOG_DIR,
        f"autopartsearch_run_{RUN_TS}.log"
    )

    logger.addHandler(logging.FileHandler(log_file, encoding="utf8"))
    logger.addHandler(logging.StreamHandler())

    formatter = logging.Formatter(
        "%(asctime)s | %(levelname)s | %(message)s"
    )

    for h in logger.handlers:
        h.setFormatter(formatter)

    return logger

logger = setup_logger()
logger.info("Starting AutoPartSearch scrape")

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

# -----------------------------------
# SAVE FIRST PAGE RESULTS
# -----------------------------------
def save_first_page_text(parts, filename="first_page_results.txt"):
    with open(filename, "w", encoding="utf8") as f:
        for p in parts:
            f.write(json.dumps(p, indent=2))
            f.write("\n\n")
    logger.info(f"Saved first page to {filename}")

def save_first_page_html(parts, filename="first_page_results.html"):
    html = "<html><body><h1>First Page Results</h1><pre>"
    html += json.dumps(parts, indent=2)
    html += "</pre></body></html>"

    with open(filename, "w", encoding="utf8") as f:
        f.write(html)

    logger.info(f"Saved first page to {filename}")

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


# -----------------------------
# OLD LAYOUT PARSER (<form.list-item>)
# -----------------------------
def parse_old_layout(soup, interchange, yard_distances, application_meta):
    parts = []

    for item in soup.select("form.list-item"):

        # part name
        pn = item.select_one("a[title*='Engine Assembly'], a[href*='itemdetail']")
        part_name = pn.get_text(strip=True) if pn else None
        detail_url = pn["href"] if pn else None

        # price
        price_tag = item.select_one(".buy-panel-sell-price")
        price = price_tag.get_text(strip=True).replace("$", "").strip() if price_tag else None

        # seller
        seller_tag = item.select_one(".item-company-address strong")
        seller = seller_tag.get_text(strip=True) if seller_tag else None

        address_block = item.select_one(".item-company-address")
        address_lines = address_block.get_text("\n", strip=True).split("\n") if address_block else []
        seller_city, seller_state, seller_phone = parse_address(address_lines)

        # table columns
        tds = item.select("td")
        mileage = tds[2].get_text(strip=True) if len(tds) > 2 else None
        grade = tds[3].get_text(strip=True) if len(tds) > 3 else None

        condition_description = {"A": "Very Good", "B": "Good", "C": "Fair"}.get(grade)

        # VIN
        vin_tag = item.select_one("td b")
        vin = vin_tag.get_text(strip=True).replace("Vin:", "") if vin_tag else None

        # position and color (best effort)
        position = None
        color = None

        if len(tds) >= 5:
            info_texts = [t.strip() for t in tds[4].stripped_strings]

            for t in info_texts:
                if t in ["Left", "Right", "Front", "Rear"]:
                    position = t
                elif (
                    t.isupper() and len(t) >= 3
                    and t not in ["VIN", "SHOW", "INFO"]
                    and not t.replace(" ", "").startswith("Vin")
                ):
                    color = t

        # stock number
        stock_tag = item.select_one(".stockno-link")
        stock_no = stock_tag.get_text(strip=True) if stock_tag else None

        # images
        img_tag = item.select_one("td img")
        thumbnail = img_tag["src"] if img_tag else None

        script = item.find("script")
        images = re.findall(r'"src":"(.*?)"', script.string) if script and script.string else []
        image_count = len(images)

        # yard id (old layout uses /XXXX/images/)
        yard_id = None
        if thumbnail:
            m = re.search(r"/([a-zA-Z0-9]{4})/images/", thumbnail)
            yard_id = m.group(1).upper() if m else None

        distance_miles = yard_distances.get(yard_id)

        # Show Info Attribute
        info_link = item.find("a", string=lambda s: s and "Show Info" in s)

        show_info = (
            info_link.get("data-original-title").strip()
            if info_link and info_link.has_attr("data-original-title")
            else None
        )


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


# -----------------------------
# NEW LAYOUT PARSER (Knockout table)
# -----------------------------
def parse_new_layout(soup, interchange, yard_distances, application_meta):
    parts = []

    rows = soup.select("table.table.table-bordered tbody tr")
    if not rows:
        return parts

    for row in rows:
        tds = row.select("td")
        if len(tds) < 5:
            continue

        # part name
        pn = tds[1].select_one("a[href*='itemdetail']")
        part_name = pn.get_text(strip=True) if pn else None
        detail_url = pn["href"] if pn else None

        # price
        price_tag = (
            tds[1].select_one(".buy-panel-sell-price")
            or tds[0].select_one(".buy-panel-sell-price")
        )
        price = price_tag.get_text(strip=True).replace("$", "") if price_tag else None

        # mileage and grade
        mileage = tds[2].get_text(strip=True)
        grade = tds[3].get_text(strip=True)
        condition_description = {"A": "Very Good", "B": "Good", "C": "Fair"}.get(grade)

        # VIN and attributes
        info_td = tds[4]
        texts = [t.strip() for t in info_td.stripped_strings]

        vin = None
        position = None
        color = None

        for t in texts:
            if t.startswith("Vin:"):
                vin = t.replace("Vin:", "").strip()

            elif t in ["Left", "Right", "Front", "Rear"]:
                position = t

            elif t.isupper() and len(t) >= 3 and t not in ["VIN", "SHOW", "INFO"]:
                color = t

        # stock number
        stock_tag = info_td.select_one(".stockno-link")
        stock_no = stock_tag.get_text(strip=True) if stock_tag else None

        # thumbnail (new layout has only first image visible)
        img = row.select_one("img")
        thumbnail = img["src"] if img else None
        images = [thumbnail] if thumbnail else []
        image_count = len(images)

        # yard id from inventory path (/WI23/inventory/)
        yard_id = None
        if thumbnail:
            m = re.search(r"//.*?/(.*?)/inventory/", thumbnail)
            yard_id = m.group(1).upper() if m else None

        distance_miles = yard_distances.get(yard_id)

        # seller info (shared layout)
        seller_tag = soup.select_one(".item-company-address strong")
        seller = seller_tag.get_text(strip=True) if seller_tag else None

        address_block = soup.select_one(".item-company-address")
        address_lines = address_block.get_text("\n", strip=True).split("\n") if address_block else []
        seller_city, seller_state, seller_phone = parse_address(address_lines)

        # Show Info tooltip text
        info_link = info_td.find("a", string=lambda s: s and "Show Info" in s)

        show_info = (
            info_link.get("data-original-title").strip()
            if info_link and info_link.has_attr("data-original-title")
            else None
        )


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

def save_run_summary(applications, parts):
    summary = {
        "run_timestamp": RUN_TS,
        "application_count": len(applications),
        "part_count": len(parts)
    }

    filename = os.path.join(
        RUN_ROOT,
        f"run_summary_{RUN_TS}.json"
    )

    with open(filename, "w", encoding="utf8") as f:
        json.dump(summary, f, indent=2)

    logger.info(f"Saved run summary to {filename}")

# def save_applications(applications, base_url):
#     filename = os.path.join(
#         RUN_ROOT,
#         f"applications_data_{RUN_TS}.json"
#     )

#     rows = []
#     for app in applications:
#         rows.append({
#             "base_url": base_url,
#             "application_text": app["application_text"],
#             "application_id": app["application_id"],
#             "application_url": app["application_url"]
#         })

#     with open(filename, "w", encoding="utf8") as f:
#         json.dump(rows, f, indent=2)

#     logger.info(f"Saved {len(rows)} applications to {filename}")

# def save_results(parts):
#     filename = os.path.join(
#         RUN_ROOT,
#         f"parts_data_{RUN_TS}.json"
#     )

#     with open(filename, "w", encoding="utf8") as f:
#         json.dump(parts, f, indent=2)

#     logger.info(f"Saved to {filename}")

# -----------------------------
# MAIN SCRAPER: auto-detect layout
# -----------------------------
def scrape_autopartsearch(response_text, application_meta):
    soup = BeautifulSoup(response_text, "html.parser")

    # global interchange (old layout only)
    interchange = None
    app_facet = soup.select_one("#applications-facet .panel-body")
    if app_facet:
        lbl = app_facet.select_one("label.checkbox")
        if lbl:
            txt = lbl.get_text(" ", strip=True)
            interchange = re.sub(r"\(\d+\)$", "", txt).strip()

    # yard distances
    yard_distances = {}
    for li in soup.select("#yard-facet li label"):
        raw = li.get_text(" ", strip=True)
        m = re.search(r"\((\d+)\s*mi\.\)", raw)
        dist = m.group(1) if m else None

        a = li.select_one("a")
        if a and "yard=" in a["href"]:
            yard_id = a["href"].split("yard=")[1]
            yard_distances[yard_id.upper()] = dist

    # detect layout
    if soup.select("form.list-item"):
        return parse_old_layout(soup, interchange, yard_distances, application_meta)

    if soup.select("table.table.table-bordered tbody tr"):
        return parse_new_layout(soup, interchange, yard_distances, application_meta)

    return []

def get_applications(base_url, timeout=10):
    html = fetch_page(base_url, timeout)
    if html is None:
        logger.error(f"Failed to load applications for {base_url}")
        return []

    soup = BeautifulSoup(html, "html.parser")

    applications = []

    for a in soup.select("#applications-facet a.name"):
        text = a.get_text(strip=True)
        href = a.get("href")

        if href and "application=" in href:
            application_id = href.split("application=")[1]
            applications.append({
                "application_text": text,
                "application_id": application_id,
                "application_url": href
            })

    return applications

def scrape_with_applications(base_url):
    """
    Scrape parts for a catalog URL.
    If applications exist, scrape per application.
    If not, scrape the base URL once.
    """
    applications = get_applications(base_url)
    all_parts = []

    if applications:
        # save_applications(applications, base_url)

        for app in applications:
            logger.info(
                f"Scraping application {app['application_id']} | {app['application_text']}"
            )

            parts = scrape_all_pages(
                app["application_url"],
                app
            )

            all_parts.extend(parts)

    else:
        logger.info("No applications found. Scraping base URL directly.")

        parts = scrape_all_pages(
            base_url,
            application_meta=None
        )

        all_parts.extend(parts)

    return applications, all_parts

def fetch_page(url, timeout):
    try:
        response = requests.get(url, timeout=timeout)
        response.raise_for_status()
        return response.text
    except requests.exceptions.RequestException as e:
        logger.error(f"Request failed | url={url} | error={e}")
        return None

def scrape_all_pages(base_url, application_meta, timeout=10, max_pages=1000):
    """
    Scrape until a page contains zero parts.
    max_pages prevents infinite loops if APS changes behavior.
    """
    all_parts = []
    page = 1

    while page <= max_pages:
        if page == 1:
            page_url = base_url
        else:
            separator = "&" if "?" in base_url else "?"
            page_url = f"{base_url}{separator}currentpage={page}"

        logger.info(f"Fetching page {page}: {page_url}")

        html = fetch_page(page_url, timeout)
        if html is None:
            logger.info("Stopping pagination for this application due to request failure.")
            break

        parts = scrape_autopartsearch(html, application_meta)
        # if page == 1:
        #     save_first_page_html(parts)
        #     return 

        logger.info(f"Found {len(parts)} parts on the Page {page}")

        # Stop condition
        if not parts:
            logger.info("No more parts found. Stopping.")
            break

        all_parts.extend(parts)
        page += 1

    logger.info(f"Finished. Total parts collected: {len(all_parts)}")
    return all_parts

def scrape_from_csv(csv_path):
    records = load_catalog_urls(csv_path)
    logger.info(f"Loaded {len(records)} catalog URLs from CSV")

    records = records[:2]

    logger.info(f"\n\nRecords: {records}\n\n")

    all_parts = []
    processed = 0

    filtered_records = []

    # Temp Logic. 

    for rec in records:
        try:
            year = int(rec["year"])
        except Exception:
            continue

        part_name = (rec.get("part_name") or "").lower()

        # if year not in (2023, 2024):
        #     continue

        if part_name == "engine assembly" or "transmission" in part_name:
            filtered_records.append(rec)

    logger.info(
        f"Filtered records: {len(filtered_records)} "
        f"out of {len(records)} after year and part filters"
    )

    records = filtered_records


    for rec in records:
        logger.info(
            f"Scraping {rec['year']} {rec['make']} {rec['model']} | {rec['part_name']}"
        )

        # build temp filename early so we can skip if it already exists
        base_name = f"{rec['make']}_{rec['year']}_{rec['model']}_{rec['part_slug']}"
        base_name = re.sub(r"[^a-zA-Z0-9_]", "_", base_name)
        safe_name = f"{base_name}.json"
        temp_path = os.path.join(TEMP_DIR, safe_name)

        # resumability check
        if os.path.exists(temp_path):
            logger.info(f"Skipping already scraped record: {safe_name}")

            with open(temp_path, "r", encoding="utf8") as f:
                parts = json.load(f)

            all_parts.extend(parts)
            processed += 1
            continue

        # scrape fresh
        applications, parts = scrape_with_applications(rec["url"])

        # attach source metadata
        for p in parts:
            p["source_year"] = rec["year"]
            p["source_make"] = rec["make"]
            p["source_model"] = rec["model"]
            p["source_part_name"] = rec["part_name"]
            p["source_part_slug"] = rec["part_slug"]
            p["source_url"] = rec["url"]

        # save intermediate results
        with open(temp_path, "w", encoding="utf8") as f:
            json.dump(parts, f, indent=2)

        all_parts.extend(parts)
        processed += 1

        logger.info(f"Completed {processed} of {len(records)} records")

    return all_parts

CSV_PATH = "output/parts_data.csv"

all_parts = scrape_from_csv(CSV_PATH)

final_path = os.path.join(
    FINAL_DIR,
    f"parts_data_{RUN_DATE}.json"
)

with open(final_path, "w", encoding="utf8") as f:
    json.dump(all_parts, f, indent=2)

logger.info(f"Saved final parts file to {final_path}")

save_run_summary([], all_parts)
