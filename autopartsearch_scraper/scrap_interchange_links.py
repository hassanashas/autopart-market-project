import requests
import json
import re
from bs4 import BeautifulSoup


CATALOG_URL = "https://www.autopartsearch.com/catalog-6/vehicle/TOYOTA/2010/HIGHLANDER/engine-assembly"


# -------------------------------------------------
# APPLICATIONS PARSER
# -------------------------------------------------
def parse_applications(soup):
    """
    Extracts application options exactly as shown in the fitment popup
    """
    applications = []

    for a in soup.select("#applications-facet a.name"):
        text = a.get_text(strip=True)
        href = a.get("href")

        application_id = None
        if href and "application=" in href:
            application_id = href.split("application=")[1]

        applications.append({
            "application_text": text,
            "application_id": application_id,
            "application_url": href
        })

    return applications


# -------------------------------------------------
# PARTS PARSER
# -------------------------------------------------
def parse_parts(soup):
    parts = []

    rows = soup.select("table.table.table-bordered tbody tr")
    if not rows:
        return parts

    for row in rows:
        tds = row.select("td")
        if len(tds) < 5:
            continue

        part_link = tds[1].select_one("a[href*='itemdetail']")
        part_name = part_link.get_text(strip=True) if part_link else None
        detail_url = part_link["href"] if part_link else None

        price_tag = row.select_one(".buy-panel-sell-price")
        price = price_tag.get_text(strip=True).replace("$", "") if price_tag else None

        mileage = tds[2].get_text(strip=True)
        grade = tds[3].get_text(strip=True)

        vin = None
        info_td = tds[4]
        for txt in info_td.stripped_strings:
            if txt.startswith("Vin:"):
                vin = txt.replace("Vin:", "").strip()

        img = row.select_one("img")
        thumbnail = img["src"] if img else None

        parts.append({
            "part_name": part_name,
            "detail_url": detail_url,
            "price": price,
            "mileage": mileage,
            "grade": grade,
            "vin": vin,
            "thumbnail": thumbnail
        })

    return parts


# -------------------------------------------------
# MAIN SCRAPER
# -------------------------------------------------
def scrape_catalog(url):
    response = requests.get(url, timeout=15)
    response.raise_for_status()

    soup = BeautifulSoup(response.text, "html.parser")

    applications = parse_applications(soup)
    parts = parse_parts(soup)

    return {
        "applications": applications,
        "parts": parts
    }


# -------------------------------------------------
# SAVE OUTPUT
# -------------------------------------------------
def save_output(data):
    with open("applications.json", "w", encoding="utf8") as f:
        json.dump(data["applications"], f, indent=2)

    with open("parts.json", "w", encoding="utf8") as f:
        json.dump(data["parts"], f, indent=2)

    print(f"Saved {len(data['applications'])} applications")
    print(f"Saved {len(data['parts'])} parts")


# -------------------------------------------------
# RUN
# -------------------------------------------------
if __name__ == "__main__":
    data = scrape_catalog(CATALOG_URL)
    save_output(data)
