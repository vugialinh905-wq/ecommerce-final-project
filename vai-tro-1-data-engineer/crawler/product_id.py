import os
import time
import random
import pandas as pd
import requests

from selenium import webdriver
from selenium.webdriver.chrome.options import Options


CATEGORY_ID = 8322
OUTPUT_DIR = "temp_data"
OUTPUT_FILE = f"product_{CATEGORY_ID}.csv"

MAX_PAGES = 2
LIMIT_PER_PAGE = 40

BASE_URL = "https://tiki.vn/api/v2/products"


def get_tiki_cookies():
    options = Options()
    options.add_argument("--headless=new")
    options.add_argument("--disable-gpu")
    options.add_argument("--no-sandbox")
    options.add_argument("--window-size=1920,1080")
    options.add_argument("--disable-blink-features=AutomationControlled")

    driver = webdriver.Chrome(options=options)
    driver.get("https://tiki.vn")
    time.sleep(6)

    selenium_cookies = driver.get_cookies()
    driver.quit()

    cookies = {c["name"]: c["value"] for c in selenium_cookies}
    return cookies


def main():
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    cookies = get_tiki_cookies()
    guest_token = cookies.get("TIKI_GUEST_TOKEN")

    print("Guest token:", guest_token)

    session = requests.Session()
    session.cookies.update(cookies)

    headers = {
        "User-Agent": "Mozilla/5.0 (Linux; Android 10)",
        "Accept": "application/json, text/plain, */*",
        "Referer": f"https://tiki.vn/nha-sach-tiki/c{CATEGORY_ID}",
        "x-guest-token": guest_token or "",
    }

    session.headers.update(headers)

    product_ids = []

    for page in range(1, MAX_PAGES + 1):
        print(f"Crawling product ids page {page}")

        params = {
            "limit": LIMIT_PER_PAGE,
            "include": "advertisement",
            "aggregations": 2,
            "category": CATEGORY_ID,
            "page": page,
            "src": "c",
        }

        response = session.get(BASE_URL, params=params, timeout=20)

        if response.status_code != 200:
            print("Request failed:", response.status_code)
            break

        js = response.json()
        products = js.get("data", [])

        if not products:
            print("No products found. Stop.")
            break

        for item in products:
            pid = item.get("id")
            if pid:
                product_ids.append({"product_id": pid})

        time.sleep(random.uniform(0.5, 1.5))

    df = pd.DataFrame(product_ids).drop_duplicates(subset=["product_id"])

    output_path = os.path.join(OUTPUT_DIR, OUTPUT_FILE)
    df.to_csv(output_path, index=False, encoding="utf-8")

    print(f"DONE! Total product ids: {len(df)}")
    print(f"Saved to: {output_path}")


if __name__ == "__main__":
    main()