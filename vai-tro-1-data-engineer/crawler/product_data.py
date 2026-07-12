import os
import time
import random
import pandas as pd
import requests

from tqdm import tqdm
from selenium import webdriver
from selenium.webdriver.chrome.options import Options


CATEGORY_ID = 8322
INPUT_DIR = "temp_data"
OUTPUT_DIR = "temp_data"

INPUT_FILE = f"product_{CATEGORY_ID}.csv"
OUTPUT_FILE = "Product.csv"

MAX_PRODUCTS = 50

BASE_URL = "https://tiki.vn/api/v2/products/{}"


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


def parser_product(js):
    return {
        "product_id": js.get("id"),
        "product_name": js.get("name"),
        "price": js.get("price"),
        "original_price": js.get("original_price"),
        "rating_average": js.get("rating_average"),
        "review_count": js.get("review_count"),
        "brand_name": (js.get("brand") or {}).get("name"),
        "short_description": js.get("short_description"),
        "url_key": js.get("url_key"),
    }


def main():
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    input_path = os.path.join(INPUT_DIR, INPUT_FILE)

    df_id = pd.read_csv(input_path)
    product_ids = df_id["product_id"].dropna().astype(int).tolist()
    product_ids = product_ids[:MAX_PRODUCTS]

    cookies = get_tiki_cookies()
    guest_token = cookies.get("TIKI_GUEST_TOKEN")

    print("Guest token:", guest_token)

    session = requests.Session()
    session.cookies.update(cookies)

    headers = {
        "User-Agent": "Mozilla/5.0 (Linux; Android 10)",
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "en-US,en;q=0.9,vi;q=0.8",
        "Referer": "https://tiki.vn/",
        "x-guest-token": guest_token or "",
    }

    session.headers.update(headers)

    results = []

    for pid in tqdm(product_ids, desc="Products"):
        url = BASE_URL.format(pid)

        try:
            response = session.get(url, params={"platform": "web"}, timeout=20)

            if response.status_code == 200:
                js = response.json()
                results.append(parser_product(js))
            else:
                print(f"Failed product {pid}: {response.status_code}")

        except Exception as e:
            print(f"Error product {pid}: {e}")

        time.sleep(random.uniform(0.5, 1.2))

    df = pd.DataFrame(results)

    output_path = os.path.join(OUTPUT_DIR, OUTPUT_FILE)
    df.to_csv(output_path, index=False, encoding="utf-8")

    print(f"DONE! Total products: {len(df)}")
    print(f"Saved to: {output_path}")


if __name__ == "__main__":
    main()