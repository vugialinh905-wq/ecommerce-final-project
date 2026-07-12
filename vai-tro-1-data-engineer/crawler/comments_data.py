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
OUTPUT_FILE = "Comments.csv"

MAX_PRODUCTS = 50
MAX_COMMENT_PAGES_PER_PRODUCT = 2
LIMIT_PER_PAGE = 20

BASE_URL = "https://tiki.vn/api/v2/reviews"


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


def comment_parser(js, product_id):
    created_by = js.get("created_by") or {}
    seller = js.get("seller") or {}

    return {
        "comment_id": js.get("id"),
        "product_id": product_id,
        "customer_id": created_by.get("id"),
        "customer_name": created_by.get("name"),
        "rating": js.get("rating"),
        "title": js.get("title"),
        "content": js.get("content"),
        "created_at": js.get("created_at"),
        "purchased_at": js.get("created_at"),
        "seller_id": seller.get("id"),
        "seller_name": seller.get("name"),
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

    all_comments = {}

    for pid in tqdm(product_ids, desc="Products"):
        for page in range(1, MAX_COMMENT_PAGES_PER_PRODUCT + 1):
            params = {
                "product_id": pid,
                "sort": "score|desc,id|desc,stars|all",
                "page": page,
                "limit": LIMIT_PER_PAGE,
                "include": "comments,contribute_info,attribute_vote_summary",
            }

            try:
                response = session.get(BASE_URL, params=params, timeout=20)

                if response.status_code != 200:
                    print(f"Failed comments product {pid}, page {page}: {response.status_code}")
                    break

                js = response.json()
                comments = js.get("data", [])

                if not comments:
                    break

                for item in comments:
                    parsed = comment_parser(item, pid)
                    cid = parsed.get("comment_id")
                    if cid:
                        all_comments[cid] = parsed

            except Exception as e:
                print(f"Error comments product {pid}, page {page}: {e}")
                break

            time.sleep(random.uniform(0.5, 1.2))

    df = pd.DataFrame(all_comments.values())

    if not df.empty:
        df = df[df["content"].notna()]
        df = df[df["content"].astype(str).str.strip() != ""]

    output_path = os.path.join(OUTPUT_DIR, OUTPUT_FILE)
    df.to_csv(output_path, index=False, encoding="utf-8")

    print(f"DONE! Total comments: {len(df)}")
    print(f"Saved to: {output_path}")


if __name__ == "__main__":
    main()