import pandas as pd
import requests
import time
import random
from tqdm import tqdm
from selenium import webdriver
from selenium.webdriver.chrome.options import Options

##  INIT VARIABLE
id = 8322  # id tại website tiki
# Example: https://tiki.vn/nha-sach-tiki/c8322  --> id = 8322

##  GET COOKIES BY Selenium
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
guest_token = cookies.get("TIKI_GUEST_TOKEN")

print("✅ Guest token:", guest_token)

##  REQUEST SESSION
session = requests.Session()
session.cookies.update(cookies)

session.headers.update({
    "User-Agent": "Mozilla/5.0 (Linux; Android 10)",
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9,vi;q=0.8",
    "Referer": "https://tiki.vn/",
    "x-guest-token": guest_token,
})

BASE_URL = "https://tiki.vn/api/v2/reviews"

##  COMMENT PARSER
def comment_parser(js):
    created_by = js.get("created_by") or {}
    seller = js.get("seller") or {}

    return {
        "comment_id": js.get("id"),
        "product_id": js.get("product_id"),
        "customer_id": js.get("customer_id"),
        "customer_name": created_by.get("name"),
        "rating": js.get("rating"),
        "title": js.get("title"),
        "content": js.get("content"),
        "created_at": js.get("created_at"),
        "purchased_at": created_by.get("purchased_at"),
        "seller_id": seller.get("id"),
        "seller_name": seller.get("name")
    }

##  LOAD PRODUCT IDS  
df_id = pd.read_csv(f"product_{id}.csv")
product_ids = df_id["product_id"].dropna().astype(int).tolist()

##  CRAWL COMMENTS  
all_comments = {}

for pid in tqdm(product_ids, desc="Products"):
    page = 1
    print(f"🔎 Crawl comments for product {pid}")

    while True:
        params = {
            "product_id": pid,
            "page": page,
            "limit": 10,
            "sort": "score|desc,id|desc,stars|all",
            "include": "comments,contribute_info,attribute_vote_summary"
        }

        for _ in range(3):  # retry
            try:
                r = session.get(BASE_URL, params=params, timeout=10)
                if r.status_code == 200:
                    break
            except:
                time.sleep(1)
        else:
            break

        data = r.json().get("data", [])
        if not data:
            break

        for item in data:
            c = comment_parser(item)
            cid = c["comment_id"]
            if cid not in all_comments:
                all_comments[cid] = c

        page += 1
        time.sleep(random.uniform(0.8, 1.5))

##  SAVE CSV
df = pd.DataFrame(all_comments.values())
df = df[df["content"].notna()]
df = df[df["content"].str.strip() != ""]

df = df[[
    "comment_id",
    "product_id",
    "customer_id",
    "customer_name",
    "rating",
    "title",
    "content",
    "created_at",
    "purchased_at",
    "seller_id",
    "seller_name",
]]

df.to_csv(f"comments_data_{id}.csv", index=False)

print(f"DONE! Total comments: {len(df)}")