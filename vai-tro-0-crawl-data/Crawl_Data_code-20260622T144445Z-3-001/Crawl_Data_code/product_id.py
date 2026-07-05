from selenium import webdriver
from selenium.webdriver.chrome.options import Options
import requests
import time
import random
import pandas as pd

##  INIT VARIABLE
id = 8322  # id tại website tiki
# Example: https://tiki.vn/nha-sach-tiki/c8322  --> id = 8322

## GET COOKIES BY Selenium
options = Options()
options.add_argument("--headless=new")
options.add_argument("--disable-gpu")
options.add_argument("--no-sandbox")
options.add_argument("--window-size=1920,1080")
options.add_argument("--disable-blink-features=AutomationControlled")

driver = webdriver.Chrome(options=options)
driver.get("https://tiki.vn")
time.sleep(6)  # đợi JS set cookie

selenium_cookies = driver.get_cookies()
driver.quit()

# Convert cookie cho requests
cookies = {c["name"]: c["value"] for c in selenium_cookies}

# Lấy guest token
guest_token = cookies.get("TIKI_GUEST_TOKEN")

print("Guest token:", guest_token)

##  CALL API BY REQUESTS
headers = {
    "User-Agent": "Mozilla/5.0 (Linux; Android 10)",
    "Accept": "application/json, text/plain, */*",
    "Referer": f"https://tiki.vn/nha-sach-tiki/c{id}",
    "x-guest-token": guest_token,
}

params = {
    "limit": 40,
    "include": "advertisement",
    "aggregations": 2,
    "category": id,
    "page": 1,
    "src": f"c{id}",
}

BASE_URL = "https://tiki.vn/api/v2/products"

product_ids = []
page = 1

while True:
    params["page"] = page
    print(f"🔎 Crawling page {page}")

    r = requests.get(
        BASE_URL,
        headers=headers,
        params=params,
        cookies=cookies,
        timeout=10
    )

    if r.status_code != 200:
        print("Request failed")
        break

    data = r.json().get("data", [])
    if not data:
        print("Hết sản phẩm")
        break

    for item in data:
        product_ids.append({"product_id": item.get("id")})

    page += 1
    time.sleep(random.uniform(2, 5))

##  SAVE CSV
df = pd.DataFrame(product_ids)
df.to_csv(f"product_{id}.csv", index=False)

print(f"DONE! Total products: {len(product_ids)}")
