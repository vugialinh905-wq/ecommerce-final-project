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

print("Guest token:", guest_token)

##  REQUEST SESSION
session = requests.Session()
session.cookies.update(cookies)

headers = {
    "User-Agent": "Mozilla/5.0 (Linux; Android 10)",
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9,vi;q=0.8",
    "Referer": "https://tiki.vn/",
    "x-guest-token": guest_token,
}

session.headers.update(headers)

params = {
    "platform": "web"
}

BASE_URL = "https://tiki.vn/api/v2/products/{}"

##  PARSER
def parser_product(js):
    return {
        "product_id": js.get("id"),
        "product_name": js.get("name")
    }

##  LOAD PRODUCT IDS  
df_id = pd.read_csv(f"product_{id}.csv")
product_ids = df_id["product_id"].dropna().astype(int).tolist()


##  CRAWL DETAIL  
results = []

for pid in tqdm(product_ids):
    url = BASE_URL.format(pid)

    for _ in range(3):  # retry tối đa 3 lần
        try:
            r = session.get(url, params=params, timeout=10)
            if r.status_code == 200:
                results.append(parser_product(r.json()))
                break
        except:
            time.sleep(1)

    time.sleep(random.uniform(0.8, 1.5))  # nhẹ nhưng an toàn

##  SAVE CSV
df = pd.DataFrame(results)
df.to_csv(
    f"data_{id}.csv",
    index=False,
    sep=",",
    decimal="."
)

print(f"DONE! Total products: {len(df)}")
