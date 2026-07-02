# Hệ thống Phân tích Hành vi Mua sắm và Gợi ý Sản phẩm Thương mại Điện tử

## Dataset
Dữ liệu thật crawl từ Tiki — 399,390 đánh giá, 230,500 khách hàng, 5,274 sản phẩm, từ 2011 đến 2026.

## Cấu trúc dự án
ecommerce-final/
├── vai-tro-1-data-engineer/     ← Thu thập, làm sạch dữ liệu (PySpark)
├── vai-tro-2-rfm-analysis/      ← Phân khúc khách hàng RFM (PySpark)
├── vai-tro-3-recommendation/    ← Gợi ý sản phẩm FP-Growth (Spark MLlib)
└── vai-tro-4-5-web/             ← Backend Flask + Frontend Web

## Kiến trúc hệ thống
Dữ liệu Tiki (crawl thật)
↓
Apache Spark (làm sạch + RFM + FP-Growth)
↓
Output CSV
↓
Backend Flask (API)
↓
Frontend Web (Dashboard + Gợi ý)

## Cách chạy toàn bộ pipeline
```bash
# Bước 1: Làm sạch dữ liệu
cd vai-tro-1-data-engineer
python3 data_ingestion.py

# Bước 2: Phân tích RFM
cd ../vai-tro-2-rfm-analysis
python3 phan_tich_rfm.py

# Bước 3: Train recommendation
cd ../vai-tro-3-recommendation
python3 src/train_fp_growth.py

# Bước 4: Chạy web
cd ../vai-tro-4-5-web
python3 app.py
```

Mở trình duyệt: `http://localhost:5000/dashboard`
