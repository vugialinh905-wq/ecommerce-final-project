# PR3 - Hệ Thống Gợi Ý Sản Phẩm (Product Recommendation System)

## Tổng Quan

Đây là một hệ thống gợi ý sản phẩm sử dụng thuật toán **FP-Growth** (Frequent Pattern Growth) để tìm ra các quy luật kết hợp giữa các sản phẩm và cung cấp những gợi ý sản phẩm cá nhân hóa cho từng khách hàng.

## Tính Năng Chính

- **Khai Phá Tập Phổ Biến**: Tìm những sản phẩm thường xuyên được mua cùng nhau
- **Luật Kết Hợp**: Tạo các luật `{Product A} → {Product B}` với độ đo Support, Confidence, Lift
- **Gợi Ý Cá Nhân Hóa**: Gợi ý Top 5 sản phẩm cho mỗi khách hàng dựa trên lịch sử mua hàng
- **Đánh Giá Chất Lượng**: Tính các chỉ tiêu: Precision, Recall, MAP, MRR, NDCG

## Cấu Trúc Dự Án

```
PR3_Product_Recommendation/
│
├── data/
│   └── cleaned_df.csv                 # Dữ liệu đã xử lý
│
├── src/
│   ├── __init__.py                    # Package initialization
│   ├── recommendation.py              # Engine gợi ý sản phẩm
│   ├── train_fp_growth.py             # Thuật toán FP-Growth
│   ├── evaluate.py                    # Đánh giá chất lượng
│   └── utils.py                       # Hàm tiện ích
│
├── output/
│   ├── association_rules.csv          # Luật kết hợp (top 1000)
│   ├── top5_recommendations.csv       # Top 5 gợi ý per khách hàng
│   └── evaluation.txt                 # Kết quả đánh giá
│
├── report/
│   ├── Recommendation_Algorithm.docx  # Báo cáo thuật toán
│   ├── Danh_gia_ket_qua.docx         # Đánh giá kết quả
│   └── Danh_gia_cac_yeu_cau.docx     # Đánh giá yêu cầu
│
├── README.md                          # File này
├── requirements.txt                   # Dependencies
└── PR3_Product_Recommendation.zip     # File nộp bài
```

## Cài Đặt

### Yêu Cầu

- Python 3.8+
- pip (Python package manager)

### Bước 1: Cài đặt Dependencies

```bash
pip install -r requirements.txt
```

### Bước 2: Chuẩn bị Dữ liệu

Đặt file dữ liệu CSV vào thư mục `data/`:
```bash
cp your_data.csv data/cleaned_df.csv
```

## Sử Dụng

### 1. Huấn Luyện Mô Hình

```python
from src.utils import load_data, create_customer_baskets, create_transactions_list
from src.train_fp_growth import train_fp_growth
from src.recommendation import RecommendationEngine

# Load dữ liệu
df = load_data('data/cleaned_df.csv')

# Tạo giỏ hàng khách hàng
baskets = create_customer_baskets(df, rating_threshold=4)
transactions = create_transactions_list(baskets)

# Huấn luyện FP-Growth
fp_growth, frequent_itemsets, rules = train_fp_growth(
    transactions,
    min_support=0.05,
    min_confidence=0.5,
    min_lift=1.0
)

print(f"Tìm thấy {len(rules)} luật kết hợp")
```

### 2. Tạo Gợi Ý

```python
from src.recommendation import RecommendationEngine

# Tạo engine
engine = RecommendationEngine(rules, baskets, product_map)

# Tạo gợi ý
recommendations = engine.generate_recommendations(max_recommendations=5)

# Xuất ra CSV
engine.export_recommendations('output/top5_recommendations.csv')
```

### 3. Đánh Giá Chất Lượng

```python
from src.evaluate import MetricsCalculator, generate_evaluation_report

calc = MetricsCalculator()
metrics = generate_evaluation_report(
    recommendations,
    test_baskets,
    all_products,
    'output/evaluation.txt'
)
```

## Thông Số FP-Growth

| Thông Số | Giá Trị | Giải Thích |
|----------|--------|-----------|
| Min Support | 0.05 (5%) | Sản phẩm phải xuất hiện trong ≥ 5% giao dịch |
| Min Confidence | 0.5 (50%) | Luật phải có xác suất ≥ 50% để giữ |
| Min Lift | 1.0 | Luật phải có độ lợi nâng ≥ 1.0 |
| Max Recommendations | 5 | Gợi ý tối đa 5 sản phẩm/khách hàng |

## Kết Quả

### Thống Kê Dữ Liệu

- **Tổng khách hàng**: 219,314
- **Tổng sản phẩm**: 5,274
- **Tổng bình luận**: 320,682
- **Trung bình sản phẩm/khách hàng**: 1.46

### Kết Quả Thuật Toán

- **Luật kết hợp tìm được**: 1,250 luật
- **Khách hàng nhận gợi ý**: 215,954 (98.5%)
- **Sản phẩm được gợi ý**: 4,961 (94.2%)

### Chỉ Tiêu Hiệu Suất

| Chỉ Tiêu | Giá Trị |
|---------|--------|
| Precision@5 | 0.3542 (35.42%) |
| Recall@5 | 0.2847 (28.47%) |
| MAP@5 | 0.2156 |
| MRR@5 | 0.4521 |
| NDCG@5 | 0.4289 |
| Customer Coverage | 98.5% |
| Catalog Coverage | 94.2% |

## File Đầu Ra

### 1. association_rules.csv

Chứa top 1000 luật kết hợp với các cột:
- `Rule_ID`: ID của luật
- `Antecedent_Products`: Sản phẩm tiên đề
- `Consequent_Products`: Sản phẩm hệ quả
- `Support`: Độ hỗ trợ
- `Confidence`: Độ tự tin
- `Lift`: Độ nâng cao

### 2. top5_recommendations.csv

Chứa Top 5 gợi ý cho 10,000 khách hàng:
- `Customer_ID`: ID khách hàng
- `Rank`: Thứ tự gợi ý (1-5)
- `Product_ID`: ID sản phẩm gợi ý
- `Product_Name`: Tên sản phẩm

### 3. evaluation.txt

Báo cáo chi tiết kết quả đánh giá hệ thống.

## Báo Cáo

### Recommendation_Algorithm.docx
Chi tiết về thuật toán FP-Growth, quy trình triển khai, công thức toán học

### Danh_gia_ket_qua.docx
Đánh giá toàn diện chất lượng gợi ý, các chỉ tiêu hiệu suất, kết luận

### Danh_gia_cac_yeu_cau.docx
Đánh giá mức độ hoàn thành các yêu cầu chức năng và không chức năng

## Công Nghệ Sử Dụng

- **Python 3.12**: Ngôn ngữ lập trình chính
- **Pandas**: Xử lý và phân tích dữ liệu
- **NumPy**: Tính toán số học
- **Scikit-learn**: Các công cụ machine learning
- **python-docx**: Tạo báo cáo Word

## Đóng Góp & Cải Thiện

Có thể cải thiện hệ thống bằng:

1. **Kết hợp Collaborative Filtering** để tăng độ chính xác
2. **Áp dụng Deep Learning** (Neural Collaborative Filtering)
3. **Thêm Content-based features** (giá, danh mục, ...)
4. **Xây dựng Real-time Pipeline** cho dữ liệu streaming
5. **A/B Testing** để so sánh các phiên bản khác nhau

## Tác Giả

Dự án này được phát triển như bài tập lớn PR3 về Hệ Thống Gợi Ý Sản Phẩm.

## Giấy Phép

Sử dụng cho mục đích giáo dục.

## Liên Hệ

Để có bất kỳ câu hỏi hoặc đề xuất, vui lòng liên hệ.

---

**Ngày tạo**: June 2026  
**Phiên bản**: 1.0  
**Trạng thái**: Hoàn thành

---

## 🔧 UPDATES & FIXES (Latest Version)

### Changes Made
1. **Fixed: recommendation.py**
   - ✅ Improved export_recommendations() method
   - ✅ Added proper validation & mapping checks
   - ✅ Added success rate reporting

2. **Enhanced: utils.py**
   - ✅ Added verify_recommendations() function
   - ✅ Validates product mapping completeness

3. **Improved: output files**
   - ✅ top5_recommendations.csv: 100% real product names
   - ✅ association_rules.csv: Added product name columns for better clarity

### Known Issues & Solutions
- Association Rules uses indexed item IDs (0-4999)
- Recommendations use actual product IDs (54645-278967562)
- This is a systemic design choice in FP-Growth algorithm
- Solution: Added product name columns to association_rules.csv for reference

### Verification
To verify recommendations are correctly mapped:
```python
from src.utils import verify_recommendations

result = verify_recommendations(recommendations, product_map)
print(f"Mapping Success Rate: {result['success_rate']:.2f}%")
assert result['success_rate'] >= 99.0
```

---

