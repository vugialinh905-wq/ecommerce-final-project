# Schema dữ liệu sau khi làm sạch

## Đường dẫn
output/cleaned_retail/
## Các cột dữ liệu

| Tên cột | Kiểu dữ liệu | Mô tả |
|---|---|---|
| InvoiceNo | String | Mã hóa đơn (dùng để nhóm sản phẩm mua cùng đơn) |
| StockCode | String | Mã sản phẩm |
| Description | String | Tên sản phẩm |
| Quantity | Integer | Số lượng mua |
| InvoiceDate | String | Ngày giờ mua, định dạng M/d/yyyy H:mm |
| UnitPrice | Double | Đơn giá |
| CustomerID | Double | Mã khách hàng |
| Country | String | Quốc gia |
| Revenue | Double | Quantity x UnitPrice |

## Thống kê tổng quan

- Tổng số dòng sau khi làm sạch: 397,884
- Số khách hàng duy nhất: 4,338
- Số sản phẩm duy nhất: 3,665
- Số hóa đơn duy nhất: 18,532
- Thời gian dữ liệu: 12/2010 đến 12/2011

## Lưu ý quan trọng

- Cột InvoiceDate là dạng String, cần convert sang timestamp bằng:
```python
to_timestamp("InvoiceDate", "M/d/yyyy H:mm")
```
- Đường dẫn đọc trong Spark cần thêm /*.csv:
```python
spark.read.csv("output/cleaned_retail/*.csv", header=True, inferSchema=True)
```

## Cách đọc dữ liệu (dùng cho Data Analyst và ML Engineer)

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("YourTask").master("local[*]").getOrCreate()

df = spark.read.csv(
    "output/cleaned_retail/*.csv",
    header=True,
    inferSchema=True
)
```
