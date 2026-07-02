from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, when

spark = SparkSession.builder \
    .appName("EcommerceRecommendation - Data Ingestion") \
    .master("local[*]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

print("=" * 60)
print("   DATA ENGINEER — DATA INGESTION & CLEANING")
print("=" * 60)

# Load Dataset 
print("\n[1] LOADING DATASET...")
df = spark.read.csv(
    "data/OnlineRetail.csv",
    header=True,
    inferSchema=True
)
print(f"    ✓ Total records loaded : {df.count():,}")
print(f"    ✓ Total columns        : {len(df.columns)}")

# Schema 
print("\n[2] DATASET SCHEMA:")
df.printSchema()

# Kiểm tra null 
print("\n[3] NULL VALUES CHECK:")
null_counts = df.select([
    count(when(col(c).isNull(), c)).alias(c)
    for c in df.columns
])
null_counts.show()

# Làm sạch dữ liệu 
print("\n[4] CLEANING DATA...")
df_clean = df.dropna(subset=["CustomerID", "InvoiceDate"])
df_clean = df_clean.filter(col("Quantity") > 0)
df_clean = df_clean.filter(col("UnitPrice") > 0)
df_clean = df_clean.filter(~col("InvoiceNo").startswith("C"))

before = df.count()
after  = df_clean.count()
print(f"    ✓ Before cleaning : {before:,} records")
print(f"    ✓ After cleaning  : {after:,} records")
print(f"    ✓ Removed         : {before - after:,} records")
# Thêm cột Revenue
print("\n[5] ADDING REVENUE COLUMN...")
df_clean = df_clean.withColumn(
    "Revenue",
    col("Quantity") * col("UnitPrice")
)
print("    ✓ Revenue = Quantity x UnitPrice")

# Thống kê tổng quan 
print("\n[6] BASIC STATISTICS:")
df_clean.select("Quantity", "UnitPrice", "Revenue").describe().show()

print("\n[7] TOP 5 COUNTRIES:")
df_clean.groupBy("Country").count().orderBy("count", ascending=False).show(5)

print("\n[8] UNIQUE CUSTOMERS & PRODUCTS:")
print(f"    ✓ Unique customers : {df_clean.select('CustomerID').distinct().count():,}")
print(f"    ✓ Unique products  : {df_clean.select('StockCode').distinct().count():,}")
print(f"    ✓ Unique invoices  : {df_clean.select('InvoiceNo').distinct().count():,}")

# Lưu dữ liệu đã làm sạch
print("\n[9] SAVING CLEANED DATA...")
df_clean.write \
    .mode("overwrite") \
    .option("header", True) \
    .csv("output/cleaned_retail")
print("    ✓ Saved to output/cleaned_retail/")

print("\n" + "=" * 60)
print("   DATA INGESTION COMPLETED SUCCESSFULLY!")
print("   Ready for Data Analyst & ML Engineer")
print("=" * 60)

spark.stop()
