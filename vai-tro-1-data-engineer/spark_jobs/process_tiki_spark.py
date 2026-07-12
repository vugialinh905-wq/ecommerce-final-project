from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

import argparse
import re


NORMALIZE_MAP = {
    "k": "không",
    "ko": "không",
    "kh": "không",
    "kg": "không",
    "dc": "được",
    "đc": "được",
    "sp": "sản_phẩm",
    "ng": "người",
    "ok": "ổn",
    "shop": "cửa_hàng",
    "mn": "mọi_người",
    "vs": "với",
    "mk": "mình",
    "r": "rồi",
    "khong": "không",
    "oke": "ổn",
    "oki": "ổn",
    "ms": "mới",
    "cx": "cũng",
    "sdt": "số_điện_thoại",
}


def clean_text_expr(col):
    c = F.coalesce(col.cast("string"), F.lit(""))
    c = F.lower(c)

    # Remove URLs
    c = F.regexp_replace(c, r"http\S+|www\S+", " ")

    # Remove mentions and hashtags
    c = F.regexp_replace(c, r"[@#]\w+", " ")

    # Keep unicode letters, numbers, underscores and spaces
    c = F.regexp_replace(c, r"[^\p{L}\p{N}_\s]", " ")

    # Normalize whitespace
    c = F.regexp_replace(c, r"\s+", " ")
    c = F.trim(c)

    # Normalize short words without Python UDF
    for src, dst in NORMALIZE_MAP.items():
        pattern = rf"(?iu)(^|\s){re.escape(src)}(?=\s|$)"
        replacement = rf"$1{dst}"
        c = F.regexp_replace(c, pattern, replacement)

    c = F.regexp_replace(c, r"\s+", " ")
    c = F.trim(c)

    return c


def safe_csv_text(col):
    """
    Remove characters that can break CSV display/parsing in Flask/Pandas.
    This is mainly for summary output columns such as product_name/customer_name.
    """
    c = F.coalesce(col.cast("string"), F.lit(""))
    c = F.regexp_replace(c, r"[\r\n]+", " ")
    c = F.regexp_replace(c, r'"', "'")
    c = F.regexp_replace(c, r",", " ")
    c = F.regexp_replace(c, r"\s+", " ")
    c = F.trim(c)
    return c


def write_csv(df, path):
    (
        df.coalesce(1)
        .write
        .mode("overwrite")
        .option("header", True)
        .option("quote", '"')
        .option("escape", '"')
        .option("quoteAll", True)
        .csv(path)
    )


def main():
    parser = argparse.ArgumentParser()

    parser.add_argument("--products", required=True)
    parser.add_argument("--comments", required=True)
    parser.add_argument("--output", required=True)

    args = parser.parse_args()

    spark = (
        SparkSession.builder
        .appName("Tiki Big Data Processing")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    # =========================
    # 1. Read raw CSV from HDFS
    # =========================
    product_df = (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .option("multiLine", True)
        .option("quote", '"')
        .option("escape", '"')
        .csv(args.products)
    )

    comment_df = (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .option("multiLine", True)
        .option("quote", '"')
        .option("escape", '"')
        .csv(args.comments)
    )

    # =========================
    # 2. Normalize column names
    # =========================
    if "id" in product_df.columns and "product_id" not in product_df.columns:
        product_df = product_df.withColumnRenamed("id", "product_id")

    if "name" in product_df.columns and "product_name" not in product_df.columns:
        product_df = product_df.withColumnRenamed("name", "product_name")

    if "product_name" not in product_df.columns:
        product_df = product_df.withColumn("product_name", F.lit(""))

    if "customer_name" not in comment_df.columns:
        comment_df = comment_df.withColumn("customer_name", F.lit(""))

    # =========================
    # 3. Drop duplicates
    # =========================
    product_df = product_df.dropDuplicates(["product_id"])
    comment_df = comment_df.dropDuplicates(["comment_id"])

    # =========================
    # 4. Join product + comments
    # =========================
    merged_df = comment_df.join(product_df, on="product_id", how="inner")

    # =========================
    # 5. Drop null important fields
    # =========================
    required_cols = [
        "comment_id",
        "product_id",
        "customer_id",
        "rating",
        "content",
        "created_at",
        "purchased_at",
    ]

    existing_required_cols = [c for c in required_cols if c in merged_df.columns]
    merged_df = merged_df.dropna(subset=existing_required_cols)

    # =========================
    # 6. Convert types
    # =========================
    merged_df = (
        merged_df
        .withColumn("created_at_num", F.col("created_at").cast("long"))
        .withColumn("purchased_at_num", F.col("purchased_at").cast("long"))
        .withColumn("created_at_ts", F.from_unixtime("created_at_num").cast("timestamp"))
        .withColumn("purchased_at_ts", F.from_unixtime("purchased_at_num").cast("timestamp"))
        .withColumn("rating", F.col("rating").cast("double"))
    )

    # Remove invalid timestamp rows
    merged_df = merged_df.dropna(subset=["created_at_ts", "purchased_at_ts", "rating"])

    # =========================
    # 7. Clean text with Spark native functions
    # =========================
    merged_df = merged_df.withColumn("content_text", clean_text_expr(F.col("content")))

    # Safe text for output
    merged_df = (
        merged_df
        .withColumn("product_name", safe_csv_text(F.col("product_name")))
        .withColumn("customer_name", safe_csv_text(F.col("customer_name")))
        .withColumn("content", safe_csv_text(F.col("content")))
        .withColumn("title", safe_csv_text(F.col("title")) if "title" in merged_df.columns else F.lit(""))
    )

    # =========================
    # 8. F3 flag: burst comments
    # =========================
    user_time_window = Window.partitionBy("customer_id").orderBy("created_at_ts")

    merged_df = (
        merged_df
        .withColumn("previous_created_at_ts", F.lag("created_at_ts").over(user_time_window))
        .withColumn(
            "delta_minutes",
            (
                F.col("created_at_ts").cast("long")
                - F.col("previous_created_at_ts").cast("long")
            ) / 60.0
        )
    )

    valid_delta_df = merged_df.filter(F.col("delta_minutes") > 0)

    if valid_delta_df.count() > 0:
        threshold_burst = valid_delta_df.approxQuantile("delta_minutes", [0.05], 0.01)[0]
    else:
        threshold_burst = 0.0

    merged_df = merged_df.withColumn(
        "small_gap",
        (F.col("delta_minutes") > 0)
        & (F.col("delta_minutes") <= F.lit(threshold_burst))
    )

    reset_col = F.when(
        (F.col("small_gap") == False) | F.col("small_gap").isNull(),
        1
    ).otherwise(0)

    merged_df = merged_df.withColumn(
        "burst_group",
        F.sum(reset_col).over(
            user_time_window.rowsBetween(Window.unboundedPreceding, 0)
        )
    )

    burst_window = (
        Window
        .partitionBy("customer_id", "burst_group")
        .orderBy("created_at_ts")
        .rowsBetween(Window.unboundedPreceding, 0)
    )

    merged_df = merged_df.withColumn(
        "burst_count",
        F.when(
            F.col("small_gap") == True,
            F.sum(F.when(F.col("small_gap") == True, 1).otherwise(0)).over(burst_window)
        ).otherwise(0)
    )

    merged_df = merged_df.withColumn(
        "F3_flag",
        F.col("burst_count") >= 4
    )

    # =========================
    # 9. F5 flag: purchase-review time gap
    # =========================
    merged_df = merged_df.withColumn(
        "time_gap_minutes",
        (
            F.col("created_at_ts").cast("long")
            - F.col("purchased_at_ts").cast("long")
        ) / 60.0
    )

    valid_gap_df = merged_df.filter(F.col("time_gap_minutes") > 0)

    if valid_gap_df.count() > 0:
        threshold_timegap = valid_gap_df.approxQuantile("time_gap_minutes", [0.05], 0.01)[0]
    else:
        threshold_timegap = 0.0

    merged_df = merged_df.withColumn(
        "F5_flag",
        (F.col("time_gap_minutes") > 0)
        & (F.col("time_gap_minutes") <= F.lit(threshold_timegap))
    )

    # =========================
    # 10. Final spam flag
    # =========================
    merged_df = merged_df.withColumn(
        "is_spam",
        (F.col("F3_flag") == True) | (F.col("F5_flag") == True)
    )

    # =========================
    # 11. Summary tables
    # =========================
    summary_df = merged_df.agg(
        F.count("*").alias("total_comments"),
        F.countDistinct("product_id").alias("total_products"),
        F.countDistinct("customer_id").alias("total_customers"),
        F.sum(F.when(F.col("is_spam") == True, 1).otherwise(0)).alias("spam_comments"),
        F.sum(F.when(F.col("is_spam") == False, 1).otherwise(0)).alias("normal_comments"),
        F.avg("rating").alias("avg_rating"),
    )

    product_summary_df = (
        merged_df
        .groupBy("product_id", "product_name")
        .agg(
            F.count("*").alias("comment_count"),
            F.avg("rating").alias("avg_rating"),
            F.sum(F.when(F.col("is_spam") == True, 1).otherwise(0)).alias("spam_count"),
        )
        .orderBy(F.desc("comment_count"))
    )

    customer_summary_df = (
        merged_df
        .groupBy("customer_id", "customer_name")
        .agg(
            F.count("*").alias("comment_count"),
            F.sum(F.when(F.col("F3_flag") == True, 1).otherwise(0)).alias("F3_count"),
            F.sum(F.when(F.col("F5_flag") == True, 1).otherwise(0)).alias("F5_count"),
            F.sum(F.when(F.col("is_spam") == True, 1).otherwise(0)).alias("spam_count"),
        )
        .orderBy(F.desc("spam_count"), F.desc("comment_count"))
    )

    # =========================
    # 12. Write output to HDFS
    # =========================
    write_csv(merged_df, args.output + "/merged")
    write_csv(summary_df, args.output + "/summary")
    write_csv(product_summary_df, args.output + "/product_summary")
    write_csv(customer_summary_df, args.output + "/customer_summary")

    print("DONE Spark processing")
    print("threshold_burst =", threshold_burst)
    print("threshold_timegap =", threshold_timegap)
    print("Output:", args.output)

    spark.stop()


if __name__ == "__main__":
    main()