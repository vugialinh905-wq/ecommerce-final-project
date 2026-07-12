import os
os.environ["HADOOP_HOME"] = r"C:\rfm\hadoop-3.3.6"
os.environ["PATH"] = os.environ["HADOOP_HOME"] + r"\bin;" + os.environ["PATH"]
os.environ["PYSPARK_PYTHON"] = "python"

import warnings
warnings.filterwarnings("ignore")

import argparse
import glob
import subprocess
import sys
import pandas as pd
import numpy as np
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import TimestampType, DoubleType

# ============================================================
# CAU HINH DUONG DAN HDFS - chi can sua o day
# ============================================================
# Namenode cua Hadoop cai tren may (mac dinh chay local, port 9000)
HDFS_NAMENODE = "hdfs://localhost:9000"

# Du lieu dau vao: doc tu output "merged" ma Spark job process_tiki_spark.py
# da ghi len HDFS (xem spark_jobs/process_tiki_spark.py va README phan 1).
#   - Chay ban chinh thuc      -> /ecommerce/output/merged
#   - Chay demo bang temp_data -> /ecommerce/output_temp/merged
INPUT_HDFS_PATH = HDFS_NAMENODE + "/ecommerce/output/merged"

# Ket qua RFM se duoc day len thu muc nay tren HDFS
OUTPUT_HDFS_DIR = "/ecommerce/output_rfm"

# Pandas.to_csv() va matplotlib khong ghi truc tiep len HDFS duoc, nen script
# se ghi tam ra thu muc local nay truoc, sau do dung lenh "hdfs dfs -put" de
# day toan bo len HDFS (giong cach lam cua scripts/upload_to_hdfs.bat trong
# phan 1 cua project).
LOCAL_TEMP_DIR = "output_rfm_temp/"
# ============================================================

# Cho phep doi input/output tu dong lenh de khong phai sua code moi lan chay
# demo (temp_data) hay chay chinh thuc (data). Neu khong truyen gi thi dung
# dung mac dinh o tren.
parser = argparse.ArgumentParser(description="Phan tich RFM khach hang Tiki (doc/ghi HDFS)")
parser.add_argument("--input", default=INPUT_HDFS_PATH,
                     help="Duong dan HDFS toi thu muc merged (mac dinh: %(default)s)")
parser.add_argument("--output-hdfs", default=OUTPUT_HDFS_DIR,
                     help="Thu muc HDFS de luu ket qua RFM (mac dinh: %(default)s)")
parser.add_argument("--local-temp", default=LOCAL_TEMP_DIR,
                     help="Thu muc local tam de ghi file truoc khi day len HDFS")
args, _ = parser.parse_known_args()

INPUT_HDFS_PATH = args.input
OUTPUT_HDFS_DIR = args.output_hdfs
OUTPUT_DIR = args.local_temp if args.local_temp.endswith(("/", "\\")) else args.local_temp + "/"

os.makedirs(OUTPUT_DIR, exist_ok=True)


# ============================================================
# BUOC 1: KHOI DONG SPARK VA DOC DU LIEU TU HDFS
# ============================================================
print("Buoc 1: Khoi dong Spark va doc du lieu tu HDFS...")

spark = (SparkSession.builder
         .appName("PhanTichRFM")
         .config("spark.driver.memory", "2g")
         .config("spark.sql.shuffle.partitions", "8")
         .getOrCreate())
spark.sparkContext.setLogLevel("ERROR")

print(f"  Dang doc du lieu tu HDFS: {INPUT_HDFS_PATH}")

# Doc du lieu merged truc tiep tu HDFS bang Spark (thay vi pandas.read_csv
# tu file local nhu truoc). Output cua process_tiki_spark.py duoc ghi bang
# coalesce(1) + quoteAll nen day la 1 thu muc chua part-*.csv, Spark doc
# thang duoc ca thu muc nay.
raw_spark_df = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .option("multiLine", True)
    .option("quote", '"')
    .option("escape", '"')
    .csv(INPUT_HDFS_PATH)
)

# Chuyen sang pandas de xu ly cot flag (True/False kieu bool) giong code cu,
# khong doi logic xu ly ben duoi.
raw_pd = raw_spark_df.toPandas()

# Chi lay cac ban ghi khong bi gian lan (F3, F4, F5 deu False)
# Luu y: tuy ban Spark job xu ly du lieu (vai tro khac trong nhom) co the
# chua tinh du ca 3 cot flag (vi du F4_Flag - phat hien trung lap bang do
# tuong dong noi dung). Code duoi day chi loc theo cot nao THUC SU CO trong
# du lieu, cot nao thieu thi bo qua, tranh loi KeyError.
cac_cot_flag_can_loc = ["F3_flag", "F4_Flag", "F5_flag"]
cac_cot_flag_co_san = [c for c in cac_cot_flag_can_loc if c in raw_pd.columns]
cac_cot_flag_bi_thieu = [c for c in cac_cot_flag_can_loc if c not in raw_pd.columns]

if cac_cot_flag_bi_thieu:
    print(f"  CANH BAO: thieu cot {cac_cot_flag_bi_thieu} trong du lieu dau vao,"
          f" bo qua cac cot nay khi loc gian lan.")
if cac_cot_flag_co_san:
    dieu_kien_sach = pd.Series(True, index=raw_pd.index)
    for cot in cac_cot_flag_co_san:
        # fillna(False): coi thieu du lieu la KHONG gian lan, tranh loai nham
        flag_col = raw_pd[cot].fillna(False).astype(bool)
        dieu_kien_sach &= (~flag_col)
    sach_pd = raw_pd[dieu_kien_sach].copy()
    print(f"  So ban ghi con lai sau loc: {len(sach_pd):,}")
else:
    print("  CANH BAO: khong co cot flag nao de loc, dung toan bo du lieu.")
    sach_pd = raw_pd.copy()

if sach_pd.empty:
    print("LOI: DataFrame rong sau khi loc, dung chuong trinh de kiem tra lai dieu kien loc.")
    sys.exit(1)

# Chuyen sang dang ngay thang
print("Kieu du lieu goc cua purchased_at:", sach_pd["purchased_at"].dtype)
print("5 gia tri mau purchased_at truoc convert:")
print(sach_pd["purchased_at"].head(5).tolist())
print("So gia tri null truoc convert:", sach_pd["purchased_at"].isna().sum())

sach_pd["purchased_at"] = pd.to_datetime(sach_pd["purchased_at"], errors="coerce", utc=True, unit="s")
sach_pd["created_at"]   = pd.to_datetime(sach_pd["created_at"],   errors="coerce", utc=True, unit="s")

# Bo thong tin mui gio (chuyen ve dang "naive"). Neu giu mui gio, PySpark
# tren Windows se dung ham mktime cua he thong de chuyen doi, ham nay hay
# bi loi OverflowError voi mot so gia tri ngay thang trong du lieu.
sach_pd["purchased_at"] = sach_pd["purchased_at"].dt.tz_convert(None)
sach_pd["created_at"]   = sach_pd["created_at"].dt.tz_convert(None)

sach_pd["rating"]       = pd.to_numeric(sach_pd["rating"],         errors="coerce")
sach_pd = sach_pd.dropna(subset=["customer_id", "purchased_at"])

# Loai bo cac ban ghi co ngay thang bat thuong (loi nhap lieu, vd nam 1900
# hay nam 9999) de tranh OverflowError khi PySpark chuyen doi datetime
# tren Windows.
gioi_han_duoi = pd.Timestamp("2000-01-01")
gioi_han_tren = pd.Timestamp("2035-12-31")
so_dong_truoc_loc = len(sach_pd)
sach_pd = sach_pd[
    sach_pd["purchased_at"].between(gioi_han_duoi, gioi_han_tren) &
    (sach_pd["created_at"].isna() | sach_pd["created_at"].between(gioi_han_duoi, gioi_han_tren))
].copy()
so_dong_bi_loai = so_dong_truoc_loc - len(sach_pd)
if so_dong_bi_loai > 0:
    print(f"  Da loai {so_dong_bi_loai:,} ban ghi co ngay thang bat thuong (ngoai khoang 2000-2035)")

print(f"  Tong so ban ghi sau loc spam: {len(sach_pd):,}")
print(f"  Khoang thoi gian: {sach_pd['purchased_at'].min().date()} den {sach_pd['purchased_at'].max().date()}")

# Dua len Spark de xu ly phan tan
spark_df = spark.createDataFrame(
    sach_pd[["customer_id","customer_name","comment_id",
             "purchased_at","created_at","rating",
             "product_id","product_name","seller_name"]]
)

print(f"  Da nap len Spark thanh cong: {spark_df.count():,} dong")


# ============================================================
# BUOC 2: TINH 3 CHI SO RFM
# ============================================================
print("\nBuoc 2: Tinh chi so RFM...")

# Ngay tham chieu = ngay mua muon nhat trong dataset + 1 ngay
ngay_lon_nhat = sach_pd["purchased_at"].max()
ngay_tham_chieu = ngay_lon_nhat + pd.Timedelta(days=1)
ref_ts = F.lit(str(ngay_tham_chieu)).cast(TimestampType())  # FIX: chuyen sang str truoc khi truyen vao F.lit()

print(f"  Ngay tham chieu: {ngay_tham_chieu.date()}")

# Tinh RFM
# - Recency  : so ngay tu lan mua gan nhat den ngay tham chieu (cang nho cang tot)
# - Frequency: tong so don hang (so comment_id)
# - Monetary : vi du lieu khong co gia -> dung so lan mua x diem rating trung binh
#              day la cach tiep can pho bien khi khong co du lieu gia
rfm = (spark_df
       .groupBy("customer_id", "customer_name")
       .agg(
           F.datediff(ref_ts, F.max("purchased_at")).alias("recency"),
           F.countDistinct("comment_id").alias("frequency"),
           F.round(
               F.countDistinct("comment_id") * F.coalesce(F.avg("rating"), F.lit(0.0)), 2
           ).alias("monetary")
       )
      )

print(f"  So khach hang: {rfm.count():,}")
print("  5 hang dau:")
rfm.show(5, truncate=False)


# ============================================================
# BUOC 3: PHAN KHUC KHACH HANG
# ============================================================
print("\nBuoc 3: Phan khuc khach hang theo RFM...")

# Dung tu phan vi (quantile) de chia diem 1-4 cho moi chieu
# Loc bo null truoc khi tinh quantile (tranh loi khi co gia tri null)
rfm_clean = rfm.filter(
    F.col("recency").isNotNull() &
    F.col("frequency").isNotNull() &
    F.col("monetary").isNotNull()
)

r_q = rfm_clean.approxQuantile("recency",   [0.25, 0.5, 0.75], 0.01)
f_q = rfm_clean.approxQuantile("frequency", [0.25, 0.5, 0.75], 0.01)
m_q = rfm_clean.approxQuantile("monetary",  [0.25, 0.5, 0.75], 0.01)

print(f"  Recency  Q25/50/75 : {[round(x,0) for x in r_q]}")
print(f"  Frequency Q25/50/75: {[round(x,1) for x in f_q]}")
print(f"  Monetary  Q25/50/75: {[round(x,1) for x in m_q]}")

# Ham cho diem: Recency -> diem cao = mua gan day (nguoc lai)
def diem_recency(col):
    return (F.when(col <= r_q[0], 4)
             .when(col <= r_q[1], 3)
             .when(col <= r_q[2], 2)
             .otherwise(1))

# Frequency va Monetary -> diem cao = mua nhieu / chi nhieu
def diem_frequency(col):
    return (F.when(col >= f_q[2], 4)
             .when(col >= f_q[1], 3)
             .when(col >= f_q[0], 2)
             .otherwise(1))

def diem_monetary(col):
    return (F.when(col >= m_q[2], 4)
             .when(col >= m_q[1], 3)
             .when(col >= m_q[0], 2)
             .otherwise(1))

rfm_diem = (rfm
    .withColumn("R", diem_recency(F.col("recency")))
    .withColumn("F", diem_frequency(F.col("frequency")))
    .withColumn("M", diem_monetary(F.col("monetary")))
    .withColumn("tong_diem", F.col("R") + F.col("F") + F.col("M"))
)

# Quy tac phan khuc
phan_khuc = (
    F.when((F.col("R") >= 3) & (F.col("F") >= 3) & (F.col("M") >= 3),
           "VIP")
     .when((F.col("R") >= 3) & (F.col("F") >= 2),
           "Khach trung thanh")
     .when((F.col("R") >= 3) & (F.col("F") == 1),
           "Khach moi")
     .when((F.col("R") == 2) & (F.col("F") >= 2),
           "Co nguy co roi bo")
     .when((F.col("R") <= 1) & (F.col("F") >= 2),
           "Can kich hoat lai")
     .otherwise("Khach tieu thu thap")
)

rfm_final = rfm_diem.withColumn("phan_khuc", phan_khuc)

# Tong hop theo phan khuc
tong_hop = (rfm_final
    .groupBy("phan_khuc")
    .agg(
        F.count("customer_id").alias("so_khach"),
        F.round(F.avg("recency"), 1).alias("recency_tb"),
        F.round(F.avg("frequency"), 2).alias("frequency_tb"),
        F.round(F.avg("monetary"), 2).alias("monetary_tb"),
        F.round(F.avg("tong_diem"), 2).alias("diem_rfm_tb")
    )
    .orderBy(F.desc("diem_rfm_tb"))
)

print("  Ket qua phan khuc:")
tong_hop.show(truncate=False)


# ============================================================
# BUOC 4: XU HUONG MUA SAM
# ============================================================
print("\nBuoc 4: Phan tich xu huong mua sam...")

# 4a. Theo thang
xu_huong_thang = (spark_df
    .withColumn("thang", F.date_format("purchased_at", "yyyy-MM"))
    .groupBy("thang")
    .agg(
        F.countDistinct("comment_id").alias("so_don"),
        F.countDistinct("customer_id").alias("so_khach"),
        F.round(F.avg("rating"), 2).alias("diem_tb")
    )
    .orderBy("thang")
)

# 4b. Theo ngay trong tuan
thu_trong_tuan = {1: "CN", 2: "T2", 3: "T3", 4: "T4", 5: "T5", 6: "T6", 7: "T7"}
xu_huong_tuan = (spark_df
    .withColumn("thu_so", F.dayofweek("purchased_at"))
    .groupBy("thu_so")
    .agg(
        F.countDistinct("comment_id").alias("so_don"),
        F.round(F.avg("rating"), 2).alias("diem_tb")
    )
    .orderBy("thu_so")
)

# 4c. Top nguoi ban
top_seller = (spark_df
    .groupBy("seller_name")
    .agg(F.countDistinct("comment_id").alias("so_don"))
    .orderBy(F.desc("so_don"))
    .limit(10)
)

print("  Xu huong theo thang (5 thang gan nhat):")
xu_huong_thang.orderBy(F.desc("thang")).show(5)
print("  Xu huong theo ngay trong tuan:")
xu_huong_tuan.show()


# ============================================================
# BUOC 5: XUAT KET QUA RA FILE
# ============================================================
print("\nBuoc 5: Xuat ket qua ra file...")

rfm_pd        = rfm_final.toPandas()
tong_hop_pd   = tong_hop.toPandas()
thang_pd      = xu_huong_thang.toPandas()
tuan_pd       = xu_huong_tuan.toPandas()
tuan_pd["ten_thu"] = tuan_pd["thu_so"].map(thu_trong_tuan)
seller_pd     = top_seller.toPandas().sort_values("so_don", ascending=False).reset_index(drop=True)

rfm_pd.to_csv(OUTPUT_DIR + "danh_sach_khach_hang.csv", index=False, encoding="utf-8-sig")
tong_hop_pd.to_csv(OUTPUT_DIR + "tong_hop_phan_khuc.csv", index=False, encoding="utf-8-sig")
thang_pd.to_csv(OUTPUT_DIR + "xu_huong_theo_thang.csv", index=False, encoding="utf-8-sig")
tuan_pd.to_csv(OUTPUT_DIR + "xu_huong_theo_tuan.csv", index=False, encoding="utf-8-sig")
seller_pd.to_csv(OUTPUT_DIR + "top_seller.csv", index=False, encoding="utf-8-sig")

print("  Da xuat:")
for f in ["danh_sach_khach_hang.csv","tong_hop_phan_khuc.csv",
          "xu_huong_theo_thang.csv","xu_huong_theo_tuan.csv","top_seller.csv"]:
    kb = os.path.getsize(OUTPUT_DIR + f) / 1024
    print(f"    {f} ({kb:.1f} KB)")


# ============================================================
# BUOC 6: VE BIEU DO
# ============================================================
print("\nBuoc 6: Ve bieu do...")

mau_nen    = "#0F1117"
mau_card   = "#1A1D27"
mau_chu    = "#E8EAF0"
mau_mo     = "#6B7280"
mau_xanh   = "#4F8EF7"
mau_cam    = "#F7764F"
mau_xanhla = "#4FF7B0"
mau_vang   = "#F7D34F"
mau_tim    = "#C44FF7"

mau_phan_khuc = {
    "VIP":                "#F7D34F",
    "Khach trung thanh":  "#4F8EF7",
    "Khach moi":          "#4FF7B0",
    "Co nguy co roi bo":  "#F7764F",
    "Can kich hoat lai":  "#C44FF7",
    "Khach tieu thu thap":"#6B7280",
}

plt.rcParams.update({
    "figure.facecolor":  mau_nen,
    "axes.facecolor":    mau_card,
    "axes.edgecolor":    mau_mo,
    "axes.labelcolor":   mau_chu,
    "xtick.color":       mau_mo,
    "ytick.color":       mau_mo,
    "text.color":        mau_chu,
    "grid.color":        "#2A2D3A",
    "grid.linewidth":    0.5,
    "font.size":         11,
})


# Bieu do 1: Tong quan 4 panel
fig, axes = plt.subplots(2, 2, figsize=(18, 12), facecolor=mau_nen)
fig.suptitle("Phan tich RFM Khach Hang - Tiki", fontsize=18, fontweight="bold",
             color=mau_chu, y=0.98)

# Panel 1: Bieu do tron phan khuc
ax1 = axes[0, 0]
so_luong = rfm_pd["phan_khuc"].value_counts()
mau_list = [mau_phan_khuc.get(s, "#888") for s in so_luong.index]
wedges, texts, autos = ax1.pie(
    so_luong.values,
    colors=mau_list,
    autopct="%1.1f%%",
    startangle=90,
    pctdistance=0.78,
    wedgeprops=dict(width=0.52, edgecolor=mau_nen, linewidth=1.5)
)
for a in autos:
    a.set_fontsize(9)
    a.set_color(mau_nen)
    a.set_fontweight("bold")
ax1.set_title("Phan khuc khach hang", fontsize=13, pad=10, color=mau_chu)
ax1.legend(so_luong.index, loc="lower center", bbox_to_anchor=(0.5, -0.1),
           ncol=2, fontsize=8, frameon=False)

# Panel 2: Don hang theo thang (12 thang gan nhat)
ax2 = axes[0, 1]
thang_gan = thang_pd.tail(12).copy()
x = range(len(thang_gan))
ax2.fill_between(x, thang_gan["so_don"], alpha=0.2, color=mau_xanh)
ax2.plot(x, thang_gan["so_don"], color=mau_xanh, lw=2, marker="o", markersize=4)
ax2.set_xticks(list(x))
ax2.set_xticklabels(thang_gan["thang"].tolist(), rotation=35, ha="right", fontsize=8)
ax2.yaxis.set_major_formatter(mticker.FuncFormatter(lambda v, _: f"{int(v):,}"))
ax2.set_title("So don hang theo thang (12 thang gan nhat)", fontsize=13, pad=10)
ax2.set_ylabel("So don")
ax2.grid(axis="y")

# Panel 3: Don hang theo thu trong tuan
ax3 = axes[1, 0]
thu_order = ["T2","T3","T4","T5","T6","T7","CN"]
tuan_sort = tuan_pd.set_index("ten_thu").reindex(thu_order).reset_index()
mau_bars = [mau_vang if t in ["T7","CN"] else mau_xanhla for t in tuan_sort["ten_thu"]]
bars = ax3.bar(tuan_sort["ten_thu"], tuan_sort["so_don"],
               color=mau_bars, edgecolor=mau_nen, linewidth=0.8, width=0.6)
ax3.set_title("Don hang theo ngay trong tuan", fontsize=13, pad=10)
ax3.set_ylabel("So don")
ax3.yaxis.set_major_formatter(mticker.FuncFormatter(lambda v, _: f"{int(v):,}"))
ax3.grid(axis="y")
for bar in bars:
    ax3.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 100,
             f"{int(bar.get_height()):,}", ha="center", va="bottom", fontsize=8, color=mau_chu)

# Panel 4: Top 10 seller
ax4 = axes[1, 1]
seller_sort = seller_pd.sort_values("so_don")
ax4.barh(seller_sort["seller_name"], seller_sort["so_don"],
         color=mau_tim, edgecolor=mau_nen, height=0.6)
ax4.set_title("Top 10 nguoi ban", fontsize=13, pad=10)
ax4.set_xlabel("So don")
ax4.xaxis.set_major_formatter(mticker.FuncFormatter(lambda v, _: f"{int(v):,}"))
ax4.grid(axis="x")

plt.tight_layout(rect=[0, 0, 1, 0.96])
plt.savefig(OUTPUT_DIR + "bieu_do_tong_quan.png", dpi=150, bbox_inches="tight",
            facecolor=mau_nen)
plt.close()
print("  Da luu: bieu_do_tong_quan.png")


# Bieu do 2: Scatter Recency vs Frequency
fig2, ax = plt.subplots(figsize=(11, 7), facecolor=mau_nen)
ax.set_facecolor(mau_card)
for pk, nhom in rfm_pd.groupby("phan_khuc"):
    mau = mau_phan_khuc.get(pk, "#888")
    ax.scatter(nhom["recency"], nhom["frequency"],
               c=mau, s=18, alpha=0.45, label=f"{pk} (n={len(nhom):,})", edgecolors="none")
ax.set_xlabel("Recency (so ngay ke tu lan mua gan nhat)", fontsize=12)
ax.set_ylabel("Frequency (so don hang)", fontsize=12)
ax.set_title("Bieu do phan tan: Recency vs Frequency theo phan khuc", fontsize=14, pad=12)
ax.legend(loc="upper right", frameon=False, fontsize=9)
ax.grid(True, alpha=0.25)
plt.tight_layout()
plt.savefig(OUTPUT_DIR + "bieu_do_scatter.png", dpi=150, bbox_inches="tight",
            facecolor=mau_nen)
plt.close()
print("  Da luu: bieu_do_scatter.png")


# Bieu do 3: Phan phoi diem RFM va diem rating theo thang
fig3, (ax_a, ax_b) = plt.subplots(1, 2, figsize=(14, 5), facecolor=mau_nen)
for a in (ax_a, ax_b):
    a.set_facecolor(mau_card)

# Boxplot diem RFM theo phan khuc
thu_tu = ["VIP","Khach trung thanh","Khach moi","Co nguy co roi bo",
          "Can kich hoat lai","Khach tieu thu thap"]
du_lieu_box = [rfm_pd[rfm_pd["phan_khuc"]==pk]["tong_diem"].dropna().values
               for pk in thu_tu]
bp = ax_a.boxplot(du_lieu_box, patch_artist=True, widths=0.5, showfliers=False,
                  medianprops=dict(color=mau_nen, linewidth=2))
for patch, pk in zip(bp["boxes"], thu_tu):
    patch.set_facecolor(mau_phan_khuc.get(pk, "#888"))
    patch.set_alpha(0.8)
ax_a.set_xticks(range(1, len(thu_tu)+1))
ax_a.set_xticklabels([pk.replace(" ","\n") for pk in thu_tu], fontsize=7)
ax_a.set_ylabel("Tong diem RFM (3-12)")
ax_a.set_title("Phan phoi diem RFM theo phan khuc", fontsize=12, pad=10)
ax_a.grid(axis="y", alpha=0.3)

# Diem rating trung binh theo thang (18 thang gan nhat)
thang_gan2 = thang_pd.tail(18).copy()
ax_b.plot(range(len(thang_gan2)), thang_gan2["diem_tb"],
          color=mau_cam, lw=2, marker="s", markersize=5)
ax_b.fill_between(range(len(thang_gan2)), thang_gan2["diem_tb"], alpha=0.15, color=mau_cam)
ax_b.set_xticks(range(len(thang_gan2)))
ax_b.set_xticklabels(thang_gan2["thang"].tolist(), rotation=40, ha="right", fontsize=8)
ax_b.set_ylim(3.5, 5.2)
ax_b.set_ylabel("Diem rating trung binh")
ax_b.set_title("Rating trung binh theo thang (18 thang gan nhat)", fontsize=12, pad=10)
tb_rating = thang_gan2["diem_tb"].mean()
ax_b.axhline(tb_rating, color=mau_mo, linestyle="--", lw=1,
             label=f"Trung binh: {tb_rating:.2f}")
ax_b.legend(frameon=False, fontsize=9)
ax_b.grid(axis="y", alpha=0.3)

plt.tight_layout()
plt.savefig(OUTPUT_DIR + "bieu_do_chi_tiet.png", dpi=150, bbox_inches="tight",
            facecolor=mau_nen)
plt.close()
print("  Da luu: bieu_do_chi_tiet.png")


# ============================================================
# BUOC 7: IN BAO CAO PHAN TICH HANH VI KHACH HANG
# ============================================================
print("\n" + "=" * 60)
print("BUOC 7: BAO CAO PHAN TICH HANH VI KHACH HANG")
print("=" * 60)

tong_kh   = rfm_pd["customer_id"].nunique()
pct       = rfm_pd["phan_khuc"].value_counts(normalize=True) * 100
tong_don  = sach_pd["comment_id"].nunique()

bao_cao = f"""
============================================================
CUSTOMER BEHAVIOR ANALYSIS
Dataset: Tiki E-commerce Review Data
============================================================

1. CONG THUC RFM
   RFM la phuong phap phan tich hanh vi khach hang dua tren 3 tieu chi:

   Recency (R) - Gan day:
      Tinh so ngay tu lan mua gan nhat cua khach den ngay tham chieu.
      Khach hang co R thap = mua hang gan day = gia tri cao hon.
      Cong thuc: R = ngay_tham_chieu - ngay_mua_gan_nhat (tinh bang so ngay)

   Frequency (F) - Tan suat:
      Tong so don hang ma khach hang da thuc hien trong toan bo du lieu.
      F cao = khach mua nhieu lan = trung thanh hon.
      Cong thuc: F = COUNT(don_hang) theo customer_id

   Monetary (M) - Gia tri:
      Tong gia tri chi tieu cua khach hang.
      Luu y: du lieu nay la du lieu review, khong co cot gia san pham.
      Thay the bang: M = F x diem_rating_trung_binh
      (the hien ca muc do tieu thu lan su hai long)

   Moi chieu duoc cham diem tu 1-4 bang phuong phap quantile:
      - Diem 4: nhom 25% tot nhat
      - Diem 1: nhom 25% thap nhat
   Tong diem RFM = R + F + M (tu 3 den 12)

2. KET QUA PHAN KHUC
   Tong so khach hang phan tich: {tong_kh:,}
   Tong so don hang hop le     : {tong_don:,}

   Quy tac phan khuc:
      VIP             : R >= 3, F >= 3, M >= 3  (khach giao dich nhieu, gan day, gia tri cao)
      Khach trung thanh: R >= 3, F >= 2          (mua gan day, tuong doi thuong xuyen)
      Khach moi       : R >= 3, F == 1           (moi mua lan dau, can cham soc them)
      Co nguy co roi bo: R == 2, F >= 2          (tung mua nhieu nhung dang giam tan suat)
      Can kich hoat lai: R <= 1, F >= 2          (lau roi khong mua du truoc do thuong xuyen)
      Khach tieu thu thap: con lai               (mua it, lau roi khong quay lai)

   Chi tiet tung nhom:
"""
for _, row in tong_hop_pd.iterrows():
    pk   = row["phan_khuc"]
    pct_val = pct.get(pk, 0)
    bao_cao += f"""
      {pk} ({pct_val:.1f}%):
         So khach  : {int(row['so_khach']):,}
         Recency TB: {row['recency_tb']:.0f} ngay
         Frequency : {row['frequency_tb']:.2f} don/nguoi
         Monetary  : {row['monetary_tb']:.2f} diem gia tri
         Diem RFM  : {row['diem_rfm_tb']:.1f}/12
"""

bao_cao += f"""
3. XU HUONG MUA SAM

   Theo thang:
      Thang co nhieu don nhat: {thang_pd.loc[thang_pd['so_don'].idxmax(), 'thang']} ({thang_pd['so_don'].max():,} don)
      Xu huong chung: tang dan qua cac nam, co dinh cao vao mua le/tet

   Theo ngay trong tuan:
"""
thu_sort = tuan_pd.sort_values("so_don", ascending=False)
for _, row in thu_sort.head(3).iterrows():
    bao_cao += f"      {row['ten_thu']}: {int(row['so_don']):,} don\n"

bao_cao += f"""
   Top nguoi ban:
      {seller_pd.iloc[0]['seller_name']}: {int(seller_pd.iloc[0]['so_don']):,} don (chiem thi phan lon nhat)

4. INSIGHT RUT RA

   a) Nhom VIP chi chiem khoang {pct.get('VIP', 0):.1f}% khach hang nhung la nhom co gia tri
      cao nhat. Can co chuong trinh uu dai rieng (loyalty program, giam gia som,
      ho tro uu tien) de giu chan nhom nay.

   b) Nhom "Co nguy co roi bo" la nhom can can thiep ngay. Truoc day ho mua
      tuong doi thuong xuyen nhung dang mat dan. Nen gui email/notification
      nhac nho hoac khuyen mai dac biet de keo ho quay lai.

   c) Nhom "Khach moi" chiem ty le {pct.get('Khach moi', 0):.1f}%. Day la co hoi lon - neu onboarding
      tot (email chao mung, ma giam gia lan 2...) co the chuyen nhom nay
      thanh "Khach trung thanh" hoac "VIP".

   d) Don hang tap trung vao ngay thuong (Thu 2 - Thu 5), cao hon cuoi tuan.
      Dieu nay nguoc voi xu huong chung cua e-commerce. Nguyen nhan co the
      do Tiki phu phuc vu nhom khach hang di lam, mua hang vao gio nghi trua
      hoac sau gio lam.

   e) Tiki Trading chiem phan lon don hang, cho thay thi truong tap trung cao.
      Cac seller thu ba (third-party) can dau tu them vao chat luong phuc vu
      va toc do giao hang de canh tranh.

============================================================
"""
print(bao_cao)

# Luu bao cao ra file txt
with open(OUTPUT_DIR + "bao_cao_rfm.txt", "w", encoding="utf-8") as f:
    f.write(bao_cao)
print("  Da luu bao cao: bao_cao_rfm.txt")

spark.stop()


# ============================================================
# BUOC 8: DAY TOAN BO KET QUA LEN HDFS
# ============================================================
print("\nBuoc 8: Day ket qua len HDFS...")


def day_len_hdfs(thu_muc_local, thu_muc_hdfs):
    danh_sach_file = glob.glob(os.path.join(os.path.abspath(thu_muc_local), "*"))

    if not danh_sach_file:
        print(f"  Khong co file nao trong {thu_muc_local} de day len HDFS")
        return

    subprocess.run(["hdfs.cmd", "dfs", "-mkdir", "-p", thu_muc_hdfs], check=True)

    for duong_dan_file in danh_sach_file:
        # -f de ghi de neu file da ton tai tren HDFS (chay lai nhieu lan)
        subprocess.run(
            ["hdfs.cmd", "dfs", "-put", "-f", duong_dan_file, thu_muc_hdfs + "/"],
            check=True
        )
        print(f"    Da day len HDFS: {thu_muc_hdfs}/{os.path.basename(duong_dan_file)}")

try:
    day_len_hdfs(OUTPUT_DIR, OUTPUT_HDFS_DIR)
    print(f"\nHoan thanh! Ket qua RFM da co tren HDFS tai: {OUTPUT_HDFS_DIR}")
    print(f"Kiem tra bang lenh: hdfs dfs -ls {OUTPUT_HDFS_DIR}")
except subprocess.CalledProcessError as loi:
    print("\n  LOI: khong day duoc len HDFS. Kiem tra lai:")
    print("    - HDFS (NameNode/DataNode) da chay chua (lenh: jps)")
    print("    - Lenh 'hdfs' co dang trong PATH khong")
    print("  Ket qua van con day du o thu muc local:", OUTPUT_DIR)
    raise loi
