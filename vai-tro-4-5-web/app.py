from flask import Flask, jsonify, send_from_directory
import pandas as pd
import subprocess
import io
import os

app = Flask(__name__)

os.environ["HADOOP_HOME"] = "D:\\hadoop"
os.environ["JAVA_HOME"] = "D:\\Java\\jdk-11"
os.environ["PATH"] = os.environ["PATH"] + ";D:\\hadoop\\bin;D:\\hadoop\\sbin;D:\\Java\\jdk-11\\bin"

HDFS_BASE = "/ecommerce/output_rfm"
HDFS_CMD = "D:\\hadoop\\bin\\hdfs.cmd"

# ── Cache data vào RAM khi khởi động ─────────────────
def doc_hdfs(hdfs_path):
    try:
        result = subprocess.run(
            [HDFS_CMD, "dfs", "-cat", hdfs_path],
            capture_output=True, text=True, encoding="utf-8"
        )
        if result.returncode == 0 and result.stdout.strip():
            return pd.read_csv(io.StringIO(result.stdout))
        raise Exception(result.stderr)
    except Exception as e:
        print(f"Lỗi đọc HDFS {hdfs_path}: {e}")
        return pd.DataFrame()

print("Loading data từ HDFS vào RAM...")
CACHE = {
    "rfm": doc_hdfs(f"{HDFS_BASE}/tong_hop_phan_khuc.csv"),
    "xu_huong": doc_hdfs(f"{HDFS_BASE}/xu_huong_theo_thang.csv"),
    "top_products": doc_hdfs(f"{HDFS_BASE}/top_products.csv"),
    "recommendations": doc_hdfs(f"{HDFS_BASE}/top5_recommendations.csv"),
}
print("Đã load xong! Web sẵn sàng.")

@app.route("/")
def home():
    return jsonify({"status": "ok", "message": "Ecommerce API chay tu HDFS!"})

@app.route("/dashboard")
def dashboard():
    return send_from_directory(".", "index.html")

@app.route("/recommend")
def recommend_page():
    return send_from_directory(".", "recommend.html")

@app.route("/api/thong-ke")
def thong_ke():
    rfm = CACHE["rfm"]
    thang = CACHE["xu_huong"]
    if rfm.empty or thang.empty:
        return jsonify({"error": "Không đọc được data"}), 500
    tong_kh = int(rfm["so_khach"].sum())
    vip_row = rfm[rfm["phan_khuc"] == "VIP"]
    vip = int(vip_row["so_khach"].values[0]) if not vip_row.empty else 0
    don_cao_nhat = int(thang["so_don"].max())
    thang_cao_nhat = thang.loc[thang["so_don"].idxmax(), "thang"]
    rating_tb = round(float(thang["diem_tb"].mean()), 2)
    return jsonify({
        "tong_khach_hang": tong_kh,
        "vip": vip,
        "don_cao_nhat": don_cao_nhat,
        "thang_cao_nhat": thang_cao_nhat,
        "rating_tb": rating_tb
    })

@app.route("/api/rfm")
def rfm():
    return jsonify(CACHE["rfm"].to_dict(orient="records"))

@app.route("/api/xu-huong")
def xu_huong():
    return jsonify(CACHE["xu_huong"].to_dict(orient="records"))

@app.route("/api/top-products")
def top_products():
    return jsonify(CACHE["top_products"].to_dict(orient="records"))

@app.route("/api/recommend-customer/<int:customer_id>")
def recommend_customer(customer_id):
    df = CACHE["recommendations"]
    if not df.empty:
        ket_qua = df[df["Customer_ID"] == customer_id]
        if not ket_qua.empty:
            return jsonify({
                "customer_id": customer_id,
                "type": "fp_growth",
                "recommendations": ket_qua[["Rank","Product_ID","Product_Name"]].to_dict(orient="records")
            })
    top = CACHE["top_products"].head(5)
    result = [{"Rank": i+1, "Product_ID": row.get("product_id"),
               "Product_Name": row.get("product_name")}
              for i, row in top.iterrows()]
    return jsonify({
        "customer_id": customer_id,
        "type": "popularity_fallback",
        "recommendations": result
    })

if __name__ == "__main__":
    app.run(debug=True, port=5000)