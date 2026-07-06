from flask import Flask, jsonify, send_from_directory
import pandas as pd
import os

app = Flask(__name__)
DATA_DIR = "data/"

def doc_csv(ten_file):
    return pd.read_csv(os.path.join(DATA_DIR, ten_file))

@app.route("/")
def home():
    return jsonify({"status": "ok", "message": "Ecommerce API đang chạy!"})

@app.route("/dashboard")
def dashboard():
    return send_from_directory(".", "index.html")

@app.route("/api/rfm")
def rfm():
    df = doc_csv("tong_hop_phan_khuc.csv")
    return jsonify(df.to_dict(orient="records"))

@app.route("/api/xu-huong")
def xu_huong():
    df = doc_csv("xu_huong_theo_thang.csv")
    return jsonify(df.to_dict(orient="records"))

@app.route("/api/top-products")
def top_products():
    df = doc_csv("top_products.csv")
    return jsonify(df.to_dict(orient="records"))

@app.route("/api/recommend/<int:product_id>")
def recommend(product_id):
    df = doc_csv("recommendations.csv")
    row = df[df["product_id"] == product_id]
    if row.empty:
        return jsonify({"error": "Không tìm thấy sản phẩm"}), 404
    rec_ids = [int(x) for x in row.iloc[0]["recommend_ids"].split(",")]
    products = doc_csv("top_products.csv")
    result = products[products["product_id"].isin(rec_ids)].to_dict(orient="records")
    return jsonify({"product_id": product_id, "recommendations": result})
@app.route("/recommend")
def recommend_page():
    return send_from_directory(".", "recommend.html")
@app.route("/api/search")
def search():
    from flask import request
    keyword = request.args.get("q", "").lower()
    df = doc_csv("top_products.csv")
    ket_qua = df[df["product_name"].str.lower().str.contains(keyword)]
    return jsonify(ket_qua.to_dict(orient="records"))
@app.route("/api/recommend-customer/<int:customer_id>")
def recommend_customer(customer_id):
    df = doc_csv("top5_recommendations.csv")
    cleaned = doc_csv("cleaned_df.csv")[["product_id","product_name"]].drop_duplicates()
    ket_qua = df[df["Customer_ID"] == customer_id]
    
    if not ket_qua.empty:
        ket_qua = ket_qua.merge(cleaned, left_on="Product_ID", right_on="product_id", how="left")
        ket_qua["Product_Name"] = ket_qua["product_name"].fillna(ket_qua["Product_Name"])
        result = ket_qua[["Rank","Product_ID","Product_Name"]].to_dict(orient="records")
        return jsonify({
            "customer_id": customer_id,
            "type": "fp_growth",
            "recommendations": result
        })
    
    top = doc_csv("top_products.csv").head(5)
    result = [{"Rank": i+1, "Product_ID": row["product_id"],
               "Product_Name": row["product_name"]}
              for i, row in top.iterrows()]
    return jsonify({
        "customer_id": customer_id,
        "type": "popularity_fallback",
        "recommendations": result
    })
@app.route("/api/thong-ke")
def thong_ke():
    rfm = doc_csv("tong_hop_phan_khuc.csv")
    thang = doc_csv("xu_huong_theo_thang.csv")
    tong_kh = int(rfm["so_khach"].sum())
    vip = int(rfm[rfm["phan_khuc"] == "VIP"]["so_khach"].values[0])
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
if __name__ == "__main__":
    app.run(debug=True, port=5000)
