from flask import Flask, render_template
import pandas as pd
import subprocess
from io import StringIO
import os


app = Flask(__name__)

HDFS_OUTPUT_BASE = os.getenv("HDFS_OUTPUT_BASE", "/ecommerce/output")


def read_hdfs_csv(hdfs_dir):
    cmd = f"hdfs dfs -cat {hdfs_dir}/part-*.csv"
    result = subprocess.check_output(cmd, shell=True)
    text = result.decode("utf-8")

    return pd.read_csv(
        StringIO(text),
        engine="python",
        quotechar='"',
        escapechar='\\',
        on_bad_lines="skip"
    )


@app.route("/")
def index():
    summary_df = read_hdfs_csv(f"{HDFS_OUTPUT_BASE}/summary")
    product_df = read_hdfs_csv(f"{HDFS_OUTPUT_BASE}/product_summary")
    customer_df = read_hdfs_csv(f"{HDFS_OUTPUT_BASE}/customer_summary")

    summary = summary_df.iloc[0].to_dict()

    top_products = product_df.head(20).to_dict(orient="records")
    top_customers = customer_df.head(20).to_dict(orient="records")

    return render_template(
        "index.html",
        summary=summary,
        top_products=top_products,
        top_customers=top_customers,
        output_base=HDFS_OUTPUT_BASE,
    )


if __name__ == "__main__":
    app.run(debug=True, port=5000)