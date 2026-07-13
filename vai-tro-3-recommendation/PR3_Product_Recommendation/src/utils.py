"""
Utility functions for Product Recommendation System
"""
import subprocess
from io import StringIO

import pandas as pd
import numpy as np
from typing import Dict, List, Tuple


def _is_hdfs_path(path: str) -> bool:
    """
    Kiểm tra xem path có phải là đường dẫn HDFS hay không

    Args:
        path: Đường dẫn cần kiểm tra

    Returns:
        True nếu là đường dẫn HDFS (bắt đầu bằng /ecommerce hoặc hdfs://)
    """
    return path.startswith("/ecommerce") or path.startswith("hdfs://")


def _read_csv_from_hdfs(hdfs_dir: str) -> pd.DataFrame:
    """
    Đọc CSV từ một thư mục HDFS do Spark ghi ra (nhiều file part-*.csv)

    Args:
        hdfs_dir: Đường dẫn thư mục trên HDFS, ví dụ "/ecommerce/output/merged"

    Returns:
        DataFrame gộp từ toàn bộ các file part-*.csv trong thư mục
    """
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


def read_csv(path: str) -> pd.DataFrame:
    """
    Đọc CSV từ local hoặc từ HDFS, tự động phát hiện dựa trên path

    Args:
        path: Đường dẫn file/thư mục. Nếu bắt đầu bằng "/ecommerce" hoặc
              "hdfs://" thì sẽ đọc từ HDFS, ngược lại đọc file local như bình thường

    Returns:
        DataFrame chứa dữ liệu đã đọc
    """
    if _is_hdfs_path(path):
        df = _read_csv_from_hdfs(path)
    else:
        df = pd.read_csv(path)

    print(f"✓ Loaded data: {df.shape[0]} rows, {df.shape[1]} columns (source: {path})")
    return df


def load_data(filepath: str) -> pd.DataFrame:
    """
    Load and validate cleaned data
    
    Args:
        filepath: Path to cleaned CSV file
        
    Returns:
        DataFrame with cleaned data
    """
    df = pd.read_csv(filepath)
    print(f"✓ Loaded data: {df.shape[0]} rows, {df.shape[1]} columns")
    return df


def create_customer_baskets(df: pd.DataFrame, rating_threshold: int = 4) -> Dict[int, set]:
    """
    Create customer-product baskets (transactions) based on ratings
    Only include products with rating >= rating_threshold
    
    Args:
        df: Input dataframe
        rating_threshold: Minimum rating to include a product (default: 4)
        
    Returns:
        Dictionary mapping customer_id to set of product_ids
    """
    baskets = {}
    
    # Filter high-rated products
    df_filtered = df[df['rating'] >= rating_threshold].copy()
    
    for customer_id, group in df_filtered.groupby('customer_id'):
        products = set(group['product_id'].unique())
        if len(products) > 0:
            baskets[customer_id] = products
    
    return baskets


def create_transactions_list(baskets: Dict[int, set]) -> List[frozenset]:
    """
    Convert customer baskets to transaction list for FP-Growth
    
    Args:
        baskets: Dictionary of customer baskets
        
    Returns:
        List of frozensets for FP-Growth algorithm
    """
    transactions = []
    for customer_id, products in baskets.items():
        if len(products) > 0:
            transactions.append(frozenset(products))
    return transactions


def get_product_name_map(df: pd.DataFrame) -> Dict[int, str]:
    """
    Create mapping from product_id to product_name
    
    Args:
        df: Input dataframe
        
    Returns:
        Dictionary mapping product_id to product_name
    """
    product_map = df.groupby('product_id')['product_name'].first().to_dict()
    return product_map


def format_rules_output(rules: List[Tuple]) -> pd.DataFrame:
    """
    Format association rules to DataFrame
    
    Args:
        rules: List of tuples (antecedents, consequents, support, confidence, lift)
        
    Returns:
        Formatted DataFrame
    """
    data = []
    for antecedent, consequent, support, confidence, lift in rules:
        data.append({
            'Antecedent_Products': ', '.join(map(str, sorted(antecedent))),
            'Consequent_Products': ', '.join(map(str, sorted(consequent))),
            'Support': f"{support:.4f}",
            'Confidence': f"{confidence:.4f}",
            'Lift': f"{lift:.4f}"
        })
    
    return pd.DataFrame(data)


def calculate_coverage(recommendations: Dict[int, List[int]], 
                       total_customers: int) -> float:
    """
    Calculate recommendation coverage (% of customers with recommendations)
    
    Args:
        recommendations: Dictionary of customer recommendations
        total_customers: Total number of customers
        
    Returns:
        Coverage percentage
    """
    covered = len([c for c in recommendations if len(recommendations[c]) > 0])
    return (covered / total_customers * 100) if total_customers > 0 else 0


def calculate_diversity(recommendations: Dict[int, List[int]]) -> float:
    """
    Calculate diversity of recommendations (average unique products per customer)
    
    Args:
        recommendations: Dictionary of customer recommendations
        
    Returns:
        Average diversity score
    """
    if not recommendations:
        return 0.0
    
    total_unique = len(set().union(*recommendations.values()))
    avg_per_customer = np.mean([len(set(recs)) for recs in recommendations.values()]) if recommendations else 0
    
    return avg_per_customer


def get_top_n_recommendations(recommendations: Dict[int, List[int]], 
                              n: int = 5) -> Dict[int, List[int]]:
    """
    Get top N recommendations for each customer
    
    Args:
        recommendations: Dictionary of customer recommendations
        n: Number of recommendations (default: 5)
        
    Returns:
        Dictionary with top N recommendations per customer
    """
    top_recs = {}
    for customer_id, recs in recommendations.items():
        top_recs[customer_id] = recs[:n]
    return top_recs


def verify_recommendations(recommendations: Dict[int, List[int]], 
                          product_map: Dict[int, str]) -> Dict:
    """
    Verify recommendations are properly mapped to product names
    
    Args:
        recommendations: Dictionary of customer recommendations
        product_map: Mapping of product_id to product_name
        
    Returns:
        Dictionary with verification results
    """
    unmapped_count = 0
    unmapped_ids = set()
    total_recs = 0
    
    for customer_id, products in recommendations.items():
        for product_id in products:
            total_recs += 1
            if product_id not in product_map:
                unmapped_count += 1
                unmapped_ids.add(product_id)
    
    success_rate = 0.0
    if total_recs > 0:
        success_rate = (1 - unmapped_count / total_recs) * 100
    
    return {
        'total_recs': total_recs,
        'mapped_count': total_recs - unmapped_count,
        'unmapped_count': unmapped_count,
        'unmapped_ids': unmapped_ids,
        'success_rate': success_rate,
        'status': '✅ OK' if success_rate >= 99.0 else '⚠️  WARNING'
    }


if __name__ == "__main__":
    print("Product Recommendation System - Utilities")
