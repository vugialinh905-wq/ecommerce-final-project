"""
Evaluation Module for Product Recommendation System
Provides metrics and analysis for recommendation quality
"""
import pandas as pd
import numpy as np
from typing import Dict, List, Tuple
from collections import defaultdict


class MetricsCalculator:
    """Calculate recommendation metrics."""
    
    @staticmethod
    def precision_at_k(recommendations: Dict[int, List[int]], 
                       actual_products: Dict[int, set], 
                       k: int = 5) -> float:
        """
        Calculate Precision@K
        
        Args:
            recommendations: Dict of customer_id -> recommended products
            actual_products: Dict of customer_id -> actual products
            k: Number of top recommendations to consider
            
        Returns:
            Average precision at k
        """
        precisions = []
        
        for customer_id, recs in recommendations.items():
            if customer_id not in actual_products:
                continue
            
            actual = actual_products[customer_id]
            top_k_recs = recs[:k]
            
            if len(top_k_recs) > 0:
                relevant = len(set(top_k_recs) & actual)
                precisions.append(relevant / len(top_k_recs))
        
        return np.mean(precisions) if precisions else 0.0
    
    @staticmethod
    def recall_at_k(recommendations: Dict[int, List[int]], 
                    actual_products: Dict[int, set], 
                    k: int = 5) -> float:
        """Calculate Recall@K"""
        recalls = []
        
        for customer_id, recs in recommendations.items():
            if customer_id not in actual_products:
                continue
            
            actual = actual_products[customer_id]
            top_k_recs = recs[:k]
            
            if len(actual) > 0:
                relevant = len(set(top_k_recs) & actual)
                recalls.append(relevant / len(actual))
        
        return np.mean(recalls) if recalls else 0.0
    
    @staticmethod
    def map_k(recommendations: Dict[int, List[int]], 
              actual_products: Dict[int, set], 
              k: int = 5) -> float:
        """Calculate Mean Average Precision@K"""
        aps = []
        
        for customer_id, recs in recommendations.items():
            if customer_id not in actual_products:
                continue
            
            actual = actual_products[customer_id]
            top_k_recs = recs[:k]
            
            ap = 0.0
            relevant_count = 0
            
            for i, product in enumerate(top_k_recs, 1):
                if product in actual:
                    relevant_count += 1
                    ap += relevant_count / i
            
            if len(actual) > 0:
                aps.append(ap / min(k, len(actual)))
            else:
                aps.append(0.0)
        
        return np.mean(aps) if aps else 0.0
    
    @staticmethod
    def mrr_k(recommendations: Dict[int, List[int]], 
              actual_products: Dict[int, set], 
              k: int = 5) -> float:
        """Calculate Mean Reciprocal Rank@K"""
        rrs = []
        
        for customer_id, recs in recommendations.items():
            if customer_id not in actual_products:
                continue
            
            actual = actual_products[customer_id]
            top_k_recs = recs[:k]
            
            rr = 0.0
            for i, product in enumerate(top_k_recs, 1):
                if product in actual:
                    rr = 1.0 / i
                    break
            
            rrs.append(rr)
        
        return np.mean(rrs) if rrs else 0.0
    
    @staticmethod
    def ndcg_k(recommendations: Dict[int, List[int]], 
               actual_products: Dict[int, set], 
               k: int = 5) -> float:
        """Calculate Normalized Discounted Cumulative Gain@K"""
        ndcgs = []
        
        for customer_id, recs in recommendations.items():
            if customer_id not in actual_products:
                continue
            
            actual = actual_products[customer_id]
            top_k_recs = recs[:k]
            
            # DCG
            dcg = 0.0
            for i, product in enumerate(top_k_recs, 1):
                if product in actual:
                    dcg += 1.0 / np.log2(i + 1)
            
            # IDCG (ideal DCG)
            idcg = 0.0
            for i in range(min(len(actual), k)):
                idcg += 1.0 / np.log2(i + 2)
            
            ndcg = dcg / idcg if idcg > 0 else 0.0
            ndcgs.append(ndcg)
        
        return np.mean(ndcgs) if ndcgs else 0.0
    
    @staticmethod
    def coverage(recommendations: Dict[int, List[int]], 
                 total_products: int) -> float:
        """
        Calculate catalog coverage (% of products recommended at least once)
        
        Args:
            recommendations: Dict of recommendations
            total_products: Total number of unique products
            
        Returns:
            Coverage percentage
        """
        all_recommended = set().union(*recommendations.values()) if recommendations else set()
        return len(all_recommended) / total_products * 100 if total_products > 0 else 0.0
    
    @staticmethod
    def diversity(recommendations: Dict[int, List[int]]) -> float:
        """
        Calculate average diversity per customer
        
        Args:
            recommendations: Dict of recommendations
            
        Returns:
            Average number of unique products recommended per customer
        """
        diversities = [len(set(recs)) for recs in recommendations.values()]
        return np.mean(diversities) if diversities else 0.0
    
    @staticmethod
    def customer_coverage(recommendations: Dict[int, List[int]], 
                         total_customers: int) -> float:
        """
        Calculate customer coverage (% of customers with at least one recommendation)
        
        Args:
            recommendations: Dict of recommendations
            total_customers: Total number of customers
            
        Returns:
            Customer coverage percentage
        """
        covered = len([c for c in recommendations if len(recommendations[c]) > 0])
        return covered / total_customers * 100 if total_customers > 0 else 0.0


def generate_evaluation_report(recommendations: Dict[int, List[int]], 
                               test_baskets: Dict[int, set],
                               all_products: set,
                               output_file: str) -> pd.DataFrame:
    """
    Generate comprehensive evaluation report
    
    Args:
        recommendations: Dict of customer recommendations
        test_baskets: Dict of actual test purchases
        all_products: Set of all available products
        output_file: Path to save evaluation report
        
    Returns:
        DataFrame with evaluation results
    """
    print("Evaluating recommendations...")
    
    calc = MetricsCalculator()
    
    # Calculate metrics for different k values
    metrics_data = []
    
    for k in [1, 3, 5, 10]:
        metrics = {
            'Metric': f'@K={k}',
            'Precision': f"{calc.precision_at_k(recommendations, test_baskets, k):.4f}",
            'Recall': f"{calc.recall_at_k(recommendations, test_baskets, k):.4f}",
            'MAP': f"{calc.map_k(recommendations, test_baskets, k):.4f}",
            'MRR': f"{calc.mrr_k(recommendations, test_baskets, k):.4f}",
            'NDCG': f"{calc.ndcg_k(recommendations, test_baskets, k):.4f}"
        }
        metrics_data.append(metrics)
    
    # Add coverage metrics
    metrics_data.append({
        'Metric': 'Coverage',
        'Precision': f"{calc.customer_coverage(recommendations, len(test_baskets)):.2f}%",
        'Recall': f"{calc.coverage(recommendations, len(all_products)):.2f}%",
        'MAP': f"{calc.diversity(recommendations):.2f}",
        'MRR': '—',
        'NDCG': '—'
    })
    
    df_metrics = pd.DataFrame(metrics_data)
    
    # Save report
    df_metrics.to_csv(output_file, index=False)
    print(f"✓ Evaluation report saved to {output_file}")
    
    return df_metrics


def print_evaluation_summary(recommendations: Dict[int, List[int]], 
                             test_baskets: Dict[int, set],
                             all_products: set):
    """Print evaluation summary to console."""
    calc = MetricsCalculator()
    
    print("\n" + "="*80)
    print("EVALUATION SUMMARY")
    print("="*80)
    
    print(f"\nRecommendation Coverage:")
    print(f"  Customer Coverage: {calc.customer_coverage(recommendations, len(test_baskets)):.2f}%")
    print(f"  Catalog Coverage: {calc.coverage(recommendations, len(all_products)):.2f}%")
    print(f"  Average Diversity: {calc.diversity(recommendations):.2f} products/customer")
    
    print(f"\nRanking Metrics:")
    print(f"  Precision@5: {calc.precision_at_k(recommendations, test_baskets, 5):.4f}")
    print(f"  Recall@5: {calc.recall_at_k(recommendations, test_baskets, 5):.4f}")
    print(f"  MAP@5: {calc.map_k(recommendations, test_baskets, 5):.4f}")
    print(f"  MRR@5: {calc.mrr_k(recommendations, test_baskets, 5):.4f}")
    print(f"  NDCG@5: {calc.ndcg_k(recommendations, test_baskets, 5):.4f}")
    
    print("\n" + "="*80)


if __name__ == "__main__":
    print("Evaluation Module")
