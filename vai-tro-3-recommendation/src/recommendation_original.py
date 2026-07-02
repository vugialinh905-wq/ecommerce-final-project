"""
Product Recommendation Engine
Uses FP-Growth association rules to generate recommendations
"""
import pandas as pd
import numpy as np
from typing import Dict, List, Tuple
from collections import defaultdict


class RecommendationEngine:
    """Generate product recommendations based on association rules."""
    
    def __init__(self, association_rules: List[Tuple], 
                 baskets: Dict[int, set], 
                 product_map: Dict[int, str]):
        """
        Initialize recommendation engine
        
        Args:
            association_rules: List of association rules from FP-Growth
            baskets: Customer-product baskets
            product_map: Mapping of product_id to product_name
        """
        self.association_rules = association_rules
        self.baskets = baskets
        self.product_map = product_map
        self.recommendations = {}
    
    def generate_recommendations(self, max_recommendations: int = 5) -> Dict[int, List[int]]:
        """
        Generate recommendations for all customers
        
        Args:
            max_recommendations: Maximum number of recommendations per customer
            
        Returns:
            Dictionary mapping customer_id to list of recommended product_ids
        """
        print(f"Generating recommendations (max {max_recommendations} per customer)...")
        
        self.recommendations = {}
        
        for customer_id, customer_products in self.baskets.items():
            recommendations = self._get_customer_recommendations(
                customer_id, 
                customer_products, 
                max_recommendations
            )
            if recommendations:
                self.recommendations[customer_id] = recommendations
        
        covered = len(self.recommendations)
        print(f"✓ Generated recommendations for {covered} customers")
        return self.recommendations
    
    def _get_customer_recommendations(self, customer_id: int, 
                                      customer_products: set, 
                                      max_recommendations: int) -> List[int]:
        """
        Get personalized recommendations for a specific customer
        
        Args:
            customer_id: Customer ID
            customer_products: Set of products purchased by customer
            max_recommendations: Maximum number of recommendations
            
        Returns:
            List of recommended product_ids
        """
        recommendations_dict = defaultdict(float)
        
        # Apply association rules
        for antecedent, consequent, support, confidence, lift in self.association_rules:
            # Check if customer has bought products in antecedent
            ant_set = set(antecedent)
            if ant_set.issubset(customer_products):
                # Add consequent products to recommendations
                cons_set = set(consequent)
                for product in cons_set:
                    # Use lift as recommendation score
                    recommendations_dict[product] += lift
        
        # Also use collaborative filtering approach
        # Find similar customers and recommend their products
        similar_customers_recs = self._get_similar_customer_recommendations(
            customer_id, 
            customer_products, 
            top_n=5
        )
        for product, score in similar_customers_recs.items():
            recommendations_dict[product] += score * 0.5  # Weight lower than association rules
        
        # Filter out products already purchased
        recommendations_dict = {
            p: score for p, score in recommendations_dict.items() 
            if p not in customer_products
        }
        
        if not recommendations_dict:
            return []
        
        # Sort by score and return top N
        sorted_recs = sorted(recommendations_dict.items(), key=lambda x: x[1], reverse=True)
        return [product_id for product_id, _ in sorted_recs[:max_recommendations]]
    
    def _get_similar_customer_recommendations(self, customer_id: int, 
                                             customer_products: set, 
                                             top_n: int = 5) -> Dict[int, float]:
        """
        Find recommendations from similar customers using Jaccard similarity
        
        Args:
            customer_id: Target customer ID
            customer_products: Set of products purchased by customer
            top_n: Number of similar customers to consider
            
        Returns:
            Dictionary of recommended products with scores
        """
        similarities = {}
        
        # Calculate Jaccard similarity with other customers
        for other_id, other_products in self.baskets.items():
            if other_id == customer_id or len(other_products) == 0:
                continue
            
            intersection = len(customer_products & other_products)
            union = len(customer_products | other_products)
            
            if union > 0:
                jaccard_sim = intersection / union
                if jaccard_sim > 0:
                    similarities[other_id] = jaccard_sim
        
        # Get top similar customers
        if not similarities:
            return {}
        
        top_similar = sorted(similarities.items(), key=lambda x: x[1], reverse=True)[:top_n]
        
        # Aggregate products from similar customers
        recommendations = defaultdict(float)
        for similar_customer_id, similarity_score in top_similar:
            similar_products = self.baskets[similar_customer_id]
            for product in similar_products:
                if product not in customer_products:
                    recommendations[product] += similarity_score
        
        return recommendations
    
    def get_top_n_products(self, n: int = 5) -> pd.DataFrame:
        """
        Get top N recommended products across all recommendations
        
        Args:
            n: Number of top products to return
            
        Returns:
            DataFrame with top products
        """
        product_rec_count = defaultdict(int)
        
        for customer_id, recs in self.recommendations.items():
            for product_id in recs:
                product_rec_count[product_id] += 1
        
        if not product_rec_count:
            return pd.DataFrame()
        
        top_products = sorted(product_rec_count.items(), key=lambda x: x[1], reverse=True)[:n]
        
        data = []
        for product_id, count in top_products:
            data.append({
                'Product_ID': product_id,
                'Product_Name': self.product_map.get(product_id, 'Unknown'),
                'Recommendation_Count': count,
                'Percentage': f"{count/len(self.recommendations)*100:.2f}%"
            })
        
        return pd.DataFrame(data)
    
    def export_recommendations(self, output_file: str):
        """
        Export recommendations to CSV file
        
        Args:
            output_file: Output CSV file path
        """
        data = []
        for customer_id, products in self.recommendations.items():
            for rank, product_id in enumerate(products, 1):
                data.append({
                    'Customer_ID': customer_id,
                    'Rank': rank,
                    'Product_ID': product_id,
                    'Product_Name': self.product_map.get(product_id, 'Unknown')
                })
        
        if data:
            df = pd.DataFrame(data)
            df.to_csv(output_file, index=False)
            print(f"✓ Recommendations exported to {output_file}")
        else:
            print("No recommendations to export")


class RecommendationEvaluator:
    """Evaluate recommendation quality."""
    
    @staticmethod
    def evaluate(original_baskets: Dict[int, set], 
                 test_baskets: Dict[int, set],
                 recommendations: Dict[int, List[int]]) -> Dict:
        """
        Evaluate recommendations using test set
        
        Args:
            original_baskets: Training baskets
            test_baskets: Test baskets (actual purchases)
            recommendations: Generated recommendations
            
        Returns:
            Dictionary of evaluation metrics
        """
        metrics = {
            'precision': 0.0,
            'recall': 0.0,
            'hit_rate': 0,
            'coverage': 0.0,
            'diversity': 0.0
        }
        
        if not recommendations or not test_baskets:
            return metrics
        
        # Calculate precision and recall
        precisions = []
        recalls = []
        hits = 0
        
        for customer_id, recommended_products in recommendations.items():
            if customer_id not in test_baskets or len(test_baskets[customer_id]) == 0:
                continue
            
            actual_products = test_baskets[customer_id]
            
            if len(recommended_products) > 0:
                # Precision: how many recommendations are relevant
                relevant = len(set(recommended_products) & actual_products)
                precisions.append(relevant / len(recommended_products))
                
                # Recall: how many actual purchases were recommended
                recalls.append(relevant / len(actual_products) if len(actual_products) > 0 else 0)
                
                if relevant > 0:
                    hits += 1
        
        metrics['precision'] = np.mean(precisions) if precisions else 0.0
        metrics['recall'] = np.mean(recalls) if recalls else 0.0
        metrics['hit_rate'] = hits / len([c for c in recommendations if c in test_baskets])
        metrics['coverage'] = len(recommendations) / len(test_baskets) * 100 if test_baskets else 0.0
        
        # Calculate diversity (unique products recommended)
        all_recommended = set().union(*recommendations.values()) if recommendations else set()
        metrics['diversity'] = len(all_recommended)
        
        return metrics


if __name__ == "__main__":
    print("Recommendation Engine Module")
