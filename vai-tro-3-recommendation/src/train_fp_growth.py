"""
FP-Growth Algorithm Implementation using PySpark
Finds frequent itemsets and generates association rules
"""
import pandas as pd
from typing import Dict, List, Tuple, Set, Any
import pickle
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, size, collect_list, array
from pyspark.ml.fpm import FPGrowth as SparkFPGrowth
from pyspark.ml.feature import VectorAssembler
import numpy as np


class FPGrowth:
    """
    FP-Growth algorithm using PySpark's implementation
    Wrapper class for Spark FPGrowth
    """
    
    def __init__(self, min_support=0.05, min_confidence=0.5, min_lift=1.0):
        """
        Initialize FP-Growth with Spark
        
        Args:
            min_support: Minimum support threshold (0-1)
            min_confidence: Minimum confidence for association rules (0-1)
            min_lift: Minimum lift for association rules
        """
        self.min_support = min_support
        self.min_confidence = min_confidence
        self.min_lift = min_lift
        self.frequent_itemsets = {}
        self.association_rules = []
        self.spark = None
        self.model = None
        self.items_mapping = {}  # Ánh xạ item -> index
        self.index_mapping = {}  # Ánh xạ index -> item
        
    def _init_spark(self):
        """Initialize Spark session"""
        if self.spark is None:
            self.spark = SparkSession.builder \
                .appName("FPGrowth") \
                .config("spark.sql.adaptive.enabled", "true") \
                .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
                .getOrCreate()
        return self.spark
    
    def _prepare_data(self, transactions: List[frozenset]) -> pd.DataFrame:
        """
        Convert transactions to Spark DataFrame format
        Mỗi dòng là một giao dịch với cột 'items' là mảng các item
        """
        # Chuyển đổi transactions thành list các list
        data = []
        for trans in transactions:
            # Chuyển frozenset thành list và đảm bảo các item là string
            items = [str(item) for item in trans]
            data.append({"items": items})
        
        return pd.DataFrame(data)
    
    def _create_item_mapping(self, transactions: List[frozenset]):
        """Tạo mapping giữa item và index để dễ dàng tra cứu"""
        all_items = set()
        for trans in transactions:
            all_items.update(trans)
        
        # Tạo mapping
        items_list = sorted(list(all_items))
        self.items_mapping = {item: idx for idx, item in enumerate(items_list)}
        self.index_mapping = {idx: item for item, idx in self.items_mapping.items()}
    
    def mine_frequent_itemsets(self, transactions: List[frozenset]) -> Dict:
        """
        Mine frequent itemsets using PySpark FPGrowth
        
        Args:
            transactions: List of frozensets (transactions)
            
        Returns:
            Dictionary of frequent itemsets with support values
        """
        print(f"Mining frequent itemsets with {len(transactions)} transactions...")
        
        # Tạo mapping items
        self._create_item_mapping(transactions)
        
        # Khởi tạo Spark
        spark = self._init_spark()
        
        # Chuẩn bị dữ liệu
        pdf = self._prepare_data(transactions)
        df = spark.createDataFrame(pdf)
        
        # Khởi tạo và train FPGrowth
        fp_growth = SparkFPGrowth(
            itemsCol="items",
            minSupport=self.min_support,
            minConfidence=self.min_confidence
        )
        
        print("Training FPGrowth model...")
        self.model = fp_growth.fit(df)
        
        # Lấy frequent itemsets
        freq_itemsets_df = self.model.freqItemsets
        freq_itemsets_pd = freq_itemsets_df.toPandas()
        
        # Chuyển đổi về dictionary
        self.frequent_itemsets = {}
        for _, row in freq_itemsets_pd.iterrows():
            itemset = frozenset(row['items'])
            support = row['freq'] / len(transactions)
            self.frequent_itemsets[itemset] = support
        
        print(f"✓ Found {len(self.frequent_itemsets)} frequent itemsets")
        return self.frequent_itemsets
    
    def generate_association_rules(self, frequent_itemsets: Dict = None) -> List[Tuple]:
        """
        Generate association rules using PySpark FPGrowth
        
        Args:
            frequent_itemsets: Not needed for Spark version, kept for compatibility
            
        Returns:
            List of association rules (antecedent, consequent, support, confidence, lift)
        """
        if self.model is None:
            raise ValueError("Model not trained yet. Call mine_frequent_itemsets first.")
        
        print("Generating association rules...")
        
        # Lấy association rules từ Spark model
        rules_df = self.model.associationRules
        
        # Lọc theo min_lift (Spark không hỗ trợ trực tiếp)
        if self.min_lift > 0:
            rules_df = rules_df.filter(col("lift") >= self.min_lift)
        
        # Chuyển về pandas để xử lý
        rules_pd = rules_df.toPandas()
        
        self.association_rules = []
        for _, row in rules_pd.iterrows():
            antecedent = tuple(row['antecedent'])
            consequent = tuple(row['consequent'])
            support = row['support']
            confidence = row['confidence']
            lift = row['lift']
            
            # Chuyển đổi từ index sang item values nếu cần
            # PySpark FPGrowth trả về item gốc, không cần chuyển đổi
            
            if confidence >= self.min_confidence and lift >= self.min_lift:
                self.association_rules.append((
                    antecedent,
                    consequent,
                    support,
                    confidence,
                    lift
                ))
        
        # Sắp xếp theo lift giảm dần
        self.association_rules.sort(key=lambda x: x[4], reverse=True)
        
        print(f"✓ Generated {len(self.association_rules)} association rules")
        return self.association_rules
    
    def predict(self, items: List) -> List[Tuple]:
        """
        Dự đoán các item được gợi ý dựa trên items đầu vào
        
        Args:
            items: List các item hiện có
            
        Returns:
            List các rule phù hợp (consequent, confidence, lift)
        """
        if self.model is None:
            raise ValueError("Model not trained yet. Call mine_frequent_itemsets first.")
        
        # Chuyển đổi items thành dạng có thể query
        items_set = set([str(item) for item in items])
        predictions = []
        
        for antecedent, consequent, support, confidence, lift in self.association_rules:
            # Kiểm tra nếu antecedent là tập con của items_set
            if set(antecedent).issubset(items_set):
                # Loại bỏ các item đã có
                new_items = [item for item in consequent if item not in items_set]
                if new_items:
                    predictions.append({
                        'items': new_items,
                        'confidence': confidence,
                        'lift': lift,
                        'support': support
                    })
        
        # Sắp xếp theo confidence và lift
        predictions.sort(key=lambda x: (x['confidence'], x['lift']), reverse=True)
        return predictions
    
    def save_model(self, filepath: str):
        """
        Save FP-Growth model to file
        
        Lưu ý: PySpark model được lưu riêng, các metadata lưu bằng pickle
        """
        if self.model is None:
            raise ValueError("No model to save. Train the model first.")
        
        # Lưu Spark model
        model_dir = filepath + "_spark_model"
        self.model.write().overwrite().save(model_dir)
        
        # Lưu metadata và rules bằng pickle
        metadata = {
            'frequent_itemsets': self.frequent_itemsets,
            'association_rules': self.association_rules,
            'min_support': self.min_support,
            'min_confidence': self.min_confidence,
            'min_lift': self.min_lift,
            'items_mapping': self.items_mapping,
            'index_mapping': self.index_mapping,
            'spark_model_path': model_dir
        }
        
        with open(filepath, 'wb') as f:
            pickle.dump(metadata, f)
        
        print(f"✓ Model saved to {filepath}")
        print(f"✓ Spark model saved to {model_dir}")
    
    def load_model(self, filepath: str):
        """
        Load FP-Growth model from file
        """
        # Load metadata
        with open(filepath, 'rb') as f:
            metadata = pickle.load(f)
        
        self.frequent_itemsets = metadata['frequent_itemsets']
        self.association_rules = metadata['association_rules']
        self.min_support = metadata['min_support']
        self.min_confidence = metadata['min_confidence']
        self.min_lift = metadata['min_lift']
        self.items_mapping = metadata.get('items_mapping', {})
        self.index_mapping = metadata.get('index_mapping', {})
        
        # Load Spark model
        spark = self._init_spark()
        model_dir = metadata.get('spark_model_path', filepath + "_spark_model")
        self.model = SparkFPGrowth.load(model_dir)
        
        print(f"✓ Model loaded from {filepath}")
        print(f"✓ Loaded {len(self.frequent_itemsets)} frequent itemsets")
        print(f"✓ Loaded {len(self.association_rules)} association rules")
    
    def stop_spark(self):
        """Stop Spark session"""
        if self.spark is not None:
            self.spark.stop()
            self.spark = None
            print("✓ Spark session stopped")


class RecommendationEngine:
    """Recommendation Engine using FP-Growth rules"""
    
    def __init__(self, fp_growth_model: FPGrowth):
        """
        Initialize recommendation engine
        
        Args:
            fp_growth_model: Trained FPGrowth model
        """
        self.model = fp_growth_model
        self.rules = fp_growth_model.association_rules
        self.frequent_itemsets = fp_growth_model.frequent_itemsets
    
    def recommend_for_items(self, items: List[str], top_n: int = 5) -> List[Dict]:
        """
        Get recommendations for a set of items
        
        Args:
            items: List of items the user has
            top_n: Number of recommendations to return
            
        Returns:
            List of recommendations with scores
        """
        predictions = self.model.predict(items)
        
        # Lấy top N recommendations
        recommendations = []
        seen_items = set()
        
        for pred in predictions[:top_n]:
            for item in pred['items']:
                if item not in seen_items:
                    recommendations.append({
                        'item': item,
                        'confidence': pred['confidence'],
                        'lift': pred['lift'],
                        'support': pred['support']
                    })
                    seen_items.add(item)
        
        return recommendations
    
    def get_rule_explanations(self, items: List[str], top_n: int = 3) -> List[Dict]:
        """
        Get rule explanations for recommendations
        
        Args:
            items: List of items the user has
            top_n: Number of rules to return
            
        Returns:
            List of rules explaining recommendations
        """
        predictions = self.model.predict(items)
        
        explanations = []
        for pred in predictions[:top_n]:
            explanations.append({
                'antecedent': pred.get('antecedent', items),
                'consequent': pred['items'],
                'confidence': pred['confidence'],
                'lift': pred['lift'],
                'support': pred['support']
            })
        
        return explanations
    
    def get_similar_items(self, item: str, top_n: int = 5) -> List[Dict]:
        """
        Find items similar to a given item based on association rules
        
        Args:
            item: Item to find similar items for
            top_n: Number of similar items to return
            
        Returns:
            List of similar items with scores
        """
        similar_items = {}
        
        for antecedent, consequent, support, confidence, lift in self.rules:
            # Check if item is in antecedent
            if item in antecedent:
                for c in consequent:
                    if c != item:
                        similar_items[c] = similar_items.get(c, 0) + lift * confidence
            
            # Check if item is in consequent
            elif item in consequent:
                for a in antecedent:
                    if a != item:
                        similar_items[a] = similar_items.get(a, 0) + lift * confidence
        
        # Sort and return top N
        sorted_items = sorted(similar_items.items(), key=lambda x: x[1], reverse=True)
        
        return [
            {'item': item, 'score': score}
            for item, score in sorted_items[:top_n]
        ]


class MetricsCalculator:
    """Calculate metrics for association rules"""
    
    @staticmethod
    def calculate_rule_quality_metrics(rules: List[Tuple]) -> Dict:
        """
        Calculate various quality metrics for rules
        
        Args:
            rules: List of rules (antecedent, consequent, support, confidence, lift)
            
        Returns:
            Dictionary of metrics
        """
        if not rules:
            return {
                'num_rules': 0,
                'avg_confidence': 0,
                'avg_lift': 0,
                'avg_support': 0,
                'max_confidence': 0,
                'max_lift': 0,
                'min_confidence': 0,
                'min_lift': 0
            }
        
        confidences = [r[3] for r in rules]
        lifts = [r[4] for r in rules]
        supports = [r[2] for r in rules]
        
        return {
            'num_rules': len(rules),
            'avg_confidence': np.mean(confidences),
            'avg_lift': np.mean(lifts),
            'avg_support': np.mean(supports),
            'max_confidence': np.max(confidences),
            'max_lift': np.max(lifts),
            'min_confidence': np.min(confidences),
            'min_lift': np.min(lifts),
            'std_confidence': np.std(confidences),
            'std_lift': np.std(lifts)
        }
    
    @staticmethod
    def calculate_itemset_coverage(frequent_itemsets: Dict, transactions: List[frozenset]) -> float:
        """
        Calculate coverage of frequent itemsets
        
        Args:
            frequent_itemsets: Dictionary of frequent itemsets
            transactions: List of transactions
            
        Returns:
            Coverage ratio (0-1)
        """
        if not transactions or not frequent_itemsets:
            return 0.0
        
        covered = 0
        for trans in transactions:
            # Check if any frequent itemset is subset of transaction
            for itemset in frequent_itemsets.keys():
                if itemset.issubset(trans):
                    covered += 1
                    break
        
        return covered / len(transactions)
    
    @staticmethod
    def get_top_rules_by_lift(rules: List[Tuple], n: int = 10) -> List[Tuple]:
        """Get top N rules by lift"""
        return sorted(rules, key=lambda x: x[4], reverse=True)[:n]
    
    @staticmethod
    def get_top_rules_by_confidence(rules: List[Tuple], n: int = 10) -> List[Tuple]:
        """Get top N rules by confidence"""
        return sorted(rules, key=lambda x: x[3], reverse=True)[:n]
    
    @staticmethod
    def print_metrics_summary(rules: List[Tuple], frequent_itemsets: Dict = None):
        """Print summary of metrics"""
        metrics = MetricsCalculator.calculate_rule_quality_metrics(rules)
        
        print("\n" + "="*60)
        print("ASSOCIATION RULES METRICS SUMMARY")
        print("="*60)
        print(f"Total Rules: {metrics['num_rules']}")
        print(f"Average Confidence: {metrics['avg_confidence']:.3f}")
        print(f"Average Lift: {metrics['avg_lift']:.3f}")
        print(f"Average Support: {metrics['avg_support']:.3f}")
        print(f"Max Confidence: {metrics['max_confidence']:.3f}")
        print(f"Max Lift: {metrics['max_lift']:.3f}")
        print(f"Min Confidence: {metrics['min_confidence']:.3f}")
        print(f"Min Lift: {metrics['min_lift']:.3f}")
        
        if frequent_itemsets:
            print(f"Total Frequent Itemsets: {len(frequent_itemsets)}")
            # Show top 5 most frequent itemsets
            top_itemsets = sorted(frequent_itemsets.items(), 
                                key=lambda x: x[1], reverse=True)[:5]
            print("\nTop 5 Most Frequent Itemsets:")
            for itemset, support in top_itemsets:
                print(f"  {set(itemset)}: {support:.3f}")
        
        print("="*60)


def train_fp_growth(transactions: List[frozenset], 
                   min_support=0.05, 
                   min_confidence=0.5, 
                   min_lift=1.0) -> Tuple[FPGrowth, Dict, List]:
    """
    Train FP-Growth model using PySpark
    
    Args:
        transactions: List of frozensets
        min_support: Minimum support threshold
        min_confidence: Minimum confidence for rules
        min_lift: Minimum lift for rules
        
    Returns:
        Tuple of (FPGrowth model, frequent itemsets dict, association rules list)
    """
    print(f"\n{'='*60}")
    print(f"TRAINING FP-GROWTH WITH PYSPARK")
    print(f"{'='*60}")
    print(f"Transactions: {len(transactions)}")
    print(f"Min Support: {min_support}")
    print(f"Min Confidence: {min_confidence}")
    print(f"Min Lift: {min_lift}")
    print(f"{'='*60}\n")
    
    fp_growth = FPGrowth(min_support, min_confidence, min_lift)
    
    # Mine frequent itemsets
    frequent_itemsets = fp_growth.mine_frequent_itemsets(transactions)
    
    # Generate association rules
    rules = fp_growth.generate_association_rules(frequent_itemsets)
    
    return fp_growth, frequent_itemsets, rules


# Ví dụ sử dụng
if __name__ == "__main__":
    # Dữ liệu mẫu
    sample_transactions = [
        frozenset(['milk', 'bread', 'eggs']),
        frozenset(['milk', 'bread']),
        frozenset(['milk', 'eggs']),
        frozenset(['bread', 'eggs']),
        frozenset(['milk', 'bread', 'butter']),
        frozenset(['milk', 'butter']),
        frozenset(['bread', 'butter']),
        frozenset(['milk', 'bread', 'eggs', 'butter']),
        frozenset(['milk', 'eggs', 'butter']),
        frozenset(['bread', 'eggs', 'butter'])
    ]
    
    # Train model
    model, frequent_itemsets, rules = train_fp_growth(
        sample_transactions,
        min_support=0.2,
        min_confidence=0.5,
        min_lift=1.0
    )
    
    # Tạo recommendation engine
    recommender = RecommendationEngine(model)
    
    # Test recommendations
    user_items = ['milk', 'bread']
    recommendations = recommender.recommend_for_items(user_items, top_n=5)
    
    print(f"\nRecommendations for {user_items}:")
    for rec in recommendations:
        print(f"  {rec['item']}: confidence={rec['confidence']:.3f}, lift={rec['lift']:.3f}")
    
    # Tính metrics
    metrics = MetricsCalculator.calculate_rule_quality_metrics(rules)
    print(f"\nMetrics Summary:")
    for key, value in metrics.items():
        print(f"  {key}: {value}")
    
    # Lưu model
    model.save_model("models/fp_growth_model.pkl")
    
    # Đóng Spark
    model.stop_spark()