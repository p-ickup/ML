# matching.py
import joblib
import numpy as np
import pandas as pd
from sklearn.preprocessing import MinMaxScaler, StandardScaler


class Matcher:
    def __init__(self, model_path="../Synthetic_Data/kmeans_model_package.pkl"):
        # Load the pre-trained KMeans model and related metadata (features and weights)
        package = joblib.load(model_path)
        self.model = package["model"]
        self.feature_weights = package["feature_weights"]
        self.features = package["features"]

    def normalize_and_weight(self, df_raw: pd.DataFrame) -> pd.DataFrame:
        """
        Apply feature weighting and normalization to the raw feature DataFrame.
        Returns a transformed DataFrame suitable for clustering.
        """
        df = df_raw.copy()

        # Multiply each feature by its respective weight
        for i, feature in enumerate(self.features):
            df[feature] = df[feature] * self.feature_weights[i]
        
        # Apply Min-Max scaling followed by Standard scaling (z-score)
        mm = MinMaxScaler()
        df[self.features] = mm.fit_transform(df[self.features])
        ss = StandardScaler()
        df[self.features] = ss.fit_transform(df[self.features])
        
        return df

    def predict_clusters(self, df_weighted: pd.DataFrame) -> np.ndarray:
        """
        Predict the cluster assignment for each row in the weighted DataFrame
        using the pre-trained KMeans model.
        """
        return self.model.predict(df_weighted[self.features])

    def apply_bag_constraint(self, df: pd.DataFrame, max_bag_sum=10) -> pd.DataFrame:
        """
        Adjust cluster assignments so that the total number of bags in each cluster
        does not exceed the specified max_bag_sum. Creates new clusters as needed.
        """
        df_result = df.copy()

        # Identify existing cluster IDs and initialize the next available cluster ID
        original_clusters = df_result['cluster'].unique()
        new_cluster_id = int(df_result['cluster'].max()) + 1

        # Iterate through each original cluster to enforce the bag constraint
        for cluster_id in original_clusters:
            cluster_df = df_result[df_result['cluster'] == cluster_id].copy()
            
            # Sort users by bag count in descending order for optimal packing
            cluster_df = cluster_df.sort_values('BagNumber', ascending=False)

            current_sum = 0
            current_cluster = cluster_id

            # Assign users to clusters, creating new ones as necessary
            for idx, row in cluster_df.iterrows():
                bag_count = row['BagNumber']
                
                if current_sum + bag_count > max_bag_sum:
                    current_cluster = new_cluster_id
                    new_cluster_id += 1
                    current_sum = 0

                df_result.loc[idx, 'cluster'] = current_cluster
                current_sum += bag_count

        return df_result
