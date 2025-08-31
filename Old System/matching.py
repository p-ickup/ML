# matching.py
import pickle

import numpy as np
import pandas as pd
from sklearn.preprocessing import MinMaxScaler, StandardScaler


class KMeansMatcher:
    def __init__(self, model_path):
        with open(model_path, "rb") as f:
            self.model = pickle.load(f)

        # If you saved a pipeline, you could load that here.
        # e.g., self.pipeline = joblib.load("scaler_pipeline.pkl")
        # Otherwise, we'll do manual scaling below.

    def two_step_normalize(self, df, feature_cols):
        """
        Replicates your code that does:
          1) MinMaxScaler
          2) StandardScaler
        on the specified columns. 
        """
        # Step 1: MinMax
        mm = MinMaxScaler()
        df[feature_cols] = mm.fit_transform(df[feature_cols])

        # Step 2: StandardScaler
        ss = StandardScaler()
        df[feature_cols] = ss.fit_transform(df[feature_cols])
        return df

    def predict_clusters(self, df_features: pd.DataFrame, feature_cols):
        """
        1) Double-scale the selected feature columns.
        2) Call the KMeans model to get cluster assignments.
        """
        df_scaled = df_features.copy()
        df_scaled = self.two_step_normalize(df_scaled, feature_cols)
        return self.model.predict(df_scaled[feature_cols])

    def form_matches(self, flights_df):
        """
        Example logic: group flights by cluster, then by (airport, date).
        This is just a placeholder — adapt to your real matching rules.
        """
        grouped = flights_df.groupby(['cluster_id', 'airport', 'date'])
        matches = []

        for (cluster_id, airport, date), group in grouped:
            user_ids = group["user_id"].tolist()

            # Simple pairing logic: group 2 at a time
            for i in range(0, len(user_ids), 2):
                pair = user_ids[i : i+2]
                if len(pair) == 2:
                    matches.append({
                        "user1": pair[0],
                        "user2": pair[1],
                        "airport": airport,
                        "date": date,
                        "cluster_id": cluster_id
                    })
        return matches
