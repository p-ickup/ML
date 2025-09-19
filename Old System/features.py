from datetime import datetime

import numpy as np
import pandas as pd


def compute_cyclical_datetime_features(df, datetime_column):
    """
    Computes cyclical datetime features (sin and cos) for a given datetime column.
    Also computes elapsed time from the earliest timestamp.
    
    Args:
        df (pd.DataFrame): Input DataFrame containing datetime data
        datetime_column (str): Name of the datetime column to process
        
    Returns:
        pd.DataFrame: DataFrame with added cyclical features
    """
    # Ensure the column is datetime type
    df[datetime_column] = pd.to_datetime(df[datetime_column])
    
    # Compute cyclical features
    df['datetime_sin'] = np.sin(2 * np.pi * df[datetime_column].dt.hour / 24)
    df['datetime_cos'] = np.cos(2 * np.pi * df[datetime_column].dt.hour / 24)
    
    # Compute elapsed time in hours from the earliest timestamp
    min_time = df[datetime_column].min()
    df['ElapsedTime'] = (df[datetime_column] - min_time).dt.total_seconds() / 3600
    
    return df

def gather_numeric_features(df, exclude_columns=None):
    """
    Gathers numeric fields from the flight DataFrame.
    
    Args:
        df (pd.DataFrame): Input DataFrame
        exclude_columns (list, optional): List of columns to exclude from numeric features
        
    Returns:
        pd.DataFrame: DataFrame containing only numeric features
    """
    if exclude_columns is None:
        exclude_columns = []
    
    # Select only numeric columns
    numeric_df = df.select_dtypes(include=[np.number])
    
    # Remove any excluded columns
    numeric_df = numeric_df.drop(columns=[col for col in exclude_columns if col in numeric_df.columns])
    
    return numeric_df

def create_raw_features(df, datetime_column, exclude_columns=None):
    """
    Creates a raw feature DataFrame by combining cyclical datetime features
    and numeric features.
    
    Args:
        df (pd.DataFrame): Input DataFrame
        datetime_column (str): Name of the datetime column to process
        exclude_columns (list, optional): List of columns to exclude from numeric features
        
    Returns:
        pd.DataFrame: Raw feature DataFrame before scaling
    """
    # Compute cyclical datetime features
    df_with_cyclical = compute_cyclical_datetime_features(df, datetime_column)
    
    # Gather numeric features
    raw_features = gather_numeric_features(df_with_cyclical, exclude_columns)
    
    return raw_features
