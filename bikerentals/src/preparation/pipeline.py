import pandas as pd
from sklearn.base import BaseEstimator, TransformerMixin
from sklearn.pipeline import make_pipeline

from bikerentals.src.cleaning.pipeline import DataCleaning
from bikerentals.src.features.pipeline import DataFeaturization
from bikerentals.src.ingestion.pipeline import DataIngestion
from bikerentals.src.utils.logging import log_transformation


class DataPreparationPipeline(BaseEstimator, TransformerMixin):
    """
    Scaffold entire pipeline: data loading, cleaning and feature engineering.

    Parameters:
    * account_name - Azure Storage account name
    * account_key - Azure Storage account key
    * bike_rental_data_container_name - the name of storage container containing bike rental data
    * raw_data_folder_path - the path to a folder with raw data to be processed
    * hard_delete - delete records marked for deletion
    """

    def __init__(self, account_name, account_key, bike_rental_data_container_name,
                 raw_data_folder_path, hard_delete):
        self.data_preparation_pipeline = make_pipeline(
            # data loading
            DataIngestion(account_name, account_key, bike_rental_data_container_name, raw_data_folder_path),
            # data cleaning
            DataCleaning(hard_delete),
            # data processing
            DataFeaturization()
        )

    def fit(self, X, y=None):
        return self

    @log_transformation(stage='DataPreparationPipeline', indent_level=0)
    def transform(self, X: pd.DataFrame) -> pd.DataFrame:
        return self.data_preparation_pipeline.transform(X)
