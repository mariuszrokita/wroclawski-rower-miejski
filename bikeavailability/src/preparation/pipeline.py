import pandas as pd
from sklearn.base import BaseEstimator, TransformerMixin
from sklearn.pipeline import make_pipeline

from bikeavailability.src.ingestion.pipeline import DataIngestion
from bikeavailability.src.utils.io import save_dataframe
from bikeavailability.src.utils.logging import log_transformation


class DataPreparationPipeline(BaseEstimator, TransformerMixin):
    """Scaffold entire pipeline: data loading, cleaning and feature engineering.

    Arguments:
        account_name {str} -- Azure Storage account name
        account_key {str} -- Azure Storage account key
        container_name {str} -- The name of storage container containing bike rental data
        raw_data_folder_path {str} -- the path to a folder with raw data to be processed
        processed_data_folder_path {str} -- the path to a folder where output file should be saved to
        processed_data_base_name {str} -- The output dataset file base name
        processed_files_base_name {str} -- The output file containing list of processed data files
        hard_delete {bool} -- delete records marked for deletion
    """

    def __init__(self, account_name: str, account_key: str, container_name: str,
                 raw_data_folder_path: str, processed_data_folder_path: str,
                 processed_data_base_name: str, processed_files_base_name: str,
                 hard_delete: bool):

        self.processed_data_folder_path = processed_data_folder_path
        self.processed_data_base_name = processed_data_base_name
        self.processed_files_base_name = processed_files_base_name
        self.data_preparation_pipeline = make_pipeline(
            # data loading
            DataIngestion(account_name, account_key, container_name,
                          raw_data_folder_path, self.processed_data_folder_path,
                          self.processed_data_base_name, self.processed_files_base_name)
            # TODO: data cleaning
            # TODO: featurization
        )

    def fit(self, X, y=None):
        return self

    @log_transformation(stage='DataPreparationPipeline', indent_level=0)
    def transform(self, X: pd.DataFrame) -> None:
        # execute pipeline
        data_df, processed_filenames_df = self.data_preparation_pipeline.transform(X)

        # save data and list of processed json files
        save_dataframe(data_df, self.processed_data_folder_path, self.processed_data_base_name)
        save_dataframe(processed_filenames_df, self.processed_data_folder_path, self.processed_files_base_name)
