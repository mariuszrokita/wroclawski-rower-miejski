import pandas as pd

from sklearn.base import BaseEstimator, TransformerMixin

from bikeavailability.src.ingestion.bike_availability_data_downloader import BikeAvailabilityDataDownloader
from bikeavailability.src.ingestion.bike_availability_records import BikeAvailabilityRecords
from bikeavailability.src.utils.logging import log_transformation


class DataIngestion(BaseEstimator, TransformerMixin):
    """Download raw data from blob storage and load it into pandas dataframe.

    Arguments:
        account_name {str} -- Azure Storage account name
        account_key {str} -- Azure Storage account key
        container_name {str} -- the name of storage container containing bike availability data
        raw_data_folderpath {str} -- path to folder where raw data should be downloaded to
        processed_data_folder_path {str} -- the path to a folder where output file should be saved to
        processed_data_base_name {str} -- The output dataset file base name
        processed_files_base_name {str} -- The output file containing list of processed data files
    """

    def __init__(self, account_name: str, account_key: str, container_name: str,
                 raw_data_folderpath: str, processed_data_folder_path: str,
                 processed_data_base_name: str, processed_files_base_name: str):

        self.blob_downloader = BikeAvailabilityDataDownloader(account_name, account_key, container_name)
        self.availability_data_loader = BikeAvailabilityRecords()
        self.raw_data_folderpath = raw_data_folderpath
        self.processed_data_folder_path = processed_data_folder_path
        self.processed_data_base_name = processed_data_base_name
        self.processed_files_base_name = processed_files_base_name

    def fit(self, X: pd.DataFrame, y=None) -> pd.DataFrame:
        return self

    @log_transformation(stage='DataIngestion', indent_level=1)
    def transform(self, X: pd.DataFrame) -> (pd.DataFrame, pd.DataFrame):
        # Download raw json files and save them
        self.blob_downloader.download_blobs_and_save(self.raw_data_folderpath)

        # Read all local csv files with bike rental data and combine it into one dataframe
        data_df, processed_filenames_df = \
            self.availability_data_loader.load(self.raw_data_folderpath,
                                               self.processed_data_folder_path,
                                               self.processed_data_base_name,
                                               self.processed_files_base_name)

        # TODO: combine bike station numbers with bike station names

        return data_df, processed_filenames_df
