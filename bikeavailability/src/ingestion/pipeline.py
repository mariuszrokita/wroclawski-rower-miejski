import pandas as pd

from sklearn.base import BaseEstimator, TransformerMixin

from bikeavailability.src.ingestion.bike_availability_data_downloader import BikeAvailabilityDataDownloader
from bikeavailability.src.ingestion.bike_availability_records import BikeAvailabilityRecords
from bikeavailability.src.utils.logging import log_transformation


class DataIngestion(BaseEstimator, TransformerMixin):

    def __init__(self, account_name: str, account_key: str,
                 container_name: str, raw_data_folderpath: str):
        """Construct and execute data loading pipeline.

        Arguments:
            account_name {str} -- Azure Storage account name
            account_key {str} -- [Azure Storage account key
            container_name {str} -- the name of storage container containing bike availability data
            raw_data_folderpath {str} -- path to folder when raw data should be downloaded to

        Returns:
            [type] -- [description]
        """
        self.account_name = account_name
        self.account_key = account_key
        self.container_name = container_name
        self.raw_data_folderpath = raw_data_folderpath

    def fit(self, X: pd.DataFrame, y=None) -> pd.DataFrame:
        return self

    @log_transformation(stage='DataIngestion', indent_level=1)
    def transform(self, X: pd.DataFrame) -> (pd.DataFrame, [str]):
        # Download all bike rental data from Azure Blob Storage and save it locally
        blob_downloader = BikeAvailabilityDataDownloader(self.account_name, self.account_key,
                                                         self.container_name)
        blob_downloader.download_blobs_and_save(self.raw_data_folderpath)

        # Read all local csv files with bike rental data and combine it into one dataframe
        df, processed_filenames = BikeAvailabilityRecords(self.raw_data_folderpath).load_data()
        return df, processed_filenames
