import pandas as pd

from sklearn.base import BaseEstimator, TransformerMixin

from bikerentals.src.ingestion.data_concatenation import combine_datasets
from bikerentals.src.ingestion.bike_rental_data_downloader import BikeRentalDataDownloader
from bikerentals.src.ingestion.bike_rental_records import BikeRentalRecords
from bikerentals.src.ingestion.bike_station_locations import BikeStationsLocations
from bikerentals.src.utils.logging import log_transformation


class DataIngestion(BaseEstimator, TransformerMixin):

    def __init__(self, account_name: str, account_key: str,
                 bike_rental_data_container_name: str, raw_data_folderpath: str):
        """
        Execute data loading pipeline.

        Parameters:
        * account_name - Azure Storage account name
        * account_key - Azure Storage account key
        * bike_rental_data_container_name - the name of storage container containing bike rental data
        * raw_data_folderpath - path to folder when raw data should be downloaded to
        """
        self.account_name = account_name
        self.account_key = account_key
        self.bike_rental_data_container_name = bike_rental_data_container_name
        self.raw_data_folderpath = raw_data_folderpath

    def fit(self, X: pd.DataFrame, y=None) -> pd.DataFrame:
        return self

    @log_transformation(stage='DataIngestion', indent_level=1)
    def transform(self, X: pd.DataFrame) -> pd.DataFrame:
        # Download all bike rental data from Azure Blob Storage and save it locally
        blob_downloader = BikeRentalDataDownloader(self.account_name, self.account_key,
                                                   self.bike_rental_data_container_name)
        blob_downloader.download_blobs_and_save(self.raw_data_folderpath)

        # Read all local csv files with bike rental data and combine it into one dataframe
        bike_rentals_df = BikeRentalRecords(self.raw_data_folderpath).load_data()

        # Load bike station information (number, street, gps coordinates)
        bike_stations_df = BikeStationsLocations().load_data()

        # Combine everything together and return one dataset
        df = combine_datasets(bike_rentals_df, bike_stations_df)
        return df
