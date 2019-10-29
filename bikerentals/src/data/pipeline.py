from src.data.bike_rental_data_downloader import BikeRentalDataDownloader
from src.data.bike_rental_records import BikeRentalRecords
from src.data.bike_station_locations import BikeStationsLocations
from src.data.data_concatenation import combine_datasets

def execute(account_name, account_key, bike_rental_data_container_name, raw_data_folderpath):
    """
    Execute data loading pipeline.

    Parameters:
    * account_name - Azure Storage account name
    * account_key - Azure Storage account key
    * bike_rental_data_container_name - the name of storage container containing bike rental data
    * raw_data_folderpath - path to folder when raw data should be downloaded to
    """
    # Download all bike rental data from Azure Blob Storage and save it locally 
    blob_downloader = BikeRentalDataDownloader(account_name, account_key, bike_rental_data_container_name)
    blob_downloader.download_blobs_and_save(raw_data_folderpath)

    # Read all local csv files with bike rental data and combine it into one dataframe
    bike_rentals_df = BikeRentalRecords(raw_data_folderpath).load_data()

    # Load bike station information (number, street, gps coordinates)
    bike_stations_df = BikeStationsLocations().load_data()

    # Combine everything together and return one 
    return combine_datasets(bike_rentals_df, bike_stations_df)
