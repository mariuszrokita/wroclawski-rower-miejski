import os

from azure.storage.blob import BlockBlobService

from bikeavailability.src.utils.logging import logger

# ! TODO: a duplicate of the BikeRentalDataDownloader:


class BikeAvailabilityDataDownloader:

    def __init__(self, account_name: str, account_key: str, container_name: str):
        """Download all files from Azure Blob storage container.

        Arguments:
            account_name {str} -- Azure Storage account name
            account_key {str} -- Azure Storage account key
            container_name {str} -- the name of storage container containing bike availability data
        """
        self.block_blob_service = BlockBlobService(account_name, account_key)
        self.container_name = container_name

    def download_blobs_and_save(self, target_folder: str) -> None:
        """Downloads all blobs (CSV files) from Azure Blob Storage and saves them in a target folder.

        Arguments:
            target_folder {str} -- folder where downloaded files should be saved
        """
        generator = self.block_blob_service.list_blobs(self.container_name)
        blob_names = [blob.name for blob in generator]

        # create folder if it doesn't exist yet
        os.makedirs(target_folder, exist_ok=True)

        for name in blob_names:
            file_path = os.path.join(target_folder, name)
            if not os.path.exists(file_path):
                logger.info(f"Downloading file {file_path}")
                self.block_blob_service.get_blob_to_path(self.container_name, name, file_path)
