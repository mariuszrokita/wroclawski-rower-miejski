import os

from azure.storage.blob import BlockBlobService


class BikeRentalDataDownloader:

    def __init__(self, account_name, account_key, container_name):
        self.block_blob_service = BlockBlobService(account_name, account_key)
        self.container_name = container_name

    def download_blobs_and_save(self, target_folder):
        """
        Downloads all blobs (CSV files) from Azure Blob Storage and saves them in a target folder.
        """
        generator = self.block_blob_service.list_blobs(self.container_name)
        blob_names = [blob.name for blob in generator]

        # create folder if it doesn't exist yet
        os.makedirs(target_folder, exist_ok=True)

        for name in blob_names:
            file_path = os.path.join(target_folder, name)
            if not os.path.exists(file_path):
                self.block_blob_service.get_blob_to_path(self.container_name, name, file_path)
