import logging
import os

from bikerentals.src.preparation.pipeline import DataPreparationPipeline
from bikerentals.src.utils.logging import logger

logging.getLogger("azure.storage.common.storageclient").setLevel(logging.WARNING)


def execute_pipeline(account_name: str, account_key: str, bike_rental_data_container_name: str,
                     raw_data_folder_path: str, processed_data_folder_path: str, hard_delete: bool,
                     save_base_name: str) -> None:
    """Execute entire pipeline: data loading, cleaning and feature engineering.

    Arguments:
        account_name {str} -- Azure Storage account name
        account_key {str} -- Azure Storage account key
        bike_rental_data_container_name {str} -- The name of storage container containing bike rental data
        raw_data_folder_path {str} -- the path to a folder with raw data to be processed
        processed_data_folder_path {str} -- the path to a folder where output file should be saved to
        hard_delete {bool} -- delete records marked for deletion
        save_base_name {str} -- The output dataset file base name
    """

    data_prep_pipeline = DataPreparationPipeline(account_name, account_key,
                                                 bike_rental_data_container_name,
                                                 raw_data_folder_path, hard_delete)

    # we're starting off our pipeline with no initiall data to process - we basically
    # need to ingest it first, and then process it.
    df = data_prep_pipeline.transform(None)

    # save as csv
    save_dirname = os.path.join(processed_data_folder_path, '{}.csv'.format(save_base_name))
    df.to_csv(save_dirname, index=False)
    logger.info(f"Data saved to: {save_dirname}")
