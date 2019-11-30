import logging

from bikeavailability.src.preparation.pipeline import DataPreparationPipeline

logging.getLogger("azure.storage.common.storageclient").setLevel(logging.WARNING)


def execute_pipeline(account_name: str, account_key: str, container_name: str,
                     raw_data_folder_path: str, processed_data_folder_path: str, hard_delete: bool,
                     processed_data_base_name: str, processed_files_base_name: str) -> None:
    """Execute entire pipeline: data loading, cleaning and feature engineering.

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

    data_prep_pipeline = DataPreparationPipeline(account_name, account_key, container_name,
                                                 raw_data_folder_path, processed_data_folder_path,
                                                 processed_data_base_name, processed_files_base_name,
                                                 hard_delete)

    # we're starting off our pipeline with no initial data to process - we basically
    # need to ingest it first, and then process it.
    data_prep_pipeline.transform(None)
