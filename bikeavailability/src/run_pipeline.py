import argparse
import json
import logging
import os
import sys

# ! TODO: fix this!!!
sys.path.insert(0, os.getcwd())
sys.path.insert(0, os.path.abspath(os.path.join(os.getcwd(), '..', '..')))

from bikeavailability.src.preparation.pipeline import DataPreparationPipeline  # noqa: E402
from bikeavailability.src.utils.argparse import str2bool                       # noqa: E402
from bikeavailability.src.utils.logging import logger                          # noqa: E402

logging.getLogger("azure.storage.common.storageclient").setLevel(logging.WARNING)


# ! TODO: move the function somewhere else
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


if __name__ == "__main__":
    # Construct documentation for the script parameters
    parser = argparse.ArgumentParser(description='Execute pipelines for bike availability data.')
    parser.add_argument('--hard-delete',
                        help='Remove data while cleaning, otherwise mark records for removal',
                        type=str2bool, default=True, dest='hard_delete')
    parser.add_argument('--processed-data-base-name',
                        help='The base name for output file with processed data',
                        type=str, default='bike_availability', dest='processed_data_base_name')
    parser.add_argument('--processed-files-base-name',
                        help='The base name for output file with list of processed data files',
                        type=str, default='processed_files', dest='processed_files_base_name')
    args = parser.parse_args()

    logger.info("Script execution started")

    # Set project root folder
    mini_project_root_folder = os.path.abspath(os.path.join(os.getcwd(), '..'))
    logger.info(f'Root folder set to: {mini_project_root_folder}')

    # Set up paths to data folders
    raw_data_folder_path = os.path.join(mini_project_root_folder, 'data', 'raw')
    processed_data_folder_path = os.path.join(mini_project_root_folder, 'data', 'processed')

    # Get Azure Storage config information from the 'local.settings.json' file
    local_settings_file_path = os.path.join(mini_project_root_folder, '..',
                                            'data-importing', 'azurefunctions',
                                            'local.settings.json')
    if os.path.exists(local_settings_file_path):
        with open(local_settings_file_path, 'r') as f:
            local_settings = json.load(f)

        account_name = local_settings['Values']['storage_account_name']
        account_key = local_settings['Values']['storage_account_key']
        container_name = local_settings['Values']['bike_availability_container_name']

    # execute pipeline
    logger.info("Pipeline execution about to start!")
    execute_pipeline(account_name, account_key, container_name,
                     raw_data_folder_path, processed_data_folder_path,
                     args.hard_delete, args.processed_data_base_name,
                     args.processed_files_base_name)
    logger.info("Pipeline execution completed!")
