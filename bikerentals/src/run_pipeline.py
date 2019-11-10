import argparse
import json
import os
import sys
from sklearn.pipeline import make_pipeline

sys.path.insert(0, os.path.abspath(os.path.join(os.getcwd(), '..', '..')))

from bikerentals.src.data.pipeline import DataIngestion
from bikerentals.src.cleaning.pipeline import DataCleaning
from bikerentals.src.features.pipeline import DataFeaturization
from bikerentals.src.utils.argparse import str2bool
from bikerentals.src.utils.logging import logger


def execute_pipeline(account_name, account_key, bike_rental_data_container_name,
                     raw_data_folder_path, processed_data_folder_path, hard_delete,
                     save_base_name):
    """
    Scaffold end execute entire pipeline: data loading, cleaning and feature engineering.

    Parameters:
    * account_name - Azure Storage account name
    * account_key - Azure Storage account key
    * bike_rental_data_container_name - the name of storage container containing bike rental data
    * raw_data_folder_path - the path to a folder with raw data to be processed
    * processed_data_folder_path - the path to a folder where output file should be saved to
    * hard_delete - delete records marked for deletion
    * save_base_name - The output dataset file base name
    """

    data_prep_pipeline = make_pipeline(
        # data loading
        DataIngestion(account_name, account_key, bike_rental_data_container_name, raw_data_folder_path),
        # data cleaning
        DataCleaning(hard_delete),
        # data processing
        DataFeaturization()
    )

    # we're starting off our pipeline with no initiall data to process - we basically
    # need to ingest it first, and then process it.
    df = data_prep_pipeline.transform(None)

    # save as csv
    save_dirname = os.path.join(processed_data_folder_path, '{}.csv'.format(save_base_name))
    df.to_csv(save_dirname, index=False)
    logger.info(f"Data saved to: {save_dirname}")


if __name__ == "__main__":
    # Construct documentation for the script parameters
    parser = argparse.ArgumentParser(description='Execute pipelines for bike rentals data.')
    parser.add_argument('--hard-delete', help='Remove data while cleaning, otherwise mark records for removal',
                        type=str2bool, default=True, dest='hard_delete')
    parser.add_argument('--save-base-name', help='The output dataset file base name',
                        type=str, default='bike_rentals', dest='save_base_name')
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
        container_name = local_settings['Values']['storage_container_name']

    # execute pipeline
    logger.info("Pipeline execution about to start")
    execute_pipeline(account_name, account_key, container_name,
                     raw_data_folder_path, processed_data_folder_path,
                     args.hard_delete, args.save_base_name)
    logger.info("Pipeline execution completed")
