import argparse
import json
import os
import sys
sys.path.insert(0, os.path.abspath(os.path.join(os.getcwd(), '..', '..')))

import bikerentals.src.data.pipeline as data_loading_pipeline
import bikerentals.src.cleaning.pipeline as data_cleaning_pipeline
import bikerentals.src.features.pipeline as data_processing_pipeline


def execute_pipeline(account_name, account_key, bike_rental_data_container_name,
                     raw_data_folder_path, processed_data_folder_path, hard_delete):
    """
    Scaffold end execute entire pipeline: data loading, cleaning and feature engineering.

    Parameters:
    * account_name - Azure Storage account name
    * account_key - Azure Storage account key
    * bike_rental_data_container_name - the name of storage container containing bike rental data
    * raw_data_folder_path - the path to a folder with raw data to be processed
    * processed_data_folder_path - the path to a folder where output file should be saved to
    * hard_delete - delete records marked for deletion
    """

    # data loading
    df = data_loading_pipeline.execute(account_name, account_key,
                                       bike_rental_data_container_name,
                                       raw_data_folder_path)

    # data cleaning
    df = data_cleaning_pipeline.execute(df, hard_delete)

    # data processing
    df = data_processing_pipeline.execute(df)

    # save as csv
    save_dirname = os.path.join(processed_data_folder_path, 'bike_rentals.csv')
    df.to_csv(save_dirname, index=False)
    print(f"Data saved to: {save_dirname}")


def str2bool(v):
    if isinstance(v, bool):
        return v
    if v.lower() in ('yes', 'true', 't', 'y', '1'):
        return True
    elif v.lower() in ('no', 'false', 'f', 'n', '0'):
        return False
    else:
        raise argparse.ArgumentTypeError('Boolean value expected.')


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Execute pipelines for bike rentals data.')
    parser.add_argument('--hard-delete', help='Remove data while cleaning, otherwise mark records for removal',
                        type=str2bool, default=True, dest='hard_delete')
    args = parser.parse_args()

    print("Pipeline execution started")

    # Set project root folder
    mini_project_root_folder = os.path.abspath(os.path.join(os.getcwd(), '..'))
    print(f'Root folder set to: {mini_project_root_folder}')

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
    execute_pipeline(account_name, account_key, container_name,
                     raw_data_folder_path, processed_data_folder_path,
                     args.hard_delete)

    print("Pipeline execution completed")
