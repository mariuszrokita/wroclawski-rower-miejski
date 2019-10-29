import json
import os

import data.pipeline as data_loading_pipeline 
import features.pipeline as data_processing_pipeline


def execute_pipeline(account_name, account_key, bike_rental_data_container_name, 
                     raw_data_folder_path, processed_data_folder_path):
    
    # data loading
    df = data_loading_pipeline.execute(account_name, account_key, container_name, raw_data_folder_path)

    # data processing
    df = data_processing_pipeline.execute(df)

    # save as csv
    bike_rentals_filepath = os.path.join(processed_data_folder_path, 'bike_rentals.csv')
    df.to_csv(bike_rentals_filepath, index=False)
    print(f"Data saved to: {bike_rentals_filepath}")


if __name__ == "__main__":
    print("Pipeline execution started")

    # Set project root folder
    mini_project_root_folder = os.path.abspath(os.path.join(os.getcwd(), '..'))
    print(f'Root folder set to: {mini_project_root_folder}')

    # Set up paths to data folders
    raw_data_folder_path  = os.path.join(mini_project_root_folder, 'data', 'raw')
    processed_data_folder_path  = os.path.join(mini_project_root_folder, 'data', 'processed')

    # Get Azure Storage config information from the 'local.settings.json' file 
    local_settings_file_path = os.path.join(mini_project_root_folder, '..', 'data-importing', 'azurefunctions', 'local.settings.json')
    if os.path.exists(local_settings_file_path):
        with open(local_settings_file_path, 'r') as f:
            local_settings = json.load(f)
            
        account_name = local_settings['Values']['storage_account_name']
        account_key = local_settings['Values']['storage_account_key']
        container_name = local_settings['Values']['storage_container_name']

    # execute pipeline
    execute_pipeline(account_name, account_key, container_name, 
                     raw_data_folder_path, processed_data_folder_path)

    print("Pipeline execution completed")
