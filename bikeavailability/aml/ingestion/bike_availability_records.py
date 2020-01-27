import json
import glob
import os
import pandas as pd

from utils import load_dataframe


class BikeAvailabilityRecords:

    def __init__(self):
        self.PROCESSED_DATA_COLUMNS = ['Timestamp', 'Available Bikes', 'Bike Station Number']
        self.PROCESSED_FILES_COLUMNS = ['Filename']

    def load(self, raw_data_folderpath: str, processed_data_folder_path: str,
             processed_data_base_name: str, processed_files_base_name: str) -> (pd.DataFrame, pd.DataFrame):
        # Explanation:
        # In order to save time and due to immutability of data in json files, we can read
        # previously loaded data and process only new json files.

        # Load list of files already processed
        processed_filenames_df = load_dataframe(processed_data_folder_path,
                                                processed_files_base_name,
                                                self.PROCESSED_FILES_COLUMNS)

        # Get list of all available raw data files
        available_raw_files = self._get_available_raw_files_list(raw_data_folderpath)

        # Determine what new json files we should load data from
        processed_filenames = processed_filenames_df[self.PROCESSED_FILES_COLUMNS[0]]
        filenames_to_load = set(available_raw_files) - set(processed_filenames)

        # Just load new data and clean it
        raw_data_df = self._load_files(raw_data_folderpath, filenames_to_load)
        clean_data_df = self._clean(raw_data_df)

        # Load previous data and combine it with fresh data
        processed_data_df = load_dataframe(processed_data_folder_path,
                                           processed_data_base_name,
                                           self.PROCESSED_DATA_COLUMNS,
                                           parse_dates=[self.PROCESSED_DATA_COLUMNS[0]])
        data_df = processed_data_df.append(clean_data_df)
        data_df = data_df.sort_values(by=data_df.columns[0])

        # Update list of processed files
        newly_processed_filenames_df = pd.DataFrame(data=filenames_to_load,
                                                    columns=self.PROCESSED_FILES_COLUMNS)
        processed_filenames_df = processed_filenames_df.append(newly_processed_filenames_df)
        processed_filenames_df = processed_filenames_df.sort_values(by=processed_filenames_df.columns[0])

        return data_df, processed_filenames_df

    def _get_available_raw_files_list(self, raw_data_folderpath):
        # return filenames only (without full paths)
        return sorted([os.path.basename(x) for x in glob.glob(os.path.join(raw_data_folderpath, '*.json'))])

    def _load_files(self, raw_data_folderpath, filenames_to_load):
        RAW_DF_COLUMNS = ['timestamp', 'bikes', 'number']

        big_frame = pd.DataFrame(data=[], columns=RAW_DF_COLUMNS)

        for filename in filenames_to_load:
            # load json file
            print(f"Loading data from file: {filename}")
            with open(os.path.join(raw_data_folderpath, filename)) as handle:
                data = json.load(handle)

            if data['success'] is True:
                # make dataframe from a single json
                records_df = pd.DataFrame(data=data['result']['records'])
                records_df['timestamp'] = data['datetime']
                # reorder columns
                records_df = records_df[RAW_DF_COLUMNS]
                big_frame = big_frame.append(records_df)
        return big_frame

    def _clean(self, df):
        # remove fractional part of seconds
        df['timestamp'] = df['timestamp'].astype('datetime64[s]')

        # Make it convenient for the English-speaking audience
        df.columns = self.PROCESSED_DATA_COLUMNS

        return df
