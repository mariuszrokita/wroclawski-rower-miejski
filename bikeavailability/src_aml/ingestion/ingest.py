import argparse
import os

from bike_availability_records import BikeAvailabilityRecords
from utils import save_dataframe


print("Inside ingest.py file")

parser = argparse.ArgumentParser("ingest")
parser.add_argument("--input_data_loc", type=str, help="Location of the raw input data")
parser.add_argument("--output_data_loc", type=str, help="Output directory of the pipeline step")
parser.add_argument("--intermediate_data_loc", type=str, help="Location of the csv data (after json file conversion)")
args = parser.parse_args()

print(f"Argument 1: {args.input_data_loc}")
print(f"Argument 2: {args.output_data_loc}")
print(f"Argument 3: {args.intermediate_data_loc}")

# Set paths
input_data_folder = args.input_data_loc
processed_data_folder = os.path.join(args.intermediate_data_loc, "intermediate")
print(f"processed_data_folder: {processed_data_folder}")
processed_data_base_name = "bike_availability_data"
processed_files_base_name = "processed_files"

# Make sure folder exists
os.makedirs(processed_data_folder, exist_ok=True)

# Read all new raw data json files and append it
data_df, processed_filenames_df = BikeAvailabilityRecords().load(
    raw_data_folderpath=input_data_folder,
    processed_data_folder_path=processed_data_folder,
    processed_data_base_name=processed_data_base_name,
    processed_files_base_name=processed_files_base_name)

print("Data summary and last records:")
print(data_df.info())
print(data_df.tail())


print("List of ingested raw data files:")
print(processed_filenames_df.info())
print(processed_filenames_df.tail())

print("Writing files to output data folder..")
save_dataframe(data_df, processed_data_folder, processed_data_base_name)
save_dataframe(processed_filenames_df, processed_data_folder, processed_files_base_name)
print("Done!")
