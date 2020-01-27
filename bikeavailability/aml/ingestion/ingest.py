import argparse
import glob
import os


from bike_availability_records import BikeAvailabilityRecords
from utils import print_nicely, save_dataframe


print_nicely("Inside ingest.py file")

parser = argparse.ArgumentParser("ingest")
parser.add_argument("--input_data_loc", type=str, help="Location of the raw input data")
parser.add_argument("--output_data_loc", type=str, help="Output directory of the pipeline step")
parser.add_argument("--intermediate_data_loc", type=str, help="Location of the csv data (after json file conversion)")
args = parser.parse_args()

print_nicely("Argument 1: %s" % args.input_data_loc)
print_nicely("Argument 2: %s" % args.output_data_loc)
print_nicely("Argument 3: %s" % args.intermediate_data_loc)

input_data_folder = args.input_data_loc
processed_data_folder = os.path.join(args.intermediate_data_loc, "intermediate")
processed_data_base_name = "bike_availability_data"
processed_files_base_name = "processed_files"

os.makedirs(processed_data_folder, exist_ok=True)

bar = BikeAvailabilityRecords()
data_df, processed_filenames_df = bar.load(
    raw_data_folderpath=input_data_folder,
    processed_data_folder_path=processed_data_folder,
    processed_data_base_name=processed_data_base_name,
    processed_files_base_name=processed_files_base_name)

print("Data summary:")
print(data_df.info())
print(data_df)

print("List of ingested raw data files:")
print(processed_filenames_df.info())
print(processed_filenames_df)

print("Writing files to output data folder..")
save_dataframe(data_df, processed_data_folder, processed_data_base_name)
save_dataframe(processed_filenames_df, processed_data_folder, processed_files_base_name)
print("Done!")
