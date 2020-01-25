import argparse
import glob
import os

from util import print_nicely

print_nicely("Inside ingest.py file")

parser = argparse.ArgumentParser("ingest")
parser.add_argument("--input_data", type=str, help="Location of the raw input data")
parser.add_argument("--output_ingest", type=str, help="Output directory of the pipeline step")
parser.add_argument("--converted_data_location", type=str, help="Location of the csv data (after json file conversion)")
args = parser.parse_args()

print_nicely("Argument 1: %s" % args.input_data)
print_nicely("Argument 2: %s" % args.output_ingest)
print_nicely("Argument 3: %s" % args.converted_data_location)


input_data = args.input_data

print("Check what files are available in the source location..")
processed_files = []
for file in glob.glob(os.path.join(input_data, '*.*')):
    print(file)
    processed_files.extend([os.path.basename(file)])

# if not (args.output_ingest is None):
#     os.makedirs(args.output_ingest, exist_ok=True)
#     print_nicely("%s created" % args.output_ingest)

# # write to pipeline output location
# with open(os.path.join(args.output_ingest, 'processed_files.csv'), 'w') as f:
#     for item in processed_files:
#         f.write(f"{item}\n")

intermediate_data_path = os.path.join(args.converted_data_location, 'intermediate')
os.makedirs(intermediate_data_path, exist_ok=True)

with open(os.path.join(intermediate_data_path, 'processed_files.csv'), 'w') as f:
    for item in processed_files:
        f.write(f"{item}\n")
