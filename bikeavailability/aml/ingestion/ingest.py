import argparse
import glob
import os

from util import print_nicely

print_nicely("In ingest.py")
print_nicely("As a data scientist, this is where I use my training code.")

parser = argparse.ArgumentParser("train")

parser.add_argument("--input_data", type=str, help="input data")
parser.add_argument("--output_ingest", type=str, help="output_ingest directory")

args = parser.parse_args()

print_nicely("Argument 1: %s" % args.input_data)
print_nicely("Argument 2: %s" % args.output_ingest)

if not (args.output_ingest is None):
    os.makedirs(args.output_ingest, exist_ok=True)
    print_nicely("%s created" % args.output_ingest)


input_data = args.input_data

processed_files = []

print("Check what files are available in the source location..")
for file in glob.glob(os.path.join(input_data, '*.*')):
    print(file)
    processed_files.extend([os.path.basename(file)])


# write to output location
with open(os.path.join(args.output_ingest, 'processed_files.txt'), 'w') as f:
    for item in processed_files:
        f.write(f"{item}\n")
