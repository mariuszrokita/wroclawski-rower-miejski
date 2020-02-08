import argparse
import pandas as pd


print("Inside featurization.py file")

parser = argparse.ArgumentParser("featurization")
parser.add_argument("--input_featurization", type=str, help="Input data for featurization")
parser.add_argument("--output_featurization", type=str, help="Output data after featurization")
args = parser.parse_args()

print(f"Argument 1: {args.input_featurization}")
print(f"Argument 2: {args.output_featurization}")

# load data
df = pd.read_csv(args.input_featurization)

# show basic info about loaded data
print("Basic info about loaded data:")
print(df.info())

# TODO: create features


print("Writing a single data file to 'pipeline data' output folder to be consumed in another step")
df.to_csv(args.output_featurization, index=True)
print(f"Data saved to: {args.output_featurization}")
print("Done!")
