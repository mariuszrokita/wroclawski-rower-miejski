import argparse
import pandas as pd

print("Inside clean.py file")

parser = argparse.ArgumentParser("cleaning")
parser.add_argument("--input_cleaning", type=str, help="Input data for cleaning")
parser.add_argument("--output_cleaning", type=str, help="Output data after cleaning")
args = parser.parse_args()

print(f"Argument 1: {args.input_cleaning}")
print(f"Argument 2: {args.output_cleaning}")

# load data
bike_availability_df = pd.read_csv(args.input_cleaning)
bike_availability_df = bike_availability_df.set_index('Timestamp')

# show basic info about loaded data
print("Basic info about loaded data:")
print(bike_availability_df.info())

#
df = bike_availability_df.pivot_table(
    values='Available Bikes',
    index='Timestamp',
    columns='Bike Station Number')

# change column name type (from int to string)
df.columns = df.columns.astype(str)

# We do not have all data - available bikes on particular bike station
# in particular date and time - thus we assume there were no bikes to rent.
df = df.fillna(0)
print(df)

print("Writing a single data file to 'pipeline data' output folder to be consumed in another step")
df.to_csv(args.output_cleaning, index=True)
print(f"Data saved to: {args.output_cleaning}")
print("Done!")
