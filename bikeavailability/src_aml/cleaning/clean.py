import argparse
import pandas as pd

from utils import detect_periods_with_missing_data


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

# STEP 1
# We do not have all data - available bikes on particular bike station
# in particular date and time - thus we assume there were no bikes to rent.
df = df.fillna(0)

# STEP 2
# Mark to delete all periods with missing data.
missing_data_periods = detect_periods_with_missing_data(df)

df['Delete'] = False
for period in missing_data_periods:
    period_start = period[0]
    period_end = period[1]
    idx = (df.index >= period_start) & (df.index <= period_end)
    df.loc[idx, 'Delete'] = True


# STEP 3
# Remove records marked for removal, and then the column itself
idx = df[df['Delete'] == True].index
df = df.drop(idx, axis=0)
df = df.drop(['Delete'], axis=1)

print("Writing a single data file to 'pipeline data' output folder to be consumed in another step")
df.to_csv(args.output_cleaning, index=True)
print(f"Data saved to: {args.output_cleaning}")
print("Done!")
