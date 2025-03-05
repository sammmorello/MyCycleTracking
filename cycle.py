import pandas as pd
import matplotlib.pyplot as plt

# import data
df = pd.read_csv('/Users/sammorello/Projects/CycleTracking/naturalcyclesdata/Daily Entries.csv')

# display first few rows
print(df.head())

# display and drop empty columns
empty_cols = df.columns[df.isna().all()]
print("completely empty columns:")
print(empty_cols)

# drop columns where all values are NaN
df = df.dropna(axis=1, how="all")  

# cols empty + removed : 'Pregnancy test', 'Source', 'Cervical Mucus Consistency','Cervical Mucus Quantity', 'Covid Test'

# drop specific columns
df.drop(columns=["Skipped", "Had sex", "Sex Type", "Libido"],  inplace=True)

default_values = {
    "Temperature": "emp Not Recorded",
    "Menstruation": "Not Menstruating",
    "LH test": "LH Not Tested",
    "Had sex": "No",
    "Notes": "None",
    "Menstruation Quantity": "Not Recorded",
    "Data Flag": "None Recorded"
}
df.fillna(default_values, inplace=True)

print(df)
