import pandas as pd
import matplotlib.pyplot as plt

# import data
df = pd.read_csv('/Users/sammorello/Projects/CycleTracking/naturalcyclesdata/Daily Entries.csv')

# display first few rows
print("First few rows of data:")
print(df.head())

# display and drop empty columns
empty_cols = df.columns[df.isna().all()]
print("Completely empty columns:")
print(empty_cols)

# drop columns where all values are NaN
df = df.dropna(axis=1, how="all")  

# cols empty + removed : 'Pregnancy test', 'Source', 'Cervical Mucus Consistency','Cervical Mucus Quantity', 'Covid Test'

# drop specific columns
df.drop(columns=["Skipped", "Had sex", "Sex Type", "Libido"],  inplace=True)

default_values = {
    "Temperature": "Temp Not Recorded",
    "Menstruation": "Not Menstruating",
    "LH test": "LH Not Tested",
    "Had sex": "No",
    "Notes": "None",
    "Menstruation Quantity": "Not Recorded",
    "Data Flag": "None Recorded"
}
df.fillna(default_values, inplace=True)

# remove entries
df["Data Flag"] = df["Data Flag"].str.replace(r"\b(SEX_MASTURBATION|SEX_ORAL)\b,?\s*", "", regex=True)
df["Data Flag"] = df["Data Flag"].replace("", "No Data")

# convert to datetime format
df["Date"] = pd.to_datetime(df["Date"], errors="coerce")

# print specific day
print(df[df["Date"] == "2023-12-17"])

# checking data flag output
print(df.loc[df["Date"] == "2023-12-17", "Data Flag"])

# filter data to 1 year of entries
start_date = "2023-10-11"
end_date = "2024-10-11"
df_filtered = df[(df["Date"] >= start_date) & (df["Date"] <= end_date)]

# check filtered range
print(df_filtered["Date"].min(), df_filtered["Date"].max())  

print("Cleaned data:")
print(df)

# EDA

# number of days menstruating
print(df_filtered["Menstruation"].value_counts())

# print(df_filtered)

# conecting postgres
from sqlalchemy import create_engine

DATABASE_URL = "postgresql://postgres:pumpkinpostgre@localhost:5432/cycletracking_db"

# creating database connection
engine = create_engine(DATABASE_URL)
print("connected to postgres (: )")

df_filtered.to_sql("cycle_tracking", engine, if_exists="replace", index=False)

print("data inserted into postgres successfully (:")