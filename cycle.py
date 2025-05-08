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
from sqlalchemy import create_engine, text # text: allows you to write raw SQL queries as text objects

# lgit ignore
import os
from dotenv import load_dotenv
load_dotenv()
api_key = os.getenv('api_key')
DATABASE_URL = api_key

# creating database connection
engine = create_engine(DATABASE_URL)
print("connected to postgres (: )")

# insert all data into cycle_tracking data
df_filtered.to_sql("cycle_tracking",  engine, if_exists="replace", index=False)

# create new df to exclude temp
df_notemp = df_filtered.drop(columns=["Temperature"])
df_notemp.to_sql("cycle_notemp",  engine, if_exists="replace", index=False)

print("HEREEEEEEEE")
print(df_filtered.head())

# create new df to only inlcude date & temp
df_datetemp = df_filtered.drop(columns=["Menstruation", "LH test", "Notes", "Data Flag", "Menstruation Quantity"])
df_datetemp.to_sql("cycle_datetemp",  engine, if_exists="replace", index=False)

print("data inserted into postgres successfully (:")

# define SQL query
# using text to write raw SQL query as text object
query = text("""      
    SELECT * FROM cycle_tracking 
    WHERE "Date" BETWEEN '2024-01-01' AND '2024-05-01' 
    AND "Menstruation" = 'MENSTRUATION'
""")

# execute the query and fetch results
with engine.connect() as connection:
    result = connection.execute(query)
    #for row in result: # iterates through the results and prints each row
        #print(row)     # prints all output
    for row in result:  # prints only date[0] & temp[1] output
        date = row[0]
        temp = row[1]
        print(f"Date: {date}, Temperature: {temp}")

print('\nPrinting only Temps from cycle_notemp & cycle_date temp!!!\n')

# define SQL query
# using text to write raw SQL query as text object
query = text("""      
    -- to pull dates menstruating from cycle_notemp and then pull temp from cycle_datetemp
SELECT cycle_notemp."Date", cycle_datetemp."Temperature" 
FROM cycle_notemp 
JOIN cycle_datetemp
  ON cycle_notemp."Date" = cycle_datetemp."Date" 
WHERE cycle_notemp."Date" BETWEEN '2024-01-01' AND '2024-05-01' AND cycle_notemp."Menstruation" = 'MENSTRUATION';
""")

# execute the query and fetch results
with engine.connect() as connection:
    result = connection.execute(query)
    #for row in result: # iterates through the results and prints each row
        #print(row)     # prints all output
    for row in result:  # prints only date[0] & temp[1] output
        temp = row[1]
        print(f"Temperature: {temp}")
