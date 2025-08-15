import pandas as pd
import matplotlib.pyplot as plt
import calendar

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

#POSTGRESQL

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

# CYCLE LENGTH
# This query calculates the average cycle length for each month
query = text("""
WITH streaks AS (
    SELECT
        "Date",
        "Menstruation",
        CASE
            WHEN "Menstruation" = 'MENSTRUATION' -- "today is a menstruation day"
                 AND LAG("Menstruation") OVER (ORDER BY "Date") != 'MENSTRUATION' -- "yesterday was NOT a menstruation day"
            THEN 1
            ELSE 0 -- mark period_start = 1 if today is menstruating and yesterday wasn’t, otherwise 0
        END AS period_start
    FROM cycle_tracking
),
cycle_starts AS (
    SELECT
        "Date" AS start_date,
        EXTRACT(MONTH FROM "Date") AS start_month,
        EXTRACT(YEAR FROM "Date") AS start_year, -- Extracts the month and year from the start date
        LEAD("Date") OVER (ORDER BY "Date") AS next_start_date -- Looks at the next period start date in the dataset
    FROM streaks
    WHERE period_start = 1 -- keeps only the first day of each period streak from streaks
)
SELECT
    start_year,
    start_month,
    ROUND(AVG(EXTRACT(DAY FROM (next_start_date - start_date))), 2) AS avg_cycle_length_days -- Gives number of days between one cycle start and the next
FROM cycle_starts
WHERE next_start_date IS NOT NULL -- Excludes the last row because it doesn’t have a next period to compare to
GROUP BY start_year, start_month
ORDER BY start_year, start_month;
""")

with engine.connect() as conn:
    df_cycle_length = pd.read_sql(query, conn)
    print(df_cycle_length)


# MENSTRUATION DAYS
# This query calculates the number of menstruation days in each cycle
query = text("""
WITH streaks AS (
    SELECT
        "Date",
        "Menstruation",
        CASE
            WHEN "Menstruation" = 'MENSTRUATION'
                 AND LAG("Menstruation") OVER (ORDER BY "Date") != 'MENSTRUATION'
            THEN 1
            ELSE 0
        END AS period_start
    FROM cycle_tracking
),
cycle_groups AS (
    SELECT
        "Date",
        "Menstruation",
        SUM(period_start) OVER (ORDER BY "Date") AS cycle_id
    FROM streaks
)
SELECT
    cycle_id,
    MIN("Date") AS period_start_date,
    COUNT(*) FILTER (WHERE "Menstruation" = 'MENSTRUATION') AS menstruation_days
FROM cycle_groups
GROUP BY cycle_id
ORDER BY cycle_id;            
""")

with engine.connect() as conn:
    df_menstruation_days = pd.read_sql(query, conn)
    print(df_menstruation_days)

# # LUTEAL PHASE AVERAGE TEMPERATURE  
# # This query calculates the average temperature during the luteal phase of each cycle
# query = text("""
# WITH streaks AS (
#     SELECT
#         "Date",
#         "Menstruation",
#         CASE
#             WHEN "Menstruation" = 'MENSTRUATION'
#                  AND LAG("Menstruation") OVER (ORDER BY "Date") != 'MENSTRUATION'
#             THEN 1
#             ELSE 0
#         END AS period_start
#     FROM cycle_tracking
# ),
# cycle_starts AS (
#     SELECT
#         "Date" AS start_date,
#         LEAD("Date") OVER (ORDER BY "Date") AS next_start_date
#     FROM streaks
#     WHERE period_start = 1
# ),
# luteal_phase AS (
#     SELECT
#         ct."Date",
#         ct."Temperature",
#         cs.start_date,
#         cs.next_start_date
#     FROM cycle_tracking ct
#     JOIN cycle_starts cs
#       ON ct."Date" >= cs.next_start_date - INTERVAL '14 days'
#      AND ct."Date" < cs.next_start_date
# )
# SELECT
#     start_date,
#     ROUND(AVG("Temperature"::numeric), 2) AS avg_luteal_temp
# FROM luteal_phase
# GROUP BY start_date
# ORDER BY start_date;
# """)
# with engine.connect() as conn:
#     df_luteal_temp = pd.read_sql(query,conn)
#     print(df_luteal_temp)


# visualizations

#bar chart showing the average cycle length for each month

# first convert numeric months to abbreviated month names
df_cycle_length['Month_Abbr'] = df_cycle_length['start_month'].astype(int).apply(lambda x: calendar.month_abbr[x])

# sort by month
df_cycle_length = df_cycle_length.sort_values('start_month')

# bar chart showing the average cycle length for each month
plt.figure(figsize=(10,6))
plt.bar(df_cycle_length['Month_Abbr'], df_cycle_length['avg_cycle_length_days'], color='pink')
plt.title("Average Cycle Length per Month")
plt.xlabel("Month")
plt.ylabel("Average Cycle Length (Days)")
plt.ylim(0, max(df_cycle_length['avg_cycle_length_days']) + 5)  # optional padding
plt.show()


# bar chart showing the number of menstruation days in each cycle
plt.figure(figsize=(10,6))
plt.bar(df_menstruation_days['cycle_id'], df_menstruation_days['menstruation_days'], color='purple')
plt.title("Number of Menstruation Days in Each Cycle")
plt.xlabel("Cycle ID")
plt.ylabel("Number of Menstruation Days")
plt.xticks(rotation=45)
plt.ylim(0, max(df_menstruation_days['menstruation_days']) + 1)  # optional padding
plt.xlim(0, max(df_menstruation_days['cycle_id']) + 1)  # optional padding
plt.show()




