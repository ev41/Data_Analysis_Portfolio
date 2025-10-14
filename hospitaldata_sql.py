# --- importing necessary libraries / modules
from sqlalchemy import create_engine
from sqlalchemy import text
import os
import pandas as pd
from dotenv import load_dotenv
load_dotenv('hospital_env_vars.env')

# --- Establishing necessary variables ---
print("\n\n\n\n")
dbname = os.getenv("pg_db_name")
user = os.getenv("pg_user")
password = os.getenv("pg_password")
host = os.getenv("pg_host")
port = 5432
#filepath = os.getenv("csv_path")
filepath = "/Users/ev/jazz/kaggle/hospital_info/HospInfo.csv"
table_name = "hospinfo_sql_practice"


# --- Conecting to Postgres ---
engine = create_engine(f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{dbname}")


# --- Loading the CSV into a Pandas DataFrame ---
print(f"Loading {filepath} into {table_name}")
df = pd.read_csv(filepath)


# --- Pushing the df to Postgres ---
df.to_sql(table_name, engine, if_exists="replace", index=True)
print(f"Loaded {len(df)} rows into {table_name}")


# --- Getting list of column names ---
# query = text(f"""
# SELECT 
#     column_name
# FROM information_schema.columns
# WHERE table_name = '{table_name}'
# ;
# """)


# --- Ranking Cities with highest hospital count ---
# query = text(f"""
# SELECT 
#     "City",
#     COUNT(*) AS hospital_count
# FROM {table_name}
# GROUP BY "City"
# ORDER BY hospital_count DESC
# LIMIT 10
# ;
# """)


# --- Hospitals with no overall rating assigned ---
# query = text(f"""
# SELECT "Hospital Name", "City", "State"
# FROM {table_name}
# WHERE "Hospital overall rating" = 'Not Available'
# ORDER BY "State" ASC
# ;
# """)


# --- Avg Hospital Ratings per State (exluding NaN values) ---
# query = text(f"""
# SELECT ROUND(AVG(CAST("Hospital overall rating" AS INTEGER)), 1) AS Avg_Rating, "State"
# FROM {table_name}
# WHERE "Hospital overall rating" <> 'Not Available'
# GROUP BY "State"
# ORDER BY Avg_Rating DESC
# ;
# """)

# --- % of hospitals with emergency services by state
# query = text(f"""
# SELECT ROUND(100 * SUM(CASE WHEN "Emergency Services" = 'True' THEN 1 ELSE 0 END) / COUNT(*), 1) AS "pct_emergency_services", "State" 
# FROM {table_name}
# GROUP BY "State"
# ORDER BY "pct_emergency_services" DESC
# ;
# """)

# --- Hospital Ownership distribution nationwide ---
# query = text(f"""
# SELECT "Hospital Ownership", COUNT(*) AS "num_hospitals"
# FROM {table_name}
# GROUP BY "Hospital Ownership"
# ORDER BY "num_hospitals" DESC
# ;
# """)


# --- Comparing avg rating by hospital type (Acute Care, Critical Access, etc) --- 
# query = text(f"""
# SELECT ROUND(AVG(CAST("Hospital overall rating" AS INTEGER)), 1) as "avg_rating", "Hospital Type"
# FROM {table_name}
# WHERE "Hospital overall rating" <> 'Not Available'
# GROUP BY "Hospital Type"
# ORDER BY "avg_rating" DESC
# ;
# """)


# --- States with mortality rating worse than national average --- 
# query = text(f"""
# SELECT 
# "State", 
# SUM(CASE WHEN "Mortality national comparison" = 'Below the national average' THEN 1 ELSE 0 END) AS hospitals_worse_than_national_avg_count
# FROM {table_name}
# GROUP BY "State"
# HAVING SUM(CASE WHEN "Mortality national comparison" = 'Below the national average' THEN 1 ELSE 0 END) > 0
# ORDER BY hospitals_worse_than_national_avg_count DESC
# ;
# """)


# --- Finding 'best' hospital in each state ---
# query = text(f"""
# SELECT
#     h."State", 
#     h."Hospital Name", 
#     h."Hospital overall rating"
# FROM {table_name} AS h
# WHERE "Hospital overall rating" ~ '^[0-9]+$'
# AND CAST(h."Hospital overall rating" AS INTEGER) = (
#     SELECT 
#         MAX(CAST(s."Hospital overall rating" AS INTEGER))
#     FROM {table_name} AS s
#     WHERE h."State" = s."State"
#     AND s."Hospital overall rating" ~ '^[0-9]+$'
#     )
# ORDER BY h."State"
# ;
# """)


# --- KPI Style summary: For each state, show: total hospitals, 
    # % with EHR adoption, avg rating, and % with emergency services. ---
query = text(f"""
SELECT
    "State",
    COUNT(*) AS total_hospitals,
    ROUND(
        SUM(CASE WHEN "Meets criteria for meaningful use of EHRs" = 'True' THEN 1 ELSE 0 END) * 100 / COUNT(*), 1) AS perc_w_EHR_Adoption,
    ROUND(
        AVG(CASE WHEN "Hospital overall rating" ~ '^[0-9]+$' THEN CAST("Hospital overall rating" AS INTEGER) END), 2) AS avg_rating,
    ROUND(
        SUM(CASE WHEN "Emergency Services" = 'True' THEN 1 ELSE 0 END) * 100 / COUNT(*), 1) AS perc_emergency_services
        
    
FROM {table_name}
WHERE "State" NOT IN ('AS', 'MP', 'GU', 'VI', 'PR')
GROUP BY "State"
ORDER BY "avg_rating" DESC, "perc_w_ehr_adoption" DESC, "perc_emergency_services"
;
""")

# query = text(f"""
# SELECT DISTINCT "Emergency Services"
# FROM {table_name}
# ;
# """)
# columns i want: 'Emergency Services', 'Meets criteria for meaningful use of EHRs' ('True' or 'None'), 'Hospital overall rating'


# --- Running the query ---
with engine.connect() as conn:
    conn.commit()
    result = conn.execute(query)
    headers = result.keys()
    output = result.fetchall()


# --- Printing the results! ---
print("\n\nOutput:\n\n")
print(headers)
for row in output:
    print(row)



print("\n\n\n\n")