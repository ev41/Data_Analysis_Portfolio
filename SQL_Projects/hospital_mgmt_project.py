#READ ME: This project involved setting up a local SQL server and creating a database from a publically available 
#        deidentified healthcare dataset on Kaggle.com. I used Postgresql to run queries on hospital mgmt data. 
#        The focus of this project was to practice basic, intermediate, and some advanced SQL functions in a field
#        relevant to my professional experience and interests. Keeping the dust off, so to speak. 

# --- importing necessary libraries / modules
from sqlalchemy import create_engine
from sqlalchemy import text
import os
import pandas as pd
from dotenv import load_dotenv

load_dotenv('hosp_mgmt_vars.env')


# --- Establishing necessary variables ---
print("\nCSV UPLOAD STATUS:")
dbname = os.getenv("pg_db_name")
user = os.getenv("pg_user")
password = os.getenv("pg_password")
host = os.getenv("pg_host")
port = 5432
# filepath = "/Users/ev/jazz/data_analysis_portfolio/hosp_mgmt_vars.env/HospInfo.csv"
table_name = "hosp_mgmt_sql_practice"


# --- this is the sqlalchemy engine to connect to postgresql
engine = create_engine(f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{dbname}")


data_dir = "data"
csv_files = {
    "appointments" : "appointments.csv",
    "billing" : "billing.csv",
    "doctors" : "doctors.csv",
    "patients" : "patients.csv",
    "treatments" : "treatments.csv"
}


# --- Loading each CSV and uploading to PostgresSQL ---
for table_name, filename in csv_files.items(): 
    filepath = os.path.join(data_dir, filename)
    print(f"\nLoading {filename} into {table_name}:")

    # --- reading the CSV into a pandas df ---
    df = pd.read_csv(filepath)

    # --- standardizing column names to make parsing easier in SQL --- 
    df.columns = [col.strip().lower().replace(" ", "_") for col in df.columns]

    # --- loading into postgres and replacing if the table already exists --- 
    df.to_sql(table_name, engine, if_exists="replace", index=False)

print("\n\n---ALL CSV'S SUCCESSFULLY LOADED INTO POSTGRESQL YAYYYYYYY---\n\n")



# --- Getting list of column names for each table ---
# with engine.connect() as conn:
#     for table in csv_files.keys():
#         print(f"\nColumns for table {table}:")
#         query = text(f"""
#             SELECT column_name
#             FROM information_schema.columns
#             WHERE table_schema = 'public'
#                 AND table_name = {'table_name'}
#             ORDER BY ordinal_position;
#         """)
#         result = conn.execute(query)
#         cols = [row[0] for row in result.fetchall()]
#         print(cols)



# --- THESE ARE THE PRIMARY KEYS AND FOREIGN KEYS FOR EACH TABLE 
"""
appointments: PK: appointment_id; FK: patient_id / treatment_id / doctor_id / bill_id
billing: PK: bill_id; FK: patient_id / treatment_id / doctor_id / appointment_id
treatments: PK: treatment_id; FK: patient_id / bill_id / doctor_id / appointment_id
doctors: PK: doctor_id; FK: patient_id / bill_id / treatment_id / appointment_id
patients: PK: patient_id; FK: doctor_id / bill_id / treatment_id / appointment_id

"""
# ------ PHRASE 1: GETTING FAMILIAR WITH MY DATA ----- 
# --- finding patients older than 60yrs old ---
# query = text(f"""
#     SELECT date_of_birth
#     FROM patients
#     WHERE date_of_birth < '1964-10-14'
#     ORDER BY date_of_birth DESC
# ;
# """)


# --- finding doctors specializing in pediatrics ---
# query = text(f"""
#     SELECT first_name, last_name
#     FROM doctors
#     WHERE specialization = 'Pediatrics'
# ;
# """)


# --- finding all appointments that occured in may 2023 ---
# query = text(f"""
#     SELECT *
#     FROM appointments
#     WHERE appointment_date BETWEEN '2023-05-1' AND '2023-05-31'
#     ORDER BY appointment_date DESC
# ;
# """)


# --- Listing all appointments that are still scheduled (not completed or cancelled) ---
# query = text(f"""
#     SELECT *
#     FROM appointments
#     WHERE status NOT IN ('Completed', 'No-show', 'Cancelled')

# ;
# """)


# --- Getting the number of patients per gender ---
# query = text(f"""
# SELECT COUNT(*) AS total_patients, gender
# FROM patients
# GROUP BY gender
# ;
# """)


# --- finding patients whose email ends with '...gmail.com' --- 
# query = text(f"""
# SELECT *
# FROM patients
# WHERE email LIKE '%gmail.com'
# ;
# """)




# --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------




# --- PHASE 2: 
# --- Count total number of appts per doctor ---
# query = text(f"""
#     SELECT COUNT(*) AS total_appointments, d.first_name, d.last_name
#     FROM appointments AS a
#     LEFT JOIN doctors AS d
#         ON d.doctor_id = a.doctor_id
#     GROUP BY d.first_name, d.last_name
#     ORDER BY COUNT(*) DESC
# ;
# """)


# --- finding avg treatment cost for each treatment type --- 
# query = text(f"""
#     SELECT ROUND(CAST(AVG(cost) AS INTEGER), 2) AS avg_cost, treatment_type
#     FROM treatments
#     GROUP BY treatment_type
# ;
# """)


# --- finding number of completed appointments by doctor ---
# query = text(f"""
# SELECT COUNT(a.*), d.first_name || ' ' || d.last_name AS doctor_name, d.doctor_id
# FROM appointments AS a
# LEFT JOIN doctors AS d
# ON a.doctor_id = d.doctor_id
# WHERE a.status = 'Completed'
# GROUP BY d.doctor_id, doctor_name
# ;
# """)


# --- Average treatment cost by treatment type --- 
# query = text(f"""
# SELECT CAST(AVG(cost) AS INTEGER) AS avg_cost_thousands, treatment_type
# FROM treatments
# GROUP BY treatment_type
# ORDER BY avg_cost_thousands DESC
# ;
# """)


# --- Identifying patients who have spent more than $5,000 total --- 
# query = text(f"""
# SELECT p.patient_id, b.amount
# FROM patients AS p
# LEFT JOIN billing AS b
# ON p.patient_id = b.patient_id
# WHERE b.amount > 2500
# ORDER BY b.amount DESC
# LIMIT 10
# ;
# """)


# --- Using a CASE statement to categorize doctors based on experience --- 
# query = text(f"""
# SELECT first_name || ' ' || last_name AS doctor_name, years_experience,
#     CASE
#         WHEN years_experience BETWEEN 0 AND 5 THEN 'Junior' 
#         WHEN years_experience BETWEEN 5 AND 15 THEN 'Average' 
#         ELSE 'Senior'
#         END AS experience_level
# FROM doctors
# ORDER BY years_experience DESC
# ;
# """)


# --- Finding patients with unpaid or failed bills --- NOT CAPITALIZING COMMANDS BECAUSE MY CAPS LOCK BUTTON IS MALFUNCTIONING
# query = text(f"""
# SELECT p.first_name || ' ' || p.last_name as patient_name, b.payment_status, ROUND(CAST(b.amount AS INTEGER), 1)
# FROM patients as p
# left join billing as b
# on p.patient_id = b.patient_id
# where b.payment_status = 'Pending' OR b.payment_status = 'Failed'
# ORDER BY b.payment_status DESC
# ;
# """)


# --- Counting how many patients each doctor has treated --- 
# query = text(f"""
# SELECT COUNT(a.doctor_id) AS num_treated, d.first_name || ' ' || d.last_name as doctor_name, d.doctor_id
# FROM appointments AS a
# left join doctors as d
# on a.doctor_id = d.doctor_id
# GROUP BY doctor_name, d.doctor_id
# ORDER BY num_treated DESC
# ;
# """)




# --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------




# --- PHASE 3: HARDER PROBLEMS / focusing on CTE's, window functions, subqueries, and data cleaning functions --- 
# --- Finding the three most expensive treatments overall --- 
# query = text(f"""
# select treatment_type, cost
# FROM treatments
# ORDER BY cost DESC
# LIMIT 3
# ;
# """)


# --- Finding each doctor's rank by total billed amount --- 
# query = text(f"""
# SELECT d.first_name || ' ' || d.last_name as doctor_name, 
#     SUM(b.amount) AS total_bill_amt,
#     RANK () OVER (ORDER BY SUM(b.amount) DESC)
# FROM appointments as A
# INNER JOIN treatments as t ON a.appointment_id = t.appointment_id
# INNER JOIN doctors as d ON a.doctor_id = d.doctor_id
# INNER JOIN billing as b ON t.treatment_id = b.treatment_id
# GROUP BY doctor_name
# ;
# """)


# --- Using CTEs to find patients who had more than 3 appointments --- 
# query = text(f"""
# # WITH patient_appointment_count AS (

# #     SELECT patient_id, count(*) as total_appts
# #     FROM appointments
# #     GROUP BY patient_id
# # )
# # SELECT p.patient_id, p.first_name || ' ' || p.last_name as patient_name, pa.total_appts
# # FROM patients as p
# # left join patient_appointment_count as pa on p.patient_id = pa.patient_id
# # WHERE pa.total_appts > 3
# # ORDER BY pa.total_appts DESC
# # ;
# # """)


# # --- calculating the average time between registration and first appointment --- *** issues with data quality 
# query = text(f"""
# SELECT 
#     p.first_name || ' ' || p.last_name as patient_name,
#     ROUND(AVG(a.appointment_date::date - p.registration_date::date), 1) AS avg_days_to_first_appt
# FROM patients as p
# left join appointments as a 
# on p.patient_id = a.patient_id
# WHERE a.appointment_date = (
#     select MIN(a2.appointment_date)
#     from appointments as a2
#     where a2.patient_id = a.patient_id)

# group by patient_name, p.registration_date
# order by avg_days_to_first_appt DESC
# ;
# """)


# --- same one as above but using CTE instead of a subquery --- 
# query = text(f"""
# WITH first_appointment AS (
#     SELECT patient_id, MIN(appointment_date::date) as first_appt
#     FROM appointments
#     group by patient_id
# )

# SELECT 
#     p.patient_id, 
#     p.first_name || ' ' || p.last_name as full_name,
#     ROUND(AVG(first_appt - CAST(p.registration_date AS DATE)), 0) AS days_to_first_appt
# FROM patients as p
# left join first_appointment as fa
# on fa.patient_id = p.patient_id
# WHERE first_appt >= CAST(p.registration_date AS DATE)
# GROUP BY p.patient_id, full_name
# ORDER BY days_to_first_appt DESC 
# ;
# """)


# --- Data cleaning example - Cleaning patient contact numbers --- 
# query = text(f"""
# SELECT 
#     first_name || ' ' || last_name as patient_name,
#     contact_number as OG_Contact_Info,
#     COALESCE(
#         NULLIF(TRIM(BOTH '' FROM contact_number::text), 'No Contact Info')) AS cleaned_contact_info
# FROM patients
# ORDER BY patient_name
# ;
# """)


# --- finding patients with multiple unpaid bills AND their total pending balance ---
# query = text(f"""

# WITH unpaid_bill_amount AS (
# SELECT 
#     COUNT(*) AS unpaid_bills,
#     patient_id,
#     payment_status
# FROM billing
# WHERE payment_status IN ('Pending')
# GROUP BY patient_id, payment_status
# )

# SELECT 
#     p.first_name || ' ' || p.last_name AS patient_name,
#     unpaid_bills,
#     ROUND(SUM(b.amount::integer), 1) AS total_amount_owed    
# FROM patients as p
# left join billing as b
# on p.patient_id = b.patient_id
# left join unpaid_bill_amount as uba
# on p.patient_id = uba.patient_id
# WHERE unpaid_bills > 1 
# GROUP BY patient_name, unpaid_bills
# ORDER BY unpaid_bills, total_amount_owed DESC

# ;
# """)


# --- rank patients by total spending / show top 5 spenders --- 
query = text(f"""


WITH top_spenders AS(
    SELECT
        p.patient_id, 
        p.first_name || ' ' || p.last_name as patient_name,
        SUM(b.amount) total_spent
    FROM billing as b
    left join patients as p
    on b.patient_id = p.patient_id
    WHERE b.payment_status = 'Paid'
    GROUP BY p.patient_id, patient_name
    ORDER BY total_spent DESC
    LIMIT 5
)

SELECT 
    patient_name,
    total_spent,
    RANK () OVER (ORDER BY total_spent DESC) AS spending_rank
    
FROM top_spenders
ORDER BY total_spent DESC
LIMIT 5
;
""")



# --- Running the query ---
with engine.connect() as conn:
    conn.commit()
    result = conn.execute(query)
    headers = result.keys()
    output = result.fetchall()




# --- Printing the results! ---
print("Query Output:\n\n")
print(headers)
for row in output:
    print(row)
