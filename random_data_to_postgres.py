import csv
import random
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import os

# Path for CSVs
CSV_FOLDER = '/Users/saitejanampally/airflow/random_csvs'
os.makedirs(CSV_FOLDER, exist_ok=True)

# Function: Generate timestamped random CSV
def generate_random_csv():
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = os.path.join(CSV_FOLDER, f'random_data_{timestamp}.csv')
    headers = ['id', 'name', 'age', 'city']
    names = ['Sai', 'Teja', 'Ravi', 'Anu', 'Priya']
    cities = ['Dallas', 'Houston', 'Austin', 'NYC', 'Chicago']

    with open(filename, mode='w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(headers)
        for i in range(1, 21):  # 20 random rows per run
            writer.writerow([i, random.choice(names), random.randint(18,50), random.choice(cities)])
    print(f"CSV generated: {filename}")

# Function: Load CSV into Postgres
def load_latest_csv_to_postgres():
    hook = PostgresHook(postgres_conn_id='postgres_sai')
    conn = hook.get_conn()
    cursor = conn.cursor()

    # Create table if not exists
    cursor.execute("""
       
                  TRUNCATE TABLE public.random_people ;
    """)


    # Find the latest CSV
    csv_files = sorted([f for f in os.listdir(CSV_FOLDER) if f.endswith('.csv')])
    if not csv_files:
        raise FileNotFoundError("No CSV files found in folder.")
    latest_csv = os.path.join(CSV_FOLDER, csv_files[-1])

    # Load CSV (only columns that exist in CSV)
    with open(latest_csv, 'r') as f:
        next(f)  # skip header
        cursor.copy_from(f, 'random_people', sep=',', columns=('id','name','age','city'))

    conn.commit()
    cursor.close()
    conn.close()
    print(f"CSV loaded into Postgres: {latest_csv}")

# DAG arguments
default_args = {
    'owner': 'sai',
    'depends_on_past': False,
    'email_on_failure': True,
    'email': ['nampallysaiteja@gmail.com'],
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define DAG
with DAG(
    'random_data_to_postgres',
    default_args=default_args,
    description='Generate random CSVs and load into Postgres',
    schedule='@daily',
    start_date=datetime(2025, 1, 1),
    catchup=False,
) as dag:

    task_generate_csv = PythonOperator(
        task_id='generate_csv',
        python_callable=generate_random_csv,
    )

    task_load_csv = PythonOperator(
        task_id='load_csv',
        python_callable=load_latest_csv_to_postgres,
    )


task_load_csv


