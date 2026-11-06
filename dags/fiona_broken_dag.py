"""
push this ONLY when demonstrating import errors, then delete it.
It uses the Airflow 2 argument `schedule_interval`, which was removed in Airflow 3,
so the file fails to parse and shows up as an import error on the Home page
instead of in the DAG list (ties back to slides 12 and 38).
"""
import pendulum
from airflow.sdk import dag, task
 
 
@dag(
    dag_id="fiona_broken_dag",
    schedule_interval="@daily",  # Airflow 2 style -> error in Airflow 3
    start_date=pendulum.datetime(2026, 11, 1, tz="UTC"),
    catchup=False,
    tags=["demo", "fiona"],
)
def broken():
    @task
    def hello():
        print("hello")
 
    hello()
 
 
broken()
 
