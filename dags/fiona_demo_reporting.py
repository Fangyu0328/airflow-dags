"""
UI demo DAG: leave this one PAUSED so the DAG list shows an off toggle
and a different tag (slide 14).
"""
import pendulum
from airflow.sdk import dag, task
 
 
@dag(
    dag_id="fiona_demo_reporting",
    schedule="0 10 * * *",
    start_date=pendulum.datetime(2026, 11, 1, tz="UTC"),
    catchup=False,
    tags=["reporting", "fiona"],
)
def demo_reporting():
    @task
    def build_report():
        print("Building the daily report")
 
    build_report()
 
 
demo_reporting()
 
