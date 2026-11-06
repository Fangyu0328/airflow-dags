"""
UI demo DAG: shows the "skipped" state (slide 16) and Delete DAG (slide 18).
Manual trigger only.
"""
import pendulum
from airflow.sdk import dag, task
 
 
@dag(
    dag_id="fiona_demo_branch",
    schedule=None,
    start_date=pendulum.datetime(2026, 11, 1, tz="UTC"),
    catchup=False,
    tags=["demo", "fiona"],
)
def demo_branch():
    @task.branch
    def check_day() -> str:
        is_weekday = pendulum.now("UTC").weekday() < 5
        return "weekday_load" if is_weekday else "weekend_load"
 
    @task
    def weekday_load():
        print("Running the weekday load")
 
    @task
    def weekend_load():
        print("Running the weekend load")
 
    check_day() >> [weekday_load(), weekend_load()]
 
 
demo_branch()


