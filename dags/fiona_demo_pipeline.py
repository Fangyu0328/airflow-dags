"""
UI demo DAG for Airflow 1, Part 2 (slides 14-18).
 
Shows: fan-out / fan-in in Graph view, several runs in Grid view,
success / running / up_for_retry / failed / upstream_failed states,
XCom, Rendered Templates, Logs, Clear and Clear + downstream.
 
Failure switch: Admin -> Variables -> key "demo_fail", value "true" or "false".
"""
import time
from datetime import timedelta
 
import pendulum
from airflow.sdk import Variable, dag, task
from airflow.providers.standard.operators.bash import BashOperator
 
 
@dag(
    dag_id="fiona_demo_pipeline",
    schedule="*/10 * * * *",  # every 10 min, so the Grid fills up with columns
    start_date=pendulum.datetime(2026, 11, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 1, "retry_delay": timedelta(seconds=30)},
    tags=["demo", "fiona"],
)
def demo_pipeline():
    @task
    def extract() -> dict:
        print("Calling shop-api ...")
        return {"orders": 42, "source": "shop-api"}
 
    @task
    def clean_orders(data: dict) -> int:
        time.sleep(20)  # long enough to see the "running" state live
        print(f"Cleaned {data['orders']} orders")
        return data["orders"]
 
    @task
    def clean_customers(data: dict) -> int:
        if Variable.get("demo_fail", default="false") == "true":
            raise ValueError("Simulated failure: customers API returned HTTP 500")
        print("Cleaned customers")
        return 10
 
    report = BashOperator(
        task_id="report",
        bash_command=(
            'echo "run_id={{ run_id }} '
            "orders={{ ti.xcom_pull(task_ids='clean_orders') }}\""
        ),
    )
 
    data = extract()
    [clean_orders(data), clean_customers(data)] >> report
 
 
demo_pipeline()

