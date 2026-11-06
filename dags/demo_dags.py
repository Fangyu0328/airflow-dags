from datetime import datetime
from airflow.sdk import dag, task

# (dag_id, schedule, tags, 是否故意失败)
DEMOS = [
    ("fangyu_daily_sales_etl", "@daily", ["fangyu", "etl"], False),
    ("fangyu_hourly_check", "@hourly", ["fangyu", "monitoring"], False),
    ("alice_customer_load", "@daily", ["alice", "etl"], False),
    ("bob_broken_pipeline", None, ["bob", "demo"], True),
]

def make_dag(dag_id, schedule, tags, should_fail):
    @dag(dag_id=dag_id, schedule=schedule, start_date=datetime(2026, 11, 1),
         catchup=False, tags=tags)
    def _dag():
        @task
        def extract():
            print(f"{dag_id}: extracting data")

        @task
        def load():
            if should_fail:
                raise ValueError("Intentional failure for demo")
            print(f"{dag_id}: loading data")

        extract() >> load()

    return _dag()

for args in DEMOS:
    make_dag(*args)
