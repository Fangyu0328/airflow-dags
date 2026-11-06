from datetime import datetime
from airflow.sdk import dag, task

@dag(schedule=None, start_date=datetime(2025, 1, 1), catchup=False, tags=["test"])
def hello_test():
    @task
    def say_hello():
        print("Hello from git-sync!")

    say_hello()

hello_test()
