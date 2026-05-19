from airflow.decorators import dag, task
from datetime import datetime, timedelta
from airflow.providers.mysql.hooks.mysql import MySqlHook
import logging

default_args = {
    "owner": "ankit",
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
    "email": ["your_email@gmail.com"],
    "email_on_failure": True,
    "email_on_retry": True
}

@dag(
    dag_id="final_mysql_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",   # runs daily
    catchup=False,
    default_args=default_args
)
@task

def get_tables():
    hook = MySqlHook(mysql_conn_id="mysql_default")

    query = """
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = 'your_db'
    """

    df = hook.get_pandas_df(query)

    tables = df["table_name"].tolist()

    print("Tables found:", tables)

    return tables

    @task
    def process(table):
        try:
            print(f"\nProcessing table: {table}")

            # connect to MySQL
            hook = MySqlHook(mysql_conn_id="mysql_default")

            query = f"SELECT * FROM {table} LIMIT 5"
            df = hook.get_pandas_df(query)

            print(df)

            if df.empty:
                raise ValueError(f"{table} is empty")

            logging.info(f"Successfully processed {table}")

        except Exception as e:
            logging.error(f"Error in table {table}: {str(e)}")
            raise 

    tables = get_tables()
    process.expand(table=tables)


dag = pipeline()