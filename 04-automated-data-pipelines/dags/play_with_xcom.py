from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils import timezone



def _hey():
    return "hey"
def _print_hey(**context):
    print(context)
    value=context["ti"].xcom_pull(task_ids="hey",key="return_value")
    print(value)



with DAG(
    dag_id="play_with_xcom",
    schedule="@hourly",
    start_date=timezone.datetime(2024, 3, 10),
    catchup=False,
    tags=["DEB", "Skooldio"],
):
    hey =PythonOperator(
        task_id="hey",
        python_callable=_hey,
    )
    print_hey =PythonOperator(
        task_id="print_hey",
        python_callable=_print_hey,
    )

hey>> print_hey

