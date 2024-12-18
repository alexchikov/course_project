from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime, timedelta
from notifiers.telegram import TelegramNotifier
from utils.settings import Settings


DAG_ID = "etl_moex_agg"
START = datetime(2018, 1, 1, 0, 0, 0)
DESCRIPTION = "DAG for ETL processing MOEX data aggregates"
DEFAULT_ARGS = {
    "owner": "alexc",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
    "email_on_retry": False,
    "depends_on_past": True,
}
CURRENT_DATE = "{{ execution_date.strftime('%Y-%m-%d') }}"
FILENAME = f"moex_aggregates_{CURRENT_DATE.replace('-', '')}.json"
CONFIG_SPARK = {
    'spark.app.name': 'etl_moex_securities'
}

with DAG(
    dag_id=DAG_ID,
    start_date=START,
    description=DESCRIPTION,
    default_args=DEFAULT_ARGS,
    schedule="@daily",
    catchup=True,
    tags=["moex"],
    on_failure_callback=TelegramNotifier('dag failed',
                                         bot_token=Settings.BOT_TOKEN,
                                         chat_id=Settings.CHAT_ID)
) as dag:
    start = BashOperator(dag=dag,
                         task_id='start',
                         bash_command=f"echo start $(pwd) {CURRENT_DATE}")

    extract = BashOperator(dag=dag,
                           task_id='extract',
                           bash_command=f"curl {Settings.MOEX_AGGREGATES_URL}?date={CURRENT_DATE} --create-dirs -o ~/data/{FILENAME}")

    submit = SparkSubmitOperator(dag=dag,
                                 task_id='submit',
                                 application='dags/utils/moex_aggregates.py',
                                 conn_id='spark_default',
                                 conf=CONFIG_SPARK,
                                 application_args=['--filename', FILENAME])

    remove_file = BashOperator(dag=dag,
                               task_id="clear_temp",
                               bash_command=f"rm ~/data/{FILENAME}")

    end = BashOperator(dag=dag,
                       task_id='end',
                       bash_command=f"echo 'end {CURRENT_DATE}'")


start >> extract >> submit >> remove_file >> end
