from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

# Configurações padrão da DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Definindo a DAG
with DAG(
    'execute_python_script',
    default_args=default_args,
    description='Uma DAG que executa um script Python via BashOperator',
    schedule_interval=timedelta(days=1),  # Agenda de execução diária
    start_date=datetime(2023, 8, 1),
    catchup=False,  # Evitar execução retroativa
) as dag:

    # Definindo o caminho para o script Python
    script_path = '/jobs/app/endpoints_ingestion.py'
    script_cleaning = '/jobs/app/cleaning_job.py'
    script_cleaning_to_postgres = '/jobs/app/cleaning_to_postgres.py'

    # BashOperator para executar o script Python
    run_python_script_landing = BashOperator(
        task_id='landing_ingestion',
        bash_command=f'python3 {script_path}',  # Comando bash para executar o Python
    )

    run_python_script_cleaning = BashOperator(
        task_id='cleaning_job',
        bash_command=f'python3 {script_cleaning}',  # Comando bash para executar o Python
    )

    run_python_script_cleaning_to_postgres = BashOperator(
        task_id='cleaning_to_postgres',
        bash_command=f'python3 {script_cleaning_to_postgres}',  # Comando bash para executar o Python
    )

    # Definir a ordem das tarefas (nesse caso, é apenas uma tarefa)
    run_python_script_landing >> run_python_script_cleaning >> run_python_script_cleaning_to_postgres