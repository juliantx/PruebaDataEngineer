from datetime import timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago
from airflow.utils.edgemodifier import Label
from airflow.operators.python_operator import PythonOperator

# Argumentos del DAG
default_args = {
    'owner': 'Selecu',
    'start_date': days_ago(0),
    'email': ['juliantx@hotmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1, #Reintentos en caso de fallo
    'retry_delay': timedelta(seconds=30), #Tiempo para reintento
}

# define the DAG
dag = DAG(
    'ETL_KAFKA_PIPELINE_PRICES',
    default_args=default_args,
    description='Proceso de Pipeline Precios',
    schedule_interval='* */15 * * *',  # Cron repeticion cada 15 minutos de las tareas
)
# Definir las tareas asociadas a cada etapa del flujo de datos
# Tarea 1: Cargar datos, transformar y enviar a partición Kafka
# Tarea 2: Enviar datos desde Kafka como consumidor a BD MySQL

# Definición tarea 1
ETL = BashOperator(
    task_id='ETL_Load_Kafka',
    bash_command='python3 ~/prueba/Solucion2Etapa1Clases.py',
    dag=dag,
)
# Definición tarea 2
LoadMySQL = BashOperator(
    task_id='Load_To_MySQL',
    bash_command='python3 ~/prueba/Solucion2Etapa2.py',
    dag=dag,
)
# task pipeline
ETL >> LoadMySQL
