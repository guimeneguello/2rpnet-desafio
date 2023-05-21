from airflow.models import DAG
import pendulum
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.macros import ds_add
import pandas as pd

## o meio que encontrei de trabalhar nesse problema
## envolve criar uma DAG e colocá-la no airflow
## para rodar todo mês, porém não consegui
## configurar um airflow local no tempo dado

with DAG(
    "dados_novos",
    start_date = pendulum.datetime(2023, 5, 1, tz="UTC"),
    schedule_interval = '0 0 1 * *', # executar toda primeira segunda feira do mes CRON expression
) as dag:

    tarefa_1 = BashOperator(
        task_id = 'cria_pasta',
        bash_command = 'mkdir -p "/Documentos/mes={{data_interval_end.strftime("%Y-%m-%d")}}"'
    )
    ## a task acima cria uma pasta em um diretorio

    def extrai_dados(data_interval_end):
        URL = 'https://opendata.nhsbsa.net/api/3/action/package_show?id=dataset_name_or_id'
        ## não consegui inferir como selecionar o nome ou o id do dataset mais novo


        dados = pd.read_csv(URL)

        file_path = f'/Documentos/mes={{data_interval_end.strftime("%Y-%m-%d")}}"'
        dados.to_csv(file_path + 'dataset.csv')
        ## a função acima, na prática, não deve funcionar devido
        ## ao tamanho do arquivo

    tarefa_2 = PythonOperator(
        task_id = 'extrai_dados',
        python_callable = extrai_dados,
        op_kwargs = {'data_interval_end': '{{data_interval_end.strftime("%Y-%m-%d")}}'}
    )

    tarefa_1 >> tarefa_2