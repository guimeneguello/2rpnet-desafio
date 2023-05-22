# 2rpnet-desafio

A validação, separação e as queries foram realizadas com Spark. Descompactei o zip e o carreguei num dataframe Spark, rodando o código em um jupyter notebook pelo Visual Studio Code.

As queries também foram realizadas em um jupyter notebook no visual studio code. Idealmente os dados seriam carregados em um banco de dados e rodaria queries SQL, porém usei a funcionalidade do Spark que permite realizar consultas SQL para trabalhar com o problema.

A rotina de extração dos dados é uma DAG para ser utilizada com Airflow, porém não a consegui testar propriamente dado que não consegui setar o airflow na minha máquina.

No momento, não possuo um computador muito potente nem com linux, portanto tive que apressadamente emular uma máquina virtual Ubuntu no meu computador, o que dificultou o processo devido à lentidão.
