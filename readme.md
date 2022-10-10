# Entrega orquestracao - KellyLyra

## Código da DAG1


## Código da DAG2

## Imagem (PNG, JPG) com o print da tabela 1 no log
![tabela final-dag1](/imagens/resultadofinal_dag1.png "tabela final-dag1")

## Imagem (PNG, JPG) com o print da tabela 2 no log
![tabela final-dag2](/imagens/resultadofinal_dag2.png "tabela final-dag2")

## Imagem com print do grafo da DAG1
![grafo-dag1](/imagens/grafo-dag1.png "grafo-dag1")

## Imagem com print do grafo da DAG2
![grafo-dag2](/imagens/grafo_dag2.png "grafo-dag2")


# TRABALHO
Tarefa avaliativa - DAGs no Airflow
Iniciar tarefa
Vencimento Quinta-feira por 23:59  Pontos 30  Enviando um URL de site
Vamos utilizar o Airflow para construir uma pipeline de processamento de dados.
Vamos utilizar os dados do TITANIC -
https://raw.githubusercontent.com/neylsoncrepalde/titanic_data_with_semicolon/main/titanic.csv

DAG 1

Ler os dados e escrever localmente dentro do container numa pasta /tmp
Processar os seguintes indicadores
Quantidade de passageiros por sexo e classe (produzir e escrever)
Preço médio da tarifa pago por sexo e classe (produzir e escrever)
Quantidade total de SibSp + Parch (tudo junto) por sexo e classe (produzir e escrever)
Juntar todos os indicadores criados em um único dataset (produzir o dataset e escrever) /tmp/tabela_unica.csv
Printar a tabela nos logs
Triggar a Dag2
DAG 2
Ler a tabela única de indicadores feitos na Dag1 (/tmp/tabela_unica.csv)
Produzir médias para cada indicador considerando o total
Printar a tabela nos logs
Escrever o resultado em um arquivo csv local no container (/tmp/resultados.csv)
Regrinhas
Pode executar o processamento utilizando pandas!

Entregas
O link para 1 repositório no Github contendo:

Código da DAG1
Código da DAG2
Imagem (PNG, JPG) com o print da tabela 1 no log
Imagem (PNG, JPG) com o print da tabela 2 no log
Imagem com print do grafo da DAG1
Imagem com print do grafo da DAG2
