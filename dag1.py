import pandas as pd
import functools as ft

from airflow.decorators import dag, task
from airflow.operators.dummy import DummyOperator
from airflow.utils.trigger_rule import TriggerRule
from datetime import datetime, timedelta

URL = "https://raw.githubusercontent.com/neylsoncrepalde/titanic_data_with_semicolon/main/titanic.csv"

default_args={
    'owner':'Kelly',
    'dependes_on_past':False,
    'start_date':datetime(2002,9,29)
}

@dag(default_args=default_args,schedule_interval='@once',catchup=False, tags=["Kelly","PUC","dag1"])
def trabalho2():

    @task
    def ingestao():
        NOME_DO_ARQUIVO = "/tmp/titanic.csv"
        df = pd.read_csv(URL, sep=';')
        df.to_csv(NOME_DO_ARQUIVO, index=False, header=True, sep=";")
        return NOME_DO_ARQUIVO

    #1. Quantidade de passageiros por sexo e classe (produzir e escrever)
    @task
    def ind_total_passageiros(nome_do_arquivo):
        NOME_TABELA = "/tmp/passageito_por_sexo_class.csv"
        df = pd.read_csv(nome_do_arquivo, sep=';')
        res = df.groupby(['Sex','Pclass']).agg({
            "PassengerId":"count"
        }).reset_index()
        res.rename(columns = {'PassengerId':'total_passageiros'}, inplace = True)
        print(res)
        res.to_csv(NOME_TABELA, index=False, sep=';')
        return NOME_TABELA

    #2. Preço médio da tarifa pago por sexo e classe (produzir e escrever)
    @task
    def ind_preco_medio_sexo_classe(nome_do_arquivo):
        PATH_SAIDA = "/tmp/preco_medio_por_sexo_classe.csv"
        df = pd.read_csv(nome_do_arquivo, sep=';')
        res = df.groupby(['Sex','Pclass']).agg({
            "Fare":"mean"
        }).reset_index()    
        res.rename(columns = {'Fare':'preco_medio'}, inplace = True)
        res['preco_medio'] = res['preco_medio'].round(decimals = 2)
        print(res)
        res.to_csv(PATH_SAIDA, index=False, sep=';')
        return PATH_SAIDA

    # 3. Quantidade total de SibSp + Parch (tudo junto) por sexo e classe (produzir e escrever)
    @task
    def ind_total_sibsp_parch_por_sexo_classe(nome_do_arquivo):
        PATH_SAIDA = "/tmp/familia_por_sexo_classe.csv"
        df = pd.read_csv(nome_do_arquivo, sep=';')
        soma = df['SibSp']+df['Parch']
        df['total']=soma
        res = df.groupby(['Sex','Pclass']
        ).agg({
            "total":"sum"
        }).reset_index()
        res.rename(columns = {'total':'total_sibsp_parch'}, inplace = True)
        print(res)
        res.to_csv(PATH_SAIDA, index=False, sep=';')
        return PATH_SAIDA

    # 3. Juntar todos os indicadores criados em um único dataset (produzir o dataset e escrever) /tmp/tabela_unica.csv
    @task
    def merge(path1, path2, path3):
        raw_passageiros = path1
        raw_tarifa = path2
        raw_familiares = path3
        
        NOME_TABELA = "/tmp/tabela_unica.csv"

        passageiros = pd.read_csv(raw_passageiros, sep=";")
        tarifa = pd.read_csv(raw_tarifa, sep=";")
        familiares = pd.read_csv(raw_familiares, sep=";")

        df_resultado = (
            passageiros
                .merge(tarifa, how="inner", on=['Sex','Pclass'])
                .merge(familiares, how="inner", on=['Sex','Pclass'])
        ).reset_index()

        print("\n"+df_resultado.to_string())
        df_resultado.rename(columns = {'Sex':'sexo'}, inplace = True)
        df_resultado.rename(columns = {'Pclass':'classe'}, inplace = True)
        
        df_resultado.to_csv(NOME_TABELA, index=False, sep=";")

        return NOME_TABELA
        
        # PATH_SAIDA = "/tmp/tabela_unica.csv"

        # df1 = pd.read_csv(path1, sep=';')
        # df2 = pd.read_csv(path2, sep=';')
        # df3 = pd.read_csv(path3, sep=';')

        # df_final = pd.merge(df1, df2, on=["Sex", "Pclass"])
        # df_final1 = pd.merge(df_final, df3, on=["Sex", "Pclass"])
        # df_final1.rename(columns = {'Sex':'sexo'}, inplace = True)
        # df_final1.rename(columns = {'Pclass':'classe'}, inplace = True)
        
        # print(df_final1)
        # df_final1.to_csv(PATH_SAIDA, index=False, sep=';')
        # return PATH_SAIDA


    fim = DummyOperator(task_id="fim")

    ing = ingestao()
    ind_tp = ind_total_passageiros(ing)
    ind_mp = ind_preco_medio_sexo_classe(ing)
    ind_ts = ind_total_sibsp_parch_por_sexo_classe(ing)
    me = merge(ind_tp, ind_mp, ind_ts)

    [ind_tp,ind_mp,ind_ts] >> me >> fim

execucao = trabalho2()
    