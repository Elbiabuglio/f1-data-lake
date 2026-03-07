#!/usr/bin/env python
# coding: utf-8

# In[9]:


"""Importa a biblioteca fastf1 para acessar dados da Fórmula 1."""

import fastf1
import argparse
import time


# In[2]:


"""Obtém a sessão da corrida (Race) do GP 7 da temporada de 2021 usando a biblioteca fastf1."""

session = fastf1.get_session(2021, 7, 'R')


# In[3]:


"""Carrega os dados da sessão selecionada para permitir a análise das informações da corrida."""

session.load()


# In[4]:


"""Importa a biblioteca pandas para manipulação de dados."""
import pandas as pd

"""Configura o pandas para exibir todas as colunas ao mostrar DataFrames."""
pd.set_option('display.max_columns', None)

"""Exibe os resultados da sessão carregada da Fórmula 1."""
session.results


# In[5]:


"""Salva os resultados da sessão em um arquivo no formato Parquet."""

session.results.to_parquet("data/2021_07_R.parquet")


# In[6]:


"""Lê o arquivo Parquet com os resultados da corrida e carrega em um DataFrame."""

df=pd.read_parquet("data/2021_07_R.parquet")
df


# In[7]:


class CollectResults:
    """
    Classe responsável por coletar resultados de corridas da Fórmula 1 utilizando a biblioteca fastf1
    e salvar os dados no formato Parquet para análise posterior.
    """

    def __init__(self, year=[2021, 2022, 2023], modes=['R', 'S']):
        """
        Inicializa a classe com os anos e tipos de sessão que serão coletados.

        Parâmetros:
            year (list): Lista de anos das temporadas de F1.
            modes (list): Tipos de sessão (ex: 'R' para Race e 'S' para Sprint).
        """
        self.years = year
        self.modes = modes

    def get_data(self, year, gp, mode)->pd.DataFrame:
        """
        Coleta os dados de uma sessão específica de um GP.

        Parâmetros:
            year (int): Ano da temporada.
            gp (int): Número do Grande Prêmio.
            mode (str): Tipo da sessão ('R' ou 'S').

        Retorno:
            pd.DataFrame: DataFrame com os resultados da sessão.
            Caso a sessão não exista, retorna um DataFrame vazio.
        """
        try:
            session = fastf1.get_session(year, gp, mode)

        except ValueError as err:
            return pd.DataFrame()

        session._load_drivers_results()
        df = session.results
        df["Mode"] = mode

        return df

    def save_data(self, df, year, gp, mode):
        """
        Salva os dados coletados em arquivo Parquet.

        Parâmetros:
            df (DataFrame): Dados da sessão.
            year (int): Ano da temporada.
            gp (int): Número do GP.
            mode (str): Tipo da sessão.
        """
        df.to_parquet(f"data/{year}_{gp:02}_{mode}.parquet")

    def process(self, year, gp, mode):
        """
        Executa o processo completo para um GP:
        coleta os dados e salva no arquivo.

        Retorno:
            bool: False se o DataFrame estiver vazio, True caso contrário.
        """
        df = self.get_data(year, gp, mode)
        self.save_data(df, year, gp, mode)
        if df.empty:
            return False
        self.save_data(df, year, gp, mode)
        return True

    def process_year_modes(self, year):
        """
        Percorre todos os GPs de um determinado ano
        e executa a coleta para cada tipo de sessão.
        """
        for i in range(1, 50):
            for mode in self.modes:
                if not self.process(year, i, mode) and mode == 'R':
                    return

    def process_years(self):
        """
        Executa a coleta de dados para todos os anos definidos na classe.
        """
        for year in self.years:
            print(f"Coletando dados do ano {year}")
            self.process_year_modes(year)
            time.sleep(10) 



# In[12]:


if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument("--years", "-y", nargs="+", type=int)
    parser.add_argument("--modes", "-m", nargs="+")

    args = parser.parse_args()

    """Cria uma instância da classe CollectResults definindo os anos e tipos de sessão a serem coletados."""
    Collect = CollectResults(args.years, args.modes)

    """Executa o processo de coleta e salvamento dos dados para todos os anos definidos."""
    Collect.process_years()

