from selenium.webdriver.chrome.options import Options 
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service as ChromeService
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
from dotenv import load_dotenv
from datetime import datetime

import pandas as pd
import boto3
import io
import logging
import os
import sqlite3

load_dotenv()

# Criar conexão com banco SQLite
db_path = "aws_etl_projeto2_fiap.db"
conn = sqlite3.connect(db_path)
cursor = conn.cursor()

# Criar tabela se não existir
cursor.execute("""
    CREATE TABLE IF NOT EXISTS pregao_b3 (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        cod TEXT,
        acao TEXT,
        tipo TEXT,
        qtde_teorica BIGINT,
        part_percent REAL,
        data_hora TEXT
      
    )
""")
conn.commit()
logging.info("Tabela 'pregao_b3' verificada/criada com sucesso.")

# Configura o logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# Variaveis de ambiente
aws_access_key_id = os.getenv("AWS_ACCESS_KEY_ID")
aws_secret_access_key = os.getenv("AWS_SECRET_ACCESS_KEY")
aws_session_token = os.getenv("AWS_SESSION_TOKEN")  
nome_bucket = "bucket-s3-b3"

# Criando agente boto3
s3_client = boto3.client(
    "s3",
    aws_access_key_id=aws_access_key_id,
    aws_secret_access_key=aws_secret_access_key,
    aws_session_token=aws_session_token,
    region_name='us-east-1'
)

def executar_scraping():
    logging.info("Iniciando o processo de scraping da B3.")
    url = "https://sistemaswebb3-listados.b3.com.br/indexPage/day/IBOV?language=pt-br"
    
    dfs_list = []
    
    # Configurar opções do navegador para rodar em segundo plano
    chrome_options = Options()
    chrome_options.add_argument("--headless")  # Executa sem interface
    chrome_options.add_argument("--disable-gpu") # Necessário no Windows
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--log-level=3")  # Only show fatal errors
    chrome_options.add_experimental_option("excludeSwitches", ["enable-logging"])
    chrome_options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36")

    # O webdriver-manager baixa e gerencia o driver do Chrome automaticamente
    driver = webdriver.Chrome(service=ChromeService(ChromeDriverManager().install()), options=chrome_options)
    
    logging.info("Aguardando o carregamento da página...")
    
    driver.get(url)
    
    logging.info(f"Página acessada. Aguardando a tabela ser carregada...")
    
    # Espera explícita de até 20 segundos pelo elemento da tabela
    wait = WebDriverWait(driver, 20)
    proxima_tabela = wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "li.pagination-next")))
    
    # Acessamos a página e alteramos os dados da tabela a cada loop do for, assim raspamos cada dado e adicionamos ao dataframe
    for i in range(5):
        wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "tbody tr")))
        html_content = driver.page_source
        # O pandas lê o HTML e converte a tabela em um DataFrame
        df_pagina = pd.read_html(io.StringIO(html_content), match='Código', attrs={'class': 'table'})[0]
        dfs_list.append(df_pagina)
        proxima_tabela.click()

    # Concatenamos as listas ao dataframe caso possua algum dado, caso não, o processo é finalizado
    if dfs_list:
        df_concatenado = pd.concat(dfs_list, ignore_index=True)
        
        # Limpando valores desnecessarios
        df_concatenado = df_concatenado[df_concatenado["Código"] != "Redutor"]
        df_concatenado = df_concatenado[df_concatenado["Código"] != "Quantidade Teórica Total"]
        
        # Ajustando o formato dos valores da tabela
        df_concatenado['Qtde. Teórica'] = df_concatenado['Qtde. Teórica'].str.replace('.', '', regex=False)
        df_concatenado['Qtde. Teórica'] = pd.to_numeric(df_concatenado['Qtde. Teórica'])
        
        df_concatenado['Part. (%)'] = df_concatenado['Part. (%)']/1000

        # Renomeando as tabelas
        df_final_completo = df_concatenado.rename(columns={"Código":"cod","Ação":'acao',"Tipo":"tipo","Qtde. Teórica":"qtde_teorica",  "Part. (%)":"part_teorica_porc"})
        df_final_completo['data_hora'] = datetime.today().strftime("%Y-%m-%d %H:%M:%S")

        logging.info("Processo de paginação concluído. DataFrame final criado com sucesso.")
        logging.info(f"Total de linhas extraídas: {len(df_final_completo)}")
    else:
        logging.warning("Nenhum dado foi extraído. Processo fianlizado!")
        return None
    
    print(df_final_completo.head(5))

    logging.info("Iniciando o upload para o S3...")
    
    # Criando o caminho particionado dos dados no S3
    data_hoje = datetime.today()
    caminho_s3 = f"raw/ano={data_hoje.year}/mes={data_hoje.month:02d}/dia={data_hoje.day:02d}/b3_dados_brutos.parquet"

    # Tranformando o dataframe em .parquet
    buffer_parquet = io.BytesIO()
    df_final_completo.to_parquet(buffer_parquet, index=False)

    # Inserir no banco de dados
        # Preparar os dados do DataFrame para inserção
    # Cria uma lista de tuplas, onde cada tupla representa uma linha do DataFrame
    dados_para_inserir = [tuple(row) for row in df_final_completo.values]

    # Inserir todos os dados de uma vez usando executemany

    try:
        cursor.executemany(
            "INSERT INTO pregao_b3 (cod, acao, tipo, qtde_teorica, part_percent) VALUES (?, ?, ?, ?, ?, ?)",
            dados_para_inserir
        )

        # Confirmar a transação
        conn.commit()
        logging.info("Dados inseridos com sucesso no banco de dados.")

    except sqlite3.Error as e:
        logging.error(f"Erro ao inserir dados no banco de dados: {e}")
        # Opcional: fazer rollback em caso de erro
        if 'conn' in locals():
            conn.rollback()

    finally:
        # Garantir que a conexão seja fechada
        if 'conn' in locals():
            conn.close()
            logging.info("Conexão com o banco de dados fechada.")

    # Upload do arquivo para a pasta local
    logging.info("Fazendo upload do arquivo parquet para a pasta local...")
    
    parquet_destination = f"parquet_arq/pregao_b3/ano={data_hoje.year}/mes={data_hoje.month:02d}/dia={data_hoje.day:02d}.parquet"
    parquet_directory = os.path.dirname(parquet_destination)
    os.makedirs(parquet_directory, exist_ok=True)

    try:
        buffer_parquet.seek(0)
        with open(parquet_destination, "wb") as f:
            f.write(buffer_parquet.getvalue())
        logging.info("Upload para a pasta local concluído.")
    except Exception as e:
        logging.error(f"Erro no upload para a pasta local: {e}")
        return

    # Upload do arquivo para o bucket S3
    try:
        buffer_parquet.seek(0)
        
        s3_client.put_object(
            Bucket=nome_bucket, Key=caminho_s3, Body=buffer_parquet.getvalue()
        )
        
        logging.info(f"Upload para s3://{nome_bucket}/{caminho_s3} concluído.")
    except Exception as e:
        logging.error(f"Erro no upload para o S3: {e}")
        return


if __name__ == "__main__":
    executar_scraping()