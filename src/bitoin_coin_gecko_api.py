import requests
import sqlite3
import logging
from datetime import datetime
import os
import pandas as pd
from io import BytesIO

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

data_hora = datetime.today()

# Criar conexão com banco SQLite
db_path = "aws_etl_projeto2_fiap.db"
conn = sqlite3.connect(db_path)
cursor = conn.cursor()

# Criar tabela se não existir
cursor.execute("""
    CREATE TABLE IF NOT EXISTS preco_bitcoin (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        data_hora TEXT,
        preco_brl REAL
    )
""")
conn.commit()
logging.info("Tabela 'preco_bitcoin' verificada/criada com sucesso.")

# URL da API
url = "https://api.coingecko.com/api/v3/simple/price?ids=bitcoin&vs_currencies=brl"
logging.info(f"Fazendo requisição para a API: {url}")

try:
    response = requests.get(url)
    logging.info(f"Status da resposta: {response.status_code}")

    if response.status_code == 200:
        data = response.json()
        preco = data["bitcoin"]["brl"]


        logging.info(f"Preço do Bitcoin obtido: R$ {preco:,.2f} em {data_hora}")
        logging.info("Inserindo dados no banco de dados...")

        # Inserir no banco
        cursor.execute(
            "INSERT INTO preco_bitcoin (data_hora, preco_brl) VALUES (?, ?)",
            (data_hora, preco)
        )
        conn.commit()
        logging.info("Dados inseridos com sucesso no banco de dados.")

        logging.info("Consultando histórico de preços...")
         # Consulta todos os registros
        cursor.execute("SELECT id, data_hora, preco_brl FROM preco_bitcoin ORDER BY id DESC")
        registros = cursor.fetchall()

        if registros:
            print("\n=== Histórico de Preços do Bitcoin ===")
            for registro in registros:
                id, data_hora, preco = registro
                print(f"ID: {id} | Data/Hora: {data_hora} | Preço: R$ {preco:,.2f}".replace(",", "X").replace(".", ",").replace("X", "."))
        else:
            print("Nenhum registro encontrado.")
    else:
        logging.error("Erro ao acessar CoinGecko - Código de status diferente de 200")

except Exception as e:
    logging.exception("Erro inesperado ao acessar a API ou salvar no banco.")

# Upload do arquivo para a pasta local
logging.info("Fazendo upload do arquivo parquet para a pasta local...")

data_hora = datetime.today()

df = pd.DataFrame({
            "data_hora": [data_hora],
            "preco_brl": [preco]
        })
buffer_parquet = BytesIO()
df.to_parquet(buffer_parquet, index=False)

parquet_destination = f"./parquet_arq/preco_bitcoin/ano={data_hora.year}/mes={data_hora.month:02d}/dia={data_hora.day:02d}.parquet"
parquet_directory = os.path.dirname(parquet_destination)
os.makedirs(parquet_directory, exist_ok=True)

try:
    buffer_parquet.seek(0)
    with open(parquet_destination, "wb") as f:
        f.write(buffer_parquet.getvalue())
    logging.info("Upload para a pasta local concluído.")
except Exception as e:
    logging.error(f"Erro no upload para a pasta local: {e}")

finally:
    conn.close()
    logging.info("Conexão com o banco de dados encerrada.")
