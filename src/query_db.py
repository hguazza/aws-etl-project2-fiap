import pandas as pd
import sqlite3
import logging
from datetime import datetime


# CODIGO CRIADO PARA EXPLORAR COM O BANCO DE DADOS E ATUALIZAR A DATA/HORA DE DIFERENTES TANTO COM PYTHON COMO COM SQL

# Configurar logging
logging.basicConfig(level=logging.INFO)

# criar conexão com banco SQLite
db_path = "aws_etl_projeto2_fiap.db"
conn = sqlite3.connect(db_path)
cursor = conn.cursor()

# query db
query = "SELECT * FROM pregao_b3"
df = pd.read_sql_query(query, conn)

# Exibir os dados
logging.info("Dados obtidos do banco de dados:")
print(df.head())

data_hora = datetime.today()
data_hora_f = data_hora.strftime("%Y-%m-%d %H:%M:%S")
data_hora_ontem = data_hora - pd.Timedelta(days=1)
data_hora_ontem_f = data_hora_ontem.strftime("%Y-%m-%d %H:%M:%S")

print(data_hora_f)
print(data_hora_ontem_f)

df['data_hora'] = data_hora_ontem_f
print(df.head())

sql_command = """
UPDATE pregao_b3
SET data_hora = datetime('now', '-1 day')
WHERE data_hora IS NULL;
"""

# Execute the command
cursor.execute(sql_command)

# Commit the changes to the database
conn.commit()

# query db
query = "SELECT * FROM pregao_b3"
print("Dados atualizados:")
df = pd.read_sql_query(query, conn)
print(df.head())