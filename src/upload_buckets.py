import os
from datetime import datetime
import boto3
from io import BytesIO
import pandas as pd
import logging
from dotenv import load_dotenv


# Criando codigo para upload os dados de datas antigas para o S3 enquanto estavamos sem acesso ao AWS Lab
# SOMENTE ESQUELETO, ESTA IMCOMPLETO

# Carregar variáveis de ambiente
load_dotenv()

# Configurar logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

data_hoje = datetime.today()
caminho_s3 = f"raw/ano={data_hoje.year}/mes={data_hoje.month:02d}/dia={data_hoje.day:02d}/b3_dados_brutos.parquet"

# create client
s3_client = boto3.client(
    "s3",
    aws_access_key_id=aws_access_key_id,
    aws_secret_access_key=aws_secret_access_key,
    aws_session_token=aws_session_token,
    region_name='us-east-1'
)

try:
    buffer_parquet.seek(0)
    
    s3_client.put_object(
        Bucket=nome_bucket, Key=caminho_s3, Body=buffer_parquet.getvalue()
    )
    
    logging.info(f"Upload para s3://{nome_bucket}/{caminho_s3} concluído.")
except Exception as e:
    logging.error(f"Erro no upload para o S3: {e}")