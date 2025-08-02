import boto3
from dotenv import load_dotenv
import os

load_dotenv()

# Variaveis de ambiente
aws_access_key_id = os.getenv("aws_access_key_id")
aws_secret_access_key = os.getenv("aws_secret_access_key")
aws_session_token = os.getenv("aws_session_token")
region_name = 'us-east-1'

# Criando agente boto3
s3_client = boto3.client(
    "s3",
    aws_access_key_id=aws_access_key_id,
    aws_secret_access_key=aws_secret_access_key,
    aws_session_token=aws_session_token
)

bucket_names = [
    "bovespa-pregao-b3-project2-fiap-08",
    "bitcoin-stream-project2-fiap-08",
    "backup-bitcoin-stream-project2-fiap-08",
]



try:
    s3_client = boto3.client(
        "s3",
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
        aws_session_token=aws_session_token,
        region_name=region_name
    )

    for bucket_name in bucket_names:
        print(f"Criando o bucket: {bucket_name}")
        
        # A criação de buckets fora de us-east-1 requer a especificação da localização
        if region_name == 'us-east-1':
            s3_client.create_bucket(Bucket=bucket_name)
        else:
            s3_client.create_bucket(
                Bucket=bucket_name,
                CreateBucketConfiguration={'LocationConstraint': region_name}
            )
        
        print(f"Bucket '{bucket_name}' criado com sucesso!")

except Exception as e:
    print(f"Ocorreu um erro: {e}")