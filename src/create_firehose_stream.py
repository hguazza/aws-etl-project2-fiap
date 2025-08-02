import boto3
import time

# --- Configurações ---
firehose_client = boto3.client('firehose', region_name='us-east-1')

# Nome que você deseja dar ao seu fluxo Firehose
stream_name = 'ingest_bitcoin_stream'

# Nome do seu bucket S3 onde os dados serão armazenados
s3_bucket_name = 'nome-do-seu-bucket-s3'

# ARN do papel do IAM que o Firehose irá assumir para escrever no S3
# Certifique-se de que este papel exista e tenha as permissões corretas
iam_role_arn = 'arn:aws:iam::123456789012:role/firehose_s3_delivery_role'

# Prefixo de pasta dentro do bucket S3 (opcional)
s3_prefix = 'dados-brutos/'


# --- Criação do Fluxo Firehose ---
def create_firehose_stream():
    """
    Cria um novo fluxo de entrega do Kinesis Data Firehose.
    """
    try:
        response = firehose_client.create_delivery_stream(
            DeliveryStreamName=stream_name,
            DeliveryStreamType='DirectPut',
            S3DestinationConfiguration={
                'RoleARN': iam_role_arn,
                'BucketARN': f'arn:aws:s3:::{s3_bucket_name}',
                'Prefix': s3_prefix,
                'CompressionFormat': 'UNCOMPRESSED', # GZIP, SNAPPY, HADOOP_SNAPPY ou ZIP
                'BufferingHints': {
                    'SizeInMBs': 64,   # Tamanho do buffer em MBs (min: 1, max: 128)
                    'IntervalInSeconds': 60 # Intervalo de tempo do buffer em segundos (min: 60, max: 900)
                }
            }
        )
        print(f"Iniciando a criação do fluxo '{stream_name}'...")
        return response
    except firehose_client.exceptions.ResourceInUseException:
        print(f"O fluxo '{stream_name}' já existe.")
        return None
    except Exception as e:
        print(f"Erro ao criar o fluxo Firehose: {e}")
        return None

# --- Verificar status ---
def wait_for_stream_active(stream_name_to_check):
    """
    Espera até que o fluxo Firehose esteja no estado 'ACTIVE'.
    """
    print(f"Verificando o status do fluxo...")
    while True:
        try:
            response = firehose_client.describe_delivery_stream(DeliveryStreamName=stream_name_to_check)
            status = response['DeliveryStreamDescription']['DeliveryStreamStatus']
            print(f"Status atual: {status}")
            if status == 'ACTIVE':
                print(f"O fluxo '{stream_name_to_check}' está ativo e pronto para uso.")
                return True
            time.sleep(30) # Espera 30 segundos antes de verificar novamente
        except Exception as e:
            print(f"Erro ao verificar o status: {e}")
            return False

# --- Execução ---
if __name__ == '__main__':
    create_firehose_stream()
    wait_for_stream_active(stream_name)