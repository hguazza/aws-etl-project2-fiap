import boto3
import json

# SOMENTE ESQUELETO, ESTA IMCOMPLETO
# Este script cria um trigger no S3 que envia notificações para uma fila SQS quando novos objetos são criados.

# --- Configurações ---
# Substitua estas variáveis com os seus valores
S3_BUCKET_NAME = "seu-nome-do-bucket"
SQS_QUEUE_NAME = "seu-nome-da-fila-sqs"
REGION_NAME = "us-east-1"  # Por exemplo

# Inicializar os clientes Boto3
s3_client = boto3.client("s3", region_name=REGION_NAME)
sqs_client = boto3.client("sqs", region_name=REGION_NAME)

# --- Passo 1: Obter a URL e o ARN da fila SQS ---
try:
    response = sqs_client.get_queue_url(QueueName=SQS_QUEUE_NAME)
    sqs_queue_url = response["QueueUrl"]

    response = sqs_client.get_queue_attributes(
        QueueUrl=sqs_queue_url, AttributeNames=["QueueArn"]
    )
    sqs_queue_arn = response["Attributes"]["QueueArn"]
    print(f"ARN da fila SQS: {sqs_queue_arn}")

except Exception as e:
    print(f"Erro ao obter informações da fila SQS: {e}")
    exit()


# --- Passo 2: Definir a política de acesso da fila SQS ---
# A política permite que o S3 envie mensagens para esta fila
sqs_policy = {
    "Version": "2012-10-17",
    "Id": "SQS-S3-Policy",
    "Statement": [
        {
            "Sid": "Allow-S3-to-send-messages",
            "Effect": "Allow",
            "Principal": {"Service": "s3.amazonaws.com"},
            "Action": "sqs:SendMessage",
            "Resource": sqs_queue_arn,
            "Condition": {
                "ArnEquals": {"aws:SourceArn": f"arn:aws:s3:::{S3_BUCKET_NAME}"}
            },
        }
    ],
}

try:
    sqs_client.set_queue_attributes(
        QueueUrl=sqs_queue_url, Attributes={"Policy": json.dumps(sqs_policy)}
    )
    print("Política da fila SQS atualizada com sucesso.")

except Exception as e:
    print(f"Erro ao definir a política da fila SQS: {e}")
    exit()


# --- Passo 3: Configurar a notificação de evento no S3 ---
# Esta configuração diz ao S3 para enviar uma notificação para a fila SQS
# sempre que um objeto for criado.
s3_notification_config = {
    "QueueConfigurations": [
        {
            "Id": "S3-to-SQS-Notification",
            "QueueArn": sqs_queue_arn,
            "Events": ["s3:ObjectCreated:*"],
        }
    ]
}

try:
    s3_client.put_bucket_notification_configuration(
        Bucket=S3_BUCKET_NAME,
        NotificationConfiguration=s3_notification_config,
    )
    print(
        "Configuração de notificação do S3 atualizada com sucesso. O trigger foi criado!"
    )

except Exception as e:
    print(f"Erro ao configurar a notificação do S3: {e}")
    exit()