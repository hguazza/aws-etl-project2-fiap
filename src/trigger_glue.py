import boto3
import os
import sys

def lambda_handler(event, context):
    glue_job_name = os.environ.get('ETL_glue_pregao_B3')

    if not glue_job_name:
        print("ERRO: A variável de ambiente 'ETL_glue_pregao_B3' não foi definida.")
        sys.exit(1) # Encerra a função com falha

    client = boto3.client('glue')

    try:
        response = client.start_job_run(JobName=glue_job_name)
        print(f"Sucesso! Iniciado o Glue Job '{glue_job_name}'. JobRunId: {response['JobRunId']}")
        return {
            'statusCode': 200,
            'body': f"Job {glue_job_name} iniciado com sucesso."
        }
    except client.exceptions.ConcurrentRunsExceededException:
        print(f"AVISO: O job '{glue_job_name}' já está em execução.")
        return {
            'statusCode': 200, # Retorna sucesso para não reprocessar o gatilho
            'body': f"Job {glue_job_name} já estava em execução."
        }
    except Exception as e:
        print(f"Erro ao iniciar o Glue Job: {e}")
        raise e