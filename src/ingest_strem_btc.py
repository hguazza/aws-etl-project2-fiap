import requests
import os
from datetime import datetime
from bs4 import BeautifulSoup
import boto3
import json
import time
import logging


# CODIGO CRIADO PARA CONSEGUIR DADOS DE CRIPTO E ENVIAR PARA O FIREHOSE
# O SCRAPE GOOGLE NAO ESTA FUNCIONANDO, PRECISA MUDAR O "FIND" DO BS4


firehoseClient = boto3.client('firehose',
                aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
                aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
                aws_session_token=os.getenv("AWS_SESSION_TOKEN"),
                region_name='us-east-1',
)

coin = "bitcoin"

def get_cripto_price(coin):
    url = "https://api.coingecko.com/api/v3/simple/price?ids=bitcoin&vs_currencies=brl"
    response = requests.get(url)
    
    if response.status_code == 200:
        data = response.json()
        return data[coin]["brl"]
    else:
        logging.error(f"Erro ao obter preço do {coin}: {response.status_code}")
        return None

def scrape_cripto_price(coin):
    url = 'https://www.google.com/search?q=' + coin + 'price'
    response = requests.get(url)
    soup = BeautifulSoup(response.text, 'html.parser')
    span = soup.find('span', class_='pclqee')
    texti = span.text if span else "-"
    return texti


while True:
    now = datetime.now()
    timestamp = now.strftime("%Y-%m-%d %H:%M:%S")
    price = scrape_cripto_price(coin)
    print(f"{timestamp} - Preço do {coin}: R$ {price}")
    # send_to_firehose = firehoseClient.put_record(
    #     DeliveryStreamName=f'ingest_{coin}_stream',
    #     Record={
    #         'Data': json.dumps({
    #             'timestamp': timestamp,
    #             'price': price
    #         })
    #     }
    # )
    time.sleep(1)