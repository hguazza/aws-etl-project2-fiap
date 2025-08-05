# Desafio Fase 2 Engenharia de Machine Learning Fiap 2025
# ETL com AWS

O projeto é iniciado com extração dos dados por meio de raspagem no site da B3 (https://sistemaswebb3-listados.b3.com.br/indexPage/theorical/IBOV?language=pt-br). Os dados são armazenados na pasta "raw" de um bucket no AWS Simple Storage Sergice (S3) no formato parquet. Sempre quando novos dados são adicionados, uma função Lambda é acionada para dar início ao pipeline no AWS Glue, que realizará transformações nos dados e então os salvará na pasta "refined" também em formato parquet. Os dados finais são particionados por ação e por data, facilitando futuras consultas.


## Bibliotecas utilizadas

| Tecnologia        | Função                                        |
| ----------------- | --------------------------------------------- |
| selenium          | Utilizado para raspagem de dados              |
| pandas            | Pré-transformação de dados                    |
| boto3             | Interação com infra da AWS                    |
| webdriver-manager | Gerencia automaticamente drivers do Selenium  |
| poetry            | Gerenciamento de dependências e ambiente      |
| dotenv            | Utilização de variáveis seguras               |

## Arquitetura
<img width="1213" height="281" alt="image" src="https://github.com/user-attachments/assets/8a40828a-873d-4481-8653-49f1128b4b61" />
