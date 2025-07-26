from selenium.webdriver.chrome.options import Options
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service as ChromeService
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
from dotenv import load_dotenv

import pandas as pd
import boto3
import botocore
import io
import logging
import os
from datetime import datetime

# --- Configuration & Setup ---

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# Environment variables (with defaults for local testing if not set)
S3_BUCKET_NAME_RAW = os.getenv("NOME_BUCKET_RAW", "your-default-raw-bucket-name") # IMPORTANT: Set a valid default or ensure env var is always present
AWS_REGION = os.getenv("AWS_REGION", "us-east-1") # Example default region
B3_SCRAPE_URL = os.getenv("B3_SCRAPE_URL", "https://sistemaswebb3-listados.b3.com.br/indexPage/day/IBOV?language=pt-br")
PAGINATION_CLICKS = int(os.getenv("PAGINATION_CLICKS", 5)) # Number of times to click next page

# --- S3 Client Initialization ---

def get_s3_client(region_name: str) -> boto3.client:
    """
    Initializes and returns an S3 client using environment variables for credentials.
    """
    try:
        s3_client = boto3.client(
            "s3",
            region_name=region_name,
            aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
            aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
            aws_session_token=os.getenv("AWS_SESSION_TOKEN")
        )
        s3_client.head_bucket(Bucket=S3_BUCKET_NAME_RAW) # Validate bucket access/existence
        logger.info(f"Successfully initialized S3 client for bucket '{S3_BUCKET_NAME_RAW}' in region '{region_name}'.")
        return s3_client
    except botocore.exceptions.NoCredentialsError:
        logger.error("AWS credentials not found. Ensure AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, and AWS_SESSION_TOKEN are set.")
        raise
    except botocore.exceptions.ClientError as e:
        error_code = int(e.response.get("Error", {}).get("Code", -1))
        if error_code == 404:
            logger.error(f"S3 bucket '{S3_BUCKET_NAME_RAW}' not found. Please create it or check the name.")
        elif error_code == 403:
            logger.error(f"Access denied to S3 bucket '{S3_BUCKET_NAME_RAW}'. Check your IAM permissions.")
        else:
            logger.error(f"An S3 client error occurred: {e}")
        raise
    except Exception as e:
        logger.error(f"An unexpected error occurred during S3 client initialization: {e}")
        raise

# --- Selenium WebDriver Setup ---

def get_chrome_driver() -> webdriver.Chrome:
    """
    Configures and returns a headless Chrome WebDriver.
    """
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    # Added a more complete user-agent for better site compatibility
    chrome_options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
    chrome_options.add_experimental_option('excludeSwitches', ['enable-logging']) # Suppress console logging from Chrome
    
    try:
        service = ChromeService(ChromeDriverManager().install())
        driver = webdriver.Chrome(service=service, options=chrome_options)
        logger.info("Chrome WebDriver initialized in headless mode.")
        return driver
    except Exception as e:
        logger.error(f"Failed to initialize Chrome WebDriver: {e}")
        raise

# --- Scraping Logic ---

def scrape_b3_data(driver: webdriver.Chrome, url: str, pagination_clicks: int) -> pd.DataFrame | None:
    """
    Navigates to the B3 page, scrapes data across multiple pages, and returns a concatenated DataFrame.
    """
    logger.info(f"Starting B3 scraping process from URL: {url}")
    dfs_list = []
    
    try:
        driver.get(url)
        wait = WebDriverWait(driver, 30) # Increased wait time for robustness

        logger.info("Page loaded. Waiting for table and pagination element...")
        
        # Wait for the table body to be present, and also the next pagination button
        wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "tbody tr")))
        next_page_button = wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, "li.pagination-next")))
        
        for i in range(pagination_clicks):
            logger.info(f"Scraping page {i+1}...")
            # Wait for the table rows to be visible after each page load/click
            wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "tbody tr")))
            
            html_content = driver.page_source
            
            # Using read_html with error handling
            try:
                df_pagina = pd.read_html(io.StringIO(html_content), match='Código', attrs={'class': 'table'})[0]
                dfs_list.append(df_pagina)
                logger.debug(f"Successfully scraped page {i+1}, found {len(df_pagina)} rows.")
            except ValueError as e:
                logger.warning(f"Could not find table on page {i+1}. Skipping. Error: {e}")
                break # Exit loop if table not found on a page

            if i < pagination_clicks - 1: # Only try to click if not on the last iteration
                # Re-locate the button to ensure it's fresh and clickable after page changes
                next_page_button = wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, "li.pagination-next")))
                driver.execute_script("arguments[0].click();", next_page_button) # Use JS click for robustness
                logger.debug("Clicked next page button.")
            
    except Exception as e:
        logger.error(f"An error occurred during scraping: {e}")
        return None
    finally:
        driver.quit() # Ensure driver is closed even if an error occurs
        logger.info("WebDriver closed.")

    if dfs_list:
        df_concatenated = pd.concat(dfs_list, ignore_index=True)
        logger.info(f"Scraping completed. Total pages scraped: {len(dfs_list)}. Raw rows: {len(df_concatenated)}")
        return df_concatenated
    else:
        logger.warning("No data frames were extracted during scraping.")
        return None

# --- Data Transformation ---

def transform_b3_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Cleans and transforms the raw B3 DataFrame.
    """
    logger.info("Starting data transformation.")
    
    # Make a copy to avoid SettingWithCopyWarning
    df_transformed = df.copy()

    # Cleaning unnecessary values
    df_transformed = df_transformed[df_transformed["Código"] != "Redutor"]
    df_transformed = df_transformed[df_transformed["Código"] != "Quantidade Teórica Total"]
    
    # Adjusting format of 'Qtde. Teórica'
    # Use .loc to avoid SettingWithCopyWarning with chained assignments
    if 'Qtde. Teórica' in df_transformed.columns:
        df_transformed.loc[:, 'valor_limpo'] = df_transformed['Qtde. Teórica'].astype(str).str.replace('.', '', regex=False)
        df_transformed.loc[:, 'Qtde. Teórica'] = pd.to_numeric(df_transformed["valor_limpo"], errors='coerce')
        df_transformed.drop(columns=['valor_limpo'], inplace=True) # Drop the temporary column
    else:
        logger.warning("'Qtde. Teórica' column not found in DataFrame. Skipping numeric conversion.")

    # Renaming columns
    new_columns = {
        "Código": "cod",
        "Ação": "acao",
        "Tipo": "tipo",
        "Qtde. Teórica": "qtde_teorica",
        "Part. (%)": "part_teorica_porc"
    }
    df_transformed = df_transformed.rename(columns=new_columns)
    
    logger.info("Data transformation completed successfully.")
    logger.info(f"Total transformed rows: {len(df_transformed)}")
    return df_transformed

# --- S3 Upload Logic ---

def upload_dataframe_to_s3(s3_client: boto3.client, df: pd.DataFrame, bucket_name: str, prefix: str = "raw") -> None:
    """
    Uploads a DataFrame to S3 as a Parquet file with a partitioned path.
    """
    if df.empty:
        logger.warning("DataFrame is empty. Skipping S3 upload.")
        return

    logger.info(f"Initiating upload to S3 bucket: '{bucket_name}'...")
    
    # Creating the partitioned data path in S3
    data_hoje = datetime.today()
    s3_path = f"{prefix}/ano={data_hoje.year}/mes={data_hoje.month:02d}/dia={data_hoje.day:02d}/b3_dados_brutos.parquet"

    # Converting DataFrame to Parquet in-memory
    buffer_parquet = io.BytesIO()
    df.to_parquet(buffer_parquet, index=False)
    buffer_parquet.seek(0) # Reset buffer position to the beginning

    # Uploading the file to the S3 bucket
    try:
        s3_client.put_object(
            Bucket=bucket_name, Key=s3_path, Body=buffer_parquet.getvalue()
        )
        logger.info(f"Successfully uploaded data to s3://{bucket_name}/{s3_path}")
    except botocore.exceptions.ClientError as e:
        logger.error(f"S3 upload failed for '{s3_path}': {e}")
        raise
    except Exception as e:
        logger.error(f"An unexpected error occurred during S3 upload: {e}")
        raise

# --- Main Execution Flow ---

def main():
    """
    Main function to orchestrate the scraping, transformation, and upload process.
    """
    logger.info("Starting the full B3 data pipeline.")
    
    try:
        # 1. Initialize S3 client
        s3 = get_s3_client(AWS_REGION)

        # 2. Get Chrome WebDriver
        driver = get_chrome_driver()
        
        # 3. Execute Scraping
        raw_df = scrape_b3_data(driver, B3_SCRAPE_URL, PAGINATION_CLICKS)
        
        if raw_df is None or raw_df.empty:
            logger.warning("No data scraped. Exiting pipeline.")
            return

        # 4. Transform Data
        transformed_df = transform_b3_data(raw_df)

        # 5. Upload to S3
        upload_dataframe_to_s3(s3, transformed_df, S3_BUCKET_NAME_RAW, prefix="raw")

        logger.info("B3 data pipeline completed successfully!")

    except Exception as e:
        logger.critical(f"A critical error occurred in the B3 data pipeline: {e}")

if __name__ == "__main__":
    main()