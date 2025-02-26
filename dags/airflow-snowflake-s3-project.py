from io import BytesIO
import boto3
import logging
import os
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import time
from datetime import datetime, timedelta
from airflow import DAG
from botocore.exceptions import ClientError
from botocore.exceptions import NoCredentialsError
from boto3.s3.transfer import TransferConfig
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeSqlApiOperator
from airflow.operators.email import EmailOperator

# Enable boto3 logging to see more detailed output
logging.basicConfig(level=logging.DEBUG)

# create a boto3 client for localStack's s3
s3_client = boto3.client('s3')

_SNOWFLAKE_CONN_ID = "snowflake_conn"
_SNOWFLAKE_DB = "REDFIN_DB_1"
_SNOWFLAKE_TABLE = "REDFIN_TABLE"

# s3 buckets
raw_data_bucket = "raw-data-bucket2025"
clean_data_bucket = "clean-data-bucket2025"

# Use absolute path or ensure correct relative path
#zip_file_path = os.path.abspath('./city_market_tracker.tsv000.gz')  # Verify path
zip_file_path = 'Download zip file link ->' # https://redfin-public-data.s3.us-west-2.amazonaws.com/redfin_market_tracker/city_market_tracker.tsv000.gz


# Check if file exists
if not os.path.exists(zip_file_path):
    raise FileNotFoundError(f"File not found: {zip_file_path}")
 

logger = logging.getLogger(__name__)


# Before processing
start_time = time.time()


def extract_data(file_path):  
    extracted_file_path = "/opt/airflow/data/extract_zip_csv"
    os.makedirs(extracted_file_path, exist_ok=True)

    now = datetime.now()
    date_now_string = now.strftime("%d%m%Y%H%M%S")
    file_str = f"redfin_data_{date_now_string}"
    output_file_path = f"{extracted_file_path}/{file_str}.parquet"
    chunk_size = 100000

    try:
        chunks = pd.read_csv(file_path, compression="gzip", sep="\t", chunksize=chunk_size)

        # Store all chunks in a list
        tables = []
        for i, chunk in enumerate(chunks):
            logging.info(f"Processing chunk {i + 1}")
            table = pa.Table.from_pandas(chunk)
            tables.append(table)  # Collect chunks instead of writing immediately

        if tables:
            # Concatenate all chunks into a single Table
            final_table = pa.concat_tables(tables)

            # Write the full Parquet file in one go
            pq.write_table(final_table, output_file_path, compression="SNAPPY")

        logging.info(f"File successfully saved: {output_file_path}")

        # Upload to S3
        try:
            with open(output_file_path, "rb") as f:
                config = TransferConfig(multipart_threshold=5 * 1024 * 1024, max_concurrency=2)
                s3_client.upload_fileobj(f, raw_data_bucket, f"extract/{file_str}.parquet", Config=config)

            logging.info(f"File uploaded to S3: s3://{raw_data_bucket}/processed/{file_str}.parquet")
        except ClientError as e:
            logging.error(f"Error uploading file to S3: {e}")
            raise

    except Exception as e:
        logging.error(f"Error processing file: {e}")
        raise

    return [output_file_path, file_str]

def transform_data(task_instance):
    """Transform the raw data and upload it to the clean-data bucket in monthly partitions"""

    # Pull the extracted data and object key from Xcom
    data_path = task_instance.xcom_pull(task_ids='tsk_extract_redfin_data')[0]
    object_key = task_instance.xcom_pull(task_ids='tsk_extract_redfin_data')[1]

     # Extract the 'redfin_data' part from the file_str
    file_prefix = object_key.split('_')[0]  # or use regex as shown earlier

    df = pd.read_parquet(data_path, engine="pyarrow", use_pandas_metadata=False)

    # Transformations (Cleaning, Filtering Columns)
    df["city"] = df["city"].str.replace(",", "", regex=True)

    selected_cols = [
        "period_begin", "period_end", "period_duration", "region_type", "region_type_id",
        "table_id", "is_seasonally_adjusted", "city", "state", "state_code", "property_type",
        "property_type_id", "median_sale_price", "median_list_price", "median_ppsf",
        "median_list_ppsf", "homes_sold", "inventory", "months_of_supply", "median_dom",
        "avg_sale_to_list", "sold_above_list", "parent_metro_region_metro_code", "last_updated"
    ]
    # Handle missing columns
    existing_cols = [col for col in selected_cols if col in df.columns]
    df = df[existing_cols].dropna()

    # Convert period_begin and period_end to datetime
    df['period_begin'] = pd.to_datetime(df['period_begin'], errors='coerce')
    df['period_end'] = pd.to_datetime(df['period_end'], errors='coerce')

    df["year"] = df['period_begin'].dt.year
    df["month"] = df['period_begin'].dt.month

    # Map month number to month name
    month_dict = {
        1: "Jan", 2: "Feb", 3: "Mar", 4: "Apr", 5: "May", 6: "Jun",
        7: "Jul", 8: "Aug", 9: "Sep", 10: "Oct", 11: "Nov", 12: "Dec"
    }
    df["month_name"] = df["month"].map(month_dict)

    # Print some statistics about the data
    logging.info(f'Total rows: {len(df)}')
    logging.info(f'Total cols: {len(df.columns)}')

    # Process and upload data month by month
    for (year, month_name), month_df in df.groupby(["year", "month_name"]):
        # Convert the DataFrame to CSV and store it in a BytesIO buffer
        csv_buffer = BytesIO()
        month_df.to_csv(csv_buffer, index=False)
        csv_buffer.seek(0)

        # Create a key for the S3 object
        s3_key = f"{file_prefix}/{year}/{month_name}/{year}_{month_name}_data.csv"

        # If no S3 client is passed, create a default one
        if s3_client is None:
            s3_client = boto3.client("s3")

        try:
            # Upload the file to S3
            s3_client.upload_fileobj(csv_buffer, clean_data_bucket, s3_key)
            logging.info(f"Uploaded data for {year}-{month_name} to s3://{clean_data_bucket}/{s3_key}")

        except NoCredentialsError:
            logging.error("Credentials not available for AWS S3. Please configure your AWS credentials.")
        except Exception as e:
            logging.error(f"Error uploading data for {year}-{month_name}: {str(e)}")

    logging.info("All monthly data uploaded successfully.")


def check_staging_data(task_instance):
    result = task_instance.xcom_pull(task_ids="check_staging_table_exist_sql")
    if not result or not result[0] or result[0][0] <= 0:
        print("Staging table has data.")
    else:
        raise ValueError("No data found in the staging table!")

# Python function to process the result and raise an error if no data
def check_real_table_data(task_instance):
    # Pull the result of the table existence check
    # Now check if the first column has data
    result_data = task_instance.xcom_pull(task_ids="check_real_table_data_sql")
    
    if not result_data or result_data[0][0] <= 0:
        raise ValueError(f"No data found in the first column of the real table { _SNOWFLAKE_TABLE }!")
    
    print(f"Real table { _SNOWFLAKE_TABLE } has data in the first column.")


# Define the Snowflake SQL to check the staging table
STAGING_TABLE_EXIST_SQL = """
   SELECT * 
FROM INFORMATION_SCHEMA.TABLES 
WHERE TABLE_NAME = 'redfin_ext_stage_yml'
  AND TABLE_SCHEMA = 'external_stage_schema'
   AND TABLE_CATALOG = 'redfin_db_1';
"""


# Define the Snowflake SQL to check the real table
REAL_TABLE_EXIST_SQL = """
   SELECT * 
FROM INFORMATION_SCHEMA.TABLES 
WHERE TABLE_NAME = 'redfin_table'
  AND TABLE_SCHEMA = 'redfin_schema'
  AND TABLE_CATALOG = 'redfin_db_1';
"""



default_args ={
    'owner':'Abdoul',
    'depends_on_past':False,
    'start_date':datetime(2025,3,1),
    'catchup':False,
    'retries': 2,
    'retry_delay': timedelta(seconds=5)

}

with DAG('redfin_analytics_dag2025',
         default_args=default_args,
         schedule='@daily', #"0 2 * * *"  # Run every day at 2 AM
         catchup=False) as dag:
   
    
    #Unzip the file  return as csv
    extract_redfin_data_to_s3 = PythonOperator(
        task_id ='extract_redfin_data_to_s3',
        python_callable = extract_data,
       op_kwargs={'file_path': zip_file_path},
       execution_timeout=timedelta(hours=2), # Timeout after 1 hour
        retries=3,  # Retry the task up to 3 times
        retry_delay=timedelta(minutes=5),  # Wait 5 minutes before retrying
        email_on_failure=True,
        dag=dag,

    )

    # Upload transformed to s3
    transform_redfin_data_to_s3 = PythonOperator(
        task_id = 'tsk_transform_redfin_data_to_s3',
        python_callable = transform_data,
        execution_timeout=timedelta(hours=2),  # Increase timeout to 15 minutes
        retries=3,  # Retry the task up to 3 times
        dag=dag,
    )


  #Snowflake query to check staging table
    check_staging_table_exist_sql = SnowflakeSqlApiOperator(
        task_id="check_staging_table_exist_sql",
        snowflake_conn_id="snowflake_conn",
        sql=STAGING_TABLE_EXIST_SQL,
        database=_SNOWFLAKE_DB,
        schema= "external_stage_schema",
        dag=dag,
    )


      # Python operator to validate staging data
    validate_staging_data = PythonOperator(
        task_id="validate_staging_data",
        python_callable=check_staging_data,
        dag=dag,
    )
  
             # Snowflake query to check real table
    check_real_table_data_sql = SnowflakeSqlApiOperator(
            task_id="check_real_table_data_sql",
            snowflake_conn_id=_SNOWFLAKE_CONN_ID,
            sql=REAL_TABLE_EXIST_SQL,
            database=_SNOWFLAKE_DB,
            schema="redfin_schema",
            dag=dag,
        )


      # Python operator to validate real table data
    validate_real_table_data = PythonOperator(
        task_id="validate_real_table_data",
        python_callable=check_real_table_data,
        dag=dag,
    )

      # Email notification task for success
    email_notification = EmailOperator(
        task_id="send_success_email",
        to="kabdul2016@gmail.com",
        subject="Data Load Successful",
        html_content="The data has been successfully loaded into both the staging and real tables.",
        email_on_failure=False,  # No email on failure for this task
        dag=dag,
    )
 
   

    # Email notification task for failure (this will send an email only when a task fails)
    email_failure_notification = EmailOperator(
        task_id="send_failure_email",
        to="kabdul2016@gmail.com",
        subject="Data Load Failed",
        html_content="There was an issue with loading data into staging or real tables. Please investigate.",
        email_on_failure=True,  # This email will be sent on failure
        dag=dag,
    )



    # Task dependencies
    extract_redfin_data_to_s3 >> transform_redfin_data_to_s3 
    transform_redfin_data_to_s3 >> check_staging_table_exist_sql >> validate_staging_data
    validate_staging_data >> check_real_table_data_sql >> validate_real_table_data
    validate_real_table_data  >> email_notification

    # Add failure email after the critical task that could fail
    transform_redfin_data_to_s3 >> email_failure_notification
    check_staging_table_exist_sql >> email_failure_notification
    check_real_table_data_sql  >> email_failure_notification


  