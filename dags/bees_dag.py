# AIRFLOW LIBRARIES
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy import DummyOperator
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

# SPARK LIBRARIES AND INITIALIZATION

from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder \
    .master("local[*]") \
    .appName("airflow_app") \
    .config('spark.executor.memory', '5g') \
    .config('spark.driver.memory', '5g') \
    .config("spark.driver.maxResultSize", "1048MB") \
    .config("spark.port.maxRetries", "100") \
    .getOrCreate()
    
# OTHER PYTHON LIBRARIES
import requests
import json
import os

default_args = {
    'owner': 'Maycon Palomo',
    'email': ['sustain@bees.com'],
    'start_date': days_ago(1),
    'email_on_failure' : False
}

with DAG(
    dag_id = 'bees_tech_challenge',
    default_args = default_args,
    catchup=False,
    max_active_runs = 1,
    schedule_interval = '0 5 * * *',
    tags=['brewery']
) as dag:
    
    # 1. Function to fetch data from the API with pagination and saving at the bronze layer
    def brewery_bronze():
        
        # Try to get the last page number ingested in the silver layer, if not found start the first load from page 1
        try:
            api_last_page = spark.read.format("parquet").load('/home/airflow/datalake/silver/').select(max("api_page_number")).collect()[0][0]
            print("Last page ingested in silver layer:", api_last_page)
            page_number = api_last_page + 1
            
            
        except:
            page_number = 1
            print("No previous data found in silver layer. Starting first load from page 1.")
            
        page = page_number

        # Pagination loop to fetch all pages from API until no more data is returned
        while True:
                        
            # Make the API request, fetching 200 records per page sorted by id
            print("Fetching page: ", page)

            response = requests.get('https://api.openbrewerydb.org/v1/breweries', params={"per_page":200, "page":page, "sort":"type,id:asc"})

            # Check if the request was successful 
            if response.status_code != 200:
                raise Exception(f"API request failed with status code {response.status_code}")
            
            data = response.json()

            # If no data is returned, exit the loop
            if len(data) == 0:
                print("No more data to fetch. Exiting.")
                return page_number
                break

            json_string = json.dumps(data)

            # Write the JSON string to a file at the bronze layer
            file_path = f'/home/airflow/datalake/bronze/'
            
            try:
                os.makedirs(os.path.dirname(file_path), exist_ok=True)
                
                with open(f'{file_path}brewery_page_{page}.json', "w") as f:
                    f.write(json_string)
                print(f"JSON data successfully saved to {file_path}")
            except IOError as e:
                print(f"Error saving JSON data to file: {e}")
                break

            # Increment the page number for the next pagination request
            page += 1
  
    # 2. Function to transform and load data from bronze to silver layer          
    def brewery_silver(ti):
        
        # Define the schema for the API data and read the JSON files from the bronze layer
        api_schema = StructType([
                                StructField("id", StringType(), False),
                                StructField("name", StringType(), True),
                                StructField("brewery_type", StringType(), True),
                                StructField("address_1", StringType(), True),
                                StructField("address_2", StringType(), True),
                                StructField("address_3", StringType(), True),
                                StructField("city", StringType(), True),
                                StructField("state_province", StringType(), True),
                                StructField("postal_code", StringType(), True),
                                StructField("country", StringType(), True),
                                StructField("longitude", DoubleType(), True),
                                StructField("latitude", DoubleType(), True),
                                StructField("phone", StringType(), True),
                                StructField("website_url", StringType(), True),
                                StructField("state", StringType(), True),
                                StructField("street", StringType(), True)
                            ])
        
        df = spark.read.json(f'/home/airflow/datalake/bronze/brewery_page_*.json')
        
        # Add a columnn to identify the page number from which the record was fetched
        df = df.withColumn('api_page_number', regexp_extract(input_file_name(), r'brewery_page_(\d+)\.json', 1).cast("int"))
        
        incremental_or_first_load = ti.xcom_pull(task_ids='brewery_bronze')
        
        print("Incremental or first load - Page number:", incremental_or_first_load)
        
        # If incremental load, filter the dataframe to only read and process new pages not yet ingested in the silver layer
            
        df = df.filter(f"api_page_number >= {incremental_or_first_load}")
        
        # Save the transformed data to the silver layer in parquet format, partitioned by country    
        df.coalesce(1) \
            .write \
            .format('parquet') \
            .partitionBy('country') \
            .mode('append') \
            .save('/home/airflow/datalake/silver/')

    # 3. Function to create a gold layer view and perform aggregations        
    def brewery_gold():
        
        spark.read.format("parquet").load('/home/airflow/datalake/silver/').createOrReplaceTempView('V_BREWERY_GOLD')
        
        # View will be shown at the task logs
        spark.sql("""
                       
                    SELECT country, brewery_type, count(*) as total
                    FROM V_BREWERY_GOLD
                    GROUP BY 1,2
                    ORDER BY 1,2
                    
                    """).show()
            
    start = DummyOperator(task_id='start')

    t01 = PythonOperator(
        task_id='brewery_bronze',
        python_callable=brewery_bronze
        )
    
    t02 = PythonOperator(
        task_id='brewery_silver',
        python_callable=brewery_silver,
        provide_context=True
        )
    
    t03 = PythonOperator(
    task_id='brewery_gold',
    python_callable=brewery_gold
    )
    
    end = DummyOperator(task_id='end')

    start >> t01 >> t02 >> t03 >> end


