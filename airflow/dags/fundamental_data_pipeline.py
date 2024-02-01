from datetime import timedelta
import pendulum
import requests
import pandas as pd
import os
import json
from azure.core.credentials import AzureNamedKeyCredential
from azure.data.tables import TableServiceClient
from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_replace, col
from airflow import DAG
from airflow.sensors.http_sensor import HttpSensor
from airflow.operators.email_operator import EmailOperator
from airflow.operators.python import PythonOperator

ticker='aapl'

credential = AzureNamedKeyCredential(os.environ['azurestoragename'], os.environ['azurestoragekey'])
service = TableServiceClient(endpoint=os.environ['azurestorageendpoint'], credential=credential)


def response_check(response):
        credential = AzureNamedKeyCredential(os.environ['azurestoragename'], os.environ['azurestoragekey'])
        service = TableServiceClient(endpoint=os.environ['azurestorageendpoint'], credential=credential)
        current_raw_entities = service.get_table_client("fundamentalsraw").query_entities(query_filter=f"PartitionKey eq '{ticker}'")
        if len(response.json()) == len([entity for entity in current_raw_entities]) | response.status_code != 200:
            return False
        else:
            return True

def query_fundamental_data(ticker: str):
    url = "https://seeking-alpha.p.rapidapi.com/symbols/get-financials"

    querystring = {"symbol": f"{ticker}", "target_currency": "USD", "period_type": "annual",
                   "statement_type": "income-statement"}

    headers = {"X-RapidAPI-Key": os.environ['rapidapi_key'],
               "X-RapidAPI-Host": "seeking-alpha.p.rapidapi.com"}

    response = requests.get(url, headers=headers, params=querystring)

    fundamental_data = pd.DataFrame(['Sep 2019', 'Sep 2020', 'Sep 2021', 'Sep 2022', 'Sep 2023', 'TTM'], columns=['RowKey'])
    fundamentals_json = pd.DataFrame(response.json())
    for index, row in pd.json_normalize(fundamentals_json.rows).iterrows():
        for r in row.dropna():
            df = (pd.json_normalize(r['cells'])
                  .drop('class', axis=1)
                  .rename({'name': 'RowKey'}, axis=1))
            df.columns = ['RowKey'] + [f'{r["name"]}_{col}' if r['name'] not in col else col for col in df.columns[1:]]
            fundamental_data = fundamental_data.merge(df, how='left', on='RowKey')
    fundamental_data = fundamental_data.query('RowKey != "TTM"').sort_values('RowKey', ascending=False).astype(str)
    fundamental_data['PartitionKey'] = ticker

    return fundamental_data

def get_fundamental_data(ticker: str, service: TableServiceClient):

    fundamentals = query_fundamental_data(ticker)
    current_raw_entities = service.get_table_client("fundamentalsraw").query_entities(query_filter=f"PartitionKey eq '{ticker}'")
    current_raw_entities = [entity['RowKey'] for entity in current_raw_entities]
    fundamentals = fundamentals[~fundamentals['RowKey'].isin(current_raw_entities)]

    fundamentals_raw_table = service.get_table_client("fundamentalsraw")
    entities = fundamentals.apply(lambda x: json.loads(x.to_json()), axis=1)
    [fundamentals_raw_table.create_entity(entity) for entity in entities]

def get_gross_margin(ticker: str, service: TableServiceClient):
    spark = (SparkSession.builder
        .appName("SparkSessionLocal")
        .master("local[*]")
        .getOrCreate()
        )

    current_raw_entities = service.get_table_client("fundamentalsraw").query_entities(
        query_filter=f"PartitionKey eq '{ticker}'")
    fundamentals_raw = pd.DataFrame.from_records([entity for entity in current_raw_entities])
    x=fundamentals_raw[['PartitionKey', 'RowKey', 'revenues_value', 'cost_revenue_value']]

    fundamental_ddl = (spark.createDataFrame(x)
             .withColumn('revenues_value', regexp_replace('revenues_value', ',', '').cast("decimal(36,3)"))
             .withColumn('cost_revenue_value', regexp_replace('cost_revenue_value', ',', '').cast("decimal(36,3)"))
             .withColumnRenamed('revenues_value', 'revenue')
             .withColumnRenamed('cost_revenue_value', 'cogs')
             .withColumn('gross_profit', col('revenue') - col('cogs'))
             .withColumn('gross_margin', (col('gross_profit') / col('revenue') * 100))
             )

    current_core_entities = service.get_table_client("fundamentals").query_entities(query_filter=f"PartitionKey eq '{ticker}'")
    fundamentals_core = pd.DataFrame.from_records([entity for entity in current_core_entities])
    new_core_entities = fundamentals_raw[~fundamentals_raw['RowKey'].isin(fundamentals_core['RowKey'].tolist())]

    fundamentals_table = service.get_table_client("fundamentals")
    entities = fundamental_ddl.filter(fundamental_ddl.RowKey.isin(new_core_entities.RowKey.tolist())).toJSON().collect()
    [fundamentals_table.create_entity(json.loads(entity)) for entity in entities]


default_args = {
            "owner": "Airflow",
            "start_date": pendulum.today('UTC').add(days=-1),
            "depends_on_past": False,
            "email_on_failure": True,
            "email_on_retry": False,
            "email": "gerardchurch133@outlook.com",
            "retries": 1,
            "retry_delay": timedelta(minutes=5)
        }

with DAG(dag_id="calculating_gross_margin", schedule="@daily", default_args=default_args, catchup=False) as dag:

    # Checking API status is available
    #wait_for_api = HttpSensor(
    #    task_id='wait_for_api',
    #    http_conn_id='rapid_api',
    #    endpoint='symbols/get-financials',
    #    headers={"X-RapidAPI-Key": os.environ['rapidapi_key'], "X-RapidAPI-Host": "seeking-alpha.p.rapidapi.com"},
    #    request_params={"symbol": f"{ticker}", "target_currency": "USD", "period_type": "annual",
    #               "statement_type": "income-statement"},
    #    method='GET',
    #    response_check=lambda response: response.status_code == 200,
    #    mode='poke',
    #    timeout=300,
    #    poke_interval=60,
    #)
    # Checking API status is available
    check_for_new_data = HttpSensor(
        task_id='checking_api_for_data',
        http_conn_id='rapid_api',
        endpoint='symbols/get-financials',
        headers={"X-RapidAPI-Key": os.environ['rapidapi_key'], "X-RapidAPI-Host": "seeking-alpha.p.rapidapi.com"},
        request_params={"symbol": f"{ticker}", "target_currency": "USD", "period_type": "annual",
                   "statement_type": "income-statement"},
        method='GET',
        response_check=response_check,
        mode='poke',
        timeout=300,
        poke_interval=60,
    )

    # Downloading fundamental data
    source_fundamentals = PythonOperator(
            task_id="downloading_fundamentals",
            python_callable=get_fundamental_data,
            op_kwargs={'ticker': ticker, 'service': service}
    )

    calculate_gross_margin = PythonOperator(
            task_id="calculating_gross_margin",
            python_callable=get_gross_margin,
            op_kwargs={'ticker': ticker, 'service': service}
    )

    # Sending email notification that data is available
    sending_email_notification = EmailOperator(
            task_id="sending_email",
            to="gerardchurch133@outlook.com",
            subject=f"New gross margin information is available for {ticker}",
            html_content=f"""
                <h3>New Data is available for {ticker}</h3>
            """
            )

    check_for_new_data >> source_fundamentals >> calculate_gross_margin >> sending_email_notification
