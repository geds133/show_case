import requests
import pandas as pd
import os

url = "https://seeking-alpha.p.rapidapi.com/symbols/get-financials"

querystring = {"symbol": "aapl", "target_currency": "USD", "period_type": "annual",
               "statement_type": "income-statement"}

headers = {"X-RapidAPI-Key": os.environ['rapidapi_key'],
           "X-RapidAPI-Host": "seeking-alpha.p.rapidapi.com"}

response = requests.get(url, headers=headers, params=querystring)

fundamentals = pd.DataFrame(['Sep 2019', 'Sep 2020', 'Sep 2021', 'Sep 2022', 'Sep 2023', 'TTM'], columns=['date'])
fundamentals_json = pd.DataFrame(response.json())
for index, row in pd.json_normalize(fundamentals_json.rows).iterrows():
    for r in row.dropna():
        df = (pd.json_normalize(r['cells'])
              .drop('class', axis=1)
              .rename({'name': 'date'}, axis=1))
        df.columns = ['date'] + [f'{r["name"]}_{col}' if r['name'] not in col else col for col in df.columns[1:]]
        fundamentals = fundamentals.merge(df, how='left', on='date')
fundamentals = fundamentals.query('date != "TTM"').sort_values('date', ascending=False)
