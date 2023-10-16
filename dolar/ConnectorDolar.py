import requests
import pandas as pd
from databases.amazonrds.PostgreSQL import PostgreSQL
from aws.Bucket import Bucket
from datetime import datetime
import logging
import json
from Mapping import parse, map

class ConnectorDolar:

    def __init__(self):
        self.bucket = Bucket('bucket-cotizacion-dolar')

    def extract(self):
        date = datetime.now().strftime('%Y-%m-%d')
        logging.info(f'Getting dolar prices for {date}')
        url = 'https://dolarapi.com/v1/dolares'
        header = {'Accept' : 'application/json'}
        r = requests.get(url, headers=header)
        data = json.dumps(r.json())
        file_name = f'valores_dolar_{date}.json'
        logging.info('Loading data to bucket.')
        self.bucket.put_file(file_name, data)
        logging.info('Data loaded.')

    def transform_load(self):
        sql = PostgreSQL()
        for object in self.bucket.list_objects():
            downloaded_object = self.bucket.get_object(object)
            data = map(parse(downloaded_object))
            sql.upsert(data, 'cotizacion_dolar')
