from weather_api.WeatherApiMap import CurrentWeatherMap, parse_response
from aws.Bucket import Bucket
from databases.amazonrds.PostgreSQL import PostgreSQL
from datetime import datetime
import pandas as pd
import json
import logging
from weather_api.Service import Service
import requests

class ConnectorWeatherApi:

    def __init__(self):
        '''
            This class has the
        '''
        self.bucket = Bucket('bucket-weather-api')

    
    def extract(self):
        '''
            Get data and upload raw json to aws bucket.
        '''
        fecha_filtro = datetime.now().strftime('%Y-%m-%d')
        service = Service()
        for city in load_cities():
            logging.info(f'Extracting data for {city} - {fecha_filtro}')
            url = f'http://api.weatherapi.com/v1/current.json?key={service.api_key}&q={city}'
            try:
                r = requests.get(url)
                data = json.dumps(r.json())
                key = f'{city}_{fecha_filtro}.json'
                self.bucket.put_file(key, data)
            except Exception as e:
                logging.error(e)
                logging.error(f'Unable to process {city}.')
                data = None
        logging.info('End.')


    def transform_load(self):
        '''
            pass
        '''
        df_list = []
        self.sql = PostgreSQL()
        for file in self.bucket.list_objects():
            fecha = file.split('_')[1].replace('.json', '')
            data = self.bucket.get_object(file)
            df = CurrentWeatherMap(parse_response(data), fecha)
            df_list.append(df)
            full_df = pd.concat(df_list)
            self.sql.upsert(full_df, 'weather')
            #self.bucket.delete_object(file)

def load_cities() -> list:
    '''
        Opens a .json file containing a list of cities.
    '''
    cities = []
    f = json.load(open('weather_api/cities.json', 'r', encoding='utf-8'))
    for city in f['cities']:
        cities.append(city)
    return cities
