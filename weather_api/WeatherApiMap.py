import pandas as pd
from datetime import datetime


def CurrentWeatherMap(data : pd.DataFrame, date : str = datetime.now().strftime('%Y-%m-%d')) -> pd.DataFrame:
    '''
        Given a dataframe, returns a mapped dataframe ready for sql upsert.
    '''
    ret_val = pd.DataFrame()
    ret_val['UKEY'] = data['pais'] + '_' + data['ciudad'] + '_' + date
    ret_val['PAIS'] = data['pais']
    ret_val['CIUDAD'] = data['ciudad']
    ret_val['TEMPERATURA'] = data['temperatura']
    ret_val['HUMEDAD'] = data['humedad']
    ret_val['ULTIMA_ACTUALIZACION_DATO'] = data['ultima_actualizacion']
    ret_val['FECHA_FILTRO'] = date
    ret_val['FECHA_CREACION'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    ret_val['FECHA_MODIFICACION'] = None
    return ret_val
    

def parse_response(data) -> dict:

    cols = ['ciudad', 'pais', 'ultima_actualizacion', 'temperatura', 'humedad']
    name = data['location']['name']
    country = data['location']['country']
    last_updated = data['current']['last_updated']
    temp_c = data['current']['temp_c']
    humidity = data['current']['humidity']
    data = [[name, country, last_updated, temp_c, humidity]]
    df = pd.DataFrame(data, columns=cols)
    return df