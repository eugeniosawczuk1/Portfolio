import pandas as pd
from datetime import datetime

def parse(data):
    
    for i in data:
        retval = i['fechaActualizacion'].split('T')
        retval = retval[0] + ' ' + retval[1].split('.')[0]
        i['fechaActualizacion'] = retval
    df = pd.json_normalize(data)
    return df

def map(df : pd.DataFrame) -> pd.DataFrame:
    retval = pd.DataFrame()
    retval['UKEY'] = df['nombre'] + df['fechaActualizacion']
    retval['CASA'] = df['casa']
    retval['NOMBRE'] = df['casa']
    retval['COMPRA'] = df['compra']
    retval['VENTA'] = df['venta']
    retval['ULTIMA_ACTUALIZACION_DATO'] = df['fechaActualizacion']
    retval['FECHA_FILTRO'] = datetime.now().strftime('%Y-%m-%d')
    retval['FECHA_CREACION'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    retval['FECHA_MODIFICACION'] = None
    return retval