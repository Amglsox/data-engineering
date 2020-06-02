# coding: utf-8
import requests
import datetime
import pandas as pd
import sys
import os
from sqlalchemy import create_engine
import logging

LEVELS = {'debug': logging.DEBUG,
          'info': logging.INFO,
          'warning': logging.WARNING,
          'error': logging.ERROR,
          'critical': logging.CRITICAL}
level_name = 'Covid19'
level = LEVELS.get(level_name, logging.NOTSET)
logging.basicConfig(level=level)
### GET DADOS DO COVID19 DA API
def getData_api(data):
    try:
        logging.info("Get dados da API Covid 19")
        url = "https://covid19-brazil-api.now.sh/api/report/v1/brazil/"+str(data)
        response = requests.get(url)
        if response.status_code == 200:
            return response.json()
    except Exception as ex:
        logging.error(ex)

### DEFINE O SCHEMA DE DADOS
def define_schema(data, dtInicio):
    try:
        logging.info("Define Schema de Dados")
        return list(map(lambda x: {'nm_uf':x['uf'],
                    'nm_estado':x['state'],
                    'qt_casos':x['cases'],
                    'qt_mortes':x['deaths'],
                    'qt_suspeitos':x['suspects'],
                    'dt_referencia':dtInicio.strftime('%Y-%m-%d'),
                    'bt_ultimacarga': 1
                   }, data['data']))
    except Exception as ex:
        logging.error(ex)

### ESCREVE ARQUIVO JSON POR DIA
def write_file(df, data):
    try:
        logging.info("Salvando Dataframe em JSON")
        path = os.path.join('/usr/local/airflow/dags/arquivos/'+str(data.strftime('%Y'))+'/'+str(data.strftime('%m'))+'/'+str(data.strftime('%d'))+'/')
        os.makedirs(os.path.join(path), exist_ok=True)
        df.to_json(str(path)+str('covid-')+data.strftime('%Y%m%d')+'.json',orient='records',force_ascii=False)
    except Exception as ex:
        logging.error(ex)

### CRIA ENGINE DE ESCRITA DE DADOS NO POSTGRESQL
def get_engine_jdbc():
    return create_engine('postgresql+psycopg2://postgres:Postgres2019!@postgres:5432/dm_covid')

### INSERT INTO JDBC
def write_jdbc(df):
    try:
        logging.info("Insert into fat_covid")
        engine = get_engine_jdbc()
        df.to_sql('fat_covid', engine, index=False, if_exists='append')
    except Exception as ex:
        logging.error(ex)

def update_covid():
    sql = """ UPDATE fat_covid
                SET bt_ultimacarga = 0"""
    conn = None
    try:
        logging.info("Update fat_covid")
        db = get_engine_jdbc()
        db.execute(sql)
    except Exception as ex:
        logging.error(ex)

def main():
    dtInicio = datetime.date.today() if len(sys.argv) == 1 else datetime.date(int(sys.argv[1][:4]), int(sys.argv[1][5:7]), int(sys.argv[1][8:10]))
    print("Data de processamento:"+str(dtInicio.strftime('%Y%m%d')))
    data = getData_api(dtInicio.strftime('%Y%m%d'))
    if data['data'] != []:
        try:
            df = pd.DataFrame(data=define_schema(data,dtInicio), columns=['nm_uf','nm_estado','qt_casos','qt_mortes','qt_suspeitos','dt_referencia','bt_ultimacarga'])
            update_covid()
            write_file(df, dtInicio)
            write_jdbc(df)
        except Exception as ex:
            logging.error(ex)
main()