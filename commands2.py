import json
from pandas_datareader import DataReader
import datetime as dt
import os 
import boto3

s3 = boto3.client('s3')
#declaracion de tiempos
hoy = dt.datetime.today()
ayer = hoy-dt.timedelta(days=1)

dia = hoy.day
mes = hoy.month
anho = hoy.year

ayer2 = dt.datetime(anho,mes,1)


avianca = 'AVHOQ'
ecopetrol = 'EC'
grupaval = 'AVAL'
cemargos = 'CMTOY'
#Extraer de yahoo
extraavianca = DataReader(avianca,'yahoo',start=ayer2)
extraecopetrol = DataReader(ecopetrol,'yahoo',start=ayer2)
extragrupaval = DataReader(grupaval,'yahoo',start=ayer2)
extracemargos = DataReader(cemargos,'yahoo',start=ayer2)

extraavianca = extraavianca.iloc[-2:-1,:]
extraecopetrol = extraecopetrol.iloc[-2:-1,:]
extragrupaval = extragrupaval.iloc[-2:-1,:]
extracemargos = extracemargos.iloc[-2:-1,:]


#nombre del bucket
name_bucket = 'parcial1bigdatahammer'
extratodos = [extraavianca,extraecopetrol,extragrupaval,extracemargos]

estructavi = 'stocks/company=AVHOQ/year='+str(anho)+'/month='+str(mes)+'/day='+str(dia)+'/AVHOQ.html'
estructeco = 'stocks/company=EC/year='+str(anho)+'/month='+str(mes)+'/day='+str(dia)+'/EC.html'
estructaval = 'stocks/company=AVAL/year='+str(anho)+'/month='+str(mes)+'/day='+str(dia)+'/AVAL.html'
estructargo = 'stocks/company=CMTOY/year='+str(anho)+'/month='+str(mes)+'/day='+str(dia)+'/CMTOY.html'

#subir avianca
extraavianca.to_csv('avianca.csv')
s3.upload_file('avianca.csv',name_bucket,estructavi)
#subir ecopetrol
extraecopetrol.to_csv('ecopetrol.csv')
s3.upload_file('ecopetrol.csv',name_bucket,estructeco)
#subir grupaval
extragrupaval.to_csv('grupo_aval.csv')
s3.upload_file('grupo_aval.csv',name_bucket,estructaval)
#subir cemento argos
extracemargos.to_csv('cemento_argos.csv')
s3.upload_file('cemento_argos.csv',name_bucket,estructargo)

#athena
client = boto3.client('athena', region_name='us-east-1')


arreglo = [avianca,ecopetrol,grupaval,cemargos]

for i in arreglo:
  params = {
    'region': 'us-east-1',
    'database': 'actionsparcial',
    'bucket': 'parcial1bigdatahammer',
    'path': 'stocks/',
    'query': 'alter table actionsyahoo add partition(company="{}",year="{}",month="{}",day="{}");'.format(i,anho,mes,dia)
  }

  response_query_execution_id = client.start_query_execution(
    QueryString = params['query'],
    QueryExecutionContext = {
      'Database' : params['database']
    },
    ResultConfiguration = {
      'OutputLocation': 's3://' + params['bucket'] + '/' + params['path']
    }

  )

  response_get_query_details = client.get_query_execution(
    QueryExecutionId = response_query_execution_id['QueryExecutionId']
  )

print("Result Data")