import json
import requests
from bs4 import BeautifulSoup
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

p = requests.get('https://www.publimetro.co/')
t = requests.get('https://www.eltiempo.com/')

soup_p = BeautifulSoup(p.text, 'lxml')
soup_t = BeautifulSoup(t.text, 'lxml')


archivo=open('publimetro.html','w', encoding='UTF-8')
archivo.write(str(soup_p))
archivo.close()

archivo=open('eltiempo.html','w', encoding='UTF-8')
archivo.write(str(soup_t))
archivo.close()

name_bucket = 'parcial2bigdatahammer'

estructatiempo = 'headlines/raw/periodico=eltiempo/year='+str(anho)+'/month='+str(mes)+'/day='+str(dia)+'/eltiempo.html'
estructapubli = 'headlines/raw/periodico=publimetro/year='+str(anho)+'/month='+str(mes)+'/day='+str(dia)+'/publimetro.html'

#subir avianca
s3.upload_file('eltiempo.html',name_bucket,estructatiempo)
s3.upload_file('publimetro.html',name_bucket,estructapubli)

print("archivos subidos a s3 headlines prueba 1.1")

"""#athena
client = boto3.client('athena', region_name='us-east-1')


arreglo = ['eltiempo','publimetro']

for i in arreglo:
  params = {
    'region': 'us-east-1',
    'database': 'basenews',
    'bucket': 'parcial1bigdatahammer',
    'path': 'news/',
    'query': 'alter table tablaperiodicos add partition(periodico="{}",year="{}",month="{}",day="{}");'.format(i,anho,mes,dia)
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

print("Result Data point2")"""
