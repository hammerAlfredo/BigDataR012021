import json
import os
import boto3
from urllib.parse import unquote_plus
s3 = boto3.client('s3')
import datetime as dt


    
hoy = dt.datetime.today()
ayer = hoy-dt.timedelta(days=1)

dia = hoy.day
mes = hoy.month
anho = hoy.year

key = 'headlines/raw/periodico=eltiempo/year='+str(anho)+'/month='+str(mes)+'/day='+str(dia)+'/eltiempo.html'
bucketname = 'parcial2bigdatahammer'
dirdescarga = '/tmp/{}'.format(key.split('/')[-1])

newkey = 'headlines/raw/periodico='+key.split('/')[-1].split('.')[0]+'/year='+str(anho)+'/month='+str(mes)+'/day='+str(dia)+'/'+key.split('/')[-1].split('.')[0]+'.html'


s3.download_file(bucketname,newkey,dirdescarga)

subira = 'news/raw/periodico='+key.split('/')[-1].split('.')[0]+'/year='+str(anho)+'/month='+str(mes)+'/day='+str(dia)+'/'+key.split('/')[-1].split('.')[0]+'.html'

s3.upload_file(dirdescarga,bucketname,subira)
print("El tiempo subido a news")

#Publimetro
key2 = 'headlines/raw/periodico=publimetro/year='+str(anho)+'/month='+str(mes)+'/day='+str(dia)+'/publimetro.html'
dirdescarga2 = '/tmp/{}'.format(key2.split('/')[-1])

newkey2 = 'headlines/raw/periodico='+key2.split('/')[-1].split('.')[0]+'/year='+str(anho)+'/month='+str(mes)+'/day='+str(dia)+'/'+key2.split('/')[-1].split('.')[0]+'.html'


s3.download_file(bucketname,newkey2,dirdescarga2)

subira2 = 'news/raw/periodico='+key2.split('/')[-1].split('.')[0]+'/year='+str(anho)+'/month='+str(mes)+'/day='+str(dia)+'/'+key2.split('/')[-1].split('.')[0]+'.html'

s3.upload_file(dirdescarga2,bucketname,subira2)
print("Publimetro subido a news")