import json
import os
import boto3
import datetime as dt
import urllib.parse
from urllib.parse import unquote_plus
from urllib.parse import unquote

s3 = boto3.client('s3')

hoy = dt.datetime.today()
ayer = hoy-dt.timedelta(days=1)

dia = hoy.day
mes = hoy.month
anho = hoy.year

key = 'headlines/raw/periodico=eltiempo/year='+str(anho)+'/month='+str(mes)+'/day='+str(dia)+'/eltiempo.html'
bucketname = 'parcial2bigdatahammer'
dirdescarga = '/tmp/{}'.format(key.split('/')[-1])
s3.download_file(bucketname,key,dirdescarga)
archivolectu = open(dirdescarga,'r')
lineas = open(dirdescarga,'r').readlines()

categoriaperiodico = ''
cabeceraperiodico = ''
linkperiodico = ''
resfin = ''

for i in lineas:
    if 'class="category page-link' in i:
        try:
            # TODO: write code...
            categoriaperiodico = i.split('class="category page-link ')[1].split(' ')[0]
        except:
            print('error hammer')
    elif 'class="title page-link"' in i:
        try:
            # TODO: write code...
            cabeceraperiodico = i.split('">')[1].split('</a>')[0]
        except:
            print('error hammer cab')
    elif 'itemid="' in i:
        try:
            # TODO: write code...
            print()
            linkperiodico = i.split('itemid="')[1].split('"')[0]
            resfin = resfin+categoriaperiodico+','+linkperiodico+','+cabeceraperiodico+'\n'
        except:
            print('error hammer link')

archivo = open('/tmp/info.txt','w')
archivo.write(''+resfin)
archivo.close()


uploadpad = 'headlines/final/periodico='+key.split('/')[-1].split('.')[0]+'/year='+str(anho)+'/month='+str(mes)+'/day='+str(dia)+'/'+key.split('/')[-1].split('.')[0]+'.csv'
s3.upload_file('/tmp/info.txt',bucketname,uploadpad)
print(uploadpad)


#publimetro
keyp = 'headlines/raw/periodico=publimetro/year='+str(anho)+'/month='+str(mes)+'/day='+str(dia)+'/publimetro.html'
dirdescarga = '/tmp/{}'.format(keyp.split('/')[-1])
s3.download_file(bucketname,keyp,dirdescarga)
archivolectu = open(dirdescarga,'r')
lineas = open(dirdescarga,'r').readlines()

categoriaperiodico = ''
cabeceraperiodico = ''
linkperiodico = ''
resfin = ''

for j in lineas:
    if 'category' in j:
        palacasa = j.split('galery')
        for l in palacasa:
            try:
                lalinea = l
                categoriaperiodico = lalinea.split('"slug":"')[1].split('"')[0].replace(',',' ')
                linkperiodico = lalinea.split('"link":"')[1].split('"')[0].replace(',',' ')
                cabeceraperiodico = lalinea.split('"title":{"rendered":"')[1].split('"')[0].replace(',',' ')
                resfin = resfin +categoriaperiodico +','+linkperiodico+','+cabeceraperiodico+'\n'
            except:
                print('Error Hammer')

archivo = open('/tmp/info.txt','w')
archivo.write(''+resfin)
archivo.close()


uploadpad2 = 'headlines/final/periodico='+keyp.split('/')[-1].split('.')[0]+'/year='+str(anho)+'/month='+str(mes)+'/day='+str(dia)+'/'+keyp.split('/')[-1].split('.')[0]+'.csv'
s3.upload_file('/tmp/info.txt',bucketname,uploadpad2)
print(uploadpad2)
