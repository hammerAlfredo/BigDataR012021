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

name_bucket = 'parcial1bigdatahammer'

estructatiempo = 'headlines/raw/periodico=eltiempo/year='+str(anho)+'/month='+str(mes)+'/day='+str(dia)+'/eltiempo.html'
estructapubli = 'headlines/raw/periodico=publimetro/year='+str(anho)+'/month='+str(mes)+'/day='+str(dia)+'/publimetro.html'

#subir avianca
s3.upload_file('eltiempo.html',name_bucket,estructatiempo)
s3.upload_file('publimetro.html',name_bucket,estructapubli)

