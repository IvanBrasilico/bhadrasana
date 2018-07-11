# -*- coding: utf-8 -*-
"""
Created on Mon Jun 25 14:30:28 2018

@author: 25052288840
"""

import os
import pandas as pd
from bhadrasana.conf import ENCODE
from bhadrasana.utils.csv_handlers import sch_processing

CAMINHO = '/home/ivan/Downloads'
CAMINHO = r'P:\SISTEMAS\roteiros\AJNA\CARGA'
ARQUIVO = 'Atracados201710.zip'
ARQUIVO = 'DESCARREGADOS201711.zip'
lista = sch_processing(os.path.join(CAMINHO, ARQUIVO),
                       dest_path='.')

'PROCURAR POR 435N'

df_conhecimento = pd.read_csv('Conhecimento.csv',
                              encoding=ENCODE, dtype=str)
df_mc =  pd.read_csv('ManifestoConhecimento.csv',
                     encoding=ENCODE, dtype=str)
df_manifesto = pd.read_csv('Manifesto.csv',
                           encoding=ENCODE, dtype=str)
df_em = pd.read_csv('EscalaManifesto.csv',
                    encoding=ENCODE, dtype=str)
df_atracacao = pd.read_csv('AtracDesatracEscala.csv',
                           encoding=ENCODE, dtype=str)

dfm1 = df_atracacao.merge(df_em)
dfm2 = dfm1.merge(df_manifesto)
dfm3 = dfm2.merge(df_mc)
df_final = dfm3.merge(df_conhecimento, on='Conhecimento')
df_MBL = df_final[df_final['Tipo'] == 'MBL']
df_MBL_resumo = df_MBL[
        ['Conhecimento', 'Tipo',
         'DataAtracacao', 'HoraAtracacao']
        ]


df_escala = pd.read_csv('Escala.csv',
                              encoding=ENCODE, dtype=str)

for column in df_final.columns:
    filtrado = df_final[df_final[column] == '43SN']
    if len(filtrado) > 0:
        print('Achou', column)
    else:
        print('NÃO', column)