'''
    App onde será carregado o ETL das Lojas e será feito a inserção dos Produtos dentro da API da shopify
    Python Version: 3.12.5
    Feito por Guilherme Dias
'''

# Bibliotecas
import pandas as pd
import numpy as np
import awswrangler as awr
import boto3
from dotenv import load_dotenv
import os
import json
import datetime as dt
import time
import binascii
import base64
from PIL import Image, UnidentifiedImageError
from io import BytesIO

# ETL
from etl.etl_alfatech import EtlAlfatech


class Scrapper:
    '''
        Classe de Extração para Retirar os produtos dos e-commerces.
        
        E-commerces:
        - Alfatech
        - Prodigital
    '''
    def __init__(self):
        # Carregamento das Variaveis de Ambiente
        print('\n -------- SCRAPPER ------------')
        time.sleep(5)
        print('\n - Carregando as variaveis de ambiente')
        dotenv_path = os.path.join(os.getcwd(), '..', 'env', '.env')
        self.env = load_dotenv(dotenv_path)
        print('\n - Carregando o bucket do S3')
        self.bucket_name = os.getenv('BUCKET_NAME')
        self.etl = EtlAlfatech()
        
        
    def configure_aws(self):
        self.env
        session = boto3.Session(
            aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
            aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
            region_name=os.getenv('AWS_REGION')
        )
        return session

    def main__(self):
        time.sleep(2)
        print('''
        \n Selecione a Rotina que Deseja
        1- Rotina de ETL (Alfatech)
        Caso contrário aperte qualquer tecla...
        ''')
        rotina = int(input('Digite: '))
        if rotina == 1:
            for cate in self.etl.cate['categorias']:
                self.etl.find_all_categorys_products(cate)

        else:
            print('\n Rotina Finalizada')

    def get_image_for_cache(self, nome_produto: str):
        ''' Função que puxa a imagem do S3 '''
        np = nome_produto.replace('/', '-').replace(' ', '-').lower()
        print(f'\n {np}.jpeg')
        time.sleep(2)

        session = self.configure_aws()
        s3_client = session.client('s3')
        key = f'data/alfatech/imgs/{np}.jpeg'

        try:
            response = s3_client.get_object(Bucket=self.bucket_name, Key=key)
            img_bytes = response['Body'].read()
            image = Image.open(BytesIO(img_bytes))
            img_path = os.path.join(os.getcwd(), '..', 'data', 'alfatech', 'imgs')
            image.save(f'{img_path}/image_cache.jpeg')
            print('\n Imagem Salva no Cache')
        except s3_client.exceptions.NoSuchKey:
            print(f'\n Imagem não encontrada no S3: {key}')
        except UnidentifiedImageError:
            print('\n Erro ao abrir a imagem. O arquivo pode estar corrompido.')


if __name__ == '__main__':
    aps_inst = Scrapper()
    aps_inst.main__()
