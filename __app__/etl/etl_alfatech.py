'''
Arquivo de Extração de Dados do Site Alfatech.
Python - 3.12.5
Made By: guilherme.dias
'''

import requests
from bs4 import BeautifulSoup
import os
import re
import time
import pandas as pd
import datetime as dt
import awswrangler as awr
import boto3
from dotenv import load_dotenv
from io import BytesIO

class EtlAlfatech:
    
    def __init__(self):
        ''' 
        ## ExtractAlfatech
        **Arquivo de Extração do Site Alfatech**.
        
        - Definição das rotas principais para a extração.
        '''
        print('\n --------------------- ETL Alfatech --------------------------')
        dotenv_path = os.path.join(os.getcwd(), '..', 'env', '.env')
        load_dotenv(dotenv_path)  # Carregar variáveis de ambiente do .env
        # Para main.py
        file_path = os.path.join(os.getcwd(),'..','data','alfatech','files')
        # Executando o elt_alfatech.py
        self.cate = pd.read_pickle(fr'{file_path}\categorys_alfatech.pickle')
        self.bucket_name = os.getenv('BUCKET_NAME')
        self.s3_key_prefix_file_bckup = os.getenv('ALFATECH_PARQUET_DUMP_BCKUP')
        self.s3_key_prefix_file = os.getenv('ALFATECH_PARQUET_DUMP')
        self.s3_key_prefix_imgs = os.getenv('ALFATECH_PARQUET_DUMP_IMGS')
        self.s3_client = self.configure_aws()
        print(f'\n Bucket do S3: {self.bucket_name}')
    
    def configure_aws(self):
        '''Configura a sessão AWS'''
        session = boto3.Session(
            aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
            aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
            region_name=os.getenv('AWS_REGION')
        )
        return session.client('s3')
    
    def salvar_imagem(self, url_imagem, nome_arquivo_s3):
        time.sleep(3)
        ''' Função Responsável por Salvar Imagens dos Produtos no S3 '''
        response = requests.get(url_imagem)
        if response.status_code == 200:
            # Preparar o conteúdo da imagem para ser carregado no S3
            image_data = BytesIO(response.content)
            self.s3_client.upload_fileobj(
                image_data,
                self.bucket_name,
                nome_arquivo_s3
                #ExtraArgs={'ContentType': 'image/jpeg'}
            )
            print(f'\n Imagem salva no S3 em: {nome_arquivo_s3}')
        else:
            print(f'\n Falha ao baixar a imagem: {url_imagem}')
    
    def baixar_primeira_imagem_produto(self, product_url):
        ''' Função Responsável por Baixar a Primeira Imagem Encontrada dentro da URL do Produto '''
        time.sleep(2)
        response = requests.get(product_url)
        soup = BeautifulSoup(response.content, 'html.parser')
        
        # Procurar a primeira tag <img> que contém o atributo 'data-original'
        primeira_imagem = soup.find('img', {'data-original': True})
        
        if primeira_imagem:
            # Extrair a URL da imagem do atributo 'data-original'
            image_url = primeira_imagem['data-original']
            hj = dt.date.today()
            # Nome do arquivo para salvar no S3
            nome_produto = product_url.split('/')[-1]
            nome_arquivo_s3 = f"{self.s3_key_prefix_imgs}{nome_produto}.jpeg"
            
            # Salvar a imagem no S3
            self.salvar_imagem(image_url, nome_arquivo_s3)
            #print('\n Imagem Salva')
        else:
            print("\n Nenhuma imagem encontrada.")
    
    def scrape_product(self, url, produto):
        ''' Função Principal de Extração dos Produtos '''
        time.sleep(5)
        print('\n ----- Transformação -----')
        response = requests.get(url)
        soup = BeautifulSoup(response.content, 'html.parser')
        
        if soup.find_all('h1')[1].text.strip() == '¡Atención! No entregamos productos en Brasil.':
            print(f'\n Produto não encontrado: {produto}')
            return pd.DataFrame()  # Retorna um DataFrame vazio para pular este produto
        
        time.sleep(5)
        print('\n Encontrando os valores do produto')
        
        title = soup.find_all('h1')[1].text.strip()
        if soup.find_all('div', attrs={'class': 'unavailable'}) != []:
            inds = 'Indisponível'
        else:
            inds = 'Produto em Estoque'

        try:
            price_div = soup.find('div', class_='price')
            usd_price = price_div.contents[0].strip().replace('U$ ', '') if price_div and price_div.contents else 0
            brl_price = price_div.find('span').text.strip().replace('R$ ', '').replace('/', '') if price_div and price_div.find('span') else 0
            gs_price = price_div.find_all('span')[1].text.strip().replace('Gs ', '') if price_div and len(price_div.find_all('span')) > 1 else 0
            descricao = soup.find_all('p')[0].text.strip() if soup.find_all('p') else "Descrição não disponível"

            data = pd.DataFrame({
                'nome_produto': [title],
                'valor_dolar': [usd_price],
                'descricao': [descricao],
                'valor_real': [brl_price],
                'valor_em_guaranis': [gs_price],
                'estoque': [inds],
                'landing_date': [dt.date.today()],
                'url': [url]
            })
            print('\n Produto Encontrado')
            return data

        except Exception as e:
            print(f"Erro ao extrair dados para o produto {produto}: {e}")
            return pd.DataFrame()


            
    def find_all_categorys_products(self, category):
        ''' Find all categorys products from the Alfatech site '''
        all_products = []  # Lista para armazenar todos os produtos do dia

        # set the url_
        url_ = fr'https://www.grupoalfatec.com.py/{category}'
        time.sleep(5)
        response = requests.get(url_)
        soup = BeautifulSoup(response.content, 'html.parser')
        titles = soup.find_all('h1')
        processed_titles = [
            re.sub(r'-+', '-', title.text.strip().lower().replace(' ', '-').replace('/', '-'))
            for title in titles
                if title.text.strip().lower() != 'menu'
        ]

        menus_ = [
            'páginas', 'categorías', 'categorias', 'novidades', 
            '¡atención!-no-entregamos-productos-en-brasil.', 
            '¡atención! no entregamos productos en brasil', 
            'hable-con-nosotros', 'sobre-nosotros', 'fale-conosco', 'marcas'
        ]
        processed_titles = [title for title in processed_titles if title.lower() not in menus_]
        processed_titles_ = processed_titles[1:]
        
        total_produtos = len(processed_titles_)
        
        for idx, produto in enumerate(processed_titles_, start=1):
            print(f"\n Produto {idx} de {total_produtos} - {produto}")
            product_data = self.scrape_product(fr'https://www.grupoalfatec.com.py/product/{produto}', produto)
            all_products.append(product_data)
            self.baixar_primeira_imagem_produto(fr'https://www.grupoalfatec.com.py/product/{produto}')
            print(f'\n Faltam {total_produtos - idx} produtos para concluir esta categoria.')

        # Consolidar todos os produtos em um único DataFrame
        all_products_df = pd.concat(all_products, ignore_index=True)
        dt_ = dt.date.today()
        s3_key = f"{self.s3_key_prefix_file}todos_os_produtos.parquet"
        s3_key_backup = f"{self.s3_key_prefix_file_bckup}todos_produtos_{dt_}.parquet"

        s3_path = f"s3://{self.bucket_name}/{s3_key}"
        s3_path_backup = f"s3://{self.bucket_name}/{s3_key_backup}"
        
        # Salvar no S3
        #all_products_df.to_parquet(s3_path, engine='pyarrow', index=False)
        #all_products_df.to_parquet(s3_path_backup, engine='pyarrow', index=False)
        awr.s3.to_parquet(
            df=all_products_df,
            path=s3_path,
            dataset=True,
            mode='overwrite'
        )
        
        awr.s3.to_parquet(
            df=all_products_df,
            path=s3_path_backup,
            dataset=True,
            mode='overwrite'
        )
                       
        print(f'\n Arquivo consolidado {s3_key} salvo no bucket {self.s3_key_prefix_file} às {dt.datetime.now()}')


if __name__ == '__main__':
    etl_inst = EtlAlfatech()
    cates = [
        
    ]
    #for cate in etl_inst.cate['categorias']:
    for cate in cates:
        etl_inst.find_all_categorys_products(cate)
