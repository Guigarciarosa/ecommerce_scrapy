import requests
from bs4 import BeautifulSoup
import os
import re
import pickle
import time
import pandas as pd
import datetime as dt

class ExtractProdigital:
    
    def __init__(self):
        '''
            Extração do site Prodigital
        '''
        self.base_url =  'https://www.prodigital.com.py/'
        self.base_dir = os.path.join(os.getcwd())
    def extract_category_url(self):
        response = requests.get(self.base_url)
        print('Achando as Categorias')
        soup = BeautifulSoup(
            response.content, 'html.parser'
        )
        categorias = soup.find_all(
            'a', class_='father'
        )
        
        url_to_extract = []
        
        print('Criando os links')
        for link in categorias:
            href = link['href']
            href = href.strip('/')
            url_to_extract.append(f'{self.base_url}{href}')
        
        time.sleep(2)
        
        for ul in url_to_extract:
            url_product = f'{ul}'
            print(f'{url_product}')
        
        
        with open(os.path.join(self.base_dir, '..','..','data','prodigital','files','categorys.pickle'),'wb') as file:
                pickle.dump(url_to_extract,file)
                print('Categorias Salvas com Sucesso!')
    
    def scrape_product(self):
        pkl = pd.read_pickle(os.path.join(self.base_dir, '..','..','data','prodigital','files','categorys.pickle'))
        
        produtos_set = set()  # Conjunto para armazenar produtos únicos
        produtos_lista = []   # Lista para armazenar os dados finais de produtos
        
        for category in pkl:
            response_1 = requests.get(category)
            soup = BeautifulSoup(response_1.content, 'html.parser')
            products = soup.find_all('h1')
            
            for produto in products:
                produto_formatado = produto.text.lower().strip().replace(' ', '-').replace('ã', 'a').replace(
                    'marcas', '').replace('hable-con-nosotros', '').replace(
                    'onde-estamos', '').replace('sobre-nosotros', '').replace(
                    'menu', '').replace('páginas', '').replace('categorías', '')
                time.sleep(2)
                # Verifica se o produto já foi processado
                if produto_formatado not in produtos_set:
                    produtos_set.add(produto_formatado)  # Adiciona ao conjunto para evitar duplicatas
                    pdr_url = f'https://www.prodigital.com.py/produto/{produto_formatado}'
                    rp = requests.get(pdr_url)
                    soup_1 = BeautifulSoup(rp.content, 'html.parser')
                    info = soup_1.find_all('div')
                    price = soup_1.find_all('div', attrs={'class': 'price convert'})
                    
                    time.sleep(3)
                    for preco in price:
                        usd_price = preco.text.replace('U$ ', '')
                        cambio = info[9].text.replace('Cambio del díaU$ x R$ ', '')
                        brl_price = float(usd_price) * float(cambio)
                        
                        produto_data = {
                            'nome_produto': produto_formatado,
                            'valor_dolar': usd_price,
                            'valor_real': brl_price
                        }
                        produtos_lista.append(produto_data)  # Armazena na lista final
                        print(produto_data)
                        df = pd.DataFrame(produtos_lista)
                        df.to_parquet(fr'{self.base_dir}/../../data/prodigital/parquet/prodigital_{produto_formatado}.parquet')
                    
if __name__ == '__main__':
    ext_inst = ExtractProdigital()
    #ext_inst.extract_category_url()
    ext_inst.scrape_product()