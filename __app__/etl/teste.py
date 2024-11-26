import requests
from bs4 import BeautifulSoup
import os
import pandas as pd

# URL base do site
url = r'https://www.grupoalfatec.com.py/'

# Caminho para o arquivo de categorias
category_url = os.path.join(os.getcwd(), '..', '..', 'data', 'alfatech', 'files', 'categorys_alfatech.pickle')
print("Carregando categorias de:", category_url)

if os.path.exists(category_url):
    categorias = pd.read_pickle(category_url)
    print("Categorias extraídas:", categorias)
else:
    print("Arquivo de categorias não encontrado.")
    categorias = pd.DataFrame()

# Teste uma categoria diretamente
if not categorias.empty:
    category = categorias.iloc[:, 0]
    print(f'\nProcessando categoria: {category}')
    response = requests.get(f'{url}{category}')
    print(f'Status da resposta para categoria {category}:', response.status_code)
    soup = BeautifulSoup(response.content, 'html.parser')

    # Verificar se há produtos na página
    titles = soup.find_all('h1')
    print("Títulos de produtos encontrados:", [title.text for title in titles])
