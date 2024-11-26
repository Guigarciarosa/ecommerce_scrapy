# **Projeto de Web Scraping e Integração com Amazon S3**

## **Objetivo do Projeto**

O principal objetivo deste projeto é automatizar o processo de **coleta de informações de produtos** de um ecommerce e **inserir esses dados em um bucket do S3**. A ideia é garantir que os produtos sejam constantemente atualizados de maneira eficiente e automatizada.

## **Tecnologias Utilizadas**

Neste projeto, utilizamos diversas tecnologias e ferramentas para garantir a execução eficiente do processo. As principais são:

- **Python**: Linguagem principal usada no projeto para desenvolvimento do web scraping e automação.
- **BeautifulSoup**: Biblioteca Python para **web scraping**, utilizada para extrair dados do HTML de páginas web.
- **Requests**: Utilizada para fazer as **requisições HTTP** e obter o conteúdo das páginas.
- **Regex (re)**: Usada para **manipulação de strings** e garantir a formatação correta dos dados, como remover múltiplos hífens.
- **Boto3 & Awswrangler**: Bibliotecas responsáveis para comunicação com o SDK da AWS.
---

## **Fluxo do Projeto**

1. **Coleta de Dados**: Fazemos o scraping das páginas do site, extraindo informações essenciais como nome, preço e descrição do produto.
2. **Formatação dos Dados**: Os dados coletados são processados para garantir a correta padronização, como a remoção de espaços e múltiplos hífens.
3. **Inserção no S3**: Usamos as  soluções das plataformas de SDK da Aws para enviar os dados formatados atualizar produtos automaticamente.
4. **Automatização**: O processo é agendado para rodar periodicamente, garantindo que as informações dos produtos estejam sempre atualizadas.

---

## **Estrutura do Código**

O código está estruturado da seguinte maneira:

- **Web Scraping**: Scripts responsáveis por acessar as páginas de produtos e extrair as informações relevantes.
- **Formatação**: Funções que formatam e limpam os dados extraídos.
- **Integração com APIs**: Funções que conectam com as APIs das plataformas de e-commerce.
- **Automatização**: Scripts que agendam e automatizam o processo de scraping e envio de produtos.

---

## **Conclusão**

Este projeto é uma solução eficiente para **automatizar a gestão de produtos em plataformas de e-commerce**, utilizando **Python** e o **Amazon S3** como armazenamento. 

