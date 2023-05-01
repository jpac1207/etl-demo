import urllib.request
from datetime import datetime

import pymongo
from bs4 import BeautifulSoup
from pymongo import MongoClient

def extract():
    try:
        url = 'https://www.google.com/search?q=c%C3%A9lulares+em+oferta&tbm=shop'
        request = urllib.request.Request(url, None, {'Referer': 'https://www.google.com/',
        'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/112.0.0.0 Safari/537.36'})    
        
        with urllib.request.urlopen(request) as response:        
            html = response.read().decode("utf-8")                             
            soup = BeautifulSoup(html, "html.parser")    
            smartphones_containers = soup.find_all(attrs={"class": "KZmu8e"})
            return smartphones_containers
    except Exception as e:
        print(str(e))
        return []

def transform(raw_data_array):
    rich_data_array = []
    for raw_data in raw_data_array:
        # Select
        product_title = raw_data.find("h3").string
        product_price = raw_data.find(attrs={"class": "T14wmb"}).find("b").string
        product_seller = raw_data.find(attrs={"class": "E5ocAb"}).string
        
        # Clear
        product_price = product_price.replace('R$\xa0', '')
        product_price = product_price.replace('.', '')
        product_price = product_price.replace(',', '.')
        product_title = product_title.strip()
        product_seller = product_seller.strip()
        
        # Transform         
        product_price = float(product_price)

        rich_data_array.append({'product_title': product_title,
                                'product_price': product_price,
                                'product_seller': product_seller,
                                'created_at': datetime.now()})
    return rich_data_array

def load(rich_data):    
    client = MongoClient('localhost', 27017)
    database = client['dados_ofertas']
    collection = database['ofertas']
    collection.insert_many(rich_data)

def main():
    raw_data_array = extract()   
    rich_data_array = transform(raw_data_array)
    load(rich_data_array)

    print("#" * 50, 'Raw Data', "#" * 50)
    print(raw_data_array)    
    print("#" * 50, 'Rich Data', "#" * 50)
    print(rich_data_array)    

if __name__ == "__main__":
    main()