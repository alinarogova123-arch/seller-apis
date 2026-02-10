import io
import logging.config
import os
import re
import zipfile
from environs import Env

import pandas as pd
import requests

logger = logging.getLogger(__file__)


def get_product_list(last_id, client_id, seller_token):
    """Получить список товаров магазина озон
    
    Args:
        last_id (str): Последний ID.
        client_id (str): ID клиента.
        seller_token (str): API токен продавца.
    
    Returns:
        (dict): Массив с информацией о товарах, выставленных на Ozon. 
    
    Example: 
        >>> print(response_object.get("result"))
        *{"Массив с ответом"}

    Example: 
        >>> print(response_object.get("result"))
        *requests.exceptions.HTTPError:
    """

    url = "https://api-seller.ozon.ru/v2/product/list"
    headers = {
        "Client-Id": client_id,
        "Api-Key": seller_token,
    }
    payload = {
        "filter": {
            "visibility": "ALL",
        },
        "last_id": last_id,
        "limit": 1000,
    }
    response = requests.post(url, json=payload, headers=headers)
    response.raise_for_status()
    response_object = response.json()
    return response_object.get("result")


def get_offer_ids(client_id, seller_token):
    """Получить артикулы товаров магазина озон

    Args:
        client_id (str): ID клиента.
        seller_token (str): API токен продавца.

    Returns:
        (list): Список с идентификаторами товаров.

    Example: 
        offer_ids = get_offer_ids(client_id, seller_token)
        >>> print(offer_ids)
        *"Список с идентификаторами"
    """

    last_id = ""
    product_list = []
    while True:
        some_prod = get_product_list(last_id, client_id, seller_token)
        product_list.extend(some_prod.get("items"))
        total = some_prod.get("total")
        last_id = some_prod.get("last_id")
        if total == len(product_list):
            break
    offer_ids = []
    for product in product_list:
        offer_ids.append(product.get("offer_id"))
    return offer_ids


def update_price(prices: list, client_id, seller_token):
    """Обновить цены товаров

    Args:
        client_id (str): ID клиента.
        seller_token (str): API токен продавца.
        prices (list): Список цен товаров для обновления.

    Returns:
        (dict): Словарь с новыми ценами.

    Example: 
        >>> print(response.json())
         *{"Массив с ответом"}

    Example: 
        >>> print(response.json())
        *requests.exceptions.HTTPError:
    """

    url = "https://api-seller.ozon.ru/v1/product/import/prices"
    headers = {
        "Client-Id": client_id,
        "Api-Key": seller_token,
    }
    payload = {"prices": prices}
    response = requests.post(url, json=payload, headers=headers)
    response.raise_for_status()
    return response.json()


def update_stocks(stocks: list, client_id, seller_token):
    """Обновить остатки

    Args:
        client_id (str): ID клиента.
        seller_token (str): API токен продавца.
        stocks (list): Список товаров для обновления.

    Returns:
        (dict): Словарь с новыми товарами.

    Example: 
        >>> print(response.json())
        *{"Массив с ответом"}

    Example: 
        >>> print(response.json())
        *requests.exceptions.HTTPError:
    """
    
    url = "https://api-seller.ozon.ru/v1/product/import/stocks"
    headers = {
        "Client-Id": client_id,
        "Api-Key": seller_token,
    }
    payload = {"stocks": stocks}
    response = requests.post(url, json=payload, headers=headers)
    response.raise_for_status()
    return response.json()


def download_stock():
    """Функция скачивает файл с остатками часов и преобразует его в словарь.

    Returns:
        (dict): Словарь с информацией об оставшихся часах.

    Example: 
        >>> watch_remnants = download_stock()
        >>> print(watch_remnants)
        *{"Массив с ответом"}
    """
    
    casio_url = "https://timeworld.ru/upload/files/ostatki.zip"
    session = requests.Session()
    response = session.get(casio_url)
    response.raise_for_status()
    with response, zipfile.ZipFile(io.BytesIO(response.content)) as archive:
        archive.extractall(".")
    excel_file = "ostatki.xls"
    watch_remnants = pd.read_excel(
        io=excel_file,
        na_values=None,
        keep_default_na=False,
        header=17,
    ).to_dict(orient="records")
    os.remove("./ostatki.xls")  
    return watch_remnants


def create_stocks(watch_remnants, offer_ids):
    """Функция создает список часов к продаже.

    Args:
        watch_remnants (dict): Словарь с информацией об оставшихся часах.
        offer_ids (list): Список с идентификаторами товаров.

    Returns:
        (list): Список с информацией о продаваемых часах.

    Example:
        >>> stocks = create_stocks(watch_remnants, offer_ids) 
        >>> print(stocks)
        *"Список с информацией о продаваемых часах"
    """
    
    stocks = []
    for watch in watch_remnants:
        if str(watch.get("Код")) in offer_ids:
            count = str(watch.get("Количество"))
            if count == ">10":
                stock = 100
            elif count == "1":
                stock = 0
            else:
                stock = int(watch.get("Количество"))
            stocks.append({"offer_id": str(watch.get("Код")), "stock": stock})
            offer_ids.remove(str(watch.get("Код")))
    for offer_id in offer_ids:
        stocks.append({"offer_id": offer_id, "stock": 0})
    return stocks


def create_prices(watch_remnants, offer_ids):
    """Создает список с информацией о ценах.
    
    Args:
        watch_remnants (dict): Словарь с информацией об оставшихся часах.
        offer_ids (list): Список с идентификаторами товаров.

    Returns:
        (list): Список с с информацией о ценах.

    Example: 
        >>> prices = create_prices(watch_remnants, offer_ids)
        >>> print(prices)
        *"Список с с информацией о ценах"
    """
    prices = []
    for watch in watch_remnants:
        if str(watch.get("Код")) in offer_ids:
            price = {
                "auto_action_enabled": "UNKNOWN",
                "currency_code": "RUB",
                "offer_id": str(watch.get("Код")),
                "old_price": "0",
                "price": price_conversion(watch.get("Цена")),
            }
            prices.append(price)
    return prices


def price_conversion(price: str) -> str:
    """Преобразование цены.
    
    Args:
        price (str): цена.
    
    Returns:
        (str): Преобразованная цена.
    
    Example: 
        5'990.00 руб. -> 5990
    """
    return re.sub("[^0-9]", "", price.split(".")[0])


def divide(lst: list, n: int):
    """Разделить список lst на части по n элементов

    Args:
        lst (list): Разделяемый список.
        n (int): Количество элементов в срезе списка.

    Returns:
        (list): Срез списка.

    Example: 
        >>> for some_stock in list(divide(stocks, 3)):
        >>>     print(some_stock)
        *[4990, 4990, 4290]
        *[43990, 2990, 4290]
    """

    for i in range(0, len(lst), n):
        yield lst[i : i + n]


async def upload_prices(watch_remnants, client_id, seller_token):
    """Запрашивает цены на товары и обновляет их.

    Args:
        watch_remnants (dict): Словарь с информацией об оставшихся часах.
        client_id (str): ID клиента.
        seller_token (str): API токен продавца.

    Returns:
        (list): Список цен.
    """

    offer_ids = get_offer_ids(client_id, seller_token)
    prices = create_prices(watch_remnants, offer_ids)
    for some_price in list(divide(prices, 1000)):
        update_price(some_price, client_id, seller_token)
    return prices


async def upload_stocks(watch_remnants, client_id, seller_token):
    """Запрашивает список с товарами и обновляет их.

    Args:
        watch_remnants (dict): Словарь с информацией об оставшихся часах.
        client_id (str): ID клиента.
        seller_token (str): API токен продавца.

    Returns:
        (list): Список товаров.
    """

    offer_ids = get_offer_ids(client_id, seller_token)
    stocks = create_stocks(watch_remnants, offer_ids)
    for some_stock in list(divide(stocks, 100)):
        update_stocks(some_stock, client_id, seller_token)
    not_empty = list(filter(lambda stock: (stock.get("stock") != 0), stocks))
    return not_empty, stocks


def main():
    env = Env()
    seller_token = env.str("SELLER_TOKEN")
    client_id = env.str("CLIENT_ID")
    try:
        offer_ids = get_offer_ids(client_id, seller_token)
        watch_remnants = download_stock()
        # Обновить остатки
        stocks = create_stocks(watch_remnants, offer_ids)
        for some_stock in list(divide(stocks, 100)):
            update_stocks(some_stock, client_id, seller_token)
        # Поменять цены
        prices = create_prices(watch_remnants, offer_ids)
        for some_price in list(divide(prices, 900)):
            update_price(some_price, client_id, seller_token)
    except requests.exceptions.ReadTimeout:
        print("Превышено время ожидания...")
    except requests.exceptions.ConnectionError as error:
        print(error, "Ошибка соединения")
    except Exception as error:
        print(error, "ERROR_2")


if __name__ == "__main__":
    main()
