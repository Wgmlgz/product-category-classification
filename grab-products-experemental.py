from functools import wraps
import json
from multiprocessing import Pool
import random
import time
from typing import List
from bs4 import BeautifulSoup as bs
import asyncio
from bs4 import BeautifulSoup
from pyppeteer import launch
from collections import deque
import asyncio


def async_decorator(f):
    """Decorator to allow calling an async function like a sync function"""
    @wraps(f)
    def wrapper(*args, **kwargs):
        ret = asyncio.run(f(*args, **kwargs))

        return ret
    return wrapper


async def get_page():
    browser = await launch()
    page = await browser.newPage()
    await page.setUserAgent('Mozilla/5.0 (Windows NT 5.1; rv:5.0) Gecko/20100101 Firefox/5.0')
    return page, browser


def grab_product_base(page_content):
    soup = BeautifulSoup(page_content, features="html5lib")
    data = soup.find('div', {'id': 'section-description'}
                     )
    if data is not None:
        data = data.find('div', {'class': 'ra-a1'})
    description = ''
    if data is not None:
        description = data.text

    return {'description': description}


def grab_product_api(page_content):
    soup = BeautifulSoup(page_content, features="html5lib")
    data = soup.find_all('pre')[0].contents[0]
    data = json.loads(data)
    widgets = data.get('widgetStates')
    title = ''
    for key, value in widgets.items():
        if 'webProductHeading' in key:
            title = json.loads(value).get('title')
            break

    layout = json.loads(data.get('layoutTrackingInfo'))
    brand = layout.get('brandName')
    category = layout.get('categoryName')
    sku = layout.get('sku')
    url = layout.get('currentPageUrl')
    hierarchy = layout.get('hierarchy')
    hierarchy = layout.get('hierarchy')
    categoryId = layout.get('categoryId')
    product = {
        'title': title,
        'brand': brand,
        'category': category,
        'sku': sku,
        'url': url,
        'hierarchy': hierarchy,
        'categoryId': categoryId,
    }
    return product


@async_decorator
async def grab(urls: List[str]):
    page, browser = await get_page()

    res = []

    for url in urls:
        path = url[19:]
        api_url = 'https://www.ozon.ru/api/composer-api.bx/page/json/v2?url=' + path
        base_url = 'https://www.ozon.ru' + path
        
        start_time = time.time()
        
        while True:
            try:
                await page.goto(api_url, {'waitUntil': 'networkidle0', 'timeout': 11000})
                page_content = await page.content()
                product = grab_product_api(page_content)

                await page.goto(base_url, {'waitUntil': 'networkidle0', 'timeout': 10000})
                page_content = await page.content()
                product.update(grab_product_base(page_content))

                res.append((path, product))
                break
            except Exception as e:
                await browser.close()
                page, browser = await get_page()
        end_time = time.time() - start_time
        print('done', end_time)
    await browser.close()
    return res


def chunks(l, n):
    """Yield n number of striped chunks from l."""
    for i in range(0, n):
        yield l[i::n]


def run_chunk(chunk_size, processes):
    used_path = './parse/products_data/used.txt'
    found_path = './parse/products_data/found.json'
    q_path = './parse/products/found.txt'

    used = set([i.strip() for i in open(used_path, 'r').readlines()])
    found: dict = json.loads(open(found_path, 'r', encoding='utf-8').read())
    q_data = [i.strip() for i in open(q_path, 'r').readlines()]
    random.shuffle(q_data)

    urls = []
    for i in q_data:
        if i not in used:
            urls.append(i)
        if len(urls) == chunk_size:
            break

    flat = []
    with Pool(processes) as p:
        res = p.map(grab, chunks(urls, processes))
        flat = [item for sublist in res for item in sublist]
    d = dict(flat)
    found.update(d)
    open(found_path, 'w+',
         encoding='utf8').write(json.dumps(found, ensure_ascii=False))


def main():
    while True:
        run_chunk(30, 10)
        print('---')
        print('big chungus!!!!')
        print('---')


if __name__ == '__main__':
    main()
