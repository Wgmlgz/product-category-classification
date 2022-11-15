import json
import random
from bs4 import BeautifulSoup as bs
import requests
import re
import asyncio
from bs4 import BeautifulSoup
from pyppeteer import launch
import os
from collections import deque
import asyncio


async def get_page():
    browser = await launch()
    page = await browser.newPage()
    await page.setUserAgent('Mozilla/5.0 (Windows NT 5.1; rv:5.0) Gecko/20100101 Firefox/5.0')
    return page, browser


async def grab(
    q_path: str,
    used_path: str,
    found_path: str,
):

    page, browser = await get_page()

    q = deque()
    found = set()
    used = set()

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
            # if 'webSale' in key:
            #     prices = json.loads(value).get('offers')[0]
            #     if prices.get('price'):
            #         price = re.search(
            #             r'[0-9]+', prices.get('price').replace(u'\u2009', ''))[0]
            #     else:
            #         price = 0
            #     if prices.get('originalPrice'):
            #         discount_price = re.search(
            #             r'[0-9]+', prices.get('originalPrice').replace(u'\u2009', ''))[0]
            #     else:
            #         discount_price = 0
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

    used = set([i.strip() for i in open(used_path, 'r').readlines()])
    found = json.loads(open(found_path, 'r', encoding='utf-8').read())
    q_data = [i.strip() for i in open(q_path, 'r').readlines()]
    random.shuffle(q_data)
    q.extend(q_data)

    count = 0

    skipped = 0
    while len(q) != 0:
        url = q.popleft()
        if url in used:
            skipped += 1
            continue
        if skipped != 0:
            print('skipped', skipped)
        skipped = 0
        'https://www.ozon.ru'
        path = url[19:]
        api_url = 'https://www.ozon.ru/api/composer-api.bx/page/json/v2?url=' + path
        base_url = 'https://www.ozon.ru' + path

        try:
            await page.goto(api_url, {'waitUntil': 'networkidle0', 'timeout': 10000})
            page_content = await page.content()
            product = grab_product_api(page_content)

            await page.goto(base_url, {'waitUntil': 'networkidle0', 'timeout': 10000})
            page_content = await page.content()
            product.update(grab_product_base(page_content))

            used.add(url)
            
            print('done', count)
            found[path] = product
            count += 1
            if count % 10 == 0:
                open(used_path, 'w+').write('\n'.join(used))
                open(found_path, 'w+',
                     encoding='utf8').write(json.dumps(found, ensure_ascii=False))
                # open(q_path, 'w+').write('\n'.join(q))
                print('write')
        except Exception as e:
            print(e)
            print('timeout error!')
            q.appendleft(url)
            await browser.close()
            page, browser = await get_page()


# if __name__ == '__main__':
#     asyncio.run(grab(
#         re.compile(r'\/category\/[^\/]+-\d+\/'),
#         re.compile(r'.+\/category\/[^\/]+-\d+\/'),
#         used_path='./parse/categories/used.txt',
#         found_path='./parse/categories/found.txt',
#         q_path='./parse/categories/queue.txt',
#         ))


if __name__ == '__main__':
    asyncio.run(grab(
        used_path='./parse/products_data/used.txt',
        found_path='./parse/products_data/found.json',
        q_path='./parse/products/found.txt',
    ))
