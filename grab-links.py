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
    pattern: re.Pattern[str],
    full_pattern: re.Pattern[str],
    used_path: str,
    q_path: str,
    found_path: str,
    update_queue=True
):

    base = 'https://www.ozon.ru'
    page, browser = await get_page()

    q = deque()
    found = set()
    used = set()

    def grab_categories(page_content):
        soup = BeautifulSoup(page_content)
        links = set()
        for a in soup.find_all('a', href=True):
            if re.fullmatch(pattern, a['href']) is not None:
                link = base + a['href']
                link = link.split('?')[0]
                if link in found:
                    continue

                links.add(link)
                found.add(link)

        return links

    used = set([i.strip() for i in open(used_path,
               'r').readlines() if re.fullmatch(full_pattern, i.strip()) is not None])
    found = set([i.strip() for i in open(found_path,
                'r').readlines() if re.fullmatch(full_pattern, i.strip()) is not None])
    q.extend([i.strip() for i in open(q_path,
             'r').readlines() if re.fullmatch(full_pattern, i.strip()) is not None])

    count = 0

    while len(q) != 0:
        url = q.popleft()
        if url in used:
            continue
        used.add(url)
        try:
            await page.goto(url, {'waitUntil': 'networkidle0', 'timeout': 10000})
            page_content = await page.content()
            links = grab_categories(page_content)
            if update_queue:
                for i in links:
                    q.append(i)
            print('done', count)

            count += 1
            if count % 10 == 0:
                open(used_path, 'w+').write('\n'.join(used))
                open(found_path, 'w+').write('\n'.join(found))
                open(q_path, 'w+').write('\n'.join(q))
                print('write')
        except:
            print('timeout error!')
            q.append(url)
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
        re.compile(r'\/product.+'),
        re.compile(r'.+'),
        used_path='./parse/products/used.txt',
        found_path='./parse/products/found.txt',
        q_path='./parse/products/queue.txt',
        update_queue=False
    ))
