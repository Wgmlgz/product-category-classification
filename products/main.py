
from multiprocessing import Pool
import re
from selenium_pages import get_browser, save_page, get_page
import asyncio
from bs4 import BeautifulSoup
from collections import deque

base = 'https://www.ozon.ru'
MAX_PAGE = 10
folder = './parse'

def parse_data(found, html: str) -> str:
    soup = BeautifulSoup(html, 'html.parser')

    links = set()
    pattern = re.compile(r'\/product.+')
    for a in soup.find_all('a', href=True):
        link = a['href'].split('?')[0]
        if re.fullmatch(pattern, link) is not None:
            link = base + link
            if link in found:
                continue

            links.add(link)
            found.add(link)

    return links

async def parse_link(page, url: str) -> set[str]:
    urls = []
    for i in range(1, MAX_PAGE + 1):
        if i == 1:
            urls.append(url)
        else:
            url_param = url + '?page=' + str(i)
            urls.append(url_param)
    return urls