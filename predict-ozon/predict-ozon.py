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
import urllib.parse
from urllib.parse import urlparse, parse_qs
import re

def async_decorator(f):
    """Decorator to allow calling an async function like a sync function"""
    @wraps(f)
    def wrapper(*args, **kwargs):
        ret = asyncio.run(f(*args, **kwargs))

        return ret
    return wrapper

async def grab(browser, text: str) -> str:
  page = await browser.newPage()
  await page.setUserAgent('Mozilla/5.0 (Windows NT 5.1; rv:5.0) Gecko/20100101 Firefox/5.0')
  url = 'https://www.ozon.ru/search/?text=' + urllib.parse.quote(text)
  response = await page.goto(url, 
                             {'waitUntil': 'networkidle2', 'timeout': 11000}
                             )
  await page.waitForSelector('#stickyHeader', { 'visible': True, 'timeout': 0 })
  
  parsed = urlparse(response.url)
  dict_result = parse_qs(parsed.query)
  await page.close()
  if 'category_was_predicted' in dict_result and dict_result['category_was_predicted'][0] == 'true':
    category = parsed.path.split('/')[-2]
    # category = re.search(r'(\d+$)', category).group(1)
    return category
  return None


@async_decorator
async def main():
  browser = await launch()
  print(await grab(browser, 'Картофель'))
  print(await grab(browser, 'Among us'))
  print(await grab(browser, 'Iphone'))
  await browser.close()
  

if __name__ == '__main__':
    main()
