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








 
async def grab(pattern: re.Pattern[str], folder: str):
  base = 'https://www.ozon.ru'
  page, browser = await get_page()
  
  queue_categories = deque()
  found_categories = set()
  used_categories = set()
  
  def grab_categories(page_content):
    soup = BeautifulSoup(page_content)
    links = set()
    for a in soup.find_all('a', href=True):
        if re.fullmatch(pattern, a['href']) is not None:
            link = base + a['href']
            if link in found_categories:
                continue

            links.add(link)
            found_categories.add(link)

    return links

  used_categories = set(map(lambda s: s.strip(), filter(None, open(
      folder + '/used.txt', 'r').readlines())))
  found_categories = set(map(lambda s: s.strip(), filter(None, open(
      folder + '/found.txt', 'r').readlines())))
  queue_categories.clear()
  queue_categories.extend(map(lambda s: s.strip(), filter(None, open(
      folder + '/queue.txt', 'r').readlines())))

  count = 0

  while len(queue_categories) != 0:
      url = queue_categories.pop()
      if url in used_categories:
          continue
      used_categories.add(url)
      try:
          await page.goto(url, {'waitUntil': 'networkidle0'})
          page_content = await page.content()
          links = grab_categories(page_content)
          queue_categories.extend(list(links))
          print('done', count)
          
          count += 1
          if count % 3 == 0:
            open(folder + '/used.txt', 'w+').write('\n'.join(used_categories), )
            open(folder + '/found.txt', 'w+').write('\n'.join(found_categories))
            open(folder + '/queue.txt', 'w+').write('\n'.join(queue_categories))
            print('write')
      except:
          print('timeout error!')
          queue_categories.extend(url)
          await browser.close()
          page, browser = await get_page()
          


asyncio.run(grab(re.compile('\/category\/[^\/]+\\d+\/'), './parse/categories'))