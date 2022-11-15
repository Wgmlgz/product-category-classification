
import re
from selenium_pages import get_browser, save_page, get_page
import asyncio
from bs4 import BeautifulSoup

base = 'https://www.ozon.ru'


def parse_data(html: str) -> str:
    soup = BeautifulSoup(html, 'html.parser')

    links = set()
    found = set()
    pattern = re.compile(r'\/category\/[^\/]+-\d+\/')
    pattern = re.compile(r'\/product.+')
    for a in soup.find_all('a', href=True):
        link = a['href'].split('?')[0]
        if re.fullmatch(pattern, link) is not None:
            link = base + link
            if link in found:
                continue

            links.add(link)
            found.add(link)

    return set(links)


async def main():
    url = "https://www.ozon.ru/category/kompasy-11461/"
    browser, page = await get_browser()
    MAX_PAGE = 10
    i = 1
    all_links = []
    while i <= MAX_PAGE:
        # filename = f'page_' + str(i) + '.html'
        if i == 1:
            html = await get_page(page, url)
        else:
            url_param = url + '?page=' + str(i)
            html = await get_page(page, url_param)
        links = parse_data(html)
        print(links)
        print('done')
        all_links = all_links + list(links)
        i += 1

    with open('product_links.txt', 'w', encoding='utf-8') as f:
        for link in all_links:
            f.write(link + '\n')

if __name__ == '__main__':
    asyncio.run(main())
