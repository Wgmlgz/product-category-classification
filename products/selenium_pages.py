from pyppeteer import launch
from os import path

async def get_browser():
    browser = await launch()
    page = await browser.newPage()
    await page.setUserAgent('Mozilla/5.0 (Windows NT 5.1; rv:5.0) Gecko/20100101 Firefox/5.0')
    return browser, page


async def get_page(page, url: str):
    try:
        await page.goto(url, {'waitUntil': 'networkidle0'})
        html = await page.content()
        return html
    except Exception as ex:
        print(ex)

async def save_page(page, url: str, filename: str):
    try:
        await page.goto(url, {'waitUntil': 'networkidle0'})
        html = await page.content()
        with open(path.join('pages', filename), 'w+', encoding='utf-8') as f:
            f.write(html)
    except Exception as ex:
        print(ex)
