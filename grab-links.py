from bs4 import BeautifulSoup as bs
import re
import asyncio
from bs4 import BeautifulSoup
from pyppeteer import launch
import os
from collections import deque
import asyncio
import sys
import collections 
if sys.version_info.major == 3 and sys.version_info.minor >= 10:
    from collections.abc import MutableSet
    collections.MutableSet = collections.abc.MutableSet
else:
    from collections import MutableSet

class OrderedSet(collections.MutableSet):

    def __init__(self, iterable=None):
        self.end = end = [] 
        end += [None, end, end]         # sentinel node for doubly linked list
        self.map = {}                   # key --> [key, prev, next]
        if iterable is not None:
            self |= iterable

    def __len__(self):
        return len(self.map)

    def __contains__(self, key):
        return key in self.map

    def add(self, key):
        if key not in self.map:
            end = self.end
            curr = end[1]
            curr[2] = end[1] = self.map[key] = [key, curr, end]

    def discard(self, key):
        if key in self.map:        
            key, prev, next = self.map.pop(key)
            prev[2] = next
            next[1] = prev

    def __iter__(self):
        end = self.end
        curr = end[2]
        while curr is not end:
            yield curr[0]
            curr = curr[2]

    def __reversed__(self):
        end = self.end
        curr = end[1]
        while curr is not end:
            yield curr[0]
            curr = curr[1]

    def pop(self, last=True):
        if not self:
            raise KeyError('set is empty')
        key = self.end[1][0] if last else self.end[2][0]
        self.discard(key)
        return key

    def __repr__(self):
        if not self:
            return '%s()' % (self.__class__.__name__,)
        return '%s(%r)' % (self.__class__.__name__, list(self))

    def __eq__(self, other):
        if isinstance(other, OrderedSet):
            return len(self) == len(other) and list(self) == list(other)
        return set(self) == set(other)

    
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

    used = OrderedSet([i.strip() for i in open(used_path,
               'r').readlines() if re.fullmatch(full_pattern, i.strip()) is not None])
    found = OrderedSet([i.strip() for i in open(found_path,
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
