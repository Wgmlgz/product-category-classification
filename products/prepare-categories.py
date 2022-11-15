import re

FIRST_PAGE = 1
MAX_PAGE = 10
full_pattern = re.compile(r'.+\/category\/[^\/]+-\d+\/')
found_path = '../parse/categories/found.txt'
found = [i.strip() for i in open(found_path,
                'r').readlines() if re.fullmatch(full_pattern, i.strip()) is not None]
print(len(found))

def get_links(url: str) -> set[str]:
    urls = []
    for i in range(FIRST_PAGE, MAX_PAGE + 1):
        if i == 1:
            urls.append(url)
        else:
            url_param = url + '?page=' + str(i)
            urls.append(url_param)
    return urls

all = []
for link in found:
  t = get_links(link)
  for i in t:
    all.append(i)

print(len(all))
import random
random.shuffle(all)
open('../parse/products/queue.txt', 'w+').write('\n'.join(all))
