import requests
from urllib.parse import urlencode
from requests.exceptions import ConnectionError, InvalidSchema
from pyquery import PyQuery as pq
from pymongo import MongoClient
import re
import time
import asyncio
import aiohttp

CLIENT = MongoClient()
DB = CLIENT['weixin']
collection = DB['articels2']
PROXY_POOL_URL = 'http://localhost:5555/random'
REQUEST_URL = 'http://weixin.sogou.com/weixin?'
headers = {
    # 'Cookie':'',
    'Cookie': 'ABTEST=0|1530849374|v1; IPLOC=CN3717; SUID=DAE560DF2028940A000000005B3EE85E; SUID=DAE560DF1F13940A000000005B3EE85E; weixinIndexVisited=1; SUV=00463B60DF60E5DA5B3EE85F994D5323; JSESSIONID=aaab0nrtLQCK9dihmUgrw; ppinf=5|1530852987|1532062587|dHJ1c3Q6MToxfGNsaWVudGlkOjQ6MjAxN3x1bmlxbmFtZToxODolRTYlOTIlOTIlRTglQTUlQkZ8Y3J0OjEwOjE1MzA4NTI5ODd8cmVmbmljazoxODolRTYlOTIlOTIlRTglQTUlQkZ8dXNlcmlkOjQ0Om85dDJsdUNjOVRsSU9oREdhbzItMDR2YUliLWdAd2VpeGluLnNvaHUuY29tfA; pprdig=aiBAh-rU0jBnWAwWkU54DTaFOwtlYwXZrRoWnlYE_EUeBWU9xWStWJIeh0_R_cQrLME-IcQzQsYZUwZFpjOJGnwdIGu0H6VVJu6T6C1B98HyVeApG_7VVNDo79RB5HeVYd-atkorHphnocQpx7bODZV-JrltClqhRj3KH2W2ARI; sgid=10-35896555-AVsib9ntHGmLicR2m7ic7Zl6dI; PHPSESSID=4tfcjq57305ghkarsguu0qt866; SUIR=A7991CA27C780C31D158EE497DC9957C; ppmdig=1530876319000000b231a6910b68387bb5185858d380fb09; ld=Plllllllll2b5C2MlllllV7@UTclllllTLROLkllll9lllllxklll5@@@@@@@@@@; SNUID=7E40C47BA4A0D4F6DB7F73F9A5A44598; sct=18',
    'Host': 'weixin.sogou.com',
    'Upgrade-Insecure-Requests': '1',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/67.0.3396.62 Safari/537.36',
}


def get_index(key, type, page):
    para = {
        'query': key,
        'type': type,
        'page': page,
    }
    url = ''.join([REQUEST_URL, urlencode(para)])
    return get_html(url)


def get_proxy():
    try:
        response = requests.get(PROXY_POOL_URL)
        if response.status_code == 200:
            proxy = response.text
            proxies = {
                'http': 'http://' + proxy,
                'https': 'https://' + proxy
            }
            return proxies
    except ConnectionError:
        return None


def get_html(url, proxies=None):
    try:
        if proxies:
            response = requests.get(url, allow_redirects=False, proxies=proxies, headers=headers)
        else:
            response = requests.get(url, allow_redirects=False, headers=headers)
        if response.status_code == 200:
            doc = pq(response.text)
            flag = doc('#sogou_next').attr('href')
            return {
                'html': response.text,
                'flag': flag
            }
        else:
            proxies = get_proxy()
            print('change proxies to', proxies['http'])
            return get_html(url, proxies)
    except ConnectionError as e:
        return get_html(url)


def get_url(html):
    doc = pq(html)
    urls = doc('h3>a')
    for i in urls.items():
        yield i.attr('href'), doc('#sogou_next').attr('href')


def parse_detail(url):
    try:
        response = requests.get(url)
    except InvalidSchema:
        print('未查询到文章')
        return None
    else:
        html = response.text
        doc = pq(html)
        title = doc('#activity-name').text()
        wechat = doc('#js_name').text()
        content = doc('.rich_media_content ').text()
        try:
            date = re.search('(\d{4}-\d{2}-\d{2})', html).group(1)
        except Exception:
            print(url)
        data = {
            'title': title,
            'wechat': wechat,
            'content': content,
            'date': date,
        }
        return data


async def save_to_mongo(data):
    condition = {'title': data['title']}
    if collection.find_one(condition):
        if collection.update_one(condition, {'$set': data}):
            print('Update MongoDB success')
        else:
            print('Update MongoDB failed')
    else:
        if collection.insert_one(data):
            print('Saved to MongoDB success')
        else:
            print('Saved to MongoDB failed')


if __name__ == '__main__':
    start = time.time()
    print('Start……')
    i = 1
    tasks = []
    while True:
        html = get_index('东明大洋福邸二手房', '2', i)
        for url in get_url(html.get('html')):
            data = parse_detail(url[0])
            task = asyncio.ensure_future(save_to_mongo(data))
            tasks.append(task)
        print("第", i, '页')
        if html['flag']:
            i = i + 1
        else:
            break
    loop = asyncio.get_event_loop()
    loop.run_until_complete(asyncio.wait(tasks))
    print("总用时", time.time() - start)
