import requests
import base64
import locale
import json
import datetime
from bs4 import BeautifulSoup, Comment, Tag
from tqdm.auto import trange
import concurrent.futures as pool
import threading, time
import hashlib
import config
from config import *
import logging
from sql import *
from urllib.parse import urlparse

locale.setlocale(locale.LC_TIME, "ru_RU")

log = logging.getLogger("parser")
# log_level = logging.INFO

rs = requests.session()
rs.headers = headers
rs.proxies = proxies[iter_proxy]

rs.verify = False


def GET(url):
    _log = logging.getLogger('parser.GET')

    def with_proxy(url, proxy):
        px = {
            'http': 'http://' + proxy,
            'https': 'http://' + proxy
        }
        try:
            _log.info(f'Try to {url} with proxy {px["https"]}')
            resp = rs.get(url, proxies=px)
            if resp.status_code in [200, 201]:
                return resp
            else:
                _log.info(f'Failed with status {resp.status_code}')
        except:
            return None

    try:
        resp = rs.get(url)
        _log.debug(f'{resp.status_code}')
        if resp.status_code in [200, 201]:
            return resp
        else:
            for p in proxies:
                try:
                    resp = with_proxy(url, p)
                    if resp.status_code in [200, 201]:
                        return resp
                    else:
                        _log.info(f'Failed with status {resp.status_code}')
                except Exception as e:
                    _log.info(f'Failed with error {e}')
                    pass
    except Exception as e:
        for p in proxies:
            try:
                resp = with_proxy(url, p)
                if resp.status_code in [200, 201]:
                    return resp
                else:
                    _log.info(f'Failed with status {resp.status_code}')
            except Exception as e:
                pass
        _log.info(f'{url} failed')
        return None


def get_articles_links(dep_link = 'ministers/'):
    links = []
    #last_date = sql_get_last_link_date()
    last_date = None
    if last_date:
        last_dt = datetime.datetime.strptime(last_date,"%Y-%m-%d")
    else:
        last_dt = datetime.datetime.strptime('1900-01-01', "%Y-%m-%d")
    _log = logging.getLogger('parser.getlinks')
    init_url = base_url + dep_link
    resp = GET(init_url)
    html = resp.text
    soup = BeautifulSoup(html, features='html.parser')
    nav = soup.select_one('div.jeg_navigation')
    a_s = nav.select('a.page_number')
    total_pages = int(a_s[-1:][0].text.strip())
    _log.info(f'Total pages: {total_pages}')
    _log.info(f'Found last date in DB: {last_date}')
    for current_page in trange(total_pages, 1, desc='Loading links...'):
        d = {}
        page_url = init_url + f'page/{current_page}/'
        resp = GET(page_url)
        if resp:
            arr = []
            html = resp.text
            soup = BeautifulSoup(html, features='html.parser')
            art_list = soup.select('div.jeg_posts>.jeg_post')
            _log.info(f'[{round(current_page/total_pages*100,2)}%] Processing {current_page} of {total_pages} -=-=- URL: {page_url}...')
            for article in art_list:
                try:
                    a = article.find('a')
                    h2 = article.find('h3').text.strip()
                    date = article.select_one('div.jeg_meta_date').text.strip()
                    dt = datetime.datetime.strptime(date, '%d/%m/%Y')
                    date = dt.strftime('%Y-%m-%d')
                    if last_date:
                        days_diff = (dt - last_dt).days
                        if days_diff <= -1:
                            _log.info(f'Links get ended with last_date = {last_date} and current date = {date}')
                            return True
                    link = a['href']
                    d = {}
                    d = {
                        'name': h2,
                        'date': date,
                        'link': link
                    }
                    #print((d['name'],d['link'],d['date']))
                    arr.append(d)
                    links.append(d)
                except Exception as e:
                    _log.error(e)
            sql_push_links(arr)
            arr.clear()
        else:
            _log.error(f'[{page_url}] FAILED!')


def clear_page(post) -> str:
    def get_img_to_base64(img_src: str):
        if img_src.find('data:image') < 0:
            if img_src[:2] == '//':
                img_src = 'https:' + img['src']
            elif img_src[0] == '/':
                img_src = config.base_url[:-1] + img_src
            rblob = GET(img_src)
            if rblob:
                jpegs = ['jpg', 'jpeg']
                try:
                    ext = urlparse(img_src).path.split('/')[-1:][0].split('.')[-1:][0].lower()
                except:
                    ext = 'jpg'
                blob = rblob.content
                img_b64 = base64.b64encode(blob).decode()
                if ext in jpegs:
                    img_src = 'data:image/jpeg;base64,' + img_b64
                else:
                    img_src = 'data:image/png;base64,' + img_b64
                # img_src = 'data:image/png;base64,' + img_b64
                return img_src
            else:
                return None
        else:
            return img_src
    try:
        buf = []
        for element in post.findChildren(recursive=True):
            buf.append(element.__str__())
            if element.name == 'div':
                element.name = 'p'

        for element in post(text=lambda text: isinstance(text, Comment)):
            element.extract()

        for element in post.findChildren(recursive=True):
            if 'class' in element.attrs.keys():
                if element.attrs['class'] == ['mainPic']:
                    element.extract()
            if element.name in ['em', 'strong', 'span', 'a']:
                element.unwrap()
            if (element.text.strip(' \n\r') in ['', ' ']) and (not (element.name in ['img', 'iframe'])) and (
                    len(element.contents) == 0):
                element.extract()
            if element.name not in ['img', 'iframe']:
                element.attrs = {}
            else:
                if 'src' in element.attrs.keys():
                    src = element.attrs['src']
                    element.attrs = {'src': src}
                    if element.parent.name == 'p':
                        parent = element.parent
                        img = element.extract()
                        parent.insert_before(img)
                else:
                    element.extract()

        buf = []
        try:
            for img in post.find_all('img'):
                # del img['width']
                img_res = get_img_to_base64(img['src'])
                if img_res:
                    img.attrs = {}
                    img['src'] = img_res
                else:
                    img.extract()
                # img_links.append(img['src'])
        except Exception as e:
            pass
        for element in post.findChildren(recursive=False):
            if (element.text.strip(' \n\r') not in ['']) or (element.name in ['img', 'iframe']):
                buf.append(element.__str__())

        post = ''.join(buf)
    except:
        post = None
    return post


def parse_article(url):
    _log = logging.getLogger('parser.parsearticle')
    resp = GET(url)
    d = None
    img = None
    if resp:
        m = hashlib.md5()
        m.update(url.encode('utf-8'))
        local_id = int(str(int(m.hexdigest(), 16))[:9])
        origin = f'{urlparse(url).scheme}://{urlparse(url).netloc}/'
        html = resp.text
        soup = BeautifulSoup(html, features='html.parser')
        full = soup.select_one('div.jeg_inner_content')
        try:
            date = datetime.datetime.strptime(full.select_one('div.jeg_meta_date').text.strip(),"%d/%m/%Y").strftime("%Y-%m-%d")
        except:
            date = None
        title = full.select_one('h1.jeg_post_title').text.strip()

        try:
            img_src = full.select_one('div.featured_image>img')['data-src']
            if img_src[0] == '/':
                img_src = base_url[:-1] + img_src
            b_data = GET(img_src).content
            try:
                ext = urlparse(img_src).path.split('/')[-1:][0].split('.')[-1:][0]
            except:
                ext = 'jpg'
            img = {
                'source': url,
                'ext': ext,
                'b_data': b_data
            }
        except:
            img = None


        post = full.select_one('div.news_one')
        if not post:
            post = full.select_one('div.post_content')

        try:
            tags = [a.text.strip() for a in full.select('div.jeg_post_tags>a')]
        except:
            tags = None

        post = clear_page(post)

        d = {
            'local_id':local_id,
            'name': title,
            'origin': origin,
            'source': url,
            'description': post,
        }
        if date:
            d.update({'date': date})
        if tags:
            d.update({'tags': '|'.join(tags)})
        if not post:
            d = None

    if d:
        if sql_add_article(d):
            config.CURRENT_LINK += 1
            if img:
                sql_add_image(img)
            _log.info(
                f'[{round(config.CURRENT_LINK / config.TOTAL_LINKS * 100, 2)}%] {config.CURRENT_LINK} of {config.TOTAL_LINKS} -=- {url} parsed and added')
        else:
            _log.info(f'{url} parsed, NOT added')
    else:
        _log.info(f'{url} FAILED')


def parse_articles(links: dict):
    _log = logging.getLogger('parser.parse_articles')
    urls = [link['link'] for link in links]
    for url in urls:
        d = parse_article(url)
        # if d:
        #     if sql_add_article(d):
        #         config.CURRENT_LINK += 1
        #         _log.info(f'[{round(config.CURRENT_LINK / config.TOTAL_LINKS * 100, 2)}%] {config.CURRENT_LINK} of {config.TOTAL_LINKS} -=- {url} parsed and added')
        #     else:
        #         _log.info(f'{url} parsed, NOT added')
        # else:
        #     _log.info(f'{url} FAILED')


def multithreaded_parse_articles(links: dict):
    _log = logging.getLogger('parser.multiparse')
    t_s = []
    tc = THREADS

    l_count, l_mod = divmod(len(links), tc)

    l_mod = len(links) % tc

    if l_mod != 0:

        l_mod = len(links) % THREADS
        if l_mod == 0:
            tc = THREADS
            l_count = len(links) // tc

        else:
            tc = THREADS - 1
            l_count = len(links) // tc

    l_c = []
    for i in range(0, THREADS):
        _log.info(f'{i + 1} of {THREADS}')

        l_c.append(links[l_count * i:l_count * i + l_count])

    for i in range(0, THREADS):
        t_s.append(
            threading.Thread(target=parse_articles, args=(l_c[i],), daemon=True))
    for t in t_s:
        t.start()

        _log.info(f'Started thread #{t_s.index(t) + 1} of {len(t_s)} with {len(l_c[t_s.index(t)])} links')

    for t in t_s:
        t.join()
        _log.info(f'Joined thread #{t_s.index(t) + 1} of {len(t_s)} with {len(l_c[t_s.index(t)])} links')


if __name__ == "__main__":
    pass
