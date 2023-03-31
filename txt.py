from bs4 import BeautifulSoup, Comment, Tag
import requests

url = 'https://ruscrime.com/how-a-socialite-from-rublyovka-owed-hundreds-of-millions-and-went-to-live-in-monaco/'
def Get(url):
    r = requests.get(url)
    html = r.text
    with open('tst.html', 'w', encoding='utf-8') as f:
        f.write(html)


#Get(url)

with open('tst.html','r',encoding='utf-8') as f:
    html = f.read()

soup = BeautifulSoup(html, features='html.parser')
full = soup.select_one('div.jeg_inner_content')
date = full.select_one('div.jeg_meta_date').text.strip()
title = full.select_one('h1.jeg_post_title').text.strip()

post = full.select_one('div.news_one')
if not post:
    post = full.select_one('div.post_content')


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
    if (element.text.strip(' \n\r') in ['',' ']) and (not (element.name in ['img', 'iframe'])) and (
            len(element.contents) == 0):
        element.extract()
    if element.name not in ['img', 'iframe']:
        element.attrs={}
    else:
        if 'src' in element.attrs.keys():
            src = element.attrs['src']
            element.attrs = {'src':src}
            if element.parent.name == 'p':
                parent = element.parent
                img = element.extract()
                parent.insert_before(img)
        else:
            element.extract()


buf = []
for element in post.findChildren(recursive=False):
    if (element.text.strip(' \n\r') not in ['']) or (element.name in ['img', 'iframe']):
        buf.append(element.__str__())

post = ''.join(buf)



with open('tst_out.html', 'w', encoding='utf-8') as f:
    f.write(f'<html><head><title>{title}</title></head><body>'+post+'</html></html>')