#로이터 뉴스 Scrape
import requests
import os
from bs4 import BeautifulSoup
import time
import datetime
os.chdir("c:/users/choys1388/desktop/creon/ideas/reuter")

cont = True
page = 2361
while cont:
    #response = requests.get(f'https://www.reuters.com/finance/markets/asia?view=page&page={page}&pageSize=10')
    response = requests.get(f'https://www.reuters.com/news/archive/marketsNews?view=page&page={page}&pageSize=10')
    # with open(f"indexes/{page}.html", "wb") as f:
    #     f.write(response.content)
    # 
    soup = BeautifulSoup(response.content, "html.parser")
    headline_list = soup.find('div', attrs = {'class':'column1 col col-10'})
    stories = headline_list.findAll('article', attrs = {'class':'story'})
    
    for i,story in enumerate(stories):
        try:
            if datetime.datetime.strptime(story.find('time').text.strip(), "%b %d %Y").year <2017:
                cont = False
                break
        except:
            print(story.find('time').text.strip())
            
        news_link = story.find('div', attrs={'class':'story-content'}).find('a')['href']
        if len(news_link.split("-id")) == 1:
            news_id = news_link.split("/id")[-1]
        else:
            news_id = news_link.split("-id")[-1]
        while True:
            try:
                req = requests.get('https://www.reuters.com'+ news_link)
                break
            except:
                pass
        soup2 = BeautifulSoup(req.content, "html.parser")
        main = soup2.findAll('div', attrs = {'class':'StandardArticleBody_body'})
        if len(main) == 0:
            with open(f"errors/{i}-{news_id}.html", "w") as f:
                f.write(news_link)
                continue
        with open(f"market/{news_id}.html","wb") as f:
            f.write(req.content)
        

        time.sleep(2)
    if page %10==1:
        print(page)
    
    
    time.sleep(2)
    page+=1