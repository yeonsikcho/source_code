#Scrape Reuter Data
import requests
import os
from bs4 import BeautifulSoup
import time
import datetime

cont = True
page = 1
while cont:
    response = requests.get(f'https://www.reuters.com/finance/markets/asia?view=page&page={page}&pageSize=10')
    soup = BeautifulSoup(response.content, "html.parser")
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
        req = requests.get('https://www.reuters.com'+ news_link)
        soup2 = BeautifulSoup(req.content, "html.parser")
        main = soup2.findAll('div', attrs = {'class':'StandardArticleBody_body'})
        if len(main) == 0:
            with open(f"errors/{i}-{news_id}.html", "w") as f:
                f.write(news_link)
                continue
        with open(f"asian_market/{news_id}.html","wb") as f:
            f.write(req.content)
        

        time.sleep(2)
    if page %10==1:
        print(page)
    
    
    time.sleep(2)
    page+=1