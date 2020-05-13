#Get full list of stock tickers from GuruFocus

import requests
from bs4 import BeautifulSoup
import pandas as pd
import time

total_pages = 693 #At time of 2020-05-13 there were 693 pages 20763 total records

dt = []
for page in range(total_pages):
    
    params = {'r': 'USA','p':page}

    response = requests.get('https://www.gurufocus.com/stock_list.php', params=params)
    soup = BeautifulSoup(response.content, "html.parser")
    
    ticker_table = soup.find('table',attrs = {'id':'R1'})

    rows = ticker_table.findAll('tr')
    
    for row in rows[1:]: #first row is headers
        cols = row.findAll('td')
        ticker = cols[0].text.strip()
        link = cols[0].find('a')['href']
        name = cols[1].text.strip()
        dt.append({'ticker':ticker, 'link':link, 'name':name})
    
    time.sleep(2) #to stop site from crashing
    
    print("page", page+1, "out of", total_pages+1, "done")

df = pd.DataFrame(dt)
df.to_excel("c:/users/choys1388/desktop/the data incubator/data/full_tickers.xlsx", index = False)