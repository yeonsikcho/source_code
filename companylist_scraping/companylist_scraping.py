import requests
from bs4 import BeautifulSoup
import time
import requests

data = {
  'currentPage': '225',
  'maxResults': '10',
  'maxLinks': '10',
  'searchIndex': '',
  'textCrpNmAddPer': 'true',
  'singleChoice': '',
  'textCrpNm': '',
  'corporationType': ''
}

con = sql.connect(host="eric-cho.com", port=3308, database="echo_blog", user="user_id", password="user_pw", charset="utf8")
cursor = con.cursor() 

page_count = 211

for i in range(1, page_count+1):
    if i<=161:
        continue
    data['currentPage'] = str(i)
    response = requests.post('http://englishdart.fss.or.kr/corp/searchCorpEngA.ax', data=data)
    soup = BeautifulSoup(response.content)

    tr_list = soup.findAll('tbody')[1].findAll('tr')
    for tr in tr_list:
        td_list = tr.findAll('td')
        company_name = td_list[0].text.strip()
        if company_name == '':
            break
        stockid = td_list[2].text.strip()
        cursor.execute("insert into companylist values (%s, %s)", [stockid, company_name])
    con.commit()
    print(i, "out of", page_count, "done")
    time.sleep(4)

con.close()