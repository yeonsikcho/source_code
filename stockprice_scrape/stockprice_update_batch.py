#2020-02-14
#Stock price scraping
#http://eric-cho.com/2020-02-14/stock-price-database-1

import requests
import json
import time
import datetime
import MySQLdb as sql


#Read Database User ID / User PW
with open("../credentials.txt", "r") as f:
    _, user_id, user_pw, _ = f.read().split("|")

headers = {
    'Connection': 'keep-alive',
    'Accept': 'application/json, text/javascript, */*; q=0.01',
    'X-Requested-With': 'XMLHttpRequest',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.100 Safari/537.36',
    'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
    'Origin': 'http://marketdata.krx.co.kr',
    'Referer': 'http://marketdata.krx.co.kr/mdi',
    'Accept-Language': 'ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7',
}

#OTP removed
data = {
  'market_gubun': 'KSQ',
  'sect_tp_cd': 'ALL',
  'schdate': '20200214',
  'pagePath': '/contents/MKD/13/1302/13020101/MKD13020101.jsp',
  'code': 'OTP removed',
  'curPage':5
}

start_date = datetime.datetime(2009,12,30) #Collection Start Date
end_date = datetime.datetime(2020,2,14) #Collection End Date

#Connect to database (id/pw removed)
con = sql.connect(host="eric-cho.com", port=3308, database="echo_blog", user=user_id, password=user_pw, charset="utf8")
cursor = con.cursor() 

trade_date = start_date
while trade_date<=end_date: #iterate through tradedates
    data['schdate'] = trade_date.strftime("%Y%m%d")
    for exchange in ['STK','KSQ']: #iterate through exchanges
        temp_dt = []
        data['market_gubun'] = exchange   
        for i in range(1, 30): #iterate through pages
            data['curPage'] = i
            response = requests.post('http://marketdata.krx.co.kr/contents/MKD/99/MKD99000001.jspx', headers=headers, data=data)
            dt = json.loads(response.content)['시가총액 상하위']
            temp_dt+=dt
            if len(dt)==0: #When tradedate/exchange is finished print summary
                n_items = len(set([td['isu_cd'][:5] for td in temp_dt]))
                print(trade_date, exchange, "page", i-1, "is last.", n_items, "items" )
                break
            for d in dt: #insert data into database
                cursor.execute("insert into stockprices values (%s,%s,%s,%s,%s,%s,%s,%s)",[d['isu_cd'],trade_date, d['opnprc'].replace(",",""),d['hgprc'].replace(",",""), d['lwprc'].replace(",",""), d['isu_cur_pr'].replace(",",""),d['lst_stk_vl'].replace(",",""), exchange])
            con.commit()
            time.sleep(2) #generally good idea to delay requests so that host server doesn't crash

    trade_date = trade_date+datetime.timedelta(1)
con.close()