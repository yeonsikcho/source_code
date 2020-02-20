import requests
import json
import time
import datetime
import MySQLdb as sql
import telegram

my_token = '979633634:AAGYggRYyVocVagOgVFhQFcTNm2jcGSpe4E'
chat_id = 869418718

bot = telegram.Bot(token = my_token)
bot.sendMessage(chat_id = chat_id, text = "Stock Price Collection Started")

try:
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

    data = {
      'market_gubun': 'KSQ',
      'sect_tp_cd': 'ALL',
      'schdate': '20200214',
      'pagePath': '/contents/MKD/13/1302/13020101/MKD13020101.jsp',
      'code': 't6Z77r6SW62aLBwg1RhRVD/lqxrUElM6p2vnxibMnyi1s5W6XhjU2Aar9u7nxLo2RkmXdfe8sPy1PN6QT9lLy8JnN36zTKKVW2ShPKTzD2A/D/Exdk350oQ7mq7A7Eu09YRXOdH4fnAwGqUNV5MO/w==',
      'curPage':5
    }

    con = sql.connect(host="eric-cho.com", port=3308, database="echo_blog", user="eric", password="1127ychL!", charset="utf8")
    cursor = con.cursor() 

    trade_date = datetime.datetime.today()
    data['schdate'] = trade_date.strftime("%Y%m%d")
    for exchange in ['STK','KSQ']:
        temp_dt = []
        data['market_gubun'] = exchange   
        for i in range(1, 30):
            data['curPage'] = i
            response = requests.post('http://marketdata.krx.co.kr/contents/MKD/99/MKD99000001.jspx', headers=headers, data=data)
            dt = json.loads(response.content)['시가총액 상하위']
            temp_dt+=dt
            if len(dt)==0:
                n_items = len(set([td['isu_cd'][:5] for td in temp_dt]))
                bot.sendMessage(chat_id = chat_id, text = f"{exchange} page {i-1} is last. {n_items} items")
                break
            for d in dt:
                cursor.execute("insert into stockprices values (%s,%s,%s,%s,%s,%s,%s,%s)",[d['isu_cd'],trade_date, d['opnprc'].replace(",",""),d['hgprc'].replace(",",""), d['lwprc'].replace(",",""), d['isu_cur_pr'].replace(",",""),d['lst_stk_vl'].replace(",",""), exchange])
            con.commit()
            time.sleep(2)
except Exception as e:
    bot.sendMessage(chat_id = chat_id, text = str(e))

bot.sendMessage(chat_id = chat_id, text = "Stock Price Collection Ended")

