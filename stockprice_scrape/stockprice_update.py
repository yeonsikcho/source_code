#2020-02-17
#Daily stock price update code
#http://eric-cho.com/2020-02-17/stock-price-database-2

#2020-02-21
#Daily adjust price computation
#http://eric-cho.com/2020-02-21/stock-price-database-3

import requests
import json
import time
import datetime
import MySQLdb as sql
import telegram
import os

#Read Database User ID / User PW
os.chdir(os.path.dirname(os.path.abspath(__file__))) #change directory to file location
with open("../credentials.txt", "r") as f:
    my_token, user_id, user_pw, _ = f.read().split("|")

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

    con = sql.connect(host="eric-cho.com", port=3308, database="echo_blog", user=user_id, password=user_pw, charset="utf8")
    cursor = con.cursor() 
    
    #>>>2020-02-21 compute most recent tradedate
    cursor.execute("select max(tradedate) from stockprices limit 1")
    last_tradedate = cursor.fetchall()[0][0]
    #>>>

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
                #2020-02-21 Added adj_prc
                #2020-02-24 Added month_end, year_end
                cursor.execute("insert into stockprices values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",[d['isu_cd'],trade_date, d['opnprc'].replace(",",""),d['hgprc'].replace(",",""), d['lwprc'].replace(",",""), d['isu_cur_pr'].replace(",",""),d['isu_cur_pr'].replace(",",""),d['lst_stk_vl'].replace(",",""), exchange, None, None])
            con.commit()
            time.sleep(2)
            
    #>>>2020-02-21 compute adjusted prices for stocks that resumed trading
    if n_items > 0:        
        #Get list of stocks that were suspended in previous trading date
        cursor.execute("select distinct(isu_cd) from stockprices where tradedate = %s and open_prc = 0",[last_tradedate])
        stockids = cursor.fetchall()
        #Get list of stocks that resumed today        
        for idx,[stockid] in enumerate(stockids):
            cursor.execute("select tradedate, open_prc, cls_prc, list_stock_vol from stockprices where isu_cd = %s order by tradedate desc", [stockid])
            result = cursor.fetchall()
            for i, [tradedate, open_prc, cls_prc, list_stock_vol] in enumerate(result):
                if i == 0: #Set scale to 1 at most recent observation
                    if open_prc == 0: #if the stock is still suspended, pass
                        break
                    scale = 1
                    bot.sendMessage(chat_id = chat_id, text = f"{stockid} adjusted price updated")
                elif next_open_prc !=0 and open_prc ==0: #Identify Suspension End Date
                    print(tradedate, open_prc, cls_prc, list_stock_vol)
                    if list_stock_vol == next_list_stock_vol: #Case 1
                        pass
                    elif next_list_stock_vol>list_stock_vol: #Case 2
                        scale *= (list_stock_vol/next_list_stock_vol)
                    elif list_stock_vol%next_list_stock_vol==0: #Case 3
                        scale *= (list_stock_vol/next_list_stock_vol)
                    else: # Case 4
                        scale *= next_open_prc / cls_prc
                #Insert computed adjust price to database
                print(scale*cls_prc, stockid, tradedate)
                cursor.execute(f"update stockprices set adj_prc = %s where isu_cd = %s and tradedate = %s", [scale*cls_prc, stockid, tradedate])
                next_tradedate, next_open_prc, next_cls_prc, next_list_stock_vol = tradedate, open_prc, cls_prc, list_stock_vol
            con.commit()
    #>>>2020-02-24 when month / year changed edit month_end, year_end index
    if n_items > 0:
        if last_tradedate.month != trade_date.month:
            cursor.execute(f"update stockprices set month_end = 1 where tradedate = %s", [last_tradedate])
        if last_tradedate.year != trade_date.year:
            cursor.execute(f"update stockprices set year_end = 1 where tradedate = %s", [last_tradedate])
        
except Exception as e:
    bot.sendMessage(chat_id = chat_id, text = str(e))
con.close()
bot.sendMessage(chat_id = chat_id, text = "Stock Price Collection Ended")

