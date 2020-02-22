#Script to compute adjusted stock prices

import MySQLdb as sql

#Read Database User ID / User PW
with open("../credentials.txt", "r") as f:
    _, user_id, user_pw = f.read().split("|")

#Connect to Database
con = sql.connect(host="eric-cho.com", port=3308, database="echo_blog", user=user_id, password=user_pw, charset="utf8")
cursor = con.cursor() 

#Get list of all stocks in the database
cursor.execute("select distinct(isu_cd) from stockprices")
stockids = cursor.fetchall()

for idx,[stockid] in enumerate(stockids):
    cursor.execute("select tradedate, open_prc, cls_prc, list_stock_vol from stockprices where isu_cd = %s order by tradedate desc", [stockid])
    result = cursor.fetchall()
    for i, [tradedate, open_prc, cls_prc, list_stock_vol] in enumerate(result):
        if i == 0: #Set scale to 1 at most recent observation
            scale = 1
        elif next_open_prc !=0 and open_prc ==0: #Identify Suspension End Date
            if list_stock_vol == next_list_stock_vol: #Case 1
                pass
            elif next_list_stock_vol>list_stock_vol: #Case 2
                scale *= (list_stock_vol/next_list_stock_vol)
            elif list_stock_vol%next_list_stock_vol==0: #Case 3
                scale *= (list_stock_vol/next_list_stock_vol)
            else: # Case 4
                scale *= next_open_prc / cls_prc
        #Insert computed adjust price to database
        cursor.execute(f"update stockprices set adj_prc = %s where isu_cd = %s and tradedate = %s", [scale*cls_prc, stockid, tradedate])
        
        next_tradedate, next_open_prc, next_cls_prc, next_list_stock_vol = tradedate, open_prc, cls_prc, list_stock_vol
    print(idx, stockid)
    con.commit()