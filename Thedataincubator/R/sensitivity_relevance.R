library(data.table)

setwd("c:/users/choys1388/desktop/the data incubator")

#read google trends data
trends_dt = fread("data/coronavirus_google_trends.csv")
trends_dt[,date:=as.Date(date)]

#list of files in stockprices folder
files = list.files("data/stockprices")

stockprices = list()
for (i in 1:length(files)){
  #read stock prices
  stock_dt = fread(paste("data/stockprices/",files[i], sep=""))
  
  #change date
  stock_dt[,date:=as.Date(date)]
  
  #merge dates in google trends data
  stock_dt = merge(trends_dt, stock_dt, by = "date", all.x = TRUE)
  
  #order by date
  stock_dt = stock_dt[order(date)]
  
  #forward fill missing adjusted close
  setnames(stock_dt, "adjusted close", "adj_cls")
  stock_dt[, adj_cls_filled := adj_cls[1], .(cumsum(!is.na(adj_cls)))]
  
  #attach ticker
  ticker = strsplit(files[i],"[.]")[[1]][1]
  stock_dt[,ticker:=ticker]
  
  #keep only ticker, date, adj_cls_filled, and index
  stockprices[[i]] = stock_dt[,.(ticker, date, adj_cls_filled, index)]
}

full_dt = rbindlist(stockprices)[!is.na(adj_cls_filled)]

#custom function to extract beta and r_squared
beta_rsq = function(adj.prc, index){
  model = lm(adj.prc~index)
  beta = model$coefficients[2]
  r_sq = summary(model)$r.squared
  
  return(list(beta = beta, r_sq = r_sq))
}

#compute beta and r_squared for each stock
ag_dt = full_dt[,beta_rsq(adj_cls_filled, index), by = ticker]

#Sort based on r squared
ag_dt = ag_dt[!is.na(beta)][order(-r_sq)]
head(ag_dt)



