library(data.table)

setwd("c:/users/choys1388/desktop/the data incubator")

files = list.files("data/stockprices")

stockprices = list()
for (i in 1:length(files)){
  stockprices[[i]] = fread(paste("data/stockprices/",files[i], sep=""))
  stockprices[[i]][ticker:=strsplit(files[1],"[.]")[[1]][1]]
}

compiled_dt = rbindlist(stockprices)
compiled_dt[,date:=as.Date(date)]


trends_dt = fread("data/coronavirus_google_trends.csv")

