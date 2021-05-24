file2db <- function(name,dbname,directory,export_directory)
{
  if(missing(directory)) directory <- choose.dir(caption="파일들이 있는 폴더를 선택하세요")
  if(missing(export_directory)) export_directory <- choose.dir(directory,caption="전처리한 파일을 저장할 폴더를 선택하세요") #0520
  directory <- normalizePath(directory)
  export_directory <- normalizePath(export_directory)
  name <- deparse(substitute(name))
  dbname <- deparse(substitute(dbname))
  if(str_detect(dbname,'-|\\.|:')) {
    message(paste0(dbname,"에는 - 또는 ., :이 포함될 수 없습니다. -, ., :은 _로 대체됩니다"))
    dbname <- gsub("-|\\.","_",dbname)
  }
  # dbname <- deparse(substitute(dbname)) v0520에서 주석화
  file_list <- list.files(directory)
  sql_temp <- file_list[menu(file_list,graphics=TRUE,title="RAW 파일을 선택하세요")]
  while(!grepl('.csv$|.xlsx$|.xls$',sql_temp)) {
    sql_temp <- file_list[menu(file_list,graphics=TRUE,title="RAW 파일을 다시 선택하세요")]
  }
  if(grepl(".csv$",sql_temp)) {
    sql_temp <- fread(paste0(directory,"/",sql_temp),data.table=FALSE,encoding='UTF-8')
  } else {
    sql_temp <- read_excel(paste0(directory,"/",sql_temp))
  }
  sql_temp <- data.table(sql_temp)
  sql_temp[,country:=substr(url,
                            unlist(str_locate_all(url,".com"),use.names=F)[seq(4,length(url),by=4)]+2,
                            unlist(str_locate_all(url,"/"),use.names=F)[seq(4,length(url),by=4)]-1)]
  cat("선택하신 RAW 데이터의 국가별 행 수는 다음과 같습니다.\n")
  print(tryCatch(sql_temp[,.(Start_Date=min(date),End_Date=max(date),N=.N),by=country],
                 error=function(e) {print(cat("\n선택하신 파일은 양식이 다릅니다."))}))
  while(askYesNo("선택하신 RAW가 정확한가요?")!=TRUE) {
    sql_temp <- file_list[menu(file_list,graphics=TRUE,title="RAW 파일을 선택하세요")]
    while(!grepl('.csv$|.xlsx$|.xls$',sql_temp)) {
      sql_temp <- file_list[menu(file_list,graphics=TRUE,title="RAW 파일을 선택하세요")]
    }
    if(grepl(".csv$",sql_temp)) {
      sql_temp <- fread(paste0(directory,"/",sql_temp),data.table=FALSE,encoding='UTF-8')
    } else {
      sql_temp <- read_excel(paste0(directory,"/",sql_temp))
    }
    sql_temp <- data.table(sql_temp)
    sql_temp[,country:=substr(url,
                              unlist(str_locate_all(url,".com"),use.names=F)[seq(4,length(url),by=4)]+2,
                              unlist(str_locate_all(url,"/"),use.names=F)[seq(4,length(url),by=4)]-1)]
    cat("선택하신 RAW 데이터의 국가별 행 수는 다음과 같습니다.\n")
    print(tryCatch(sql_temp[,.(Start_Date=min(date),End_Date=max(date),N=.N),by=country],
                   error=function(e) {print(cat("\n선택하신 파일은 양식이 다릅니다."))}))
  }
  list_contain <- file_list[menu(file_list,graphics=TRUE,title="contain.xlsx을 선택하세요")]
  while(!grepl('.xlsx',list_contain) || length(read_excel(paste0(directory,"/",list_contain)))!=4 ||
        read_excel(paste0(directory,"/",list_contain))[4][[1]]!='contains') {
    list_contain <- file_list[menu(file_list,graphics=TRUE,title="contain.xlsx을 다시 선택하세요")]
  }
  list_contain <- read_excel(paste0(directory,"/",list_contain))
  list_equal <- file_list[menu(file_list,graphics=TRUE,title="equals.xlsx을 선택하세요")]
  while(!grepl('.xlsx',list_equal) || length(read_excel(paste0(directory,"/",list_equal)))!=4 ||
        read_excel(paste0(directory,"/",list_equal))[4][[1]]!='equal') {
    list_equal <- file_list[menu(file_list,graphics=TRUE,title="equals.xlsx을 다시 선택하세요")]
  }
  list_equal <- read_excel(paste0(directory,"/",list_equal))
  list_dnc <- file_list[menu(file_list,graphics=TRUE,title="doesnotcontains.xlsx을 선택하세요")]
  while(!grepl('.xlsx',list_dnc) || length(read_excel(paste0(directory,"/",list_dnc)))!=4 ||
        read_excel(paste0(directory,"/",list_dnc))[4][[1]]!='does not contains') {
    list_dnc <- file_list[menu(file_list,graphics=TRUE,title="doesnotcontains.xlsx을 다시 선택하세요")]
  }
  list_dnc <- read_excel(paste0(directory,"/",list_dnc))
  list_pf <- file_list[menu(file_list,graphics=TRUE,title="기초정보 파일을 선택하세요")]

  while(!grepl('.xlsx',list_pf) || length(read_excel(paste0(directory,"/",list_pf)))!=0 ||
        suppressMessages(read_excel(paste0(directory,"/",list_pf),sheet='Page Type',col_names=FALSE)[[6]][3])!='Page Type') {
    list_pf <- file_list[menu(file_list,graphics=TRUE,title="기초정보 파일을 다시 선택하세요")]
  }
  list_pf <- suppressMessages(read_excel(paste0(directory,"/",list_pf),sheet='Page Type'))
  colnames(list_pf) <- list_pf[2,]
  list_pf <- list_pf[-c(1:2),c(4:6)]
  colnames(list_pf)[3] <- 'page_type'
  sql_temp$date <- as.Date(sql_temp$date)
  sql_temp[,product_type:=NA]
  cl <- makeCluster(detectCores()-1)
  registerDoParallel(cl)
  temp1 <- foreach(i=1:length(list_dnc$제품군), .combine=rbind, .packages=c('stringr','data.table')) %dopar%
    {
      data.table(loc=which(str_detect(sql_temp$page,list_dnc$page[i])),
                 pd=list_dnc$제품군[i])
    }
  stopCluster(cl)
  temp1 <- na.omit(temp1)
  sql_temp$product_type[temp1$loc] <- temp1$pd
  df_dnc <- sql_temp %>% filter(!is.na(product_type))

  df_equal <- left_join(sql_temp %>% filter(is.na(product_type)),list_equal[,c(1,3)],by='page') %>%
    select(date,page,clicks,impressions,ctr,position,url,country,product_type=제품군)

  df_contain <- df_equal %>% filter(is.na(product_type))
  df_equal <- df_equal %>% filter(!is.na(product_type))

  cl <- makeCluster(detectCores()-1)
  registerDoParallel(cl)
  temp3 <- foreach(i=1:length(list_contain$제품군), .combine=rbind, .packages=c('stringr','data.table')) %dopar%
    {
      data.table(loc=which(str_detect(df_contain$page,list_contain$page[i])),
                 pd=list_contain$제품군[i])
    }
  stopCluster(cl)
  temp3 <- na.omit(temp3)
  df_contain$product_type[temp3$loc] <- temp3$pd
  df_all <- bind_rows(df_equal,df_contain,df_dnc) %>% filter(!is.na(product_type))
  
  df_all <- left_join(df_all,list_pf,by=c('page'='URL'))
  df_all$page_type[is.na(df_all$page_type)] <- 'PD'
  df_all$country <- toupper(df_all$country)
  df_all[,`:=`(country_tier=NA,index_position=impressions*position,year_week=isoyear(date),year_month=year(date),week=isoweek(date),month=month(date))]
  df_all$country_tier[df_all$country %in% c('US','UK','IN','DE','NL','RU')] <- 1
  df_all$country_tier[df_all$country %in% c('IT','FR','ID','AU','ES','BR')] <- 2
  df_all$country_tier[is.na(df_all$country_tier)] <- 3
  df_all <- df_all %>% select(country,country_tier,date,url=page,clicks,impressions,ctr,position,index_position,product_type,page_type,year_week,year_month,week,month)
  df_all$product_type[df_all$product_type=='기타'] <- 'Others'
  if(file.exists(paste0(export_directory,"/",name,".csv"))) {
    write.table(df_all,paste0(export_directory,"/",name,".csv"),row.names=F,append=T,col.names=F,sep=",")
  } else {
    write.table(df_all,paste0(export_directory,"/",name,".csv"),row.names=F,append=T,col.names=T,sep=",")
  }
  assign(name,df_all,.GlobalEnv)
  sql_temp[,date:=as.character(date)]
  a <- dbConnect(dbDriver("MySQL"),
                 dbname="si_4_team",
                 user="si_4_team_root",
                 password='dkxldjstmsi4root!1',
                 host='192.168.1.57',
                 port=3306)
  dbSendQuery(a, 'set character set "euckr"')
  dbWriteTable(a,dbname,df_all,append=TRUE,row.names=F,overwrite=F)
  dbDisconnect(a)
}
