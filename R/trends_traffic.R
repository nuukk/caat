trends_traffic <- function(name,dbname)
{
  name <- deparse(substitute(name))
  if(missing(dbname)) dbname <- name

  keyword_info <- readxl::read_excel(normalizePath(enc2native(choose.files(caption='Select Keyword Data File'))),range='B4:E1000')
  keyword_info <- data.table(keyword_info[complete.cases(keyword_info),])
  keyword_info$국가[keyword_info$국가=='UK'] <- 'GB'

  gen0 <- data.table::data.table(reduce(map2(keyword_info$General,keyword_info$국가,googletrendscrawling),plyr::rbind.fill))
  brn0 <- data.table::data.table(reduce(map2(keyword_info$Brand,keyword_info$국가,googletrendscrawling),plyr::rbind.fill))

  keyword_info$국가[keyword_info$국가=='GB'] <- 'UK'
  gen0$geo[gen0$geo=='GB'] <- 'UK'
  brn0$geo[brn0$geo=='GB'] <- 'UK'

  gen0[,product_type:=as.character(map2(gen0$geo,gen0$keyword,function(x,y) {keyword_info$제품군[keyword_info$국가==x & keyword_info$General==y]}))]
  brn0[,product_type:=as.character(map2(brn0$geo,brn0$keyword,function(x,y) {keyword_info$제품군[keyword_info$국가==x & keyword_info$Brand==y]}))]
  gen0 <- select(gen0,date,country=geo,kw_G=keyword,G_trends=hits,product_type)
  brn0 <- select(brn0,date,country=geo,kw_B=keyword,B_trends=hits,product_type)

  code <- c('오스트레일리아'='AU','브라질'='BR','독일'='DE','스페인'='ES','프랑스'='FR',
            '인도'='ID','인도네시아'='IN','이탈리아'='IT','네덜란드'='NL','러시아'='RU',
            '태국'='TH','영국'='GB','미국'='US')
  sec_gen_filelist <- normalizePath(enc2native(choose.files(caption='추가 General RAW를 선택하세요')))
  cl <- parallel::makeCluster(detectCores()-1)
  doParallel::registerDoParallel(cl)
  sec_gen <- foreach::foreach(i=seq_along(sec_gen_filelist),.combine=rbind, .packages=c('data.table','stringr')) %dopar%
    {
      data.table(fread(sec_gen_filelist[1],skip=3,header=FALSE),
                 kw_G=str_split_fixed(fread(sec_gen_filelist[1],skip=1,nrow=1,encoding='UTF-8',header=FALSE)$V2,":",2)[[1]],
                 country=str_split_fixed(fread(sec_gen_filelist[1],skip=1,nrow=1,encoding='UTF-8',header=FALSE)$V2,":",2)[[2]])
    }
  stopCluster(cl)
  sec_gen[,country:=code[trimws(gsub("\\(|\\)","",country),'left')]]
  sec_gen <- sec_gen[,.(date=as.Date(paste0(V1,"-01")),country,kw_G,G_trends=as.character(V2))]
  sec_gen[,product_type:=as.character(map2(sec_gen$country,sec_gen$kw_G,function(x,y) {keyword_info$제품군[keyword_info$국가==x & keyword_info$General==y]}))]
  gen0 <- filter(bind_rows(gen0,sec_gen),year(date)>=2004)

  sec_brn_filelist <- normalizePath(enc2native(choose.files(caption='추가 Brand RAW를 선택하세요')))
  cl <- parallel::makeCluster(detectCores()-1)
  doParallel::registerDoParallel(cl)
  sec_brn <- foreach::foreach(i=seq_along(sec_brn_filelist),.combine=rbind, .packages=c('data.table','stringr')) %dopar%
    {
      data.table(fread(sec_brn_filelist[1],skip=3,header=FALSE),
                 kw_B=str_split_fixed(fread(sec_brn_filelist[1],skip=1,nrow=1,encoding='UTF-8',header=FALSE)$V2,":",2)[[1]],
                 country=str_split_fixed(fread(sec_brn_filelist[1],skip=1,nrow=1,encoding='UTF-8',header=FALSE)$V2,":",2)[[2]])
    }
  stopCluster(cl)
  sec_brn[,country:=code[trimws(gsub("\\(|\\)","",country),'left')]]
  sec_brn <- sec_brn[,.(date=as.Date(paste0(V1,"-01")),country,kw_B,B_trends=as.character(V2))]
  sec_brn[,product_type:=as.character(map2(sec_brn$country,sec_brn$kw_B,function(x,y) {keyword_info$제품군[keyword_info$국가==x & keyword_info$General==y]}))]
  brn0 <- filter(bind_rows(brn0,sec_brn),year(date)>=2004)

  setkey(gen0,date,country,product_type)
  setkey(brn0,date,country,product_type)
  trends <- full_join(gen0,brn0)
  trends[,`:=`(date=as.Date(date),country=toupper(country),product_type=toupper(product_type),G_trends=as.numeric(G_trends),B_trends=as.numeric(B_trends))]

  traffic <- data.table(read_excel(normalizePath(enc2native(choose.files()))))
  traffic[,`:=`(month=as.Date(month),country=toupper(country),product=toupper(product))]
  setkey(traffic,country,month,product)

  setkey(trends,country,date,product_type)
  raw <- traffic[,c('country','month','product',traffic='natural traffic')][trends][,.(country,product_type=product,month,traffic=`natural traffic`,kw_G,G_trends,kw_B,B_trends)]
  raw <- arrange(filter(raw,year(month)>=2019),country,product_type,kw_G,kw_B,month)

  directory <- choose.dir(caption='Select Save Folder')
  writexl::write_xlsx(raw,paste0(directory,"/",name,".xlsx"))
  tre_trf <- assign(tre_trf,.GlobalEnv)
}
