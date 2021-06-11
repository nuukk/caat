googletrendscrawling <- function(x,y,date)
{
  if(missing(date)) date <- paste0("2004-01-01 ",ceiling_date(as.Date(gsub(month(Sys.Date()),month(Sys.Date())-1,Sys.Date())),unit='months')-1)
  temp <- tryCatch(gtrends(keyword=x,
                           geo=y,
                           time=date,
                           onlyInterest=TRUE)$interest_over_time,
                   error=function(e) {})
  if(!is.null(temp)) return(temp)
  if(is.null(temp)) return(data.table(date=as.POSIXct('1900-01-02 GMT'),hits=as.integer(-100),
                                      keyword=x,
                                      geo=y,time=as.character('1900-01-01'),gprop=as.character('NA'),category=as.integer(0)))
}
