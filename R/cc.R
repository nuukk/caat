cc <- function(...)
{
  write.table(...,file='clipboard',row.names=F,sep="\t")
}
