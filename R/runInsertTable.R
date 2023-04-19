library(RMySQL)
library(DBI)
#--------------------------------------------------------------------------------------
#' Inserts multiple rows into a database table
#'
#' @param mat data frame containing the data, with the column names corresponding
#' @param table name of the database table to which data will be inserted
#' @param db the name of the database
#' @param do.halt if TRUE, halt on errors or warnings
#' @param verbose if TRUE, print diagnostic information
#--------------------------------------------------------------------------------------
runInsertTable <- function(mat,table,db,do.halt=T,verbose=F,get.id=T) {
  if(!exists("DB.SERVER")) {
    cat("DB.SERVER not defined\n")
    return(NULL)
  }
  if(!exists("DB.USER")) {
    cat("DB.USER not defined\n")
    return(NULL)
  }
  if(!exists("DB.PASSWORD")) {
    cat("DB.PASSWORD not defined\n")
    return(NULL)
  }
  if(verbose) {
    printCurrentFunction()
    cat("mat: ",dim(mat),"\n")
    cat("table: ",table,"\n")
    cat("db: ",db,"\n")
  }
  tryCatch({
    con <- dbConnect(drv=RMySQL::MySQL(),user=DB.USER,password=DB.PASSWORD,host=DB.SERVER,dbname=db)
    res = dbWriteTable(con,name=table,value=mat,row.names=F,overwrite=F,append=T)
    if(get.id) {
      rs2 <- dbSendQuery(con, "select LAST_INSERT_ID()")
      d2 <- dbFetch(rs2, n = -1)
      id <- d2[1,1]
      dbHasCompleted(rs2)
      dbClearResult(rs2)
    }
    cat(">>> runInsertTable finished writing ",table,":",dim(mat),"\n")
    dbDisconnect(con)
  }, warning = function(w) {
    cat("WARNING:",table," : [",db,"]\n",sep="")
    dbDisconnect(con)
    if(do.halt) browser()
  }, error = function(e) {
    cat("ERROR:",table," : [",db,"]\n",sep="")
    print(e)
    dbDisconnect(con)
    if(do.halt) browser()
  })
  if(get.id) return(id)
}
