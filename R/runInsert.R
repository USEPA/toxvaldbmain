library(RMySQL)
library(DBI)

#--------------------------------------------------------------------------------------
#' Insert a record into a database. if auto.increment=TRUE, return the auto incremented
#' primary key of the record. otherwise, return -1
#' @param query a properly formatted SQL query as a string
#' @param db the name of the database
#' @param do.halt if TRUE, halt on errors or warnings
#' @param verbose if TRUE, print diagnostic information
#' @param auto.increment if TRUE, add the auto increment primary key even if not part of the query
#' @return Returns the database table auto incremented primary key ID
#' @export
#--------------------------------------------------------------------------------------
runInsert <- function(query,db,do.halt=F,verbose=F,auto.increment.id=F) {

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
    cat("query: ",query,"\n")
    cat("db: ",db,"\n")
  }
  id <- -1
  tryCatch({
    con <- dbConnect(drv=RMySQL::MySQL(),user=DB.USER,password=DB.PASSWORD,host=DB.SERVER,dbname=db)
    rs <- dbSendQuery(con, query)
    dbHasCompleted(rs)
    dbClearResult(rs)
    if(auto.increment.id) {
      rs2 <- dbSendQuery(con, "select LAST_INSERT_ID()")
      d2 <- dbFetch(rs2, n = -1)
      id <- d2[1,1]
      dbHasCompleted(rs2)
      dbClearResult(rs2)
    }
    dbDisconnect(con)
  }, warning = function(w) {
    cat("WARNING:",query," : [",db,"]\n",sep="")
    if(do.halt) browser()
    dbDisconnect(con)
    if(auto.increment.id) return(-1)
  }, error = function(e) {
    cat("ERROR:",query," : [",db,"]\n",sep="")
    print(e)
    if(do.halt) browser()
    dbDisconnect(con)
    if(auto.increment.id) return(-1)
  })
  if(auto.increment.id) return(id)
}

