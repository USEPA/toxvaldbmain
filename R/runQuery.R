library(RMySQL)
library(DBI)
#--------------------------------------------------------------------------------------
#' Runs a database query and returns a result set
#'
#' @param query a properly formatted SQL query as a string
#' @param db the name of the database
#' @param do.halt if TRUE, halt on errors or warnings
#' @param verbose if TRUE, print diagnostic information
#' @export
#--------------------------------------------------------------------------------------
runQuery <- function(query=NULL,db,do.halt=T,verbose=F) {
  if(is.null(query)){
    cat("No query provided...\n")
    return(NULL)
  }

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
  tryCatch({
    con <- dbConnect(drv=RMySQL::MySQL(),user=DB.USER,password=DB.PASSWORD,host=DB.SERVER,dbname=db)
    rs <- suppressWarnings(dbSendQuery(con, query))
    d1 <- dbFetch(rs, n = -1)
    if(verbose) {
      print(d1)
      flush.console()
    }
    dbHasCompleted(rs)
    dbClearResult(rs)
    dbDisconnect(con)
    return(d1)
  }, warning = function(w) {
    cat("WARNING:",query,"\n")
    dbDisconnect(con)
    if(do.halt) browser()
    return(NULL)
  }, error = function(e) {
    #cat("ERROR:",query,"\n")
    cat("Error messge: ",paste0(e, collapse=" | "), "\n")
    dbDisconnect(con)
    if(do.halt) browser()
    return(NULL)
  })
}


