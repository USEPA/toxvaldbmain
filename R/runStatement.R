# Script to add timestamp columns to toxval_source tables
# By: Jonathan Taylor Wall
# Created: 2022-05-25
# R version 4.1.2 (2021-11-01)
# RMySQL_0.10.23    DBI_1.1.2

#--------------------------------------------------------------------------------------
#' @title runStatement
#' @description Run a SQL statement, such as an ALTER or UPDATE
#' @param query a properly formatted SQL query as a string
#' @param db the name of the database
#' @param do.halt if TRUE, halt on errors or warnings
#' @param verbose if TRUE, print diagnostic information
#' @return None. SQL statement is run.
#' @import RMySQL DBI
#' @export
#' @details DETAILS
#' @examples
#' \dontrun{
#' if(interactive()){
#'  #EXAMPLE1
#'  }
#' }
#' @seealso
#'  \code{\link[RMySQL]{character(0)}}, \code{\link[RMySQL]{MySQLDriver-class}}
#' @rdname runStatement
#' @importFrom RMySQL dbConnect MySQL dbSendQuery dbHasCompleted dbClearResult dbDisconnect
#--------------------------------------------------------------------------------------
runStatement <- function(query,db,do.halt=F,verbose=F) {

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
    con <- RMySQL::dbConnect(drv=RMySQL::MySQL(),user=DB.USER,password=DB.PASSWORD,host=DB.SERVER,dbname=db)
    rs <- RMySQL::dbSendQuery(con, query)
    RMySQL::dbHasCompleted(rs)
    RMySQL::dbClearResult(rs)
  }, warning = function(w) {
    cat("WARNING:",query," : [",db,"]\n",sep="")
    if(do.halt) browser()
  }, error = function(e) {
    cat("ERROR:",query," : [",db,"]\n",sep="")
    print(e)
    if(do.halt) browser()
  }, finally = {
    RMySQL::dbDisconnect(con)
  })
}
