# library(RMySQL)
# library(DBI)
#--------------------------------------------------------------------------------------
#' @description set SQL connection to the database
#' @param server SQL server on which relevant database lives
#' @param user SQL username to access database
#' @param password SQL password corresponding to username
#' @param api_auth API Key for CCTE API's
#' @param port Port for the server connection. Default of 3306.
#' @title FUNCTION_TITLE
#' @return OUTPUT_DESCRIPTION
#' @details DETAILS
#' @examples
#' \dontrun{
#' if(interactive()){
#'  #EXAMPLE1
#'  }
#' }
#' @rdname setDBConn
#' @export
#--------------------------------------------------------------------------------------
setDBConn <- function(server="ccte-mysql-res.epa.gov",user=NULL,password=NULL,api_auth=NULL,port=3306) {
  printCurrentFunction()
  DB.SERVER <<- server
  DB.USER <<- user
  DB.PASSWORD <<- password
  DB.PORT <<- port
  API_AUTH <<- api_auth
}
