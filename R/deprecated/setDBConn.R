#' @description set SQL connection to the database
#' @param server SQL server on which relevant database lives
#' @param user SQL username to access database
#' @param password SQL password corresponding to username
#' @param api_auth API Key for CCTE API's
#' @param port Port for the server connection. Default of 3306.
#' @title setDBConn
#' @return None
#' @details DETAILS
#' @examples
#' \dontrun{
#' if(interactive()){
#'  #EXAMPLE1
#'  }
#' }
#' @rdname setDBConn
#--------------------------------------------------------------------------------------
setDBConn <- function(server, user, password, api_auth, port) {
  printCurrentFunction()
  DB.SERVER <<- server
  DB.USER <<- user
  DB.PASSWORD <<- password
  DB.PORT <<- port
  API_AUTH <<- api_auth
}











