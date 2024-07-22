# library(RMySQL)
# library(DBI)
#--------------------------------------------------------------------------------------
#' @description Get the names the database server, user, and pass or returns error message
#' @return print the database connection information
#' @title getDBConn
#' @details DETAILS
#' @examples
#' \dontrun{
#' if(interactive()){
#'  #EXAMPLE1
#'  }
#' }
#' @rdname getDBConn
#' @export
#--------------------------------------------------------------------------------------
getDBConn <- function() {
  printCurrentFunction()
  # Check environment variables for database credentials are set
  credentials = c("db_user", "db_pass", "db_server", "db_port")
  for(cred in credentials){
    if(Sys.getenv(cred) == ""){
      cat(paste0("'", cred, "' environment variable not defined\n"))
    } else {
      cat(cred, ": ", Sys.getenv(cred), "\n")
    }
  }
}
