#' @description Runs a database query and returns a result set
#'
#' @param query a properly formatted SQL query as a string
#' @param db the name of the database
#' @param do.halt if TRUE, halt on errors or warnings
#' @param verbose if TRUE, print diagnostic information
#' @export
#' @title runQuery
#' @return Query results
#' @details DETAILS
#' @examples
#' \dontrun{
#' if(interactive()){
#'  #EXAMPLE1
#'  }
#' }
#' @seealso
#'  \code{\link[RMySQL]{character(0)}}, \code{\link[RMySQL]{MySQLDriver-class}}
#'  \code{\link[utils]{flush.console}}
#' @rdname runQuery
#' @importFrom RMySQL dbConnect MySQL dbSendQuery dbFetch dbHasCompleted dbClearResult dbDisconnect
#' @importFrom utils flush.console
#--------------------------------------------------------------------------------------
runQuery <- function(query=NULL, db, do.halt=TRUE, verbose=FALSE) {

  if(is.null(query)){
    cat("No query provided...\n")
    return(NULL)
  }

  # Check environment variables for database credentials are set
  credentials = c("db_user", "db_pass", "db_server", "db_port")
  for(cred in credentials){
    if(Sys.getenv(cred) == ""){
      cat(paste0("'", cred, "' environment variable not defined\n"))
      return(NULL)
    }
  }

  if(verbose) {
    printCurrentFunction()
    cat("query: ",query,"\n")
    cat("db: ",db,"\n")
  }

  tryCatch({
    con <- RMySQL::dbConnect(drv=RMySQL::MySQL(),
                             user=Sys.getenv("db_user"),
                             password=Sys.getenv("db_pass"),
                             host=Sys.getenv("db_server"),
                             dbname=db,
                             port=as.numeric(Sys.getenv("db_port"))
                             )
    rs <- suppressWarnings(RMySQL::dbSendQuery(con, query))
    d1 <- RMySQL::dbFetch(rs, n = -1)
    if(verbose) {
      print(d1)
      utils::flush.console()
    }
    RMySQL::dbHasCompleted(rs)
    RMySQL::dbClearResult(rs)
    RMySQL::dbDisconnect(con)
    return(d1)
  }, warning = function(w) {
    cat("WARNING:",query,"\n")
    RMySQL::dbDisconnect(con)
    if(do.halt) browser()
    return(NULL)
  }, error = function(e) {
    #cat("ERROR:",query,"\n")
    cat("Error messge: ",paste0(e, collapse=" | "), "\n")
    RMySQL::dbDisconnect(con)
    if(do.halt) browser()
    return(NULL)
  })
}


