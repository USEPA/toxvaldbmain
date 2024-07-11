#' @description Insert a record into a database. if auto.increment=TRUE, return the auto incremented
#' primary key of the record. otherwise, return -1
#' @param query a properly formatted SQL query as a string
#' @param db the name of the database
#' @param do.halt if TRUE, halt on errors or warnings
#' @param verbose if TRUE, print diagnostic information
#' @param auto.increment if TRUE, add the auto increment primary key even if not part of the query
#' @return Returns the database table auto incremented primary key ID
#' @export
#' @title runInsert
#' @param auto.increment.id PARAM_DESCRIPTION, Default: F
#' @details DETAILS
#' @examples
#' \dontrun{
#' if(interactive()){
#'  #EXAMPLE1
#'  }
#' }
#' @seealso
#'  \code{\link[RMySQL]{character(0)}}, \code{\link[RMySQL]{MySQLDriver-class}}
#' @rdname runInsert
#' @importFrom RMySQL dbConnect MySQL dbSendQuery dbHasCompleted dbClearResult dbFetch dbDisconnect
#--------------------------------------------------------------------------------------
runInsert <- function(query,db,do.halt=F,verbose=F,auto.increment.id=F) {

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
  id <- -1
  tryCatch({
    con <- RMySQL::dbConnect(drv=RMySQL::MySQL(),
                             user=Sys.getenv("db_user"),
                             password=Sys.getenv("db_pass"),
                             host=Sys.getenv("db_server"),
                             dbname=db,
                             port=as.numeric(Sys.getenv("db_port"))
    )
    rs <- RMySQL::dbSendQuery(con, query)
    RMySQL::dbHasCompleted(rs)
    RMySQL::dbClearResult(rs)
    if(auto.increment.id) {
      rs2 <- RMySQL::dbSendQuery(con, "select LAST_INSERT_ID()")
      d2 <- RMySQL::dbFetch(rs2, n = -1)
      id <- d2[1,1]
      RMySQL::dbHasCompleted(rs2)
      RMySQL::dbClearResult(rs2)
    }
    RMySQL::dbDisconnect(con)
  }, warning = function(w) {
    cat("WARNING:",query," : [",db,"]\n",sep="")
    if(do.halt) browser()
    RMySQL::dbDisconnect(con)
    if(auto.increment.id) return(-1)
  }, error = function(e) {
    cat("ERROR:",query," : [",db,"]\n",sep="")
    print(e)
    if(do.halt) browser()
    RMySQL::dbDisconnect(con)
    if(auto.increment.id) return(-1)
  })
  if(auto.increment.id) return(id)
}

