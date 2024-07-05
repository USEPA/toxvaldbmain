#' @description Runs a database query and returns a result set
#'
#' @param query a properly formatted SQL query as a string
#' @param db the name of the database (or SQLite file) to query
#' @param do.halt if TRUE, halt on errors or warnings
#' @param verbose if TRUE, print diagnostic information
#' @param con_type The type of connection to use. Options: MySQL (default), SQLite, PostgreSQL
#' @param schema If con_type=PostgreSQL, schema must be specified (Default: NULL)
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
#' @importFrom RSQLite dbConnect SQLite dbSendQuery dbFetch dbHasCompleted dbClearResult dbDisconnect
#' @importFrom RPostgreSQL dbConnect dbDriver dbSendQuery fetch dbHasCompleted dbClearResult dbDisconnect
#--------------------------------------------------------------------------------------
runQuery <- function(query=NULL,db,do.halt=TRUE,verbose=FALSE,con_type="MySQL",schema=NULL) {
  if(is.null(query)){
    cat("No query provided...\n")
    return(NULL)
  }

  con_type = tolower(con_type)
  if(con_type != "sqlite") {
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
  }

  if(verbose) {
    printCurrentFunction()
    cat("query: ",query,"\n")
    cat("db: ",db,"\n")
    cat("connection type:",con_type,"\n")
  }

  # Initialize appropriate connection type
  switch(
    con_type,
    "mysql" = {
      con <- RMySQL::dbConnect(drv=RMySQL::MySQL(),
                               user=DB.USER,
                               password=DB.PASSWORD,
                               host=DB.SERVER,
                               dbname=db,
                               port=as.numeric(ifelse(!is.null(DB.PORT), DB.PORT, 3306))
      )
      query_func = RMySQL::dbSendQuery
      fetch_func = RMySQL::dbFetch
      complete_func = RMySQL::dbHasCompleted
      clear_func = RMySQL::dbClearResult
      disconnect_func = RMySQL::dbDisconnect
    },
    "sqlite" = {
      con = RSQLite::dbConnect(RSQLite::SQLite(), db)
      query_func = RSQLite::dbSendQuery
      fetch_func = RSQLite::dbFetch
      complete_func = RSQLite::dbHasCompleted
      clear_func = RSQLite::dbClearResult
      disconnect_func = RSQLite::dbDisconnect
    },
    "postgresql" = {
      if(is.null(schema)) opt = ""
      else opt = paste0("-c search_path=", schema)
      con <- RPostgreSQL::dbConnect(drv=RPostgreSQL::dbDriver("PostgreSQL"),
                                    user=DB.USER,
                                    password=DB.PASSWORD,
                                    host=DB.SERVER,
                                    dbname=db,
                                    port=as.numeric(ifelse(!is.null(DB.PORT), DB.PORT, 3306)),
                                    options=opt)
      query_func = RPostgreSQL::dbSendQuery
      fetch_func = RPostgreSQL::fetch
      complete_func = RPostgreSQL::dbHasCompleted
      clear_func = RPostgreSQL::dbClearResult
      disconnect_func = RPostgreSQL::dbDisconnect
    },

    {
      cat("Invalid connection type. Must use MySQL, SQLite, or PostgreSQL\n")
      if(do.halt) browser()
      return(NULL)
    }
  )

  tryCatch({
    rs <- suppressWarnings(query_func(con, query))
    d1 <- fetch_func(rs, n = -1)
    if(verbose) {
      print(d1)
      utils::flush.console()
    }
    complete_func(rs)
    clear_func(rs)
    disconnect_func(con)
    return(d1)
  }, warning = function(w) {
    cat("WARNING:",query,"\n")
    disconnect_func(con)
    if(do.halt) browser()
    return(NULL)
  }, error = function(e) {
    #cat("ERROR:",query,"\n")
    cat("Error message: ",paste0(e, collapse=" | "), "\n")
    disconnect_func(con)
    if(do.halt) browser()
    return(NULL)
  })
}


