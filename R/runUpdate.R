#--------------------------------------------------------------------------------------
#' @description Runs a database query and returns a result set
#'
#' @param updateQuery a properly formatted SQL query as a string in the form of an UPDATE INNER JOIN
#' @param updated_df a dataframe of updated data to temporarily write to database for INNER JOIN
#' @param db the name of the database
#' @param do.halt if TRUE, halt on errors or warnings
#' @param verbose if TRUE, print diagnostic information
#' @export
#' @title FUNCTION_TITLE
#' @param table PARAM_DESCRIPTION
#' @return OUTPUT_DESCRIPTION
#' @details DETAILS
#' @examples
#' \dontrun{
#' if(interactive()){
#'  #EXAMPLE1
#'  }
#' }
#' @seealso
#'  \code{\link[RMySQL]{character(0)}}, \code{\link[RMySQL]{MySQLDriver-class}}
#'  \code{\link[DBI]{dbSendStatement}}
#' @rdname runUpdate
#' @importFrom RMySQL dbConnect MySQL dbWriteTable dbSendQuery dbDisconnect
#' @importFrom DBI dbSendStatement
#--------------------------------------------------------------------------------------
runUpdate <- function(table, updateQuery=NULL, updated_df=NULL, db, do.halt=TRUE, verbose=FALSE){
  if(is.null(updateQuery)){
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
  if(is.null(db)){
    cat("No 'db' database paramter provided...\n")
    return(NULL)
  }

  if(verbose) {
    printCurrentFunction()
    cat("updateQuery: ",updateQuery,"\n")
    cat("db: ",db,"\n")
  }
  tryCatch({
    # Drop temp table
    runStatement(query="DROP TABLE IF EXISTS z_updated_df", db=db)
    # Push temp table of updates
    # Create a table like the source table so the COLLATE and encoding arguments match
    runQuery(paste0("CREATE TABLE z_updated_df LIKE ", table), db)
    con <- RMySQL::dbConnect(drv=RMySQL::MySQL(),user=DB.USER,password=DB.PASSWORD,host=DB.SERVER,dbname=db)

    res = RMySQL::dbWriteTable(con,
                               name="z_updated_df",
                               value=updated_df,
                               row.names=FALSE,
                               append=TRUE)
    # Send update
    DBI::dbSendStatement(con, updateQuery)
    RMySQL::dbDisconnect(con)
    # Drop temp table
    runStatement(query="DROP TABLE IF EXISTS z_updated_df", db=db)
    #}, warning = function(w) {
    #  cat("WARNING:",updateQuery,"\n")
    #  if(do.halt) browser()
  }, error = function(e) {
    #cat("ERROR:",updateQuery,"\n")
    cat("Error message: ",paste0(e, collapse=" | "), "\n")
    if(do.halt) browser()
  }# , finally = { dbDisconnect(con) }
  )
}
