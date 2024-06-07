#--------------------------------------------------------------------------------------
#' @description Runs a database query and returns a result set

#' @param table table to update
#' @param updateQuery a properly formatted SQL query as a string in the form of an UPDATE INNER JOIN
#' @param updated_df a dataframe of updated data to temporarily write to database for INNER JOIN
#' @param db the name of the database
#' @param do.halt if TRUE, halt on errors or warnings
#' @param verbose if TRUE, print diagnostic information
#' @param trigger_check if FALSE, audit triggers are ignored/bypassed
#' @export
#' @title runUpdate
#' @return None
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
#' @importFrom digest digest
#--------------------------------------------------------------------------------------
runUpdate <- function(table, updateQuery=NULL, updated_df=NULL, db, do.halt=TRUE,verbose=FALSE,
                      trigger_check=TRUE){
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
    # Add session_stem in case multiple users use runUpdate() at the same time
    session_stem = paste0(DB.USER,"_",
                          substr(digest::digest(Sys.time()), 1, 5))
    updateTable = paste0("z_updated_df_", session_stem)

    updateQuery = updateQuery %>%
      gsub("z_updated_df", updateTable, .)
    # Drop temp table

    runStatement(query=paste0("DROP TABLE IF EXISTS ", updateTable), db=db)
    # Push temp table of updates
    # Create a table like the source table so the COLLATE and encoding arguments match
    runQuery(paste0("CREATE TABLE ", updateTable," LIKE ", table), db)
    con <- RMySQL::dbConnect(drv=RMySQL::MySQL(),user=DB.USER,password=DB.PASSWORD,host=DB.SERVER,dbname=db)

    res = RMySQL::dbWriteTable(con,
                       name=updateTable,
                       value=updated_df,
                       row.names=FALSE,
                       append=TRUE)
    # Enable/Disable triggers (custom global variable in triggers)
    RMySQL::dbSendQuery(con, paste0("SET @TRIGGER_CHECKS = ", trigger_check))
    # Send update
    DBI::dbSendStatement(con, updateQuery)
    RMySQL::dbDisconnect(con)
    # Drop temp table
    runStatement(query=paste0("DROP TABLE IF EXISTS ", updateTable), db=db)
    #}, warning = function(w) {
    #  cat("WARNING:",updateQuery,"\n")
    #  if(do.halt) browser()
  }, error = function(e) {
    #cat("ERROR:",updateQuery,"\n")
    cat("Error messge: ",paste0(e, collapse=" | "), "\n")
    if(do.halt) browser()
  }# , finally = { dbDisconnect(con) }
  )
}
