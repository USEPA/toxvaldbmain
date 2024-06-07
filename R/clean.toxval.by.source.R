#--------------------------------------------------------------------------------------
#
#' Delete a portion of the contents of the toxval database
#' @param toxval.db The version of toxval from which the data is deleted.
#' @param source The data source name
#' @return The database will be altered
#' @export
#-------------------------------------------------------------------------------------
clean.toxval.by.source <- function(toxval.db,source) {
  printCurrentFunction(paste(toxval.db,":",source))

  count = runQuery(paste0("select count(*) from toxval where source='",source,"'"),toxval.db)[1,1]
  if(count > 0){
    # Small sources
    if(count <= 100000){
      cat("...Deleting from toxval_notes...\n")
      runQuery(paste0("delete from toxval_notes where toxval_id in (select toxval_id from toxval where source='",source,"')"),toxval.db)
      cat("...Deleting from toxval_qc_notes...\n")
      runQuery(paste0("delete from toxval_qc_notes where toxval_id in (select toxval_id from toxval where source='",source,"')"),toxval.db)
      cat("...Deleting from record_source...\n")
      runQuery(paste0("delete from record_source where toxval_id in (select toxval_id from toxval where source='",source,"')"),toxval.db)
      cat("...Deleting from toxval_uf\n")
      runQuery(paste0("delete from toxval_uf where toxval_id in (select toxval_id from toxval where source='",source,"')"),toxval.db)
      runQuery(paste0("delete from toxval_uf where parent_id in (select toxval_id from toxval where source='",source,"')"),toxval.db)
      cat("...Deleting from toxval_relationship...\n")
      runQuery(paste0("delete from toxval_relationship where toxval_id_1 in (select toxval_id from toxval where source='",source,"')"),toxval.db)
      runQuery(paste0("delete from toxval_relationship where toxval_id_2 in (select toxval_id from toxval where source='",source,"')"),toxval.db)
      cat("...Deleting from toxval...\n")
      runQuery(paste0("delete from toxval where source='",source,"'"),toxval.db)
    } else {
      del_list <- list(toxval_notes = "toxval_id",
                       toxval_qc_notes = "toxval_id",
                       record_source = "toxval_id",
                       toxval_uf = c("toxval_id", "parent_id"),
                       toxval_relationship = c("toxval_id_1", "toxval_id_2"))
      # Larger sources require a different approach, since DELETE is slower with 100k+ records
      # Get list of toxval_id values to query with
      toxval_id_ls <- runQuery(paste0("select toxval_id from toxval where source='",source,"'"), toxval.db)$toxval_id

      # Delete tables by copying table without ID values to delete
      for(tbl_n in names(del_list)){
        message("Creating temp_copy table of ",tbl_n," at: ", Sys.time())
        runQuery(paste0("CREATE TABLE temp_copy LIKE ", tbl_n), toxval.db)

        message("Populating temp_copy table with ",tbl_n," table (except selected source: ",source,") at: ",Sys.time())
        runQuery(paste0("INSERT INTO temp_copy SELECT * FROM ",tbl_n," WHERE ",
                        paste0(del_list[[tbl_n]], " not in (",
                               toString(toxval_id_ls),
                               ")", collapse = " and ")), toxval.db)

        message("Truncating ", tbl_n, " table at: ", Sys.time())
        # Special case of handling foreign key checks
        if(tbl_n == "toxval"){
          con <- RPostgreSQL::dbConnect(drv=RMySQL::MySQL(),user=DB.USER,password=DB.PASSWORD,host=DB.SERVER,dbname=db)
          dbSendStatement(con, "SET FOREIGN_KEY_CHECKS = 0;")
          dbSendStatement(con, paste0("TRUNCATE TABLE ", tbl_n))
          dbDisconnectcon(con)
        } else {
          runQuery(paste0("TRUNCATE TABLE ", tbl_n), toxval.db)
        }

        message("Re-populating ",tbl_n," table with temp_copy data at: ",Sys.time())
        runQuery(paste0("INSERT INTO ", tbl_n, " SELECT * FROM temp_copy"), toxval.db)

        message("Dropping temp_copy table at: ", Sys.time())
        runQuery("DROP TABLE temp_copy", toxval.db)
      }
      message("Deleting from toxval table at: ", Sys.time())
      runQuery(paste0("delete from toxval where toxval_id in (", toString(toxval_id_ls),")"), toxval.db)
    }
  }
  message("Deleting from source_chemical table at: ", Sys.time())
  runQuery(paste0("delete from source_chemical where source='",source,"'"),toxval.db)
  message("Done clearing source at: ", Sys.time())
}
