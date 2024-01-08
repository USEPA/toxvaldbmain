#--------------------------------------------------------------------------------------
#' Make BMDH calculations for each source in ToxVal
#' @param toxval.db The version of toxvaldb to use.
#' @param source Source(s) for which BMDH calculations should be made
#' @export
#--------------------------------------------------------------------------------------
set.toxval.bmdh.by.source <- function(toxval.db, source=NULL){
  printCurrentFunction(toxval.db)

  # Check if table exists
  table_exists = nrow(runQuery("SHOW TABLES LIKE 'toxval_bmdh'", toxval.db))
  if (!table_exists) {
    cat("Creating toxval_bmdh table...\n")
    create_query = paste0("CREATE TABLE toxval_bmdh (",
                          "toxval_id int NOT NULL PRIMARY KEY, ",
                          "source varchar(255), ",
                          "bmdh_value float, ",
                          "bmdh_units varchar(255))")
    runQuery(query=create_query, db=toxval.db)
    cat("Table created\n")
  }

  # Iterate through sources
  slist = runQuery("SELECT DISTINCT source FROM toxval",toxval.db)[,1]
  if(!is.null(source)) slist = source
  for(curr_source in slist) {
    cat(paste0("Sending ", curr_source, " data to toxval_bmdh - ", Sys.time(),"\n"))

    # ============================TO BE UPDATED=================================
    # Retrieve/process necessary fields and perform calculations (logic TBD)
    # For testing purposes, just use toxval_numeric and toxval_units
    # TODO Add logic to calculate BMDH
    query = paste0("SELECT toxval_id, toxval_numeric, toxval_units FROM toxval WHERE source = '",
                   curr_source, "'")
    source_data = runQuery(query, toxval.db)
    bmdh_data = source_data %>%
      dplyr::mutate(source = curr_source) %>%
      dplyr::rename(bmdh_value=toxval_numeric, bmdh_units=toxval_units)
    # ==========================END TO BE UPDATED===============================

    # Optimize row deletion strategy for number of entries that must be deleted
    if (nrow(bmdh_data) <= 200000) {
      # For smaller tables, delete/replace toxval_bmdh entries normally
      runQuery(query = paste0("DELETE FROM toxval_bmdh WHERE toxval_id in (",
                              toString(bmdh_data$toxval_id),
                              ") AND source = '", curr_source, "' ORDER BY toxval_id"),
               db = toxval.db)
    } else {
      # Create temporary table
      runQuery(paste0("CREATE TABLE bmdh_temp_copy LIKE toxval_bmdh"), toxval.db)

      # Only copy over information that will be retained
      runQuery(paste0("INSERT INTO bmdh_temp_copy SELECT * FROM toxval_bmdh WHERE toxval_id ",
                      " not in (", toString(bmdh_data$toxval_id), ") AND source = '",
                      curr_source, "'"), toxval.db)

      # Empty previous table
      runQuery("TRUNCATE TABLE toxval_bmdh", toxval.db)

      # Repopulate table with data that won't be replaced
      runQuery(paste0("INSERT INTO toxval_bmdh SELECT * FROM bmdh_temp_copy"), toxval.db)

      # Delete temp table
      runQuery("DROP TABLE bmdh_temp_copy", toxval.db)
    }

    # Send new data to toxval_bmdh
    runInsertTable(bmdh_data, "toxval_bmdh", toxval.db)

    cat(paste0("--- Finished with source ", curr_source, "\n"))
    cat("==========================================\n")
  }
  cat(paste0("Successfully updated toxval_bmdh - ", Sys.time(),"\n"))
  cat("==========================================\n")
}
