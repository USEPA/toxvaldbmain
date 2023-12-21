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
    create_query = "CREATE TABLE toxval_bmdh ("
    var1 = "source_hash varchar(45), "
    var2 = "source varchar(255), "
    var3 = "bmdh_value float, "
    var4 = "bmdh_units varchar(255))"
    query = paste0(create_query, var1, var2, var3, var4)
    runQuery(query, toxval.db)
    cat("Table created\n")
  }

  # Iterate through sources
  slist = runQuery("SELECT DISTINCT source FROM toxval",toxval.db)[,1]
  if(!is.null(source)) slist = source
  for(curr_source in slist) {
    cat(paste0("Sending ", curr_source, " data to toxval_bmdh\n"))

    # ============================TO BE UPDATED=================================
    # Retrieve/process necessary fields and perform calculations (logic TBD)
    # For testing purposes, just use toxval_numeric and toxval_units
    query = paste0("SELECT source_hash, toxval_numeric, toxval_units FROM toxval WHERE source = '",
                   curr_source, "'")
    source_data = runQuery(query, toxval.db)
    bmdh_data = source_data %>%
      dplyr::mutate(source = curr_source, source_hash = as.character(source_hash)) %>%
      dplyr::rename(bmdh_value="toxval_numeric", bmdh_units="toxval_units") %>%
      tidyr::drop_na("source_hash")
    # ==========================END TO BE UPDATED===============================

    # Delete toxval_bmdh entries that will be replaced
    # Use both source and source_hash to account for source_hash values of "-"
    runQuery(paste0("DELETE FROM toxval_bmdh WHERE source_hash in ('",
                    paste(as.list(bmdh_data$source_hash),collapse="', '"),
                    "') AND source='", curr_source, "'"), toxval.db)

    # Send data to toxval_bmdh
    runInsertTable(bmdh_data, "toxval_bmdh", toxval.db)

    cat(paste0("Finished with source ", curr_source, "\n"))
    cat("==========================================\n")
  }
  cat("Successfully updated toxval_bmdh\n")
  cat("==========================================\n")
}
