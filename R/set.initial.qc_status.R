#--------------------------------------------------------------------------------------
#' Set toxval qc_status to source table qc_status
#' @param toxval.db The database version to use
#' @param source.db The source database name
#' @param source The source name
#' @param subsource The subsource to update, if desired (Default: NULL)
#--------------------------------------------------------------------------------------
set.initial.qc_status <- function(toxval.db, source.db, source, subsource=NULL){
  printCurrentFunction(toxval.db)

  slist = runQuery("select distinct source from toxval",toxval.db)[,1]
  if(!is.null(source)) slist = source

  # Handle addition of subsource for queries
  query_addition = ""
  if(!is.null(subsource)) {
    query_addition = paste0(" and subsource='", subsource, "'")
  }

  for(source in slist){
    # Get unique source tables for current source/subsource pair
    source_table_query = paste0("SELECT DISTINCT source_table FROM toxval WHERE source='",
                                source, "'", query_addition)
    source_tables = runQuery(source_table_query, toxval.db) %>%
      dplyr::pull(source_table)

    # Iterate through list of source tables (or single source table)
    for(source_table in source_tables) {
      # Check if direct load and handle appropriately
      if(source_table %in% c("direct load", "direct_load")) {
        direct_load_query = paste0("UPDATE toxval SET qc_status='not determined' WHERE source='",
                                   source, "'", query_addition)
        runQuery(direct_load_query, toxval.db)
        next
      }

      # Handle entries with corresponding source table
      update_query = paste0(
        "UPDATE ", toxval.db, ".toxval a ",
        "INNER JOIN ", source.db, ".", source_table, " b ",
        "ON a.source_hash=b.source_hash ",
        "SET a.qc_status=b.qc_status"
      )
      runQuery(update_query, toxval.db)
    }
    cat("qc_status set to intitial values for", paste(source, subsource), "\n")
  }
}
