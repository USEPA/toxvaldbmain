#' @title get.toxval_source.tbl.list
#' @description Get a list of database tables in the source database to release. Ignore all retired or archived source tables.
#' @param toxval.db The database version to use.
#' @param source.db The source database name.
#' @return Vector of table names.
get.toxval_source.tbl.list <- function(toxval.db, source.db){

  # Get live source table list from toxval
  toxval_src_tblList = runQuery("SELECT distinct source_table FROM toxval",
                                db = toxval.db) %>%
    dplyr::pull(source_table)

  # Get full list of tables in source database
  toxval_src_tblList_full = runQuery(query = paste0("SHOW TABLES FROM ", source.db),
                                     db = source.db) %>% unlist() %>% unname()

  # Get all non-source utility tables
  toxval_src_util_tblList = toxval_src_tblList_full %>%
    .[(!grepl("^source", .) | . %in% c("source_chemical", "source_audit"))] %>%
    .[!. %in% c("zzz_grants_test")]

  # Filter out source tables and utils tables
  toxval_src_tblList_full = toxval_src_tblList_full[!toxval_src_tblList_full %in% toxval_src_util_tblList]
  toxval_src_tblList_full = toxval_src_tblList_full[!toxval_src_tblList_full %in% toxval_src_tblList]

  # Review excluded
  message("Excluded source tables:")
  cat(paste0("- ", toxval_src_tblList_full), sep = "\n")

  # Return filtered list
  return(c(toxval_src_tblList, toxval_src_util_tblList))

}
