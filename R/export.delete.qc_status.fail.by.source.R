#' @title export.delete.qc_status.fail.by.source
#' @description Function to export log of qc_status "fail" records and delete fail records from ToxValDB.
#' @param toxval.db The database version to use
#' @param source The source name
#' @param subsource The specific subsource to process, if desired (Default: NULL)
#' @return None. XLSX log genrated and SQL UPDATE statements are pushed to the database.
export.delete.qc_status.fail.by.source <- function(toxval.db, source, subsource){
  printCurrentFunction(paste(toxval.db,":", source, subsource))

  # Handle addition of subsource for queries
  query_addition = ""
  if(!is.null(subsource)) {
    query_addition = paste0(" and a.subsource = '", subsource, "'")
  }

  # Get list of sources
  slist = runQuery("select distinct source from toxval", toxval.db) %>%
    dplyr::pull(source)
  if(!is.null(source)) slist = source
  source_string = slist %>%
    paste0(collapse="', '")

  # Pull data
  mat = runQuery(
    paste0(
      "select * from toxval ",
      "where qc_status like '%fail%' ",
      "and source in ('", source_string, "')", query_addition
    ),
    toxval.db)

  # If no cases found, return
  if(!nrow(mat)) return()

  # Log qc_status fail records by source
  outDir = paste0(toxval.config()$datapath, "export_qc_fail/", toxval.db)
  if(!dir.exists(outDir)){
    dir.create(outDir, recursive = TRUE)
  }

  del_toxval_id = paste0(mat$toxval_id, collapse = "', '")

  # Split by source
  mat = mat %>%
    dplyr::group_split(source)

  for(df in mat){
    source = unique(df$source)
    subsource = NULL
    if(source == "ECHA IUCLID"){
      subsource = unique(df$subsource)
    }
    out_file = paste0(outDir, "/qc_status_fail_",
                      source, "_", subsource, "_", toxval.db, ".xlsx") %>%
      gsub("__", "_", .)

    # Decided to just overwrite, since it's supposed to be a refresh by source
    # # Check if file exists
    # # Edge case for sources like EFSA where postprocessing is rerun twice
    # # without running the toxval.load that would clear it out of the database first
    # if(file.exists(out_file)){
    #   # Read in old log and append new records
    #   df_old = readxl::read_xlsx(out_file)
    #   df = df_old %>%
    #     dplyr::bind_rows(df) %>%
    #     dplyr::distinct()
    #
    #   # Check for duplicate source_hash entries
    #   if(anyDuplicated(df$source_hash)){
    #     stop("Duplicate source_hash values being logged for qc_status fail logs...")
    #   }
    # }

    writexl::write_xlsx(df, out_file)
  }

  # Delete qc_status fail records by toxval_id
  cat("...Deleting from toxval_notes...\n")
  runQuery(paste0("delete from toxval_notes where toxval_id in ('", del_toxval_id, "')"), toxval.db)
  cat("...Deleting from toxval_qc_notes...\n")
  runQuery(paste0("delete from toxval_qc_notes where toxval_id in ('", del_toxval_id, "')"), toxval.db)
  cat("...Deleting from record_source...\n")
  runQuery(paste0("delete from record_source where toxval_id in ('", del_toxval_id, "')"), toxval.db)
  cat("...Deleting from toxval_uf\n")
  runQuery(paste0("delete from toxval_uf where toxval_id in ('", del_toxval_id, "')"), toxval.db)
  runQuery(paste0("delete from toxval_uf where parent_id in ('", del_toxval_id, "')"), toxval.db)
  cat("...Deleting from toxval_relationship...\n")
  runQuery(paste0("delete from toxval_relationship where toxval_id_1 in ('", del_toxval_id, "')"), toxval.db)
  runQuery(paste0("delete from toxval_relationship where toxval_id_2 in ('", del_toxval_id, "')"), toxval.db)
  cat("...Deleting from toxval...\n")
  runQuery(paste0("delete from toxval where toxval_id in ('", del_toxval_id, "')"),toxval.db)
}
