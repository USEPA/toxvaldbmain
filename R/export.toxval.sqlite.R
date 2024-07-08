#--------------------------------------------------------------------------------------
#' Exports copies of ToxVal source/main databases to SQLite files
#' @param toxval.db The version of toxval into which the tables are loaded
#' @param source.db The source database to use
#' @param export_repo The filepath to the folder to save SQLite files in
#' @param toxval_out The name of the SQLite file to export toxval data to
#' @param source_out The name of the SQLite file to export toxval_source data to
#--------------------------------------------------------------------------------------
export.toxval.sqlite <- function(toxval.db, source.db,
                                 export_repo=paste0("Repo/export/sqlite_export_", Sys.Date(), "/"),
                                 toxval_out="toxvaldbmain_v95.sqlite", source_out="toxvaldbstage_v95.sqlite") {
  # Ensure output directory exists and is empty
  if(dir.exists(export_repo)) {
    cat(paste0("Directory '", export_repo, "' already exists. Continuing will overwrite previous data.\n"))
    browser()
    unlink(export_repo, recursive = TRUE, force = TRUE)
    cat(paste0("Overwriting directory: ", export_repo, "\n"))
  } else {
    cat(paste0("Initializing directory: ", export_repo, "\n"))
  }
  dir.create(export_repo)

  # Append repo information to output filenames
  tox_sqlite = paste0(export_repo, toxval_out)
  source_sqlite = paste0(export_repo, source_out)

  # Get list of source tables to export from main toxval table
  stage_table_list = runQuery("SELECT DISTINCT source_table FROM toxval", toxval.db) %>%
    dplyr::filter(!source_table %in% c("direct load", "direct_load")) %>%
    dplyr::pull(source_table) %>%
    # Add additional source database tables to add
    c("source_chemical", "documents", "documents_records", "documents_lineage", "chemical_source_index")

  # Get list of toxval tables to export
  toxval_table_list = c("cancer_summary", "chemical", "genetox_details", "genetox_summary",
                        "record_source", "record_source_full", "skin_eye", "source_chemical",
                        "source_info", "species", "toxval", "toxval_dictionary",
                        "toxval_relationship", "toxval_type_dictionary")

  # Write stage output to SQLite
  cat("Writing source tables to SQLite\n")
  export_table_list_to_sqlite(table_list=stage_table_list, input_db=source.db, output_file=source_sqlite)

  # Write toxval output to SQLite
  cat("Writing toxval tables to SQLite\n")
  export_table_list_to_sqlite(table_list=toxval_table_list, input_db=toxval.db, output_file=tox_sqlite)

  cat("SQLite files written to", export_repo, "\n")
}

#--------------------------------------------------------------------------------------
#' @title export_table_list_to_sqlite
#' @description Helper function to export data from list of tables to SQLite
#' @return None; data is written to SQLite file
#--------------------------------------------------------------------------------------
export_table_list_to_sqlite <- function(table_list, input_db, output_file) {
  # Get output SQLite DB to write data to
  output_db = RSQLite::dbConnect(RSQLite::SQLite(), output_file)

  for(table in table_list) {
    cat("...pushing table ", table, " (", which(table_list == table), " of ",
        length(table_list), " - ", format(Sys.time(), usetz = TRUE), ")", "\n")
    # Pull data to write from MySQL DB and write to SQLite
    data = runQuery(paste0("SELECT * FROM ", table), input_db)
    DBI::dbWriteTable(output_db, table, data)
  }

  RSQLite::dbDisconnect(output_db)
}
