# Function to generate general summaries of the ToxValDB for a given release version
#' @param toxval.db The version of toxval to summarize
#' @param compare.toxval.db The version of toxval to compare with the input toxval.db version
#' @return . Exported files are generated in a summary folder.
toxvaldb_release_summary <- function(toxval.db, compare.toxval.db){

  # Create subfolders to store outputs by toxval version
  outDir <- file.path(toxval.config()$datapath, "toxvaldb_release_summaries", toxval.db)
  if(!dir.exists(outDir)) {
    dir.create(outDir, recursive = TRUE)
    dir.create(file.path(outDir, "DDL"))
  }

  # List of tables, fields, and field types
  field_list <- runQuery(paste0("SELECT TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME, COLUMN_DEFAULT, IS_NULLABLE, COLUMN_TYPE, COLLATION_NAME, COLUMN_KEY, EXTRA ",
                                "From INFORMATION_SCHEMA. COLUMNS WHERE table_schema = '",
                         toxval.db, "'"),
                         toxval.db)
  # Record counts
  # Estimate row count from information schema
  n_tbl_row_est <- runQuery(paste0("SELECT table_name, TABLE_ROWS as n_tbl_row_est FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = '",
                               toxval.db, "'"),
                        toxval.db)

  # Loop through tables
  n_tbl_row <- lapply(tbl_list, function(tbl_n){
    message("Pulling DDLs and row counts for '", tbl_n, "'")
    # Write DDLs by table
    tmp <- runQuery(paste0("SHOW CREATE TABLE ", tbl_n),
                    toxval.db)
    writeLines(tmp$`Create Table`, paste0(outDir, "/DDL/", tbl_n, "_DDL.txt"))
    # Get accurate row count
    runQuery(paste0("SELECT count(*) as n_tbl_row FROM ", tbl_n), toxval.db) %>%
      mutate(table_name = tbl_n) %>%
      return()
  }) %>%
    dplyr::bind_rows()

  # Combine output
  out = list(
    tbl_list = field_list %>%
      dplyr::group_by(TABLE_SCHEMA, TABLE_NAME) %>%
      dplyr::summarise(n_tbl_columns = dplyr::n(),
                       .groups = "keep") %>%
      dplyr::ungroup() %>%
      dplyr::left_join(n_tbl_row_est,
                       by=c("TABLE_NAME"="table_name")) %>%
      dplyr::left_join(n_tbl_row,
                       by=c("TABLE_NAME"="table_name")),
    field_list = field_list
  )

  # Export output
  writexl::write_xlsx(out, paste0(outDir, "/", toxval.db, "_release_summary_", Sys.Date(), ".xlsx"))

}
