#-------------------------------------------------------------------------------------
#' Function to generate general summaries of the ToxValDB for a given release version
#' @param in.toxval.db The version of toxval to summarize
#' @param compare.toxval.db The version of toxval to compare with the input toxval.db version
#' @return None. Exported files are generated in a summary folder.
#-------------------------------------------------------------------------------------
toxvaldb_release_summary <- function(in.toxval.db, compare.toxval.db=NULL){

  # Prep vector of toxval versions to summarize
  to_summarize <- sort(c(in.toxval.db, compare.toxval.db))

  # Check if input databases exist on server
  tbl_check <- runQuery("SHOW databases", in.toxval.db) %>%
    dplyr::filter(Database %in% to_summarize)

  if(!all(to_summarize %in% tbl_check$Database)){
    stop("Input database does not exist on server...")
  }

  # Helper function to check if any field names match sql keywords
  check_keywords <- function(database, words_file, outDir){
    keywords <- stringi::stri_trans_tolower(readLines(words_file))
    field_list <- runQuery(paste0("SELECT TABLE_NAME, COLUMN_NAME, COLUMN_TYPE FROM INFORMATION_SCHEMA. COLUMNS WHERE TABLE_SCHEMA = '",
                                  database, "'"),
                           database)

    occurrences <- field_list %>%
      # Check for exact matches
      filter(tolower(COLUMN_NAME) %in% keywords) %>%
      mutate(keyword = tolower(COLUMN_NAME))

      # Logic for checking for contained keywords (not exact matches)
      #mutate(keyword = str_extract_all(stri_trans_tolower(COLUMN_NAME), paste(keywords, collapse="|"))) %>%
      #unnest(keyword) %>%
      #filter(!is.na(keyword))

    words_file <- sub("^Repo/|\\.txt$", "", words_file)
    out_file <- paste(words_file, "_occurrences.xlsx")

    write_xlsx(occurrences, file.path(outDir, out_file))
  }

  # Loop through input databases
  for(toxval.db in to_summarize){
    message("Summarizing '", toxval.db, "'")
    # Create subfolders to store outputs by toxval version
    outDir <- file.path(toxval.config()$datapath, "toxvaldb_release_summaries", toxval.db)
    if(!dir.exists(outDir)) {
      dir.create(outDir, recursive = TRUE)
      dir.create(file.path(outDir, "DDL"))
    }

    reserved_words_file = paste0(toxval.config()$datapath, "reserved.txt")
    nonreserved_words_file = paste0(toxval.config()$datapath,"keywords.txt")

    check_keywords(toxval.db, reserved_words_file, outDir)
    check_keywords(toxval.db, nonreserved_words_file, outDir)

    # List of tables, fields, and field types
    field_list <- runQuery(paste0("SELECT TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME, COLUMN_DEFAULT, IS_NULLABLE, COLUMN_TYPE, COLLATION_NAME, COLUMN_KEY, EXTRA ",
                                  "FROM INFORMATION_SCHEMA. COLUMNS WHERE TABLE_SCHEMA = '",
                                  toxval.db, "'"),
                           toxval.db)
    # Record counts
    # Estimate row count from information schema
    n_tbl_row_est <- runQuery(paste0("SELECT table_name, TABLE_ROWS as n_tbl_row_est FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = '",
                                     toxval.db, "'"),
                              toxval.db)

    # Loop through tables
    n_tbl_row <- lapply(tbl_list, function(tbl_n){
      message("...Pulling DDLs and row counts for '", tbl_n, "'")
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

  # Perform comparison of table fields
  if(is.null(compare.toxval.db)){
    message("No comparison database input...returning...Done...")
  }

  #'@description Helper function to read all Excel file sheets
  read_excel_allsheets <- function(filename) {
    sheets <- readxl::excel_sheets(filename)
    x <- lapply(sheets, function(X) readxl::read_excel(filename, sheet = X))
    names(x) <- sheets
    return(x)
  }

  old <- read_excel_allsheets(paste0(file.path(toxval.config()$datapath, "toxvaldb_release_summaries", compare.toxval.db),
                                     "/", compare.toxval.db, "_release_summary_", Sys.Date(), ".xlsx"))
  new <- read_excel_allsheets(paste0(file.path(toxval.config()$datapath, "toxvaldb_release_summaries", in.toxval.db),
                                     "/", in.toxval.db, "_release_summary_", Sys.Date(), ".xlsx"))

  comparison <- list(
    # Tables in new, not in old
    new_tables = new$tbl_list %>%
      dplyr::filter(!TABLE_NAME %in% old$tbl_list$TABLE_NAME),
    # Tables in old, not in new
    deleted_tables = old$tbl_list %>%
      dplyr::filter(!TABLE_NAME %in% new$tbl_list$TABLE_NAME),
    # Fields in new, not in old
    new_fields = new$field_list %>%
      dplyr::filter(!COLUMN_NAME %in% old$field_list$COLUMN_NAME),
    # Fields in old, not in new
    deleted_fields = old$field_list %>%
      dplyr::filter(!COLUMN_NAME %in% new$field_list$COLUMN_NAME),
    # Same field name, different type
    updated_fields = new$field_list %>%
        dplyr::select(TABLE_NAME, COLUMN_NAME, COLUMN_TYPE) %>%
        dplyr::distinct() %>%
        dplyr::left_join(old$field_list %>%
                           dplyr::select(TABLE_NAME, COLUMN_NAME, OLD_COLUMN_TYPE = COLUMN_TYPE) %>%
                           dplyr::distinct(),
                         by=c("TABLE_NAME", "COLUMN_NAME")) %>%
        dplyr::filter(!is.na(OLD_COLUMN_TYPE),
               COLUMN_TYPE != OLD_COLUMN_TYPE) %>%
        dplyr::distinct(),
    # Record count differences for table overlap
    row_count_diff = new$tbl_list %>%
      select(-TABLE_SCHEMA) %>%
      dplyr::left_join(old$tbl_list %>%
                         select(-TABLE_SCHEMA) %>%
                         dplyr::rename(old_n_tbl_columns=n_tbl_columns,
                                       old_n_tbl_row_est=n_tbl_row_est,
                                       old_n_tbl_row=n_tbl_row),
                       by="TABLE_NAME") %>%
      dplyr::mutate(n_tbl_column_diff = round(n_tbl_columns-old_n_tbl_columns, 3),
                    n_tbl_row_est_diff = round(n_tbl_row_est-old_n_tbl_row_est, 3),
                    n_tbl_row_diff = round(n_tbl_row-old_n_tbl_row, 3))
  )

  writexl::write_xlsx(comparison,
                      paste0(toxval.config()$datapath, "/toxvaldb_release_summaries/",
                                    in.toxval.db, "_", compare.toxval.db, "_release_comparison_", Sys.Date(), ".xlsx"))
  message("Done comparing...")
}
