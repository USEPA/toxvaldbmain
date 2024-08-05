#-------------------------------------------------------------------------------------
#' Function to generate general summaries of the ToxValDB for a given release version
#' @param in.toxval.db The version of toxval to summarize
#' @param in.toxval.host The host of the toxval database
#' @param compare.toxval.db The version of toxval to compare with the input toxval.db version
#' @param compare.toxval.host The host of teh comparison toxval database
#' @return None. Exported files are generated in a summary folder.
#-------------------------------------------------------------------------------------
toxvaldb_release_summary <- function(in.toxval.db, in.toxval.host, compare.toxval.db, compare.toxval.host){

  # Helper function to check if any field names match sql keywords
  check_keywords <- function(database, words_file, outDir){
    keywords <- stringi::stri_trans_tolower(readLines(words_file, warn=FALSE))
    field_list <- runQuery(paste0("SELECT TABLE_NAME, COLUMN_NAME, COLUMN_TYPE FROM INFORMATION_SCHEMA. COLUMNS WHERE TABLE_SCHEMA = '",
                                  database, "'"),
                           database)

    occurrences <- field_list %>%
      # Check for exact matches
      dplyr::filter(tolower(COLUMN_NAME) %in% keywords) %>%
      dplyr::mutate(keyword = tolower(COLUMN_NAME))

      # Logic for checking for contained keywords (not exact matches)
      #dplyr::mutate(keyword = stringr::str_extract_all(stringi::stri_trans_tolower(COLUMN_NAME), paste(keywords, collapse="|"))) %>%
      #tidyr::unnest(keyword) %>%
      #dplyr::filter(!is.na(keyword))

    words_file <- gsub("Repo/dictionary/|\\.txt", "", words_file)
    out_file <- paste0(database,"_",words_file,"_keyword_occurrences.xlsx")

    writexl::write_xlsx(occurrences, file.path(outDir, out_file))
  }

  db_server_list = list(in.toxval.db, compare.toxval.db)
  names(db_server_list) = c(in.toxval.host, compare.toxval.host)

  # Loop through input databases
  for(toxval.db in names(db_server_list)){

    # Set server host
    Sys.setenv(db_server = toxval.db)
    toxval.db = db_server_list[[toxval.db]]

    message("Summarizing '", toxval.db, "' from ", Sys.getenv("db_server"))

    # Check if input databases exist on server
    tbl_check <- runQuery("SHOW databases", in.toxval.db) %>%
      dplyr::filter(Database %in% toxval.db)

    if(!toxval.db %in% tbl_check$Database){
      stop("Input database does not exist on server...")
    }

    # Create subfolders to store outputs by toxval version
    outDir <- file.path(toxval.config()$datapath, "toxvaldb_release_summaries", Sys.getenv("db_server"), toxval.db)
    if(!dir.exists(outDir)) {
      dir.create(outDir, recursive = TRUE)
      dir.create(file.path(outDir, "DDL"))
    } else {
      # Clear out old files
      unlink(outDir, recursive = TRUE)
      dir.create(outDir, recursive = TRUE)
      dir.create(file.path(outDir, "DDL"))
    }

    reserved_words_file = paste0(toxval.config()$datapath, "dictionary/mysql_reserved_terms.txt")
    nonreserved_words_file = paste0(toxval.config()$datapath, "dictionary/mysql_non_reserved_terms.txt")

    # database, words_file, outDir
    check_keywords(database=toxval.db, words_file=reserved_words_file, outDir=outDir)
    check_keywords(database=toxval.db, words_file=nonreserved_words_file, outDir=outDir)

    # List of tables, fields, and field types
    field_list <- runQuery(paste0("SELECT TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME, COLUMN_DEFAULT, IS_NULLABLE, COLUMN_TYPE, COLLATION_NAME, COLUMN_KEY, EXTRA ",
                                  "FROM INFORMATION_SCHEMA. COLUMNS WHERE TABLE_SCHEMA = '",
                                  toxval.db, "'"),
                           toxval.db)
    # Record counts
    # Estimate row count from information schema
    n_tbl_row_est <- runQuery(paste0("SELECT TABLE_NAME as table_name, TABLE_ROWS as n_tbl_row_est FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = '",
                                     toxval.db, "'"),
                              toxval.db)

    # Loop through tables
    n_tbl_row <- lapply(unique(field_list$TABLE_NAME), function(tbl_n){
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

  old <- read_excel_allsheets(paste0(file.path(toxval.config()$datapath, "toxvaldb_release_summaries", compare.toxval.host, compare.toxval.db),
                                     "/", compare.toxval.db, "_release_summary_", Sys.Date(), ".xlsx"))
  new <- read_excel_allsheets(paste0(file.path(toxval.config()$datapath, "toxvaldb_release_summaries", in.toxval.host, in.toxval.db),
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
                                    in.toxval.host, "_", in.toxval.db, "_", compare.toxval.host, "_", compare.toxval.db, "_release_comparison_", Sys.Date(), ".xlsx"))
  message("Done comparing...")
}
