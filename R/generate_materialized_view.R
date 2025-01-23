#' @title Generate Materialized View
#' @description Function to create a new materialized view database table by querying
#' an input database version based on an input field_dictionary.xlsx file.
#' @param version_db Version of the database to pull data for the materialized view.
#' @param toxval.db Version of the database to store the new view.
#' @return None. SQL statements are executed to generate a database table.
#' @details DETAILS
#' @examples
#' \dontrun{
#' if(interactive()){
#'  #EXAMPLE1
#'  }
#' }
#' @seealso
#'  \code{\link[readxl]{read_excel}}
#'  \code{\link[tidyr]{unite}}
#'  \code{\link[dplyr]{rename}}, \code{\link[dplyr]{mutate}}, \code{\link[dplyr]{case_when}}, \code{\link[dplyr]{select}}, \code{\link[dplyr]{mutate-joins}}, \code{\link[dplyr]{filter}}, \code{\link[dplyr]{bind_rows}}
#'  \code{\link[RMySQL]{character(0)}}, \code{\link[RMySQL]{MySQLDriver-class}}
#' @rdname generate_materialized_view
#' @export
#' @importFrom readxl read_xlsx
#' @importFrom tidyr unite
#' @importFrom dplyr rename mutate case_when select left_join filter bind_rows
#' @importFrom RMySQL dbConnect MySQL dbWriteTable dbDisconnect
generate_materialized_view <- function(version_db, toxval.db){

  message("Generating Materialized View -- ", Sys.time())

  # Required default columns for materialized view
  mv_default_cols = data.frame(
    field_name = c(
      "export_date",
      "data_version"
    ),
    COLUMN_TYPE = c(
      # Default to current time
      "datetime DEFAULT now()",
      # Default database version
      paste0("varchar(50) DEFAULT '", version_db, "'")
    ),
    field_comment = c(
      "Date when data was exported from database version",
      "Database version"
    )
  )

  # Get field definitions for the query
  field_def = readxl::read_xlsx(paste0(toxval.config()$datapath, "release_files/field_dictionary.xlsx")) %>%
    tidyr::unite(col = "dict_key",
                 db_table, db_field_name,
                 sep = "_", remove=FALSE) %>%
    dplyr::rename(field_comment = `Short Description`,
                  field_name = `Field Name`) %>%
    dplyr::mutate(field_name = tolower(field_name))

  # Pull tables, columns, and datatype definitions from the database
  db_col_list = runQuery(paste0("SELECT TABLE_NAME, COLUMN_NAME, COLUMN_TYPE, NUMERIC_PRECISION ",
                                "FROM INFORMATION_SCHEMA.COLUMNS ",
                                "WHERE TABLE_NAME IN ('",
                                paste0(unique(field_def$db_table), collapse = "', '"), "') ",
                                "AND COLUMN_NAME IN ('", paste0(unique(field_def$db_field_name), collapse = "', '"), "') ",
                                "AND TABLE_SCHEMA = '", version_db, "'"),
                         version_db) %>%
    dplyr::mutate(COLUMN_TYPE = dplyr::case_when(
      COLUMN_TYPE == "float" ~ paste0(COLUMN_TYPE, "(", NUMERIC_PRECISION, ")"),
      TRUE ~ COLUMN_TYPE
    )) %>%
    dplyr::select(-NUMERIC_PRECISION) %>%
    tidyr::unite(col = "dict_key",
                 TABLE_NAME, COLUMN_NAME,
                 sep = "_", remove=FALSE) %>%
    dplyr::left_join(field_def %>%
                       dplyr::select(dict_key, field_name, field_comment),
                     by = "dict_key") %>%
    dplyr::filter(!is.na(field_comment)) %>%
    # Append default columns at the beginning
    dplyr::bind_rows(mv_default_cols, .)
  ##############################################################################
  ## Create template materialized table
  ##############################################################################

  # Set name of materialized view based on database version
  mv_name = paste0("mv_",
                   gsub("res_", "", version_db))

  message("Dropping old table if exists -- ", Sys.time())
  # Drop old table
  runQuery(paste0("DROP TABLE IF EXISTS ", mv_name),
           toxval.db)

  # Get materialized view column names for template table
  mv_cols = c(mv_default_cols$field_name, field_def$field_name)

  mv_tbl = data.frame(
    matrix(ncol=length(mv_cols), nrow=0,
           dimnames = list(NULL, mv_cols))
  )

  message("Creating empty table -- ", Sys.time())
  # Write empty table
  con <- RMySQL::dbConnect(drv=RMySQL::MySQL(),
                           user=Sys.getenv("db_user"),
                           password=Sys.getenv("db_pass"),
                           host=Sys.getenv("db_server"),
                           dbname=toxval.db)

  res = RMySQL::dbWriteTable(con,
                             name=mv_name,
                             value=mv_tbl,
                             row.names=FALSE,
                             append=TRUE)

  RMySQL::dbDisconnect(con)

  # Check empty table created
  tbl_check = runQuery(paste0("SHOW TABLES LIKE '", mv_name, "'"),
                       toxval.db)

  if(!nrow(tbl_check)){
    stop(paste0("Table ", mv_name, " not created..."))
  }

  message("Adding comments to columns -- ", Sys.time())
  # Add required default id key column with comment
  runQuery(
    paste0(
      "ALTER TABLE ", mv_name," ADD id INT PRIMARY KEY AUTO_INCREMENT FIRST"
  ),
  toxval.db)

  runQuery(paste0("ALTER TABLE ", mv_name, " CHANGE id id INT AUTO_INCREMENT COMMENT 'Autoincrement unique record identifier column'"),
           toxval.db)

  # Add field comments to empty table for all columns
  for(i in seq_len(nrow(db_col_list))){
    cat("...", i, " of ", nrow(db_col_list), "\n")
    runQuery(paste0("ALTER TABLE ", mv_name, " CHANGE ",
                    db_col_list$field_name[i], " ",
                    db_col_list$field_name[i], " ",
                    db_col_list$COLUMN_TYPE[i], " COMMENT '",
                    db_col_list$field_comment[i], "'"
    ),
    toxval.db)
  }

  # Auto generate the query of data to insert into the empty materialized view table
  mv_query = paste0("SELECT ",
                    paste0(field_def$db_table, "..", field_def$db_field_name, " as ", field_def$field_name, collapse = ", "),
                    " FROM ", version_db, ".toxval b ",
                    "LEFT JOIN ", version_db, ".source_chemical a on a.chemical_id=b.chemical_id ",
                    "LEFT JOIN ", version_db, ".species d on b.species_id=d.species_id ",
                    "LEFT JOIN ", version_db, ".toxval_type_dictionary e on b.toxval_type=e.toxval_type ",
                    "LEFT JOIN ", version_db, ".record_source f on b.toxval_id=f.toxval_id ",
                    "WHERE b.qc_status NOT LIKE 'fail%'"
  ) %>%
    gsub("source_chemical..", "a.", ., fixed = TRUE) %>%
    gsub("toxval..", "b.", ., fixed = TRUE) %>%
    gsub("species..", "d.", ., fixed = TRUE) %>%
    gsub("toxval_type_dictionary..", "e.", ., fixed = TRUE) %>%
    gsub("record_source..", "f.", ., fixed = TRUE)

  message("Inserting data into table -- ", Sys.time())
  # Insert data into table
  runQuery(paste0("INSERT INTO ", mv_name, " ",
                  "(", paste0(field_def$field_name, collapse = ", "), ") ",
                  mv_query),
           toxval.db)

  message("Done -- ", Sys.time())
}
