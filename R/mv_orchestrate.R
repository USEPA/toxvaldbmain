#' @title Orchestrate Materialized View(s) Creation
#' @description Function to create a new materialized view database table by querying
#' an input database version based on an input field_dictionary.xlsx file.
#' @param toxval.db Version of the database to store the new view.
#' @param include.qc.status Boolean whether to include the qc_status field, or filter out "fail" records. Default is FALSE.
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
#' @rdname mv_orchestrate
#' @export
#' @importFrom readxl read_xlsx
#' @importFrom tidyr unite
#' @importFrom dplyr rename mutate case_when select left_join filter bind_rows
#' @importFrom RMySQL dbConnect MySQL dbWriteTable dbDisconnect
mv_orchestrate <- function(toxval.db, include.qc.status = FALSE){

  # Pull materialized view dictionary
  mv_dict = readxl::read_xlsx(paste0(toxval.config()$datapath, "release_files/mv_dict.xlsx")) %>%
    # Filter to those marked for inclusion
    dplyr::filter(include == 1)

  # Loop through materialized views to generate
  for(mv_i in seq_len(nrow(mv_dict))){

    # mv_fields = names(mv_dict)[startsWith(names(mv_dict), "mv_")]

    # Set variables from dictionary fields
    # Map(function(x, y) assign(x, y, envir=.GlobalEnv),
    #     mv_fields,
    #     mv_dict[mv_i, names(mv_dict)[startsWith(names(mv_dict), "mv_")]])

    mv_name = mv_dict$mv_name[mv_i]
    mv_table = mv_dict$mv_table[mv_i]
    mv_db = mv_dict$mv_db[mv_i]
    mv_version_label = mv_dict$mv_version_label[mv_i]

    message("Generating ", mv_name, " Materialized View -- ", Sys.time())

    # Required default columns for materialized view
    mv_default_cols = data.frame(
      field_name = c(
        "export_date",
        "data_version"
      ),
      COLUMN_TYPE = c(
        # Default to current time
        "datetime DEFAULT now()",
        # Default database version (formatted from database name to version number)
        paste0("varchar(50) DEFAULT '", mv_version_label, "'")
      ),
      field_comment = c(
        "Date when data was exported from database version",
        "Database version"
      )
    )

    # Get field definitions for the query
    field_def = readxl::read_xlsx(paste0(toxval.config()$datapath, "release_files/field_dictionary.xlsx"),
                                  sheet = mv_name,
                                  col_types = "text") %>%
      tidyr::unite(col = "dict_key",
                   db_table, db_field_name,
                   sep = "_", remove=FALSE) %>%
      dplyr::rename(field_comment = `Short Description`,
                    field_name = `Field Name`) %>%
      dplyr::mutate(field_name = tolower(field_name))

    # Check if any field definitions are available
    if(!nrow(field_def)){
      stop("No field definition entries provided in 'release_files/field_dictionary.xlsx' for '", mv_name, "'")
    }

    # If not including qc_status, filter "fail" out and remove columns
    if(!include.qc.status){
      field_def = field_def %>%
        dplyr::filter(!field_name %in% c("qc_status"))
    }

    # Check for any field entries missing a comment
    if(anyNA(field_def$field_comment)){
      stop("All field definitions in release_files/field_dictionary.xlsx must have a 'field_comment' value.")
    }

    # Pull tables, columns, and datatype definitions from the database
    db_col_list = runQuery(paste0("SELECT TABLE_NAME, COLUMN_NAME, COLUMN_TYPE, NUMERIC_PRECISION ",
                                  "FROM INFORMATION_SCHEMA.COLUMNS ",
                                  "WHERE TABLE_NAME IN ('",
                                  paste0(unique(field_def$db_table), collapse = "', '"), "') ",
                                  "AND COLUMN_NAME IN ('", paste0(unique(field_def$db_field_name), collapse = "', '"), "') ",
                                  "AND TABLE_SCHEMA = '", mv_db, "'"),
                           mv_db)

    # Check if any field definitions are present in the provided table and database
    if(!nrow(db_col_list)){
      stop("Table '", mv_table, "' with specified fields was not found in database '", mv_db, "' not found...")
    }

    db_col_list = db_col_list %>%
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

    # Check if input name already has "mv_" prefix
    if(!startsWith(mv_name, "mv_")){
      mv_name = paste0("mv_", mv_name)
    }

    message("...Dropping old table if exists -- ", Sys.time())
    # Drop old table
    runQuery(paste0("DROP TABLE IF EXISTS ", mv_name),
             toxval.db)

    # Get materialized view column names for template table
    mv_cols = c(mv_default_cols$field_name, field_def$field_name) %>%
      .[!. %in% c(NA)]

    mv_tbl = data.frame(
      matrix(ncol=length(mv_cols), nrow=0,
             dimnames = list(NULL, mv_cols))
    )

    message("...Creating empty table -- ", Sys.time())
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

    message("...Adding comments to columns -- ", Sys.time())
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
    mv_query = switch(mv_name,
                      "mv_toxvaldb" = {
                        tmp = paste0("SELECT ",
                                     paste0(field_def$db_table, "..", field_def$db_field_name, " as ", field_def$field_name, collapse = ", "),
                                     " FROM ", mv_db, ".toxval b ",
                                     "LEFT JOIN ", mv_db, ".source_chemical a on a.chemical_id=b.chemical_id ",
                                     "LEFT JOIN ", mv_db, ".species d on b.species_id=d.species_id ",
                                     "LEFT JOIN ", mv_db, ".toxval_type_dictionary e on b.toxval_type=e.toxval_type ",
                                     "LEFT JOIN ", mv_db, ".record_source f on b.toxval_id=f.toxval_id ",
                                     # Filter out origin documents
                                     "WHERE f.record_source_level not in ('origin')"
                        ) %>%
                          gsub("source_chemical..", "a.", ., fixed = TRUE) %>%
                          gsub("toxval..", "b.", ., fixed = TRUE) %>%
                          gsub("species..", "d.", ., fixed = TRUE) %>%
                          gsub("toxval_type_dictionary..", "e.", ., fixed = TRUE) %>%
                          gsub("record_source..", "f.", ., fixed = TRUE) %>%
                          # Set source as concat with supersource and source
                          gsub("b.source as source, ", "CONCAT(b.supersource, ': ', b.source) as source, ", .)

                        # If not including qc_status, filter "fail" out
                        if(!include.qc.status){
                          tmp = paste0(tmp, " and b.qc_status NOT LIKE 'fail%'")
                        }
                        tmp
                      },
                      # Default case
                      paste0("SELECT ",
                             paste0(field_def$db_table, ".", field_def$db_field_name, " as ", field_def$field_name, collapse = ", "),
                             " FROM ", mv_table)
    )

    message("...Inserting data into table -- ", Sys.time())
    # Insert data into table
    runQuery(paste0("INSERT INTO ", mv_name, " ",
                    "(", paste0(field_def$field_name, collapse = ", "), ") ",
                    mv_query),
             toxval.db)
    message("Done -- ", Sys.time())
    cat("\n")
  }
}
