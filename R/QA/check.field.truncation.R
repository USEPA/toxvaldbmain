#' @title Check Field Truncation
#' @description QA script to check for field truncation.
#' @param db Name of database to query
#' @return Dataframe list by database table checked. Also writes XLSX and RData file to "Repo/QC Reports" folder
#' @details DETAILS
#' @examples
#' \dontrun{
#' if(interactive()){
#'  #EXAMPLE1
#'  }
#' }
#' @seealso
#'  \code{\link[dplyr]{filter}}, \code{\link[dplyr]{select}}, \code{\link[dplyr]{reexports}}, \code{\link[dplyr]{distinct}}, \code{\link[dplyr]{mutate}}, \code{\link[dplyr]{mutate-joins}}
#'  \code{\link[tidyr]{pivot_longer}}
#'  \code{\link[stringr]{str_length}}
#'  \code{\link[purrr]{keep}}
#'  \code{\link[writexl]{write_xlsx}}
#' @rdname check.field.truncation
#' @export
#' @importFrom dplyr filter select any_of distinct mutate left_join
#' @importFrom tidyr pivot_longer
#' @importFrom stringr str_length
#' @importFrom purrr compact
#' @importFrom writexl write_xlsx
check.field.truncation <- function(db){

  # List of optional table identifier fields to pull
  id_cols = c("id", "toxval_id", "source_hash", "source", "source_table",
              "chemical_id", "species_id", "clowder_doc_id",
              "genetox_details_id", "skin_eye_id", "toxval_type_dictionary_id")

  # Get all tables with character/string text fields
  tbl_f_list = runQuery(paste0(
    "SELECT DISTINCT TABLE_NAME, COLUMN_NAME, CHARACTER_MAXIMUM_LENGTH as char_limit FROM information_schema.columns ",
    "WHERE table_schema = '", db, "' AND ",
    "DATA_TYPE IN ('varchar', 'text', 'mediumtext', 'json')"
    ),
    db)

  # Loop through each table, pull data, and flag potential issues
  out = lapply(unique(tbl_f_list$TABLE_NAME), function(tbl_n){

    message("Pulling table ", tbl_n, " (", which(unique(tbl_f_list$TABLE_NAME) == tbl_n),
            " of ", length(unique(tbl_f_list$TABLE_NAME)), ") - ", Sys.time())

    # Filter to table field limits
    f_limit = tbl_f_list %>%
      dplyr::filter(TABLE_NAME == tbl_n) %>%
      dplyr::select(-TABLE_NAME)

    # Pull data
    in_data = runQuery(paste0("SELECT * FROM ", tbl_n),
                       db) %>%
      # Select relevant id and text columns
      dplyr::select(dplyr::any_of(c(id_cols, tbl_f_list$COLUMN_NAME[tbl_f_list$TABLE_NAME == tbl_n]))) %>%
      tidyr::pivot_longer(cols=-dplyr::any_of(id_cols)) %>%
      dplyr::distinct() %>%
      # Calculate character length
      dplyr::mutate(value_n = stringr::str_length(value)) %>%
      dplyr::left_join(f_limit,
                       by = c("name"="COLUMN_NAME")) %>%
      # Filter to those that match the limit (which would most likely be truncated)
      dplyr::filter(value_n >= char_limit)

    # Check if any records were flagged
    if(nrow(in_data)){
      return(in_data)
    }
    return(NULL)
  }) %T>% {
    # Add table name to list
    names(.) <- unique(tbl_f_list$TABLE_NAME)
  } %>%
    # Remove NULL/empty dataframes
    purrr::compact()

  # Save output
  save(out, file = paste0(toxval.config()$datapath, "QC Reports/qc_field_truncation_", Sys.Date(), ".RData"))
  writexl::write_xlsx(out, paste0(toxval.config()$datapath, "QC Reports/qc_field_truncation_", Sys.Date(), ".xlsx"))

  return(out)
}
