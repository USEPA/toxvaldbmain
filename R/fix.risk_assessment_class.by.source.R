#-------------------------------------------------------------------------------------
#' Set the risk assessment class of toxval according to an excel dictionary.
#' Values may beset multiple times, so the excel sheet should be ordered so that
#' the last ones to be set are last
#'
#' @param toxval.db The version of toxval in which the data is altered.
#' @param source The source to be updated
#' @param subsource The subsource to be updated (NULL default)
#' @param restart If TRUE, delete all values and start from scratch
#' @param report.only Whether to report or write/export data. Default is FALSE (write/export data)
#' @export
#--------------------------------------------------------------------------------------
fix.risk_assessment_class.by.source <- function(toxval.db, source=NULL, subsource=NULL, restart=TRUE, report.only=FALSE) {
  printCurrentFunction(paste(toxval.db,":", source, subsource))

  # Get list of sources to use
  slist = source
  if(is.null(source)) {
    slist = runQuery("select distinct source from toxval",toxval.db) %>%
      dplyr::pull(source)
  }
  source_string = slist %>%
    paste0(collapse="', '")

  # Handle addition of subsource for queries
  query_addition = ""
  if(!is.null(subsource)) {
    query_addition = paste0(query_addition, " and subsource='", subsource, "'")
  }

  # Read in RAC dictionary
  rac_dict = readxl::read_xlsx("Repo/dictionary/RAC_toxval_type_dict.xlsx") %>%
    dplyr::distinct() %>%
    dplyr::mutate(dplyr::across(dplyr::where(is.character), ~tidyr::replace_na(., "-")))

  # Split into subtype and no subtype dictionaries
  rac_no_subtype = rac_dict %>%
    dplyr::filter(toxval_subtype == "-")
  rac_subtype = rac_dict %>%
    dplyr::filter(toxval_subtype != "-")

  # Reset values if specified
  if(restart & !report.only) {
    query = paste0("UPDATE toxval SET risk_assessment_class='-' WHERE source IN ('",source_string,"')",
                   query_addition)
    # runQuery(query, toxval.db)
  }

  if(!report.only) {
    # Make changes not based on subtype
    case_block = rac_no_subtype %>%
      dplyr::mutate(
        component = stringr::str_c("WHEN toxval_type='", toxval_type, "' THEN '", risk_assessment_class, "'")
      ) %>%
      dplyr::pull(component) %>%
      paste0(collapse=" ")
    query = paste0("UPDATE toxval SET risk_assessment_class = CASE ",
                   case_block, " ",
                   "ELSE risk_assessment_class END ",
                   "WHERE source IN ('", source_string, "')",
                   query_addition)
    # runQuery(query, toxval.db)

    # Make changes that are based on subtype
    case_block = rac_subtype %>%
      dplyr::mutate(
        component = stringr::str_c(
          "WHEN toxval_type='", toxval_type, "' AND toxval_subtype LIKE '", toxval_subtype, "' ",
          "THEN '", risk_assessment_class, "'"
        )
      ) %>%
      dplyr::pull(component) %>%
      paste0(collapse=" ")
    query = paste0("UPDATE toxval SET risk_assessment_class = CASE ",
                   case_block, " ",
                   "ELSE risk_assessment_class END ",
                   "WHERE source IN ('", source_string, "')",
                   query_addition)
    # runQuery(query, toxval.db)
  }

  # Get missing entries
  query = paste0("SELECT * FROM toxval WHERE risk_assessment_class='-' AND source IN ('", source_string, "')")
  missing_rac = runQuery(query, toxval.db)

  missing_check = rac_dict %>%
    dplyr::select(toxval_type) %>%
    dplyr::distinct() %>%
    dplyr::mutate(missing_toxval_type_dict_entry = 0)

  missing_rac = missing_rac %>%
    dplyr::left_join(missing_check, by=c("toxval_type")) %>%
    dplyr::mutate(missing_toxval_type_dict_entry = missing_toxval_type_dict_entry %>% tidyr::replace_na(1))

  for(source in missing_rac %>% dplyr::pull(source) %>% unique()) {
    out_file = paste0(writexl::write_xlsx("dictionary/missing/missing_rac/missing_RAC_",source, " ",subsource,".xlsx"))
    curr_missing = missing_rac %>%
      dplyr::filter(source == !!source_string)
    writexl::write_xlsx(curr_missing, out_file)
  }
}
