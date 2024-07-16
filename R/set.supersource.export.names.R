#--------------------------------------------------------------------------------------
#' Populate export_source_name and supersource fields in ToxVal
#'
#' @param toxval.db The version of toxval from which to set defaults.
#' @param source The source to be fixed (if NULL then edit all sources)
#' @param version_date The date of the source_info dictionary to be used
#' @export
#--------------------------------------------------------------------------------------
set.supersource.export.names <- function(toxval.db, source=NULL, version_date="2024-05-31"){
  printCurrentFunction()
  # Read source_info dictionary
  file = paste0(toxval.config()$datapath,"dictionary/source_info ", version_date, ".xlsx")
  source_info = readxl::read_xlsx(file) %>%
    dplyr::mutate(dplyr::across(dplyr::where(is.character), ~tidyr::replace_na(., "-")))

  # Get list of sources if NULL source provided
  slist = source
  if(is.null(source)) {
    slist = runQuery("select distinct source from toxval", toxval.db)$source
  }
  source_string = slist %>%
    paste0(collapse="', '")

  # Get relevant data for current source
  src_data = source_info %>%
    dplyr::filter(source %in% !!slist) %>%
    dplyr::select(source, supersource) %>%
    dplyr::distinct()

  # Check for multiple mappings
  if(nrow(src_data) > length(slist)) {
    cat("Multiple mappings exist for export_source_name or supersource.\n")
    View(src_data)
    browser()
  }

  src_data = src_data %>%
    dplyr::mutate(
      case_block = stringr::str_c("WHEN source='", source, "' THEN '", supersource, "'")
    )

  case_string = src_data %>%
    dplyr::pull(case_block) %>%
    paste0(collapse = " ")

  # Push export_source_name and supersource values to ToxVal
  query = paste0("UPDATE toxval SET ",
                 "supersource = CASE ",
                 case_string, " ELSE supersource END ",
                 "WHERE source IN ('", source_string, "')")
  runQuery(query, toxval.db)
}
