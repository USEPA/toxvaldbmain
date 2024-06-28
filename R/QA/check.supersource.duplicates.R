#-------------------------------------------------------------------------------------
#' Report supersource duplicates
#' @param toxval.db The current version of toxval
#' @param source Source to be checked
#' @param subsource Subsource to be checked
#' @param criteria List of parameters used to make deduping decisions
#' @return A tibble with supersource duplicate information
#' @export
#-------------------------------------------------------------------------------------
report.missing.dictionary.by.source <- function(toxval.db, criteria=c('toxval_type','toxval_numeric','dtxsid')) {
  printCurrentFunction(toxval.db)

  data = runQuery(paste0("SELECT supersource, source, ", toString(criteria), " FROM toxval", query_addition),
                  toxval.db) %>%
    # Group by everything except source
    dplyr::group_by(dplyr::across(c(-source))) %>%
    # Collapse groupings by source
    dplyr::summarise(source = toString(unique(source))) %>%
    # Only account for those with duplicates
    dplyr::filter(grepl(",", source)) %>%
    dplyr::distinct()

  writexl::write_xlsx(data, paste0("Repo/QC Reports/supersource_duplicate_report ", Sys.Date(), ".xlsx"))
  return(data)
}
