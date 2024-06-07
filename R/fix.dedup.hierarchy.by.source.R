#-------------------------------------------------------------------------------------
#'
#' Set qc_status as "fail" for lesser priority duplicates
#'
#' @param toxval.db The version of toxvaldb to use.
#' @param source Source to be fixed
#' @param subsource Subsource to be fixed (NULL default)
#' @param priority_list Named list describing source priority, with low priority index and high priority value
#' @param criteria List of parameters used to make deduping decisions
#' @param report.only Whether to report or write/export data. Default is FALSE (write/export data)
#' @export
#-------------------------------------------------------------------------------------
fix.dedup.hierarchy.by.source <- function(toxval.db, source=NULL, subsource=NULL, priority_list=NULL, criteria=c("dtxsid"), report.only=FALSE) {
  printCurrentFunction(paste(toxval.db,":", source))

  slist = runQuery("select distinct source from toxval",toxval.db)[,1]
  if(!is.null(source)) slist = source

  # Handle addition of subsource for queries
  query_addition = ""
  if(!is.null(subsource)) {
    query_addition = paste0(" and subsource='", subsource, "'")
  }

  # Create two priority lists for both directions
  if(is.null(priority_list)) {
    # Set default priority_list value if parameter is NULL
    priority_list_low_high = c(
      "PPRTV (NCEA)" = "PPRTV (CPHEA)",
      "HEAST" = "IRIS",
      "HAWC PFAS 150" = "PFAS 150 SEM v2",
      "California DPH" = "Cal OEHHA",
      "EnviroTox_v2" = "ECOTOX"
    )
  } else {
    # Use input priority_list if available
    priority_list_low_high = priority_list
  }
  # Invert initial priority_list to switch directions
  priority_list_high_low = setNames(names(priority_list_low_high), priority_list_low_high)

  # Track sources already completed to ensure unnecessary work is not repeated
  already_finished = NULL
  report.out = data.frame()

  for(source in slist) {
    if(source %in% already_finished) {
      cat("Already handled hierarchical deduping relevant to", source, "\n")
      next
    }

    # No changes needed if source is not mentioned in priority list
    if(!(source %in% priority_list_low_high | source %in% priority_list_high_low)) {
      cat("No hierarchical relationship specified for", source, "\n")
      next
    }

    # Identify high/low priority source
    if(source %in% priority_list_low_high) {
      high_priority = source
      low_priority = priority_list_high_low[[source]]
    } else {
      low_priority = source
      high_priority = priority_list_low_high[[source]]
    }

    if(any(!c(high_priority, low_priority) %in% runQuery("select distinct source from toxval",toxval.db)[,1])){
      cat("...Priority source pair", high_priority, "and", low_priority, "missing data. Skipping...\n")
      next
    }

    already_finished = c(already_finished, high_priority, low_priority)

    cat("\nHandling dedup hierarchy for", low_priority, "and", high_priority, "\n")

    # Get both high and low priority data using criteria
    criteria_string = paste(criteria, collapse=", ")
    low_entries = runQuery(paste0("SELECT DISTINCT toxval_id, ", criteria_string,
                                  " FROM toxval WHERE source='", low_priority, "'", query_addition),
                           toxval.db)
    high_entries = runQuery(paste0("SELECT DISTINCT ", criteria_string,
                                   " FROM toxval WHERE source='", high_priority, "'", query_addition),
                           toxval.db)

    # Identify entries present in both sets of data
    intersection = dplyr::inner_join(low_entries %>%
                                       dplyr::select(-toxval_id) %>%
                                       dplyr::distinct(),
                                     high_entries %>%
                                       dplyr::distinct(),
                                     by=criteria) %>%
      dplyr::pull(dtxsid)

    # Get toxval_id values to fail
    ids_to_fail = low_entries %>%
      dplyr::filter(dtxsid %in% intersection) %>%
      dplyr::pull(toxval_id) %>%
      unique() %>%
      toString()

    if(ids_to_fail != ""){
      if(!report.only){
        # Set qc_status="fail" for low priority source in appropriate entries
        update_query = paste0("UPDATE toxval SET qc_status='fail:Duplicate of ", high_priority," chemical entry' WHERE ",
                              "source='", low_priority, "' AND toxval_id IN (", ids_to_fail, ")", query_addition)
        runQuery(update_query, toxval.db)
      } else {
        report.out = report.out %>%
          dplyr::bind_rows(.,
                           runQuery(paste0("SELECT * FROM toxval WHERE toxval_id in (", ids_to_fail, ")"),
                                    toxval.db))
      }
      cat("\n")
    }
  }
  if(report.only){
    if(length(slist) > 1) source = "all sources"
    cat("Exporting report...\n")
    writexl::write_xlsx(report.out %>% dplyr::distinct(),
                        paste0("Repo/QC Reports/fix_dedup_hierarchy_", source, "_", subsource, "_", Sys.Date(), ".xlsx") %>%
                          gsub("__", "_", .) %>%
                          stringr::str_squish())
    return(report.out)
  }
}
