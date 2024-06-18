#-------------------------------------------------------------------------------------
#'
#' Set qc_status as "fail" for lesser priority duplicates
#'
#' @param toxval.db The version of toxvaldb to use.
#' @param source Source to be fixed
#' @param subsource Subsource to be fixed (NULL default)
#' @param priority_list Named list describing source priority, with low priority index and high priority value
#' @param subsource_priority_list Named list of subsources to deprecate per source, source index/subsource value
#' @param criteria List of parameters used to make deduping decisions
#' @param report.only Whether to report or write/export data. Default is FALSE (write/export data)
#' @export
#-------------------------------------------------------------------------------------
fix.dedup.hierarchy.by.source <- function(toxval.db, source=NULL, subsource=NULL,
                                          priority_list=NULL, subsource_priority_list=NULL,
                                          criteria=c("dtxsid"), report.only=FALSE) {
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
      "EnviroTox_v2" = "ECOTOX",
      "OW Drinking Water Standards" = "EPA OW NPDWR"
    )
  } else {
    # Use input priority_list if available
    priority_list_low_high = priority_list
  }
  # Invert initial priority_list to switch directions
  priority_list_high_low = setNames(names(priority_list_low_high), priority_list_low_high)

  # Create subsource priority list
  if(is.null(subsource_priority_list)) {
    subsource_priority_list = list(
      "RSL" = c("IRIS", "PPRTV Screening Level", "PPRTV")
    )
  }

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
      if(!(source %in% names(subsource_priority_list))) {
        cat("No hierarchical relationship specified for", source, "\n")
        next
      }
    }

    if(source %in% priority_list_low_high | source %in% priority_list_high_low) {
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

      ids_to_fail = low_entries %>%
        dplyr::filter(dtxsid %in% intersection) %>%
        dplyr::select(toxval_id)
    } else {
      high_priority = as.character(NA)
      low_priority = as.character(NA)
      ids_to_fail = data.frame()
    }

    # Get toxval_id values to fail from subsource hierarchy
    if(source %in% names(subsource_priority_list)) {
      subsources_to_fail = paste0(subsource_priority_list[[source]], collapse="', '") %>%
        paste0("'", ., "'")
      no_apostrophe = gsub("'", "", subsources_to_fail)
      subsource_fail_query = paste0("SELECT DISTINCT toxval_id FROM toxval ",
                                    "WHERE source='", source, "' ",
                                    "AND subsource IN (", subsources_to_fail, ")")
      subsource_fails = runQuery(subsource_fail_query, toxval.db)
    } else {
      subsource_fails = data.frame()
    }

    # Get toxval_id values to fail
    ids_to_fail = ids_to_fail %>%
      dplyr::bind_rows(subsource_fails) %>%
      dplyr::pull(toxval_id) %>%
      unique() %>%
      toString()

    if(ids_to_fail != ""){
      if(!report.only){
        # Set qc_status="fail" for low priority source in appropriate entries
        # CONCAT reason if already has a fail status
        update_query = paste0("UPDATE toxval SET qc_status = CASE ",
                              "WHEN qc_status like '%Duplicate of ", high_priority," chemical entry%' THEN qc_status ",
                              "WHEN qc_status like '%fail%' THEN CONCAT(qc_status, '; Duplicate of ", high_priority," chemical entry') ",
                              "ELSE 'fail:Duplicate of ", high_priority," chemical entry' ",
                              "END ",
                              "WHERE source = '", low_priority, "' AND toxval_id IN (", ids_to_fail, ")", query_addition)
        if(nrow(subsource_fails)) {
          update_query = paste0("UPDATE toxval SET qc_status = CASE ",
                                "WHEN subsource in (", subsources_to_fail, ") THEN 'fail: Subsource in ", no_apostrophe, "' ",
                                "WHEN qc_status like '%Duplicate of ", high_priority," chemical entry%' THEN qc_status ",
                                "WHEN qc_status like '%fail%' THEN CONCAT(qc_status, '; Duplicate of ", high_priority," chemical entry') ",
                                "ELSE 'fail: Duplicate of ", high_priority," chemical entry' ",
                                "END ",
                                "WHERE source = '", low_priority, "' AND toxval_id IN (", ids_to_fail, ")", query_addition)
        }

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
