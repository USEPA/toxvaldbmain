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
      "OW Drinking Water Standards" = "EPA OW NPDWR",
      "USGS HBSL" = "EPA OPP",
      "TEST" = "ChemIDplus",
      "RSL" = "HEAST"
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
      source_ids_to_fail = dplyr::inner_join(low_entries %>%
                                               dplyr::distinct(),
                                             high_entries %>%
                                               dplyr::distinct(),
                                             by=criteria) %>%
        dplyr::select(toxval_id)
    } else {
      high_priority = as.character(NA)
      low_priority = as.character(NA)
      source_ids_to_fail = data.frame()
    }

    # Get toxval_id values to fail from subsource hierarchy
    if(source %in% names(subsource_priority_list)) {
      subsources_to_fail = paste0(subsource_priority_list[[source]], collapse="', '") %>%
        paste0("'", ., "'")
      no_apostrophe = gsub("'", "", subsources_to_fail)
      subsource_fail_query = paste0("SELECT DISTINCT toxval_id FROM toxval ",
                                    "WHERE source='", source, "' ",
                                    "AND subsource IN (", subsources_to_fail, ")")
      subsource_ids_to_fail = runQuery(subsource_fail_query, toxval.db)
    } else {
      subsource_ids_to_fail = data.frame()
    }

    if(nrow(source_ids_to_fail) | nrow(subsource_ids_to_fail)){
      # Set qc_status="fail" for low priority source in appropriate entries
      # CONCAT reason if already has a fail status
      source_fail_string = as.character(NA)
      subsource_fail_string = as.character(NA)
      # no_apostrophe = as.character(NA)

      # Handle failing source priority entries
      if(nrow(source_ids_to_fail)) {
        # Get toxval_id values to fail
        source_fail_string = source_ids_to_fail %>%
          dplyr::pull(toxval_id) %>%
          unique() %>%
          toString()

        source_query = paste0("UPDATE toxval SET qc_status = CASE ",
                              "WHEN qc_status like '%Duplicate of ", high_priority," chemical entry%' THEN qc_status ",
                              "WHEN qc_status like '%fail%' THEN CONCAT(qc_status, '; Duplicate of ", high_priority," chemical entry') ",
                              "ELSE 'fail:Duplicate of ", high_priority," chemical entry' ",
                              "END ",
                              "WHERE source = '", low_priority, "' AND toxval_id IN (", source_fail_string, ")", query_addition)
        if(!report.only) runQuery(source_query, toxval.db)
      }
      # Handle failing subsource priority entries
      if(nrow(subsource_ids_to_fail)) {
        # Get toxval_id values to fail
        subsource_fail_string = subsource_ids_to_fail %>%
          dplyr::pull(toxval_id) %>%
          unique() %>%
          toString()

        subsource_query = paste0("UPDATE toxval SET qc_status = CASE ",
                                 "WHEN qc_status like '%Subsource in ", no_apostrophe, "%' THEN qc_status ",
                                 "WHEN qc_status like '%fail%' THEN CONCAT(qc_status, '; Subsource in ", no_apostrophe, "') ",
                                 "ELSE 'fail: Subsource in ", no_apostrophe, "' ",
                                 "END ",
                                 "WHERE source = '", source, "' AND toxval_id IN (", subsource_fail_string, ")", query_addition)
        if(!report.only) runQuery(subsource_query, toxval.db)
      }

      if(report.only) {
        all_failures = source_ids_to_fail %>%
          dplyr::mutate(add_qc_status = stringr::str_c("Duplicate of ", !!high_priority, " chemical entry"))

        if(nrow(subsource_ids_to_fail)){
          all_failures = all_failures %>%
            dplyr::bind_rows(subsource_ids_to_fail) %>%
            dplyr::mutate(add_qc_status = tidyr::replace_na(add_qc_status,
                                                            paste0("Subsource in ", !!no_apostrophe)))
        }

        all_failures_string = all_failures %>%
          dplyr::pull(toxval_id) %>%
          unique() %>%
          toString()

        report_query = paste0("SELECT * FROM toxval WHERE toxval_id IN (", all_failures_string, ")")
        current_report = runQuery(report_query, toxval.db) %>%
          dplyr::left_join(all_failures, by="toxval_id") %>%
          dplyr::mutate(
            previous_qc_status = qc_status,
            qc_status = dplyr::case_when(
              stringr::str_detect(qc_status, add_qc_status) ~ qc_status,
              stringr::str_detect(qc_status, "fail") ~ stringr::str_c(qc_status, "; ", add_qc_status),
              TRUE ~ stringr::str_c("fail: ", add_qc_status)
            )
          ) %>%
          dplyr::select(-add_qc_status)

        report.out = report.out %>%
          dplyr::bind_rows(., current_report)

      }
      cat("\n")
    }
  }
  if(report.only){
    if(length(slist) > 1) source = "all sources"
    cat("Exporting report...\n")

    report.out = report.out %>%
      dplyr::distinct() %>%
      dplyr::rename(new_qc_status = qc_status)

    writexl::write_xlsx(report.out,
                        paste0("Repo/QC Reports/fix_dedup_hierarchy_", source, "_", subsource, "_", Sys.Date(), ".xlsx") %>%
                          gsub("__", "_", .) %>%
                          stringr::str_squish())
    return(report.out)
  }
}
