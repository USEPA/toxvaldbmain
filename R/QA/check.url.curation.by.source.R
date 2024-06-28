#-------------------------------------------------------------------------------------
#' Report proportion of entries with URL fields
#'
#' @param toxval.db The current version of toxval
#' @param source The source to be checked (NULL default; checks all sources)
#' @param subsource The subsource to be checked (NULL default)
#' @return Output is written to a file
#' @export
#--------------------------------------------------------------------------------------
check.url.curation.by.source <- function(toxval.db, source=NULL, subsource=NULL){
  printCurrentFunction()

  # Handle addition of subsource for queries
  query_addition = ""
  if(!is.null(subsource)) {
    query_addition = paste0(" and subsource='", subsource, "'")
  }

  # If source is NULL, check every source
  slist = source
  if(is.null(source)) slist = runQuery("select distinct source from toxval",toxval.db)$source

  # Track all URL data and aggregate report
  all_url_data = data.frame()
  aggregate_report = data.frame()

  for(src in slist) {
    # Query for all URL-related fields in toxval and record_source
    url_query = paste0("SELECT a.source_url, a.subsource_url, b.url AS record_url ",
                       "FROM toxval a ",
                       "LEFT JOIN record_source b on b.toxval_id=a.toxval_id ",
                       "WHERE b.source='", src, "'",
                       query_addition
    )
    curr_url_data = runQuery(url_query, toxval.db) %>%
      dplyr::mutate(
        dplyr::across(dplyr::where(is.character), ~dplyr::na_if(., "-")),
        has_url = (!is.na(source_url) | !is.na(subsource_url) | !is.na(record_url))
      )

    # Add current URL data to aggregate URL data
    all_url_data = all_url_data %>%
      dplyr::bind_rows(curr_url_data)

    # Generate URL report for current source
    if(!is.null(subsource)) src = paste0(src, " ", subsource)
    curr_report = tibble::tibble(
      "source" = src,
      "prop_with_source_url" = nrow(tidyr::drop_na(curr_url_data, source_url)) / nrow(curr_url_data),
      "prop_with_subsource_url" = nrow(tidyr::drop_na(curr_url_data, subsource_url)) / nrow(curr_url_data),
      "prop_with_record_url" = nrow(tidyr::drop_na(curr_url_data, record_url)) / nrow(curr_url_data),
      "prop_with_any_url" = nrow(dplyr::filter(curr_url_data, has_url)) / nrow(curr_url_data)
    )

    # Add current source report to aggregate report
    aggregate_report = aggregate_report %>%
      dplyr::bind_rows(curr_report)
  }

  if(length(slist) > 1) {
    # Generate URL report for all sources as a whole
    curr_report = tibble::tibble(
      "source" = "All Sources",
      "prop_with_source_url" = nrow(tidyr::drop_na(all_url_data, source_url)) / nrow(all_url_data),
      "prop_with_subsource_url" = nrow(tidyr::drop_na(all_url_data, subsource_url)) / nrow(all_url_data),
      "prop_with_record_url" = nrow(tidyr::drop_na(all_url_data, record_url)) / nrow(all_url_data),
      "prop_with_any_url" = nrow(dplyr::filter(all_url_data, has_url)) / nrow(all_url_data)
    )

    # Add report to aggregate report
    aggregate_report = aggregate_report %>%
      dplyr::bind_rows(curr_report)

    # Create appropriate filename
    file = paste0(toxval.config()$datapath, "QC Reports/url_curation_summary_ALL SOURCES_", Sys.Date(), ".xlsx")
  } else {
    # Create filename for source specified
    file = paste0(toxval.config()$datapath, "QC Reports/url_curation_summary_", src, "_", Sys.Date(), ".xlsx")
  }

  # Write report to QC Reports folder
  writexl::write_xlsx(aggregate_report, file)
  cat("Report written to", file, "\n")
}
