#-------------------------------------------------------------------------------------
#' Report summary of missing dictionary entries for single, subset, or all sources
#' @param toxval.db The current version of toxval
#' @param source_name The source to be reported (single name or list). If source=NULL, report all sources
#' @return A tibble with summarized missing dictionary information
#' @export
#-------------------------------------------------------------------------------------
report.missing.dictionary.by.source <- function(toxval.db, source_name=NULL) {
  printCurrentFunction(toxval.db)

  # Get list of all sources if source_name is null
  if(is.null(source_name)) {
    slist = runQuery("select distinct source from toxval",toxval.db)[,1]
  } else {
    slist = source_name
  }

  # Report source or number of sources to check
  if(is.null(source_name)) {
    cat("Checking missing dictionary entries for ", length(slist), " sources\n")
  } else {
    cat("Checking missing dictionary entries for ", slist, "\n")
  }

  # Run functions to get missing dictionary info
  missing_dictionary_entries = export.missing.dictionary.entries(toxval.db, slist, report.only=TRUE)
  num_missing_dictionary_entries = missing_dictionary_entries %>%
    dplyr::select(-source) %>%
    dplyr::distinct() %>%
    nrow() %>%
    max(., 0)

  missing_toxval_type = export.missing.toxval_type(toxval.db, report.only=TRUE)
  if (!is.null(missing_toxval_type)) {
    # Ensure that only specified sources are included in output
    missing_toxval_type = missing_toxval_type %>%
      dplyr::filter(source %in% slist)
  }
  num_missing_toxval_type = missing_toxval_type %>%
    dplyr::select(-source) %>%
    dplyr::distinct() %>%
    nrow() %>%
    max(., 0)

  missing_exposure_params = fix.exposure.params(toxval.db, slist, report.only=TRUE)
  num_missing_exposure_params = missing_exposure_params %>%
    dplyr::select(index1) %>%
    dplyr::distinct() %>%
    nrow() %>%
    max(., 0)

  missing_rac = fix.risk_assessment_class.by.source(toxval.db, slist, report.only=TRUE)
  num_missing_rac = max(nrow(missing_rac), 0)

  missing_single_param = data.frame()
  for(s in slist) {
    for(param in c("exposure_form", "exposure_method", "exposure_route", "generation", "lifestage",
                   "media", "sex", "study_duration_class", "study_duration_units", "study_type",
                   "toxval_numeric_qualifier", "toxval_subtype", "toxval_type", "toxval_units")) {
      curr_missing = fix.single.param.by.source(toxval.db, param, s, report.only=TRUE)
      if(length(curr_missing) > 0) {
        curr_missing_df = tibble::enframe(curr_missing, name=NULL, value="value") %>%
          dplyr::mutate(
            param = !!param,
            source = !!s
          )
        missing_single_param = rbind(missing_single_param, curr_missing_df)
      }
    }
  }
  num_missing_single_param = missing_single_param %>%
    dplyr::select(-source) %>%
    dplyr::distinct() %>%
    nrow() %>%
    max(., 0)

  missing_study_duration = fix.study_duration.params(toxval.db, slist, report.only=TRUE)
  num_missing_study_duration = missing_study_duration %>%
    dplyr::select(index1) %>%
    dplyr::distinct() %>%
    nrow() %>%
    max(., 0)

  missing_study_type = fix.study_type.manual(toxval.db, slist, report.only=TRUE)
  num_missing_study_type = max(nrow(missing_study_type), 0)

  # Get list of sources with missing information
  missing_sources = missing_dictionary_entries %>%
    dplyr::select(source) %>%
    dplyr::distinct()
  num_missing_sources = max(nrow(missing_sources), 0)

  # Generate summary report
  report_summary = tibble::tibble(
    `Sources with Missingness` = !!num_missing_sources,
    `Missing Dictionary Entries` = !!num_missing_dictionary_entries,
    `Missing toxval_type` = !!num_missing_toxval_type,
    `Missing Exposure Params` = !!num_missing_exposure_params,
    `Missing RAC` = !!num_missing_rac,
    `Missing Single Param` = !!num_missing_single_param,
    `Missing Study Duration` = !!num_missing_study_duration,
    `Missing Study Type` = !!num_missing_study_type
  )

  # Write reports to Excel
  if(is.null(source_name)) {
    file = paste0(toxval.config()$datapath, "dictionary/missing/",
                  "missing_dict_summary_aggregate_", Sys.Date(), ".xlsx")
  } else {
    file = paste0(toxval.config()$datapath, "dictionary/missing/",
                  "missing_dict_summary_", source_name, "_", Sys.Date(), ".xlsx")
  }
  output = list(report_summary, missing_sources, missing_dictionary_entries, missing_toxval_type,
             missing_exposure_params, missing_rac, missing_single_param,
             missing_study_duration, missing_study_type)
  names(output) = c("report_summary", "missing_sources", "missing_dictionary_entries", "missing_toxval_type",
                    "missing_exposure_params", "missing_rac", "missing_single_param",
                    "missing_study_duration", "missing_study_type")
  writexl::write_xlsx(output, file)

  return(report_summary)
}
