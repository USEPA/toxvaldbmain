#-------------------------------------------------------------------------------------
#' Normalize study_type within study_group
#' @param toxval.db The version of toxvaldb to use.
#' @param source Source to check
#' @param subsource Subsource to check (NULL default)
#' @param report.only Whether to alter ToxVal (FALSE) or simply record suggestions (TRUE), default TRUE
#' @param filter.matching Whether to ignore entries whose current study_type matches suggestion, default FALSE
#' @export
#-------------------------------------------------------------------------------------
set.study_type.by.study_group <- function(toxval.db,
                                          source=NULL,
                                          subsource=NULL,
                                          report.only=TRUE,
                                          filter.matching=FALSE) {
  printCurrentFunction(paste(toxval.db,":", source))

  # Initialize values for slist and output_file to match all sources
  slist = runQuery("select distinct source from toxval",toxval.db)[,1]
  output_file = paste0("Repo/QC Reports/set.study_type.by.study_group_suggestions_ALL SOURCES_",
                       Sys.Date(), ".xlsx")

  # Alter slist and output_file if source is specified
  if(!is.null(source)) {
    slist = source
    output_file = paste0("Repo/QC Reports/set.study_type.by.study_group_suggestions_",
                         source, "_", Sys.Date(), ".xlsx")
  }

  # Handle addition of subsource for queries and output_file
  query_addition = ""
  if(!is.null(subsource)) {
    query_addition = paste0(" and subsource='", subsource, "'")
    output_file = paste0("Repo/QC Reports/set.study_type.by.study_group_suggestions",
                         source, "_", subsource, "_", Sys.Date(), ".xlsx")
  }

  # Create recommendation mapping tibble from all ToxVal sources
  study_type_recs = runQuery("SELECT study_type, study_group FROM toxval", toxval.db) %>%
    # Get mapping that occurs most frequently
    dplyr::group_by(study_type, study_group) %>%
    dplyr::mutate(num_occurrences = dplyr::n()) %>%
    dplyr::ungroup() %>%
    dplyr::distinct() %>%
    dplyr::group_by(study_group) %>%
    dplyr::slice_max(n=1, order_by=num_occurrences) %>%
    # Do not record ties
    dplyr::mutate(num_mappings = dplyr::n()) %>%
    dplyr::ungroup() %>%
    dplyr::filter(num_mappings == 1) %>%
    dplyr::select(-c(num_occurrences, num_mappings)) %>%
    dplyr::rename(study_type_suggestion = study_type)

  # Track all identified study_type suggestions
  all_suggestions = tibble::tibble(
    source = character(),
    study_group = character(),
    current_study_type = character(),
    study_type_suggestion = character()
  )

  for(source in slist) {
    # Get study_type/study_group pairings for current source
    curr_mapping_query = paste0("SELECT DISTINCT source, study_type, study_group FROM toxval WHERE ",
                                "source='", source, "'", query_addition)
    curr_mappings = runQuery(curr_mapping_query, toxval.db)

    # Map recommendations to current source data
    all_suggestions = curr_mappings %>%
      dplyr::rename(current_study_type = study_type) %>%
      dplyr::left_join(study_type_recs, by=c("study_group")) %>%
      dplyr::bind_rows(all_suggestions) %>%
      dplyr::distinct()

    # Filter out entries that already match suggestion if specified
    if(filter.matching) {
      all_suggestions = all_suggestions %>%
        dplyr::filter(current_study_type != study_type_suggestion)
    }

    if(!report.only) {
      # TODO: add logic to overwrite study_type values
      cat("Logic to overwrite study_type not yet implemented\n")
    }
  }

  # Write combinations to output file
  if(report.only) {
    if(nrow(all_suggestions) > 0) {
      writexl::write_xlsx(all_suggestions, output_file)
      cat("study_type suggestions recorded in", output_file, "\n")
    } else {
      cat("No study_type suggestions to report\n")
    }
  }
}
