#' @title toxval_relationship.suggestions
#' @description Provide suggestions for linking records to each other.
#' @param toxval.db The version of toxvaldb to use.
#' @param source Source to be fixed
#' @param subsource Subsource to be fixed (NULL default)
toxval_relationship.suggestions <- function(toxval.db, source=NULL, subsource=NULL){

  criteria = c("dtxsid", "toxval_type", "toxval_numeric", "toxval_units")
  criteria_orig = paste0(criteria, "_original") %>%
    gsub("dtxsid_original", "dtxsid", .)
  criteria_string = c(criteria, criteria_orig) %>%
    unique() %>%
    paste0(., collapse=", ")

  slist = runQuery("select distinct source from toxval",toxval.db)[,1]
  if(!is.null(source)) slist = source

  # Handle addition of subsource for queries
  query_addition = ""
  if(!is.null(subsource)) {
    query_addition = paste0(" and subsource='", subsource, "'")
  }

  # Track sources already completed to ensure unnecessary work is not repeated
  already_finished = NULL
  report.out = data.frame()

  priority_list_low_high = toxval.config()$dedup_hierarchy
  priority_list_high_low = setNames(names(priority_list_low_high), priority_list_low_high)

  out = lapply(slist, function(source){
    if(source %in% already_finished) {
      cat("Already handled hierarchical deduping relevant to", source, "\n")
      return(NULL)
    }

    # No changes needed if source is not mentioned in priority list
    if(!(source %in% priority_list_low_high | source %in% priority_list_high_low)) {
      cat("No hierarchical relationship specified for", source, "\n")
      return(NULL)
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
        return(NULL)
      }

      already_finished = c(already_finished, high_priority, low_priority)

      cat("\nHandling dedup hierarchy for", low_priority, "and", high_priority, "\n")

      # Get both high and low priority data using criteria
      low_entries = runQuery(paste0("SELECT DISTINCT source_hash, toxval_id, ", criteria_string,
                                    " FROM toxval WHERE source='", low_priority, "'", query_addition),
                             toxval.db)
      high_entries = runQuery(paste0("SELECT DISTINCT source_hash as source_hash_higher, toxval_id as toxval_id_higher, ", criteria_string,
                                     " FROM toxval WHERE source='", high_priority, "'", query_addition),
                              toxval.db)

      # Identify entries present in both sets of data
      source_ids_to_rel_1 = low_entries %>%
        dplyr::select(dplyr::any_of(c("source_hash", "toxval_id", criteria))) %>%
        dplyr::left_join(high_entries %>%
                           dplyr::select(dplyr::any_of(c("source_hash_higher", "toxval_id_higher", criteria))),
                         by = criteria,
                         relationship = "many-to-many") %>%
        dplyr::filter(!toxval_id_higher %in% c(NA)) %>%
        # dplyr::select(toxval_id, toxval_id_higher) %>%
        # dplyr::distinct() %>%
        dplyr::mutate(match_type = "final")

      source_ids_to_rel_2 = low_entries %>%
        dplyr::select(dplyr::any_of(c("source_hash", "toxval_id", criteria_orig))) %>%
        dplyr::left_join(high_entries %>%
                           dplyr::select(dplyr::any_of(c("source_hash_higher", "toxval_id_higher", criteria_orig))),
                         by = criteria_orig,
                         relationship = "many-to-many") %>%
        dplyr::filter(!toxval_id_higher %in% c(NA)) %>%
        dplyr::mutate(match_type = "original")

      out_s = source_ids_to_rel_1 %>%
        dplyr::bind_rows(source_ids_to_rel_2) %>%
        tidyr::unite(col = "rel_key", toxval_id, toxval_id_higher, sep = "_", remove=FALSE) %>%
        dplyr::mutate(rel_name = paste0(low_priority, "_", high_priority))

    }
  }) %>%
    dplyr::bind_rows()

  return(out)
}
