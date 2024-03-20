#--------------------------------------------------------------------------------------
#' @title toxval.source.import.dedup
#' @description Perform deduping on data before it is sent to toxval_source
#' @param res dataframe containing the source data to dedup
#' @param dedup_fields vector containing field names to dedup, Default: NULL (all fields but hashing cols)
#' @param hashing_cols vector containing field names of hashing columns, Default: toxval.config()$hashing_cols
#' @param delim string used to separate collapsed values, Default: ' |::| '
#' @return dataframe containing deduped source data
#' @details DETAILS
#' @examples
#' \dontrun{
#' if(interactive()){
#'  #EXAMPLE1
#'  }
#' }
#' @seealso
#'  \code{\link[dplyr]{select}}, \code{\link[dplyr]{group_by}}, \code{\link[dplyr]{summarise}}, \code{\link[dplyr]{context}}, \code{\link[dplyr]{filter}}, \code{\link[dplyr]{mutate}}, \code{\link[dplyr]{across}}, \code{\link[dplyr]{reexports}}, \code{\link[dplyr]{na_if}}, \code{\link[dplyr]{distinct}}
#' @rdname toxval.load.dedup
#' @export
#' @importFrom dplyr select group_by summarise n filter mutate across any_of na_if ungroup distinct
#--------------------------------------------------------------------------------------
toxval.load.dedup <- function(res, dedup_fields=NULL, hashing_cols=NULL, delim=" |::| ") {
  cat("Deduping data\n")

  # If no hashing_cols provided, use source toxval.config()$hashing_cols without study_duration_qualifier/long_ref
  # Reasoning: study_duration_qualifier and long_ref are not stored in ToxVal
  if(is.null(hashing_cols)) {
    hashing_cols = c('name', 'casrn', 'toxval_type', 'toxval_subtype', 'toxval_numeric', 'toxval_units',
                     'toxval_numeric_qualifier', 'study_type', 'study_duration_class',
                     'study_duration_value', 'study_duration_units',
                     'species', 'strain', 'sex', 'critical_effect', 'population',
                     'exposure_route', 'exposure_method', 'exposure_form', 'media',
                     'lifestage', 'generation', 'year')
  }

  # If no dedup fields provided, set dedup_fields to be all cols but source_hash and hashing_cols
  if(is.null(dedup_fields)) {
    dedup_fields = names(res %>% dplyr::select(-dplyr::any_of(c("source_hash", "toxval_id", hashing_cols))))
  }

  # Add source_hash_temp column
  cat("Using vectorized hashing! \n")
  res.temp = res %>%
    tidyr::unite(hash_col, all_of(sort(names(.)[names(.) %in% hashing_cols])), sep="") %>%
    dplyr::rowwise() %>%
    dplyr::mutate(source_hash = paste0("ToxValhc_", digest(hash_col, serialize = FALSE))) %>%
    dplyr::ungroup()
  res$source_hash_temp = res.temp$source_hash

  # Check for immediate duplicate hashes
  dup_hashes = res %>%
    dplyr::group_by(source_hash_temp) %>%
    dplyr::summarise(n = dplyr::n()) %>%
    dplyr::filter(n > 1)

  # Perform deduping only if there are duplicate entries
  if(nrow(dup_hashes)) {
    cat(paste0("Duplicate records identified (", sum(dup_hashes$n) - nrow(dup_hashes), " records are duplicates)\n"))
    cat("Performing deduping...\n")
    # Dedup by collapsing non hashing columns to dedup
    res = res %>%
      dplyr::group_by(source_hash_temp) %>%
      dplyr::mutate(dplyr::across(dplyr::any_of(!!dedup_fields),
                                  ~paste0(.[!is.na(.)], collapse=!!delim) %>%
                                    dplyr::na_if("NA") %>%
                                    dplyr::na_if("") %>%
                                    dplyr::na_if("-")
      )) %>%
      dplyr::ungroup() %>%
      dplyr::distinct()

    # Check if success
    dup_hashes = res %>%
      dplyr::group_by(source_hash_temp) %>%
      dplyr::summarise(n = dplyr::n()) %>%
      dplyr::filter(n > 1)

    # Remove source_hash_temp field if it was not already present in data
    res = res %>% dplyr::select(-source_hash_temp)

    if(nrow(dup_hashes)) {
      cat("Deduping failed. Duplicate records still present.\n")
      browser()
    } else {
      cat("Deduping was successful. Returning...\n")
    }
  } else {
    cat("No duplicate records found.\n")
  }

  return(res)
}




