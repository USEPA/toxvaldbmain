#--------------------------------------------------------------------------------------
#' Generic function for setting record relationships based on standardized rules
#'
#' @param toxval.db The version of toxval into which the tables are loaded.
#' @param res The data that has relationships to be represented
#' @export
#--------------------------------------------------------------------------------------
set_toxval_relationship_by_toxval_type <- function(res, toxval.db){
  res1 <- res %>%
    # Split deduped records
    tidyr::separate_rows(study_reference, document_type, sep=" \\|::\\| ") %>%

    dplyr::mutate(
      # Standardize study_reference to improve groupby (comma/period usage interchanged, parts of studies)
      study_reference = study_reference %>%
        gsub("\\.|,", "", .) %>%
        gsub("a$|b$|c$|d$", "", .) %>%
        stringr::str_squish()
    ) %>%

    # Get summary data
    dplyr::filter(grepl("Summary|Toxicological", document_type),
                  !study_reference %in% c("-")) %>%
    dplyr::mutate(preceding_text = toxval_type %>%
                    gsub("\\s*\\(.*", "", .) %>%
                    stringr::str_squish()
                  ) %>%
    dplyr::select(toxval_id, toxval_type, study_reference, study_type, exposure_route, preceding_text)

  # Identify and capture ex. NOAEL (HEC) -> NOAEL (ADJ) type relationship
  relationships_adj_hec <- tryCatch({
    res1 %>%
      dplyr::group_by(study_reference, preceding_text) %>%
      dplyr::filter(
        any(grepl("\\(ADJ\\)", toxval_type, ignore.case=TRUE)) & any(grepl("\\(HEC\\)", toxval_type, ignore.case=TRUE))
      ) %>%
      dplyr::reframe(
        toxval_id_1 = toxval_id[grepl("\\(ADJ\\)", toxval_type, ignore.case=TRUE)],
        toxval_id_2 = toxval_id[grepl("\\(HEC\\)", toxval_type, ignore.case=TRUE)],
        toxval_type_1 = toxval_type[grepl("\\(ADJ\\)", toxval_type, ignore.case=TRUE)],
        toxval_type_2 = toxval_type[grepl("\\(HEC\\)", toxval_type, ignore.case=TRUE)],
        relationship = "derived from"
      ) %>%
      dplyr::filter(!is.na(toxval_id_1) & !is.na(toxval_id_2))
  },
  error = function(e){
    cat("Error with relationships_adj_hec\n")
    message(e)
    return(data.frame())
  })

  # Identify and capture ex. NOAEL (ADJ) -> NOAEL type relationship
  relationships_adj_base <- tryCatch({
    res1 %>%
      dplyr::group_by(study_reference, preceding_text) %>%
      dplyr::filter(
        any(grepl("\\(ADJ\\)", toxval_type, ignore.case=TRUE)) & any(!grepl("\\(HEC\\)|\\(ADJ\\)", toxval_type, ignore.case=TRUE))
      ) %>%
      dplyr::reframe(
        toxval_id_1 = toxval_id[!grepl("\\(ADJ\\)|\\(HEC\\)", toxval_type, ignore.case=TRUE)],
        toxval_id_2 = toxval_id[grepl("\\(ADJ\\)", toxval_type, ignore.case=TRUE)],
        toxval_type_1 = toxval_type[!grepl("\\(ADJ\\)|\\(HEC\\)", toxval_type, ignore.case=TRUE)],
        toxval_type_2 = toxval_type[grepl("\\(ADJ\\)", toxval_type, ignore.case=TRUE)],
        relationship = "derived from"
      ) %>%
      dplyr::filter(!is.na(toxval_id_1) & !is.na(toxval_id_2))
  },
  error = function(e){
    cat("Error with relationships_adj_base\n")
    message(e)
    return(data.frame())
  })

  # Identify and capture ex. NOAEL (HEC)/NOAEL (HED) -> NOAEL type relationship
  relationship_hec_base <- tryCatch({
    res1 %>%
      # Keep only HEC/HED entries and entries without other parenthetic stems
      dplyr::filter(grepl("\\(HEC\\)|\\(HED\\)", toxval_type, ignore.case=TRUE) | !grepl("\\(", toxval_type)) %>%
      dplyr::group_by(study_reference, study_type, exposure_route, preceding_text) %>%
      dplyr::filter(
        any(grepl("\\(HEC\\)|\\(HED\\)", toxval_type, ignore.case=TRUE)) & any(!grepl("\\(HEC\\)|\\(HED\\)", toxval_type, ignore.case=TRUE))
      ) %>%
      dplyr::reframe(
        toxval_id_1 = toxval_id[!grepl("\\(HED\\)|\\(HEC\\)", toxval_type, ignore.case=TRUE)],
        toxval_id_2 = toxval_id[grepl("\\(HED\\)|\\(HEC\\)", toxval_type, ignore.case=TRUE)],
        toxval_type_1 = toxval_type[!grepl("\\(HED\\)|\\(HEC\\)", toxval_type, ignore.case=TRUE)],
        toxval_type_2 = toxval_type[grepl("\\(HED\\)|\\(HEC\\)", toxval_type, ignore.case=TRUE)],
        relationship = "derived from"
      ) %>%
      dplyr::filter(!is.na(toxval_id_1) & !is.na(toxval_id_2) & !(toxval_id_2 %in% relationships_adj_base$toxval_id_2))
  },
  error = function(e){
    cat("Error with relationship_hec_base\n")
    message(e)
    return(data.frame())
  })

  # Combine relationships before insertion
  all_relationships <- dplyr::bind_rows(relationships_adj_hec, relationships_adj_base, relationship_hec_base)

  all_relationships <- all_relationships %>%
    dplyr::ungroup() %>%
    dplyr::select(toxval_id_1, toxval_id_2, relationship)

  # Insert into toxval_relationship
  if(nrow(all_relationships)){
    runInsertTable(mat=all_relationships, table='toxval_relationship', db=toxval.db)
  }
}
