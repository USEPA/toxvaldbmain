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
  tidyr::separate_rows(study_reference,
                       document_type, sep=" \\|::\\| ") %>%
    dplyr::mutate(
      # Clean toxval_subtype
      toxval_subtype = toxval_subtype %>%
        stringr::str_squish()

      # Standardize study_reference to improve groupby (comma/period usage interchanged, parts of studies)
      study_reference = study_reference %>%
        gsub("\\.|,", "", .) %>%
        gsub("a$|b$|c$|d$", "", .) %>%
        stringr::str_squish()
    ) %>%

    # Get summary data
    dplyr::filter(grepl("Summary|Toxicological", document_type),
                  !study_reference %in% c("-")) %>%
    # Select all columns needed for setting relationships
    dplyr::select(toxval_id, toxval_type, study_reference, study_type, exposure_route)

  # Identify and capture ex. NOAEL (HEC) -> NOAEL (ADJ) type relationship
  relationships_adj_hec <- res1 %>%
    dplyr::group_by(study_reference, toxval_type) %>%
    dplyr::reframe(
      toxval_id_1 = ifelse("ADJ" %in% toxval_subtype & "HEC" %in% toxval_subtype,
                            toxval_id[which(toxval_subtype == "ADJ")], NA),
      toxval_id_2 = ifelse("ADJ" %in% toxval_subtype & "HEC" %in% toxval_subtype,
                           toxval_id[which(toxval_subtype == "HEC")], NA),
      relationship = ifelse("ADJ" %in% toxval_subtype & "HEC" %in% toxval_subtype,
                             "derived from", NA)) %>%
    ungroup() %>%
    dplyr::filter(!is.na(toxval_id_1) & !is.na(toxval_id_2))

  # Identify and capture ex. NOAEL (ADJ) -> NOAEL type relationship
  relationships_adj_base <- res1 %>%
    dplyr::group_by(study_reference, toxval_type) %>%
    dplyr::reframe(
      toxval_id_1 = ifelse("ADJ" %in% toxval_subtype & is.na(toxval_subtype),
                           toxval_id[which(toxval_subtype == "ADJ")], NA),
      toxval_id_2 = ifelse("ADJ" %in% toxval_subtype & is.na(toxval_subtype),
                           toxval_id[which(is.na(toxval_subtype))], NA),
      relationship = ifelse("ADJ" %in% toxval_subtype & is.na(toxval_subtype),
                            "derived from", NA)) %>%
    ungroup() %>%
    dplyr::filter(!is.na(toxval_id_1) & !is.na(toxval_id_2))

  # Identify and capture ex. NOAEL (HEC)/NOAEL (HED) -> NOAEL type relationship
  relationship_hec_base <- res1 %>%
    dplyr::group_by(study_reference, toxval_type) %>%
    dplyr::reframe(
      toxval_id_1 = ifelse(("HED" %in% toxval_subtype | "HEC" %in% toxval_subtype) & is.na(toxval_subtype),
                           toxval_id[which(toxval_subtype == "HED" | toxval_subtype == "HEC")], NA),
      toxval_id_2 = ifelse(("HED" %in% toxval_subtype | "HEC" %in% toxval_subtype) & is.na(toxval_subtype),
                           toxval_id[which(is.na(toxval_subtype))], NA),
      relationship = ifelse(("HED" %in% toxval_subtype | "HEC" %in% toxval_subtype) & is.na(toxval_subtype),
                            "derived from", NA)) %>%
    ungroup() %>%
    dplyr::filter(!is.na(toxval_id_1) & !is.na(toxval_id_2) & !(toxval_id_2 %in% relationships_adj_base$toxval_id_2))

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
