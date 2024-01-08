#--------------------------------------------------------------------------------------
#' Generic function for setting record relationships based on standardized rules
#'
#' @param toxval.db The version of toxval into which the tables are loaded.
#' @param res The data that has relationships to be represented
#' @export
#--------------------------------------------------------------------------------------
set_toxval_relationship_by_toxval_type <- function(res, toxval.db){
  res1 <- res %>%
    filter(grepl("Summary", document_type))
  res1$preceding_text <- gsub("\\s*\\(.*", "", res1$toxval_type)

  # Identify and capture ex. NOAEL (HEC) -> NOAEL (ADJ) type relationship
  relationships_adj_hec <- res1 %>%
    group_by(study_reference, preceding_text) %>%
    filter(
      any(grepl("\\(ADJ\\)", toxval_type)) & any(grepl("\\(HEC\\)", toxval_type))
    ) %>%
    summarize(
      source_hash_1 = source_hash[grepl("\\(ADJ\\)", toxval_type)],
      source_hash_2 = source_hash[grepl("\\(HEC\\)", toxval_type)],
      toxval_type_1 = toxval_type[grepl("\\(ADJ\\)", toxval_type)],
      toxval_type_2 = toxval_type[grepl("\\(HEC\\)", toxval_type)],
      relationship = "derived from"
    ) %>%
    filter(!is.na(source_hash_1) & !is.na(source_hash_2))

  # Identify and capture ex. NOAEL (ADJ) -> NOAEL type relationship
  relationships_adj_base <- res1 %>%
    group_by(study_reference, preceding_text) %>%
    filter(
      any(grepl("\\(ADJ\\)", toxval_type)) & any(!grepl("\\(HEC\\)|\\(ADJ\\)", toxval_type))
    ) %>%
    summarize(
      source_hash_1 = source_hash[!grepl("\\(ADJ\\)|\\(HEC\\)", toxval_type)],
      source_hash_2 = source_hash[grepl("\\(ADJ\\)", toxval_type)],
      toxval_type_1 = toxval_type[!grepl("\\(ADJ\\)|\\(HEC\\)", toxval_type)],
      toxval_type_2 = toxval_type[grepl("\\(ADJ\\)", toxval_type)],
      relationship = "derived from"
    ) %>%
    filter(!is.na(source_hash_1) & !is.na(source_hash_2))

  # Combine relationships before insertion
  all_relationships <- rbind(relationships_adj_hec, relationships_adj_base)
  all_relationships <- all_relationships %>%
    ungroup() %>%
    select(source_hash_1, source_hash_2, relationship)

  # Insert into toxval_relationship
  if(nrow(all_relationships)){
    runInsertTable(mat=all_relationships, table='toxval_relationship', db='res_toxval_v95')
  }

}
