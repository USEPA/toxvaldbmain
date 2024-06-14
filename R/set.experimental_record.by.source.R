#-------------------------------------------------------------------------------------
#' Sets experimental_record flag by source for records in toxval
#' @param toxval.db The version of toxval into which the tables are loaded.
#' @param source Name of source to set. Default NULL means set experimental record for all sources
#' @return None. SQL update statement is performed
#' @export
#-------------------------------------------------------------------------------------
set.experimental_record.by.source <- function(toxval.db, source=NULL){
  printCurrentFunction(toxval.db)
  if(!is.null(source)) {
    slist = source
  } else {
    slist = runQuery("select distinct source from toxval",toxval.db)[,1]
  }

  # Input dictionary of experimental_record designations
  file = paste0(toxval.config()$datapath,"dictionary/toxval_source_type_species_20240225.xlsx")
  exp_rec_tag = readxl::read_xlsx(file) %>%
    dplyr::filter(!experimental %in% c(NA, "-", "unsure")) %>%
    dplyr::mutate(experimental = dplyr::case_when(
      experimental == 0 ~ "not experimental",
      experimental == 1 ~ "experimental",
      TRUE ~ experimental
    )) %>%
    dplyr::select(source, source_url, toxval_type, species_original, new_experimental_tag = experimental)

  # Normalized experimental record entries
  runQuery("UPDATE toxval SET experimental_record = 'not experimental' WHERE experimental_record = 'No'",
           toxval.db)
  runQuery("UPDATE toxval SET experimental_record = 'experimental' WHERE experimental_record = 'Yes'",
           toxval.db)
  runQuery("UPDATE toxval SET experimental_record = 'undetermined' WHERE experimental_record = 'Undetermined'",
           toxval.db)
  runQuery("UPDATE toxval SET experimental_record = 'undetermined' WHERE experimental_record = '-'",
           toxval.db)

  # Pull records to assign
  subset_data <- runQuery(paste0("SELECT toxval_id, source, toxval_type, species_original, source_url, experimental_record
                                 FROM toxval
                                 WHERE source IN ('", paste(slist, collapse="','"), "')
                                 AND (experimental_record in ('-', 'undetermined') OR experimental_record IS NULL)"),
                          toxval.db)

  res = subset_data %>%
    # Join to dictionary
    dplyr::left_join(exp_rec_tag,
                     by=c("source", "source_url", "toxval_type", "species_original")) %>%
    dplyr::mutate(new_experimental_tag = new_experimental_tag %>%
                    tidyr::replace_na("undetermined")) %>%
    # Source specific designations
    dplyr::mutate(new_experimental_tag = dplyr::case_when(
      source %in% c("Copper Manufacturers",
                    "ECOTOX",
                    "ECHA IUCLID",
                    "ToxRefDB") ~ "experimental",
      TRUE ~ new_experimental_tag
    )) %>%
    # Filter to differing flag assignments
    dplyr::select(toxval_id, source, experimental_record, new_experimental_tag) %>%
    dplyr::mutate(compare = experimental_record != new_experimental_tag) %>%
    dplyr::filter(compare == TRUE) %>%
    dplyr::select(-compare)

  res %>%
    dplyr::group_by(source, experimental_record, new_experimental_tag) %>%
    dplyr::summarise(n=dplyr::n()) %>%
    dplyr::ungroup() %>%
    View()

  # User review comparison and decide if changes are acceptable
  # browser()

  # Accept new assignments
  res = res %>%
    dplyr::select(toxval_id, experimental_record = new_experimental_tag)

  if(nrow(res)){
    # Query to inner join and update toxval with experimental_record tag (temp table added/dropped)
    updateQuery = paste0("UPDATE toxval a INNER JOIN z_updated_df b ",
                         "ON (a.toxval_id = b.toxval_id) ",
                         "SET a.experimental_record = b.experimental_record")
    # Run update query
    runUpdate(table="toxval", updateQuery=updateQuery, updated_df=res, db=toxval.db)
  }

  # Bulk assign based on study_type
  runQuery("UPDATE toxval SET experimental_record = 'not experimental' WHERE study_type in ('epidemiologic', 'occupational', 'toxicity value')",
           toxval.db)
}
