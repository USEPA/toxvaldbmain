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
  runQuery(paste0("UPDATE toxval SET experimental_record = 'not experimental' ",
                  "WHERE experimental_record in ('No', 'epidemiological')"),
           toxval.db)
  runQuery(paste0("UPDATE toxval SET experimental_record = 'experimental' ",
                  "WHERE experimental_record in ('Yes', 'clinical')"),
           toxval.db)
  runQuery(paste0("UPDATE toxval SET experimental_record = 'undetermined' ",
                  "WHERE experimental_record in ('Undetermined', '-', 'not determined', '?', ",
                  "'Unable to identify a specific study', 'other', 'unclear'",
                  ")"),
           toxval.db)
  runQuery(paste0("UPDATE toxval SET experimental_record = 'undetermined' ",
                  "WHERE experimental_record like '%unknown%'"),
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

  # Bulk assignment based on source
  runQuery(paste0("UPDATE toxval SET experimental_record='experimental' ",
                  "WHERE source IN ('HAWC Project', 'HAWC PFAS 150', 'HAWC PFAS 430', 'EPA HAWC')"),
           toxval.db)

  # Update WHO JECFA Tox Studies experimental_record using hash file
  if(source == "WHO JECFA Tox Studies"){
    file = paste0(toxval.config()$datapath, "who_jecfa_tox_studies/who_jecfa_tox_studies_files/WHO JECFA tox_not experimental.xlsx")
    hashes = readxl::read_xlsx(file) %>%
      tidyr::separate_rows(source_hash, sep=",") %>%
      dplyr::pull(source_hash) %>%
      unique() %>%
      paste0(collapse="', '")

    query = paste0("UPDATE toxval SET experimental_record='not experimental' ",
                   "WHERE source='WHO JECFA Tox Studies' ",
                   "AND source_hash IN ('", hashes, "')")
    runQuery(query, toxval.db)
  }

  # Set as "not experimental" for toxval_type_supercategories of "toxicity value" and "exposure guidelines"
  tts_tox_id = runQuery(
    paste0("select a.toxval_id ",
           "from toxval a ",
           "left join toxval_type_dictionary b ",
           "on a.toxval_type = b.toxval_type ",
           "where source = '", source, "' and (",
           "b.toxval_type_supercategory = 'Toxicity Value' or ",
           "b.toxval_type_supercategory like '%Exposure Guidelines%')"),
    toxval.db
  ) %>%
    dplyr::pull(toxval_id)

  if(length(tts_tox_id)){
    runQuery(
      paste0(
        "UPDATE toxval SET experimental_record = 'not experimental' ",
        "WHERE toxval_id in (", toString(tts_tox_id), ")"),
      toxval.db)
  }
}
