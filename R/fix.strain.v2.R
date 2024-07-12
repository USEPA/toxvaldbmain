#--------------------------------------------------------------------------------------
#'
#' Set the strain information in toxval
#'
#' @param toxval.db The version of the database to use
#' @param source The source to be fixed. If NULL, fix for all sources
#' @param subsource The subsource to be fixed (NULL default)
#' @param date_string The date of the latest dictionary version
#' @export
#--------------------------------------------------------------------------------------
fix.strain.v2 <- function(toxval.db, source=NULL, subsource=NULL, date_string="2024-04-08", reset=FALSE) {
  printCurrentFunction()

  file = paste0(toxval.config()$datapath,"species/strain_dictionary_",date_string,".xlsx")
  dict = openxlsx::read.xlsx(file)
  dict = dplyr::distinct(dict)
  dict$common_name = tolower(dict$common_name)
  if(is.null(source)) {
    slist = runQuery("select distinct source from toxval",toxval.db)[,1]
  } else {
    slist = source
  }

  # Handle addition of subsource for queries
  query_addition = ""
  if(!is.null(subsource)) {
    query_addition = paste0(" and subsource='", subsource, "'")
  }

  for(source in slist) {
    if(reset) {
      runQuery(paste0("update toxval set strain='-', strain_group='-' ",
                      "WHERE source='", source, "'", query_addition),
               toxval.db)
    }

    cat("fix strain:", source, subsource, "\n")
    query = paste0("select DISTINCT a.species_original, a.strain_original, b.species_id ",
                   "from toxval a, species b ",
                   "where a.species_id=b.species_id ",
                   # "and a.human_eco='human health' ",
                   "and a.source='", source, "'", query_addition)
    # Pull species-strain entries for source
    t1 = runQuery(query,toxval.db) %>%
      dplyr::mutate(strain_original = strain_original %>%
                      stringr::str_replace_all(.,"\\'",""))

    # Join to dictionary by original species and strain
    mapped_strain = t1 %>%
      dplyr::left_join(dict,
                       by=c("species_original", "strain_original")) %>%
      dplyr::distinct()

    # Filter to missing
    missing = mapped_strain %>%
      dplyr::filter(is.na(strain)) %>%
      dplyr::select(strain_original, strain, strain_group, species_original, common_name)

    # Export for dictionary curation if any missing
    if(nrow(missing)){
      writexl::write_xlsx(missing, file.path(toxval.config()$datapath,"dictionary/missing/missing_strain_mapping.xlsx"))
    }

    # Filter out missing
    mapped_strain = mapped_strain %>%
      dplyr::filter(!is.na(strain)) %>%
      dplyr::select(-common_name)

    # Check for duplicate mappings - if species_original and strain_original ever map to different strain and strain group
    dup_check = mapped_strain %>%
      dplyr::select(-species_id) %>%
      dplyr::distinct() %>%
      dplyr::group_by(species_original, strain_original) %>%
      dplyr::summarise(n=dplyr::n()) %>%
      dplyr::filter(n>1)

    if(nrow(dup_check)){
      message("Multiple strain mappings identified for matching species_id...how to proceed?")
      browser()
    }

    # Batch update strain
    batch_size <- 50000
    startPosition <- 1
    endPosition <- nrow(mapped_strain)
    incrementPosition <- batch_size

    while(startPosition <= endPosition){
      if(incrementPosition > endPosition) incrementPosition = endPosition
      message("...Inserting new data in batch: ", batch_size, " startPosition: ", startPosition," : incrementPosition: ", incrementPosition,
              " (",round((incrementPosition/endPosition)*100, 3), "%)", " at: ", Sys.time())

      update_query <- paste0("UPDATE toxval a ",
                             "INNER JOIN z_updated_df b ",
                             "ON (a.species_id = b.species_id AND ",
                             "a.species_original = b.species_original AND ",
                             "a.strain_original = b.strain_original) ",
                             "SET a.strain = b.strain, ",
                             "a.strain_group = b.strain_group ",
                             "WHERE a.source = '", source, "'",
                             query_addition)

      runUpdate(table="toxval",
                updateQuery = update_query,
                updated_df = mapped_strain[startPosition:incrementPosition,],
                db=toxval.db)

      startPosition <- startPosition + batch_size
      incrementPosition <- startPosition + batch_size - 1
    }

    # Generic strain fix
    cat("Handle quotes in strains\n")
    runQuery(paste0("update toxval SET strain"," = ", "REPLACE", "( strain",  ",\'\"\',", " \"'\" ) WHERE strain"," LIKE \'%\"%\' and source = '",source,"'",query_addition),toxval.db)
  }
}
