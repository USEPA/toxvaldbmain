#--------------------------------------------------------------------------------------
#'
#' Check for species duplicates - same common name but multiple species_ids
#'
#' @param toxval.db The version of the database to use
#' @param source The source to be checked. If source=NULL, check all sources
#' @param subsource The subsource to be checked (NULL default)
#' @export
#--------------------------------------------------------------------------------------
fix.species.duplicates <- function(toxval.db, source=NULL, subsource=NULL) {
  printCurrentFunction()

  slist = runQuery("select distinct source from toxval",toxval.db)[,1]
  output_file = "Repo/species/duplicate_species_id_ALL SOURCES.xlsx"
  if(!is.null(source)) {
    slist = source
    output_file = paste0("Repo/species/duplicate_species_id", source, ".xlsx")
  }

  slist = slist %>%
    paste0(., collapse="', '")

  # Handle addition of subsource for queries
  query_addition = ""
  if(!is.null(subsource)) {
    query_addition = paste0(" AND a.subsource='", subsource, "'")
  }

  query = paste0("SELECT DISTINCT a.species_original, b.species_id, b.common_name ",
                 "FROM toxval a LEFT JOIN species b ON a.species_id=b.species_id ",
                 "WHERE a.human_eco='human health' AND a.source IN ('", slist, "')",
                 query_addition)
  mat = runQuery(query,toxval.db)

  duplicate_species = mat %>%
    dplyr::distinct() %>%
    dplyr::group_by(species_original) %>%
    dplyr::mutate(n = dplyr::n()) %>%
    dplyr::ungroup() %>%
    dplyr::filter(n > 1) %>%
    dplyr::select(-n)

  cat("Number of species duplicates:",nrow(duplicate_species),"\n")

  if(nrow(duplicate_species)) {
    cat("Duplicate species identified and written to", output_file, "\n")
    writexl::write_xlsx(duplicate_species, output_file)
    browser()
  }
}
