#-------------------------------------------------------------------------------------
#' Fix the human_eco flag
#' @param toxval.db The version of toxval in which the data is altered.
#' @param source The source to be fixed. If NULL, fix all sources
#' @param subsource The subsource to be fixed (NULL default)
#' @return The database will be altered
#' @export
#--------------------------------------------------------------------------------------
fix.human_eco.by.source <- function(toxval.db,source=NULL,subsource=NULL){
  printCurrentFunction(paste(toxval.db,":", source,subsource))

  # Fix all sources if source not specified
  slist = source
  if(is.null(source)) slist = runQuery("select distinct source from toxval",toxval.db)[,1]

  # Handle addition of subsource for queries
  query_addition = ""
  if(!is.null(subsource)) {
    query_addition = paste0(" and subsource='", subsource, "'")
  }

  # Initialize list of eco sources
  eco_list = c(
    "DOD ERED",
    "DOE LANL ECORISK",
    "DOE Protective Action Criteria",
    "DOE Wildlife Benchmarks",
    "EnviroTox_v2"
  ) %>%
    paste0("'", ., "'") %>%
    toString()

  for(source in slist) {
    # Set human_eco values according to eco_list and special EFSA case
    query = paste0(
      "UPDATE toxval SET human_eco = CASE ",
      "WHEN source='EFSA' AND study_type='ecotoxicity' THEN 'eco' ",
      "WHEN source='EFSA' AND study_type!='ecotoxicity' THEN 'human health ",
      "WHEN source IN (", eco_list, ") THEN 'eco' ",
      "ELSE 'human health' END ",
      "WHERE source = '", source, "'",
      query_addition
    )
    runQuery(query)
  }
}

