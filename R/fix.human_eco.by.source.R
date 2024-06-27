#-------------------------------------------------------------------------------------
#' Fix the human_eco flag
#' @param toxval.db The version of toxval in which the data is altered.
#' @param source The source to be fixed. If NULL, fix all sources
#' @param subsource The subsource to be fixed (NULL default)
#' @return The database will be altered
#' @export
#--------------------------------------------------------------------------------------
fix.human_eco.by.source <- function(toxval.db, source=NULL, subsource=NULL){
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
    "EnviroTox_v2",
    "EPA OW OPP-ALB",
    "EPA OW NRWQC-ALC"
  )

  # Set human_eco values according to eco_list and special cases
  query = paste0(
    "UPDATE toxval SET human_eco = CASE ",
    "WHEN source='EFSA' AND study_type='ecotoxicity' THEN 'eco' ",
    "WHEN source='DOE Wildlife Benchmarks' AND",
    " (species_original IN ('rat', 'mouse', 'rhesus macaque', 'dog', 'guinea pig', 'hamster') OR",
    " species_id IN (SELECT species_id FROM species WHERE common_name IN ('Rat', 'Mouse', 'Rhesus Macaque', 'Dog', 'Guinea Pig', 'Hamster')))",
    " THEN 'human_health' ",
    "WHEN source='DOE Wildlife Benchmarks' AND experimental_record='not experimental' THEN 'eco' ",
    "WHEN source IN ('", paste0(eco_list, collapse="', '"), "') THEN 'eco' ",
    "ELSE 'human health' END ",
    "WHERE source IN ('", paste0(slist, collapse="', '"), "')",
    query_addition
  )
  runQuery(query, toxval.db)
}

