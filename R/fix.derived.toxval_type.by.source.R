#--------------------------------------------------------------------------------------
#'
#' Set select normalized toxval fields to '-' if the record is a select toxval_type
#'
#' @param toxval.db The version of the database to use
#' @param source The source to be fixed
#' @param subsource The subsource to be fixed (NULL default)
#' @export
#--------------------------------------------------------------------------------------
fix.derived.toxval_type.by.source <- function(toxval.db, source=NULL, subsource=NULL) {

  #####################################################################
  cat("Set select derived toxval_type fields to '-'\n")
  #####################################################################

  # Check fields in toxval table
  nlist = runQuery(paste0("desc toxval"), toxval.db)[,1]

  # Hardcode species as human for RfD, RfD, HED, HED, Slope Factor, Unit Risk
  derived_toxval_type = c("RfD", "unit risk", "RfC", "slope factor")
  blank_hash_cols = c("exposure_method", "exposure_form", "media",
                      "generation", "lifestage", "population",
                      "study_duration_qualifier", "study_duration_value", "study_duration_units",
                      "sex", "strain") %>%
    # Filter to only those present in toxval table
    .[. %in% nlist]

  filter_fields = c("toxval_type", "toxval_type_original")

  toxval_type_filter = paste(rep(filter_fields,
                                  each = length(derived_toxval_type)),
                              derived_toxval_type,
                              sep = " like '%",
                              collapse = "%' or ") %>%
    paste0("(", . ,"%')")

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

    query = paste0("select ", toString(blank_hash_cols), " ",
                   "from toxval ",
                   "where source ='",source,"'",query_addition,
                   " and ", toxval_type_filter) %>%
      stringr::str_squish()

    t1 = runQuery(query, toxval.db)
  }
}
