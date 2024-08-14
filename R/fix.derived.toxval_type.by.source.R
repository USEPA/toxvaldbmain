#--------------------------------------------------------------------------------------
#'
#' Set select normalized toxval fields to '-' if the record is a select toxval_type
#'
#' @param toxval.db The version of the database to use
#' @param source The source to be fixed
#' @param subsource The subsource to be fixed (NULL default)
#' @param report.only Whether to update database or only report the results. Default FALSE.
#' @export
#--------------------------------------------------------------------------------------
fix.derived.toxval_type.by.source <- function(toxval.db, source=NULL, subsource=NULL, report.only=FALSE) {

  #####################################################################
  cat("Set select derived toxval_type fields to '-'\n")
  #####################################################################

  # Hardcode species as human for RfD, RfD, HED, HED, Slope Factor, Unit Risk
  derived_toxval_type = c("RfD", "unit risk", "RfC", "slope factor", "MRL")
  blank_hash_cols = c("exposure_method", "exposure_form", "media",
                      "generation", "lifestage", "population",
                      "study_duration_qualifier", "study_duration_value", "study_duration_units",
                      "sex", "strain") %>%
    c(., paste0(., "_original"))

  # Check fields in toxval table
  toxval_tbl = runQuery(paste0("SELECT COLUMN_NAME, COLUMN_DEFAULT, DATA_TYPE ",
                               "FROM INFORMATION_SCHEMA.COLUMNS WHERE table_name = 'toxval' ",
                               "AND TABLE_SCHEMA = '", toxval.db,"'"), toxval.db) %>%
    dplyr::mutate(COLUMN_DEFAULT = dplyr::case_when(
      is.na(COLUMN_DEFAULT) & DATA_TYPE %in% c("float", "int") ~ "-999",
      TRUE ~ paste0("'", COLUMN_DEFAULT, "'")
    )) %>%
    # Filter to only those present in toxval table
    dplyr::filter(COLUMN_NAME %in% blank_hash_cols)

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

  out_report = data.frame()

  for(source in slist) {
    cat("fix.derived.toxval_type.by.source: ", source, "\n")
    query = paste0("select distinct toxval_id, source_hash, source, toxval_type_original, toxval_type, ",
                   toString(toxval_tbl$COLUMN_NAME), " ",
                   "from toxval ",
                   "where source ='",source,"'",query_addition,
                   " and ", toxval_type_filter) %>%
      stringr::str_squish()
    # Query matches, only use toxval_id except for troubleshooting
    toxval_id_list = runQuery(query, toxval.db)

    if(report.only){
      out_report = out_report %>%
        dplyr::bind_rows(toxval_id_list)
      # If report only, skip update
      next
    } else {
      toxval_id_list = toxval_id_list %>%
        dplyr::pull(toxval_id)
    }

    # Batch update
    batch_size <- 20000
    startPosition <- 1
    endPosition <- length(toxval_id_list)
    incrementPosition <- batch_size

    while(startPosition <= endPosition){
      if(incrementPosition > endPosition) incrementPosition = endPosition
      message("...Inserting new data in batch: ", batch_size, " startPosition: ", startPosition," : incrementPosition: ", incrementPosition,
              " (",round((incrementPosition/endPosition)*100, 3), "%)", " at: ", Sys.time())

      updateQuery = paste0("UPDATE toxval set ",
                           paste0(toxval_tbl$COLUMN_NAME, "=", toxval_tbl$COLUMN_DEFAULT, collapse = ", "),

                           ", species_original='human' WHERE toxval_id in (",
                           toString(toxval_id_list[startPosition:incrementPosition]),
                           ")")

      runQuery(updateQuery, toxval.db)
      startPosition <- startPosition + batch_size
      incrementPosition <- startPosition + batch_size - 1
    }
  }
  if(report.only){
    out_report %>%
      dplyr::distinct() %>%
      return()
  }
}
