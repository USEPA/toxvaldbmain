#-------------------------------------------------------------------------------------
#' Set all empty cells in toxval to '-'
#' @param toxval.db The version of toxval in which the data is altered.
#' @param source The source to be fixed
#' @param subsource The subsource to be fixed (NULL default)
#' @return The database will be altered
#' @export
#--------------------------------------------------------------------------------------
fix.empty.by.source <- function(toxval.db,source=NULL,subsource=NULL){
  printCurrentFunction(paste(toxval.db,":", source,subsource))
  cols <- runQuery("desc toxval",toxval.db) %>%
    dplyr::filter(grepl("varchar|text", Type)) %>%
    dplyr::pull(Field)

  # Handle addition of subsource for queries
  query_addition = ""
  if(!is.null(subsource)) {
    query_addition = paste0(" and subsource='", subsource, "'")
  }

  slist = source
  if(is.null(source)) slist = runQuery("select distinct source from toxval",toxval.db)[,1]
  source_string = slist %>%
    paste0(., collapse="', '")


  # For each field, set empty values to "-"
  for(col in cols) {
    print(col)
    query <- paste0("update toxval set ",col,"='-' where ",col,"='' and source in ('",source_string,"')",query_addition)
    runQuery(query,toxval.db)
    query <- paste0("update toxval set ",col,"='-' where ",col," is null and source in ('",source_string,"')",query_addition)
    runQuery(query,toxval.db)
    query <- paste0("update toxval set ",col,"='-' where ",col,"=' ' and source in ('",source_string,"')",query_addition)
    runQuery(query,toxval.db)
    query <- paste0("update toxval set ",col,"='-' where ",col,"='  ' and source in ('",source_string,"')",query_addition)
    runQuery(query,toxval.db)
  }
}
