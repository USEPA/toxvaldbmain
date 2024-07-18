#-------------------------------------------------------------------------------------
#' Set all empty cells in record_source to '-'
#' @param toxval.db The version of toxval in which the data is altered.
#' @param source The source to be fixed
#' @return The database will be altered
#' @export
#--------------------------------------------------------------------------------------
fix.empty.record_source.by.source <- function(toxval.db, source=NULL){
  printCurrentFunction(paste(toxval.db,":", source))
  cols <- runQuery("desc record_source",toxval.db) %>%
    dplyr::filter(grepl("varchar|text", Type)) %>%
    dplyr::pull(Field)

  slist = source
  if(is.null(source)) slist = runQuery("select distinct source from toxval",toxval.db)[,1]
  source_string = slist %>%
    paste0(., collapse="', '")

  # For each field, set empty values to "-"
  for(col in cols) {
    print(col)
    query <- paste0("update record_source set ",col,"='-' where ",col,"='' and source in ('",source_string,"')")
    runQuery(query,toxval.db)
    query <- paste0("update record_source set ",col,"='-' where ",col," is null and source in ('",source_string,"')")
    runQuery(query,toxval.db)
    query <- paste0("update record_source set ",col,"='-' where ",col,"=' ' and source in ('",source_string,"')")
    runQuery(query,toxval.db)
    query <- paste0("update record_source set ",col,"='-' where ",col,"='  ' and source in ('",source_string,"')")
    runQuery(query,toxval.db)
 }
}
