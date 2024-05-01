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
  res <- runQuery("desc toxval",toxval.db)
  mask <- vector(mode="integer",length=dim(res)[1])
  mask[] <- 0
  for(i in 1:dim(res)[1]) {
    if(tidyselect::contains(res[i,"Type"],"varchar")) mask[i] <- 1
    if(tidyselect::contains(res[i,"Type"],"text")) mask[i] <- 1
  }
  cols <- res[mask==1,"Field"]

  # Handle addition of subsource for queries
  query_addition = ""
  if(!is.null(subsource)) {
    query_addition = paste0(" and subsource='", subsource, "'")
  }

  slist = runQuery("select distinct source from toxval",toxval.db)[,1]
  if(!is.null(source)) slist = source
  for(source in slist) {
    cat(source,"\n")
    for(col in cols) {
      print(col)
      query <- paste0("update toxval set ",col,"='-' where ",col,"='' and source like '",source,"'",query_addition)
      runQuery(query,toxval.db)
      query <- paste0("update toxval set ",col,"='-' where ",col," is null and source like '",source,"'",query_addition)
      runQuery(query,toxval.db)
      query <- paste0("update toxval set ",col,"='-' where ",col,"=' ' and source like '",source,"'",query_addition)
      runQuery(query,toxval.db)
      query <- paste0("update toxval set ",col,"='-' where ",col,"='  ' and source like '",source,"'",query_addition)
      runQuery(query,toxval.db)
    }
  }
}
