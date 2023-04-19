#-------------------------------------------------------------------------------------
#' Set all empty cells in record_source to '-'
#' @param toxval.db The version of toxval in which the data is altered.
#' @param source The source to be fixed
#' @return The database will be altered
#' @export
#--------------------------------------------------------------------------------------
fix.empty.record_source.by.source <- function(toxval.db, source=NULL){
  printCurrentFunction(paste(toxval.db,":", source))
  res <- runQuery("desc record_source",toxval.db)
  mask <- vector(mode="integer",length=dim(res)[1])
  mask[] <- 0
  for(i in 1:dim(res)[1]) {
    if(contains(res[i,"Type"],"varchar")) mask[i] <- 1
    if(contains(res[i,"Type"],"text")) mask[i] <- 1
  }
  cols <- res[mask==1,"Field"]

  slist = runQuery("select distinct source from toxval",toxval.db)[,1]
  if(!is.null(source)) slist = source
  for(source in slist) {
    cat(source,"\n")
      for(col in cols) {
      print(col)
      query <- paste0("update record_source set ",col,"='-' where ",col,"='' and source like '",source,"'")
      runQuery(query,toxval.db)
      query <- paste0("update record_source set ",col,"='-' where ",col," is null and source like '",source,"'")
      runQuery(query,toxval.db)
      query <- paste0("update record_source set ",col,"='-' where ",col,"=' ' and source like '",source,"'")
      runQuery(query,toxval.db)
      query <- paste0("update record_source set ",col,"='-' where ",col,"='  ' and source like '",source,"'")
      runQuery(query,toxval.db)
     }
  }
}
