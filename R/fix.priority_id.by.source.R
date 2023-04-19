#-------------------------------------------------------------------------------------
#' Fix the priority_id in the toxval table based on source
#' @param toxval.db The version of toxvaldb to use.
#' @param source The source to be fixed, If NULL, set for all sources
#' @export
#-------------------------------------------------------------------------------------
fix.priority_id.by.source <- function(toxval.db, source=NULL) {
  printCurrentFunction(paste(toxval.db,":", source))

  slist = source
  if(is.null(source)) slist = runQuery("select distinct source from toxval",toxval.db)[,1]
  for(source in slist) {
    cat(source,"\n")
    query = paste0("update toxval set priority_id=-1 where source like '",source,"'")
    runQuery(query,toxval.db)
    pid = runQuery(paste0("select priority_id from source_info where source='",source,"'"),toxval.db)[1,1]
    query = paste0("update toxval set priority_id=",pid," where source='",source,"'")
    runQuery(query,toxval.db)
  }
  x = runQuery("select distinct source from toxval where priority_id=-1",toxval.db)
  if(nrow(x)>0) print(x)
  cat("Number of sources with missing priority_id:",nrow(x),"\n")
}
