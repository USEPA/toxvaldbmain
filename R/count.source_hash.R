#-------------------------------------------------------------------------------------
#' Look for duplicated source_hash
#' @param toxval.db The version of toxval in which the data is altered.
#' @export
#--------------------------------------------------------------------------------------
count.source_hash = function(toxval.db){
  printCurrentFunction(toxval.db)
  slist = runQuery("select distinct source from toxval",toxval.db)[,1]
  slist = sort(slist)
  for(source in slist) {
    query = paste0("select count(*) from toxval where source='",source,"'")
    n1 = runQuery(query,toxval.db)[,1]
    query = paste0("select count(distinct source_hash) from toxval where source='",source,"'")
    n2 = runQuery(query,toxval.db)[,1]
    if(n1!=n2) cat(source,n1,n2,"\n")
  }
}
