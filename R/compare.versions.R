#-------------------------------------------------------------------------------------
#' Compare versions of toxval
#' @param db1 The old version of the database
#' @param db2 The new version of the database
#' @export
#--------------------------------------------------------------------------------------
compare.versions <- function(db1, db2) {
  printCurrentFunction(paste(db1,":", db2))
  slist = runQuery("select distinct source from toxval",db1)[,1]
  for(source in slist) {
    n1 = runQuery(paste0("select count(*) from toxval where source='",source,"'"),db1)[,1]
    n2 = runQuery(paste0("select count(*) from toxval where source='",source,"'"),db2)[,1]
    if(n1!=n2) cat(source,n1,n2,"\n")
  }
}
