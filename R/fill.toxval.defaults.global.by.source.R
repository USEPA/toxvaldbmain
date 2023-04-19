#--------------------------------------------------------------------------------------
#' Set Toxval Defaults globally,  replacing blanks with -
#'
#' @param toxval.db The version of toxval from which to set defaults.
#' @param source The source to be fixed
#' @export
#--------------------------------------------------------------------------------------
fill.toxval.defaults.global.by.source <- function(toxval.db, source=NULL){
  printCurrentFunction(paste(toxval.db,":", source))
  defs <- runQuery("desc toxval",toxval.db)
  defs <- defs[,c(1,5)]
  col.list <- defs[is.element(defs[,"Default"],c("-","=")),1]
  col.list <- col.list[!is.na(col.list)]

  slist = runQuery("select distinct source from toxval",toxval.db)[,1]
  if(!is.null(source)) slist = source
  for(source in slist) {
    cat(source,"\n")
    for(col in col.list){
      n <- runQuery(paste0("select count(*) from toxval where ",col," ='' and source = '",source,"'") ,toxval.db)[1,1]
      if(n>0) {
        cat(col,n,"\n")
        query <- paste0("update toxval set ",col,"='-' where ",col," ='' and source = '",source,"'")
        runQuery(query,toxval.db)
      }
    }
  }
}
