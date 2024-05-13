#--------------------------------------------------------------------------------------
#' Set Toxval Defaults globally,  replacing blanks with -
#'
#' @param toxval.db The version of toxval from which to set defaults.
#' @param source The source to be fixed
#' @param subsource The subsource to be fixed (NULL default)
#' @export
#--------------------------------------------------------------------------------------
fill.toxval.defaults.global.by.source <- function(toxval.db, source=NULL, subsource=NULL){
  printCurrentFunction(paste(toxval.db,":", source,subsource))
  defs <- runQuery("desc toxval",toxval.db)
  defs <- defs[,c(1,5)]
  col.list <- defs[is.element(defs[,"Default"],c("-","=")),1]
  col.list <- col.list[!is.na(col.list)]

  # Handle addition of subsource for queries
  query_addition = ""
  if(!is.null(subsource)) {
    query_addition = paste0(" and subsource='", subsource, "'")
  }

  slist = runQuery("select distinct source from toxval",toxval.db)[,1]
  if(!is.null(source)) slist = source
  for(source in slist) {
    cat(source,"\n")
    # For each column, set NA or empty values to "-"
    for(col in col.list){
      n <- runQuery(paste0("select count(*) from toxval where ",col," ='' and source = '",source,"'",query_addition) ,toxval.db)[1,1]
      if(n>0) {
        cat(col,n,"\n")
        query <- paste0("update toxval set ",col,"='-' where ",col," ='' and source = '",source,"'",query_addition)
        runQuery(query,toxval.db)
      }
    }
  }
}
