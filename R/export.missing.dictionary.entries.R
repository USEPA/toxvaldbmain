#-------------------------------------------------------------------------------------
#' Find "original" values that have not been included in the dictionaries
#' and export them
#' @param toxval.db The version of toxval in which the data is altered.
#' @param source The source to be fixed. If source=NULL, fix all sources
#' @return An excel file in dictionaries with the missing entries
#' "missing dictionary entries {Sys.Date}.xlsx"
#' @export
#-------------------------------------------------------------------------------------
export.missing.dictionary.entries <- function(toxval.db,source=NULL,subsource=NULL) {
  printCurrentFunction(toxval.db)
  flist = runQuery("select distinct field from toxval_fix",toxval.db)[,1]
  if(is.null(source)) slist = runQuery("select distinct source from toxval",toxval.db)[,1]
  if(!is.null(source)) slist = source
  res.all = NULL
  for(source in slist) {
    res = NULL
    for(field in flist) {
      field0 = paste0(field,"_original")
      tlist = runQuery(paste0("select term_original from toxval_fix where field='",field,"'"),toxval.db)[,1]
      res0 = unique(runQuery(paste0("select source,",field0," from toxval where source='",source,"'"),toxval.db))
      names(res0) = c("source","term_original")
      n0 = nrow(res0)
      res0 = res0[!is.element(res0[,2],tlist),]
      n1 = nrow(res0)
      cat(source,field0,n0,n1,"\n")
      if(nrow(res0)>0) {
        res0$field = field
        res = rbind(res,res0)
      }
    }
    if(!is.null(res)) {
      nmiss = nrow(res)
      res.all = rbind(res.all,res)
      cat("Number of missing terms=",nmiss,"\n")
      file = paste0(toxval.config()$datapath, "dictionary/missing/missing_dictionary_entries ",source," ",Sys.Date(),".xlsx")
      if(!is.null(subsource)) file = paste0(toxval.config()$datapath, "dictionary/missing/missing_dictionary_entries ",source," ",subsource," ",Sys.Date(),".xlsx")
      if(nrow(res)>0) write.xlsx(res,file)
    }
  }
  if(!is.null(res.all)) {
    file = paste0(toxval.config()$datapath, "dictionary/missing/missing_dictionary_entries all sources ",Sys.Date(),".xlsx")
    if(nrow(res.all)>0) write.xlsx(res.all,file)
  }
}
