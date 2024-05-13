#-------------------------------------------------------------------------------------
#' Find "original" values that have not been included in the dictionaries
#' and export them
#' @param toxval.db The version of toxval in which the data is altered.
#' @param source The source to be fixed. If source=NULL, fix all sources
#' @param subsource The subsource to be fixed (NULL default)
#' @param report.only Whether to report or write/export data. Default is FALSE (write/export data)
#' @return An excel file in dictionaries with the missing entries (if report.only=TRUE, return tibble)
#' "missing dictionary entries {Sys.Date}.xlsx"
#' @export
#-------------------------------------------------------------------------------------
export.missing.dictionary.entries <- function(toxval.db,source=NULL,subsource=NULL,report.only=FALSE) {
  printCurrentFunction(toxval.db)
  flist = runQuery("select distinct field from toxval_fix",toxval.db)[,1]
  if(is.null(source)) slist = runQuery("select distinct source from toxval",toxval.db)[,1]
  if(!is.null(source)) slist = source

  # Handle addition of subsource for queries
  query_addition = ""
  if(!is.null(subsource)) {
    query_addition = paste0(" and subsource='", subsource, "'")
  }

  res.all = NULL

  for(source in slist) {
    res = NULL
    for(field in flist) {
      # Get missing dictionary entries for each field
      field0 = paste0(field,"_original")
      tlist = runQuery(paste0("select term_original from toxval_fix where field='",field,"'"),toxval.db)[,1]
      res0 = runQuery(paste0("select distinct source,",field0," as term_original from toxval where source='",source,"'",query_addition),toxval.db)
      n0 = nrow(res0)
      res0 = res0[!res0$term_original %in% tlist,]
      n1 = nrow(res0)
      cat(source,field0,n0,n1,"\n")
      if(nrow(res0)>0) {
        res0$field = field
        res = rbind(res,res0)
      }
    }
    # message("Pausing here to check for empty/unhandled entries...")
    # browser()
    if(!is.null(res)) {
      nmiss = nrow(res)
      res.all = rbind(res.all,res)
      cat("Number of missing terms=",nmiss,"\n")
      # Write output to file if not report.only
      if (!report.only) {
        file = paste0(toxval.config()$datapath, "dictionary/missing/missing_dictionary_entries ",source," ",Sys.Date(),".xlsx")
        if(!is.null(subsource)) file = paste0(toxval.config()$datapath, "dictionary/missing/missing_dictionary_entries ",source," ",subsource," ",Sys.Date(),".xlsx")
        if(nrow(res)>0) openxlsx::write.xlsx(res,file)
      }
    }
  }
  # Write output to file if not report.only
  if (!report.only) {
    if(!is.null(res.all)) {
      file = paste0(toxval.config()$datapath, "dictionary/missing/missing_dictionary_entries all sources ",Sys.Date(),".xlsx")
      if(nrow(res.all)>0) openxlsx::write.xlsx(res.all,file)
    }
  } else {
    return(res.all)
  }
}
