#-------------------------------------------------------------------------------------
#' Export any toxval_types that are not in the toxval_type dictionary
#' @param toxval.db The version of toxval in which the data is altered.
#' @return An excel file in dictionaries with the missing entries
#' "dictionary/missing/missing_toxval_type {Sys.Date}.xlsx"
#' @export
#-------------------------------------------------------------------------------------
export.missing.toxval_type <- function(toxval.db) {
  printCurrentFunction(toxval.db)
  res = runQuery("select source,toxval_type from toxval",toxval.db)
  res = unique(res)

  tlist2 = runQuery("select toxval_type from toxval_type_dictionary",toxval.db)[,1]
  res = res[!is.element(res$toxval_type,tlist2),]
  cat("missing values:",nrow(res),"\n")
  if(nrow(res)>0) {
    file = paste0(toxval.config()$datapath,"/dictionary/missing/missing_toxval_type ",Sys.Date(),".xlsx")
    write.xlsx(res,file)
  }
}
