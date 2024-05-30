#-------------------------------------------------------------------------------------
#' Export any toxval_types that are not in the toxval_type dictionary
#' @param toxval.db The version of toxval in which the data is altered.
#' @param report.only Whether to report or write/export data. Default is FALSE (write/export data)
#' @return An excel file in dictionaries with the missing entries (if report.only=TRUE, return tibble)
#' "dictionary/missing/missing_toxval_type {Sys.Date}.xlsx"
#' @export
#-------------------------------------------------------------------------------------
export.missing.toxval_type <- function(toxval.db, report.only=FALSE) {
  printCurrentFunction(toxval.db)
  # Get all toxval_type values from toxval
  res = runQuery("select source,toxval_type from toxval WHERE qc_status!='fail'",toxval.db)
  res = unique(res)

  # Compare toxval_type values in toxval to toxval_type values currently handled in dictionary
  tlist2 = runQuery("select toxval_type from toxval_type_dictionary",toxval.db)[,1]
  res = res[!is.element(res$toxval_type,tlist2),]
  cat("missing values:",nrow(res),"\n")

  # Write/return results
  if (!report.only) {
    if(nrow(res)>0) {
      file = paste0(toxval.config()$datapath,"/dictionary/missing/missing_toxval_type ",Sys.Date(),".xlsx")
      openxlsx::write.xlsx(res,file)
    }
  } else {
    return(res)
  }
}
