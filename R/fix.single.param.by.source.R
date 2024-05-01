#-------------------------------------------------------------------------------------
#' Alter the contents of toxval according to an excel dictionary
#' @param toxval.db The version of toxval in which the data is altered.
#' @param param The parameter value to be fixed
#' @param source The source to be fixed
#' @param subsource The subsource to be fixed (NULL default)
#' @param ignore If TRUE allow missing values to be ignored
#' @param report.only Whether to report or write/export data. Default is FALSE (write/export data)
#' @return The database will be altered
#' @export
#-------------------------------------------------------------------------------------
fix.single.param.by.source <- function(toxval.db, param, source, subsource=NULL, ignore = FALSE, report.only=FALSE) {
  printCurrentFunction(paste(toxval.db,":",param, ":", source,subsource))

  # Handle addition of subsource for queries
  query_addition = ""
  if(!is.null(subsource)) {
    query_addition = paste0(" and subsource='", subsource, "'")
  }

  file <- paste0(toxval.config()$datapath,"dictionary/2021_dictionaries/",param,"_5.xlsx")
  mat <- openxlsx::read.xlsx(file, na.strings = "NOTHING")
  #print(View(mat))
  mat_flag_change <- grep('\\[\\.\\.\\.\\]',mat[,2])
  mat[mat_flag_change,2] <- stringr::str_replace_all(mat[mat_flag_change,2],'\\[\\.\\.\\.\\]','XXX')
  print(dim(mat))

  db.values <- runQuery(paste0("select distinct ",param,"_original from toxval where source like '",source,"'",query_addition),
                        toxval.db)[,1]
  missing <- db.values[!is.element(db.values,c(mat[,2],""))]

  missing = missing[!is.na(missing)]
  # If only reporting, return without making changes
  if(report.only) return(missing)

  if(length(missing)>0 & !ignore) {
    cat("values missing from the dictionary\n")
    for(i in 1:length(missing)) cat(missing[i],"\n",sep="")
    #browser()
  }

  # file <- paste0("./dictionary/missing_values_",param,"_",source,"_",Sys.Date(),".xlsx")
  # write.xlsx(missing,file)

  cat("  original list: ",nrow(mat),"\n")

  query <- paste0("select distinct (",param,"_original) from toxval where source like '",source,"'",query_addition)
  param_original_values <- runQuery(query,toxval.db)[,1]
  #print(View(param_original_values))
  mat <- mat[is.element(mat[,2],param_original_values),]

  #print(View(mat))
  cat("  final list: ",nrow(mat),"\n")

  for(i in 1:dim(mat)[1]) {
    v0 <- mat[i,2]
    v1 <- mat[i,1]
    cat(v0,":",v1,"\n"); utils::flush.console()
    query <- paste0("update toxval set ",param,"='",v1,"' where ",param,"_original='",v0,"' and source like '",source,"'",query_addition)
    runInsert(query,toxval.db,T,F,T)
  }
  query <- paste0("update toxval set ",param,"='-' where ",param,"_original is NULL and source like '",source,"'",query_addition)
  runInsert(query,toxval.db,T,F,T)
}
