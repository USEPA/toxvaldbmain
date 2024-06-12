#-----------------------------------------------------------------------------------
#' Diagnose duplicates
#'
#' @param toxval.db Database version
#' @param source The source to be updated
#' @return Write a file with the results
#'
#-----------------------------------------------------------------------------------
duplicate.hunter <- function(toxval.db,source=NULL,source_table=NULL) {
  printCurrentFunction(toxval.db)
  dir = paste0(toxval.config()$datapath,"duplicate_hunter/")

  if(is.null(source)) slist = runQuery("select distinct source from toxval",toxval.db)[,1]
  else slist = source
  nlist = c("source","source_table","rows","unique","drop.source_hash","drop.toxval_numeric","drop.critical_effect")
  res = as.data.frame(matrix(nrow=length(slist),ncol=length(nlist)))
  names(res) = nlist
  for(i in 1:length(slist)) {
    source = slist[i]
    cat(source,"\n")
    x1 = runQuery(paste0("select * from toxval where source='",source,"'"),toxval.db)
    res[i,"source"] = source
    res[i,"source_table"] = source_table
    res[i,"rows"] = nrow(x1)
    x2 = unique(subset(x1,select= -c(toxval_id,toxval_uuid)))
    res[i,"unique"] = nrow(x2)
    x3 = unique(subset(x2,select= -c(source_hash)))
    res[i,"drop.source_hash"] = nrow(x3)
    x4 = unique(subset(x3,select= -c(toxval_numeric,toxval_numeric_original)))
    res[i,"drop.toxval_numeric"] = nrow(x4)
    x5 = unique(subset(x4,select= -c(critical_effect,critical_effect_original)))
    res[i,"drop.critical_effect"] = nrow(x5)
    browser()
  }

  file = paste0(dir,"duplicate.hunter ",Sys.Date(),".xlsx")
  openxlsx::write.xlsx(res,file)
}
