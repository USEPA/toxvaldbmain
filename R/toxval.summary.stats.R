#--------------------------------------------------------------------------------------
#' Generate summary statistics on the toxval database
#'
#' @param toxval.db The version of toxval into which the tables are loaded.
#'
#--------------------------------------------------------------------------------------
toxval.summary.stats <- function(toxval.db) {
  printCurrentFunction(toxval.db)
  slist = runQuery("select distinct source from toxval",toxval.db)[,1]
  qlist = runQuery("select distinct qc_status from toxval",toxval.db)[,1]
  nlist = c("source","chemicals","total","pass_percent",qlist)
  res = as.data.frame(matrix(nrow=length(slist),ncol=length(nlist)))
  names(res) = nlist
  for(i in 1:length(slist)) {
    source = slist[i]
    res[i,"source"] = source
    query = paste0("select count(distinct dtxsid) from source_chemical where source='",source,"'")
    if(source=="ECHA IUCLID") query = paste0("select count(distinct dtxsid) from source_chemical where source like '%iuclid%'")
    res[i,"chemicals"] = runQuery(query,toxval.db)[1,1]

    query = paste0("select count(*) from toxval where source='",source,"'")
    res[i,"total"] = runQuery(query,toxval.db)[1,1]
    for(qc in qlist) {
      query = paste0("select count(*) from toxval where source='",source,"' and qc_status='",qc,"'")
      res[i,qc] = runQuery(query,toxval.db)[1,1]
    }
    res[i,"pass_percent"] = 100*res[i,"pass"]/res[i,"total"]
  }
  file = paste0(toxval.config()$datapath,"export/source_count ",Sys.Date(),".xlsx")
  write.xlsx(res,file)
}
