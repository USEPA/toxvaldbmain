#-------------------------------------------------------------------------------------
#' Fix the qa_status flag
#' @param toxval.db The version of toxval in which the data is altered.
#' @param source The source to be fixed
#' @param subsource The subsource to be fixed (NULL default)
#' @param reset If TRUE, reset all values to 'pass' before setting
#' @return The database will be altered
#' @export
#--------------------------------------------------------------------------------------
fix.qc_status.by.source <- function(toxval.db,source=NULL, subsource=NULL, reset=T){
  printCurrentFunction(paste(toxval.db,":", source,subsource))
  slist = source
  if(is.null(source)) slist = runQuery("select distinct source from toxval",toxval.db)[,1]
  cat("  set all to 'not classified'\n")

  # Handle addition of subsource for queries
  query_addition = ""
  if(!is.null(subsource)) {
    query_addition = paste0(" and subsource='", subsource, "'")
  }

  for(source in slist) {
    cat(source,"\n")
    if(reset) runQuery(paste0("update toxval set qc_status='pass' where source like '",source,"'",query_addition) ,toxval.db)

    runQuery(paste0("update toxval set qc_status='fail:toxval_numeric<0' where toxval_numeric<=0 and source = '",source,"'",query_addition) ,toxval.db)
    runQuery(paste0("update toxval set qc_status='fail:toxval_numeric is null' where toxval_numeric is null and source = '",source,"'",query_addition) ,toxval.db)
    runQuery(paste0("update toxval set qc_status='fail:toxval_type not specified' where toxval_type='-' and source = '",source,"'",query_addition) ,toxval.db)
    runQuery(paste0("update toxval set qc_status='fail:toxval_units not specified' where toxval_units='-' and source = '",source,"'",query_addition),toxval.db)
    #runQuery(paste0("update toxval set qc_status='fail:risk_assessment_class not specified' where risk_assessment_class='other' and source = '",source,"'"),toxval.db)
    runQuery(paste0("update toxval set qc_status='fail:dtxsid not specified' where dtxsid='NODTXSID' and source = '",source,"'",query_addition),toxval.db)
    #if(!is.element(source,"WHO IPCS")) runQuery(paste0("update toxval set qc_status='fail:species not specified' where species_id=1000000 and source = '",source,"'") ,toxval.db)
    runQuery(paste0("update toxval set qc_status='fail:human_eco not specified' where human_eco in ('-','not specified') and source = '",source,"'",query_addition),toxval.db)
    runQuery(paste0("update toxval set qc_status='fail:risk_assessment_class not specified' where risk_assessment_class in ('-','not specified') and source = '",source,"'",query_addition),toxval.db)
  }
}
