#--------------------------------------------------------------------------------------
#' Set the molecular weight in the toxval table, for use in unit conversions
#' @param toxval.db The database version to use
#' @param source The source
#--------------------------------------------------------------------------------------
toxval.set.mw <- function(toxval.db, source=NULL){
  printCurrentFunction(toxval.db)
  dsstox.db <- toxval.config()$dsstox.db
  slist = runQuery("select distinct source from toxval",toxval.db)[,1]
  if(!is.null(source)) slist = source
  for(source in slist) {
    cat(source,"\n")
    runQuery(paste0("update toxval set mw=-1 where mw is null and source='",source,"'"),toxval.db)
    dlist = runQuery(paste0("select distinct dtxsid from toxval where source='",source,"' and mw<0"),toxval.db)[,1]
    counter = 0
    for(dtxsid in dlist) {
      query = paste0(
        "select ",dsstox.db,".compounds.mol_weight
        from ",
        dsstox.db,".compounds, ",
        dsstox.db,".generic_substance_compounds, ",
        dsstox.db,".generic_substances
        where ",
        dsstox.db,".compounds.id = ",dsstox.db,".generic_substance_compounds.fk_compound_id
        and ",
        dsstox.db,".generic_substance_compounds.fk_generic_substance_id = ",dsstox.db,".generic_substances.id
        and ",dsstox.db,".generic_substances.dsstox_substance_id='",dtxsid,"'"
      )
      mw = runQuery(query,toxval.db)[1,1]
      if(!is.na(mw)) {
        query = paste0("update toxval set mw=",mw," where dtxsid='",dtxsid,"' and source='",source,"'")
        runQuery(query,toxval.db)
      }
      counter = counter+1
      if(counter%%1000==0) cat("  finished",counter," out of ",length(dlist),"\n")
    }
  }
}
