#--------------------------------------------------------------------------------------
#' Set the molecular weight in the toxval table, for use in unit conversions
#' @param toxval.db The database version to use
#' @param source The source
#--------------------------------------------------------------------------------------
toxval.set.mw <- function(toxval.db, source=NULL){
  printCurrentFunction(toxval.db)
  dsstox.db <- toxval.config()$dsstox.db
  if(!is.null(source)) {
    slist = source
  } else {
    slist = runQuery("select distinct source from toxval",toxval.db)[,1]
  }

  for(source in slist) {
    cat(source,"\n")
    runQuery(paste0("update toxval set mw=-1 where mw is null and source='",source,"'"),toxval.db)
    dlist = runQuery(paste0("select distinct dtxsid from toxval where source='",source,"' and mw<0"),toxval.db)[,1]

    # Query DSSTox once for source dlist
    query = paste0(
      "select ",
      dsstox.db,".generic_substances.dsstox_substance_id as dtxsid, ",
      dsstox.db,".compounds.mol_weight as mw
        from ",
      dsstox.db,".compounds, ",
      dsstox.db,".generic_substance_compounds, ",
      dsstox.db,".generic_substances
        where ",
      dsstox.db,".compounds.id = ",dsstox.db,".generic_substance_compounds.fk_compound_id
        and ",
      dsstox.db,".generic_substance_compounds.fk_generic_substance_id = ",dsstox.db,".generic_substances.id
        and ",dsstox.db,".generic_substances.dsstox_substance_id IN ('",
      paste0(dlist, collapse = "', '"),
      "')"
    )
    mw = runQuery(query,toxval.db) %>%
      filter(!is.na(mw))
    # Only run if mw values are present for selected DTXSID values
    if(nrow(mw)){
      # Query to inner join and make updates with mw dataframe (temp table added/dropped)
      updateQuery = paste0("UPDATE toxval a INNER JOIN z_updated_df b ",
                           "ON (a.dtxsid = b.dtxsid) SET a.mw = b.mw ",
                           "WHERE a.mw < 0")
      # Run update query
      runUpdate(table="toxval", updateQuery=updateQuery, updated_df=mw, db=toxval.db)
    }
  }
}
