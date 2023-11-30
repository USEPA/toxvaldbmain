#-------------------------------------------------------------------------------------
#' Insert extraction document clowder_id into toxval field
#' @param df The dataframe to be processed
#' @param The source to be fixed, NULL by default
#' @return The dataframe with non ascii characters replaced with cleaned versions
#' @export
#-------------------------------------------------------------------------------------
set_clowder_id <- function(toxval.db, source.db, source=NULL){
  printCurrentFunction(toxval.db)
  if(!is.null(source)) {
    slist = source
  } else {
    slist = runQuery("select distinct source from toxval",toxval.db)[,1]
  }
  res <- data.frame()
  for(source in slist) {
    res0 = runQuery(paste0("select t.toxval_id, d.clowder_id FROM ", toxval.db, ".toxval t LEFT JOIN ",
                          source.db,".documents_records dr ON t.source_hash = dr.source_hash
                          LEFT JOIN ", source.db, ".documents d ON dr.fk_doc_id = d.id
                          WHERE d.document_type = 'extraction' and source='",source,"'"),toxval.db)
    res <- bind_rows(res, res0)
  }
  # Query to inner join and update toxval with clowder_id (temp table added/dropped)
  updateQuery = paste0("UPDATE toxval a INNER JOIN z_updated_df b ",
                      "ON (a.toxval_id = b.toxval_id) SET a.clowder_id = b.clowder_id ")
  # Run update query
  runUpdate(table="toxval", updateQuery=updateQuery, updated_df=res, db=toxval.db)
}
