#-------------------------------------------------------------------------------------
#' Insert extraction document clowder_id into toxval field
#' @param toxval.db The version of toxval into which the tables are loaded.
#' @param source.db The source database to use.
#' @param source Name of source to set. Default NULL means set Clowder ID for all sources
#' @return None. SQL update statements are performed
#' @export
set_extraction_doc_clowder_id <- function(toxval.db, source.db, source=NULL){
  printCurrentFunction(toxval.db)
  if(!is.null(source)) {
    slist = source
  } else {
    slist = runQuery("select distinct source from toxval",toxval.db)[,1]
  }
  res <- data.frame()
  for(source in slist) {
    message("Pulling source ", which(source == slist), " of ", length(slist))
    # Get source table based on source name from toxval table
    src_tbl = runQuery(paste0("SELECT distinct source_table FROM ", toxval.db, ".toxval WHERE source = '", source, "'"),
                       toxval.db)[1,]
    res0 = runQuery(paste0("select t.toxval_id, t.source_hash, d.clowder_id ",
                           "FROM ", toxval.db, ".toxval t ",
                           "LEFT JOIN ", source.db, ".documents_records dr ON t.source_hash = dr.source_hash ",
                           "LEFT JOIN ", source.db, ".documents d ON dr.fk_doc_id = d.id ",
                           "WHERE d.document_type = 'extraction' and dr.source_table='", src_tbl, "'"),
                    toxval.db)
    # Combine results
    res <- bind_rows(res, res0)
  }
  # Query to inner join and update toxval with clowder_id (temp table added/dropped)
  updateQuery = paste0("UPDATE toxval a INNER JOIN z_updated_df b ",
                      "ON (a.toxval_id = b.toxval_id) SET a.clowder_id = b.clowder_id")
  message("Pushing updated extraction document Clowder ID values...")
  message("Stopping function until clowder_id field is added to toxval table...")
  browser()
  # Run update query
  runUpdate(table="toxval", updateQuery=updateQuery, updated_df=res, db=toxval.db)
}
