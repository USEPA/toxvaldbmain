#-------------------------------------------------------------------------------------
#' Inserts clowder document information into record_source table
#' @param toxval.db The version of toxval into which the tables are loaded.
#' @param source.db The source database to use.
#' @param source Name of source to set. Default NULL means set Clowder ID for all sources
#' @return None. SQL insert statement is performed
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
                       toxval.db)
    src_tables = unique(src_tbl$source_table)
    # Loop over all source_tables per source (for IUCLID's multiple source tables)
    for(src in src_tables){
      print(source)
      res0 = runQuery(paste0("select t.toxval_id, d.clowder_id, t.source, d.document_type, d.url, d.document_name,
                           d.title, d.author, d.year, d.doi, d.clowder_metadata ",
                             "FROM ", toxval.db, ".toxval t ",
                             "LEFT JOIN ", source.db, ".documents_records dr ON t.source_hash = dr.source_hash ",
                             "LEFT JOIN ", source.db, ".documents d ON dr.fk_doc_id = d.id ",
                             "WHERE dr.source_table ='", src, "'"),
                      toxval.db)
      # Combine results
      res <- bind_rows(res, res0)
    }
  }
  res <- res %>% dplyr::rename(clowder_doc_id = clowder_id,
                               record_source_level = document_type)
  message("Pushing clowder_doc_id information to record_source...")
  # Run insert query
  runInsertTable(mat=res, table='record_source', db=toxval.db)
}
