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

  # Get a list of dataframes by source, then combine
  res = lapply(slist, function(source){
    message("Pulling source ", source, " (", which(source == slist), " of ", length(slist), ")")
    # Get source table based on source name from toxval table
    src_tbl = runQuery(paste0("SELECT distinct source_table FROM ", toxval.db, ".toxval WHERE source = '", source, "'"),
                       toxval.db) %>%
      dplyr::pull(source_table)

    # Loop over all source_tables per source (for IUCLID's multiple source tables)
    lapply(src_tbl, function(src){
      if(src == "direct load"){
        src = source
      }
      cat(paste0("...", src, "\n"))
      # Split into 2 queries to filter on R-side because it was faster
      source_docs = runQuery(paste0("SELECT CONCAT(toxval_id, clowder_doc_id) as toxval_id_clowder_id ",
                                    "FROM ", toxval.db, ".record_source ",
                                    "WHERE source = '", source, "'"), toxval.db)
      res0 = runQuery(paste0("select t.toxval_id, d.clowder_id, t.source, d.document_type, d.url, d.document_name, ",
                             "d.title, d.author, d.year, d.doi, d.clowder_metadata ",
                             "FROM ", toxval.db, ".toxval t ",
                             "LEFT JOIN ", source.db, ".documents_records dr ON t.source_hash = dr.source_hash ",
                             "LEFT JOIN ", source.db, ".documents d ON dr.fk_doc_id = d.id ",
                             "WHERE dr.source_table ='", src, "'"
      ),
      toxval.db) %>%
        dplyr::mutate(toxval_id_clowder_id = paste0(toxval_id, clowder_id)) %>%
        # Filter out any already pushed (toxval_id - clowder_id pairs)
        # Handled by the database as a UNIQUE Key, but multiple layers to ensure duplicates aren't pushed
        dplyr::filter(!toxval_id_clowder_id %in% source_docs$toxval_id_clowder_id) %>%
        dplyr::select(-toxval_id_clowder_id)
    }) %>%
      dplyr::bind_rows() %>%
      return()
  }) %>%
    dplyr::bind_rows() %>%
    dplyr::rename(clowder_doc_id = clowder_id,
                  clowder_doc_metadata = clowder_metadata,
                  record_source_level = document_type) %>%
    dplyr::filter(!is.na(record_source_level)) %>%
    dplyr::mutate(
      # Convert document IDs to URLs
      clowder_doc_id = stringr::str_c("https://clowder.edap-cluster.com/files/", clowder_doc_id)
    )

  # Split into 2 dataframes
  extraction = res %>%
    dplyr::filter(record_source_level == "extraction")
  origin = res %>%
    dplyr::filter(record_source_level == "origin")

  # Check for multiple extraction documents
  extract_multi = extraction %>%
    dplyr::group_by(toxval_id) %>%
    dplyr::summarise(n = dplyr::n()) %>%
    dplyr::filter(n > 1)

  # Error if multiple extraction documents
  if(nrow(extract_multi)){
    message("Source ", source, " with records with multiple extraction documents...")
    browser()
    stop("Source ", source, " with records with multiple extraction documents...")
  }

  # Merge extraction document information with default record_source entries
  extraction = extraction %>%
    dplyr::select(toxval_id, clowder_doc_id, clowder_doc_metadata, document_name)

  ##############################################################################
  ### Batch Update
  ##############################################################################
  batch_size <- 50000
  startPosition <- 1
  endPosition <- nrow(extraction)
  incrementPosition <- batch_size

  while(startPosition <= endPosition){
    if(incrementPosition > endPosition) incrementPosition = endPosition
    message("...Inserting new data in batch: ", batch_size, " startPosition: ", startPosition," : incrementPosition: ", incrementPosition,
            " (",round((incrementPosition/endPosition)*100, 3), "%)", " at: ", Sys.time())

    update_query <- paste0("UPDATE record_source a ",
                           "INNER JOIN z_updated_df b ",
                           "ON (a.toxval_id = b.toxval_id) ",
                           "SET a.clowder_doc_id = b.clowder_doc_id, a.clowder_doc_metadata = b.clowder_doc_metadata, ",
                           "a.document_name = b.document_name ",
                           "WHERE a.toxval_id in (",toString(extraction$toxval_id[startPosition:incrementPosition]),") ",
                           "AND b.record_source_level not in ('extraction', 'origin')")

    runUpdate(table="record_source",
              updateQuery = update_query,
              updated_df = extraction[startPosition:incrementPosition,],
              db=toxval.db)

    startPosition <- startPosition + batch_size
    incrementPosition <- startPosition + batch_size - 1
  }

  message("Pushing clowder_doc_id information to record_source...")
  # Push origin documents
  if(nrow(origin)){
    # Run insert query
    runInsertTable(mat=origin, table='record_source', db=toxval.db)
  }
}
