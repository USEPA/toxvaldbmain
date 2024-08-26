
remap_source_by_source_table <- function(toxval.db, source.db){
  # Get list of current source, supersource, and source_table fields
  # Ignore direct load and ECHA IUCLID
  tbl_list = runQuery(paste0("SELECT distinct source, supersource, source_table, ",
                             "CONCAT(source, '_', source_table) as tbl_key ",
                             "FROM toxval WHERE source_table not in ('direct load') ",
                             "AND source not in ('ECHA IUCLID')"),
                      toxval.db)

  # Query source tables to get source value
  source_list = lapply(tbl_list$source_table, function(s_tbl){
    runQuery(paste0("SELECT distinct source FROM ", s_tbl),
             source.db) %>%
      dplyr::mutate(source_table = s_tbl)
  }) %>%
    dplyr::bind_rows() %>%
    tidyr::unite(col = "tbl_key",
                 source, source_table,
                 sep = "_",
                 remove=FALSE)

  # Get list that do not match
  compare = source_list %>%
    dplyr::filter(!tbl_key %in% tbl_list$tbl_key) %>%
    # Generate update query
    dplyr::mutate(u_query = paste0("UPDATE toxval SET source = '", source, "' WHERE source_table = '", source_table, "'"))

  # Run update queries
  for(query in compare$u_query){
    message("Running query ", which(compare$u_query == query), " of ", length(compare$u_query))
    runQuery(query, toxval.db)
  }
}

