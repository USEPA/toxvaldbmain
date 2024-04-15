#-----------------------------------------------------------------------------------
#' Set the study_group field
#'
#' @param toxval.db Database version
#' @param source The source to be updated
#' @param subsource The subsource to be updated (NULL default)
#' @param reset Whether or not to set entire study_group field to "-" before logic, default FALSE
#' @return for each source writes an Excel file with the name
#'  ../export/export_by_source_{data}/toxval_all_{toxval.db}_{source}.xlsx
#' @export
#-----------------------------------------------------------------------------------
fix.study_group <- function(toxval.db, source=NULL, subsource=NULL, reset=FALSE) {
  printCurrentFunction(toxval.db)

  if(reset) runQuery("update toxval set study_group='-'",toxval.db)
  slist = runQuery("select distinct source from toxval",toxval.db)[,1]
  if(!is.null(source)) slist = source
  slist = slist[!is.element(slist,c("ECOTOX"))]

  # Handle addition of subsource for queries
  query_addition = ""
  if(!is.null(subsource)) {
    query_addition = paste0(" and subsource='", subsource, "'")
  }

  for(source in slist) {
    sglist = runQuery(paste0("select distinct study_group from toxval where source='",source,"'",query_addition),toxval.db)[,1]
    doit = FALSE
    # Check for unassigned study group values
    if(is.element("-",sglist)) doit = TRUE
    if(doit) {
      cat(source,"\n")
      # Reset to "-"
      runQuery(paste0("update toxval set study_group='-' where source='",source,"'",query_addition),toxval.db)
      # Query unique study fields
      query = paste0("select a.toxval_id, a.dtxsid,c.common_name, a.toxval_units, ",
                     "a.target_species, a.study_type, a.exposure_route,a.exposure_method, ",
                     "a.study_duration_value, a.study_duration_units, ",
                     "a.strain, b.year, b.long_ref, b.title, b.author ",
                     "from toxval a, record_source b, species c ",
                     "where a.species_id=c.species_id and a.toxval_id=b.toxval_id and a.source='",source,"'")
      if(!is.null(subsource)) {
        query = paste0(query, " and a.subsource='",subsource,"'")
      }
      # Pull data
      temp = runQuery(query,toxval.db)
      # Hash to identify duplicate groups
      temp.temp = temp %>%
        tidyr::unite(hash_col, all_of(sort(names(.)[!names(.) %in% c("toxval_id")])), sep="") %>%
        dplyr::rowwise() %>%
        dplyr::mutate(source_hash = paste0("ToxValhc_", digest(hash_col, serialize = FALSE))) %>%
        dplyr::ungroup()

      temp_sg = temp %>%
        dplyr::mutate(source_hash = temp.temp$source_hash) %>%
        dplyr::select(toxval_id, source_hash) %>%
        # Collapse toxval_id for duplicate hashes
        dplyr::group_by(dplyr::across(c(-toxval_id))) %>%
        dplyr::summarise(toxval_id = toString(toxval_id)) %>%
        # Only account for those with duplicates
        dplyr::filter(grepl(",", toxval_id)) %>%
        # Assign study group
        dplyr::mutate(study_group = 1:n() %>%
                        paste0(!!source, "_dup_", .)) %>%
        dplyr::ungroup() %>%
        dplyr::select(-source_hash) %>%
        # Separate collapse toxval_id groups
        tidyr::separate_rows(toxval_id, sep=", ") %>%
        dplyr::mutate(toxval_id = as.numeric(toxval_id))

      # Check/report if any duplicates present
      nr = nrow(temp)
      nsg = length(unique(temp_sg$study_group)) + length(temp$toxval_id[!temp$toxval_id %in% temp_sg$toxval_id])
      cat("  nrow:",nr," unique values:",nsg,"\n")
      # Set default study group to toxval_id
      query = paste0("update toxval set study_group=CONCAT(source,'_',toxval_id) where source='",source,"'",query_addition))
      runQuery(query,toxval.db)

      # If duplicate groups, set to generated study group
      if(nsg!=nr) {
        cat("   Number of dups:", length(unique(temp_sg$study_group)),"\n")
        # Query to inner join and make updates with mw dataframe (temp table added/dropped)
        updateQuery = paste0("UPDATE toxval a INNER JOIN z_updated_df b ",
                             "ON (a.toxval_id = b.toxval_id) SET a.study_group = b.study_group ",
                             "WHERE a.study_group IS NOT NULL")
        # Run update query
        runUpdate(table="toxval", updateQuery=updateQuery, updated_df=temp_sg, db=toxval.db)
      }
    }
  }
}
