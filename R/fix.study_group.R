#-----------------------------------------------------------------------------------
#' Set the study_group field
#'
#' @param toxval.db Database version
#' @param source The source to be updated
#' @param subsource The subsource to be updated (NULL default)
#' @param report.only Whether to apply study_group fix or just report what fixes would be applied.
#' @return for each source writes an Excel file with the name
#'  ../export/export_by_source_{data}/toxval_all_{toxval.db}_{source}.xlsx
#' @export
#-----------------------------------------------------------------------------------
fix.study_group <- function(toxval.db, source=NULL, subsource=NULL, report.only=FALSE) {
  printCurrentFunction(toxval.db)

  slist = runQuery("select distinct source from toxval",toxval.db)[,1]
  if(!is.null(source)) slist = source
  # Handle addition of subsource for queries
  query_addition = NULL
  if(!is.null(subsource)) {
    query_addition = paste0(" and subsource='", subsource, "'")
  }

  report.out = data.frame()

  for(source in slist) {
    cat(source,"\n")
    if(!report.only){
      # Reset to "-"
      runQuery(paste0("update toxval set study_group='-' where source='",source,"'", query_addition), toxval.db)
    }
    # Query unique study fields
    query = paste0("select a.toxval_id,a.dtxsid,c.common_name, a.toxval_units,  ",
                   "a.study_type, a.exposure_route,a.exposure_method,a.exposure_form, ",
                   "a.study_duration_value, a.study_duration_units, a.sex, a.lifestage, a.generation, a.year,",
                   "a.strain, b.year as record_year, b.long_ref, b.title ",
                   "from toxval a ",
                   "left join record_source b on a.toxval_id=b.toxval_id ",
                   "left join species c on a.species_id=c.species_id ",
                   # Only use record_source entries from the source, not ToxVal team cataloging
                   "where b.clowder_doc_id = '-' and record_source_level = 'primary (risk assessment values)' ",
                   "and a.source='",source,"'", query_addition)

    # Pull data
    temp = runQuery(query,toxval.db)
    # Hash to identify duplicate groups
    temp.temp = temp %>%
      tidyr::unite(hash_col, dplyr::all_of(sort(names(.)[!names(.) %in% c("toxval_id")])), sep="") %>%
      dplyr::rowwise() %>%
      dplyr::mutate(source_hash = paste0("ToxValhc_", digest::digest(hash_col, serialize = FALSE))) %>%
      dplyr::ungroup()

    temp_sg = temp %>%
      dplyr::mutate(source_hash = temp.temp$source_hash) %>%
      dplyr::select(toxval_id, source_hash) %>%
      # Collapse toxval_id for duplicate hashes
      dplyr::group_by(dplyr::across(c(-toxval_id))) %>%
      dplyr::summarise(toxval_id = toString(toxval_id)) %>%
      # Only account for those with duplicates
      dplyr::filter(grepl(",", toxval_id))

    if(nrow(temp_sg)){
      temp_sg = temp_sg %>%
        # Assign study group
        dplyr::mutate(study_group = 1:n() %>%
                        paste0(!!source, "_dup_", .)) %>%
        dplyr::ungroup() %>%
        dplyr::select(-source_hash) %>%
        # Separate collapse toxval_id groups
        tidyr::separate_rows(toxval_id, sep=", ") %>%
        dplyr::mutate(toxval_id = as.numeric(toxval_id))
    } else {
      # Set up empty duplicate study_group dataframe
      temp_sg = temp %>%
        dplyr::mutate(study_group = NA) %>%
        .[0,]
    }

    # Check/report if any duplicates present
    nr = nrow(temp)
    nsg = length(unique(temp_sg$study_group)) + length(temp$toxval_id[!temp$toxval_id %in% temp_sg$toxval_id])
    cat("  nrow:",nr," unique values:",nsg,"\n")
    # Set default study group to toxval_id
    if(!report.only){
      query = paste0("update toxval set study_group=CONCAT(source,':',toxval_id,':',sex,':',generation,lifestage) where source='",
                     source,"'",
                     query_addition)
      runQuery(query,toxval.db)
    } else {
      query = paste0("SELECT toxval_id, CONCAT(source,':',toxval_id,':',sex,':',generation,lifestage) as study_group from toxval where source='",
                     source,"'",
                     query_addition)
      src.report.out = runQuery(query,toxval.db) %>%
        # Filter out dups
        dplyr::filter(!toxval_id %in% temp_sg$toxval_id)
    }

    # If duplicate groups, set to generated study group
    if(nsg!=nr) {
      cat("   Number of dups:", length(unique(temp_sg$study_group)),"\n")
      if(!report.only){
        # Query to inner join and make updates with mw dataframe (temp table added/dropped)
        updateQuery = paste0("UPDATE toxval a INNER JOIN z_updated_df b ",
                             "ON (a.toxval_id = b.toxval_id) SET a.study_group = CONCAT(b.study_group,':',a.sex,':',a.generation,a.lifestage) ",
                             "WHERE a.study_group IS NOT NULL")
        # Run update query
        runUpdate(table="toxval", updateQuery=updateQuery, updated_df=temp_sg, db=toxval.db)
      } else {
        query = paste0("SELECT toxval_id, CONCAT(sex,':',generation,lifestage) as study_group_addition FROM toxval WHERE toxval_id in (",
                       toString(temp_sg$toxval_id),")")
        src.dups = runQuery(query,toxval.db)

        src.report.out = temp_sg %>%
          dplyr::left_join(src.dups,
                           by="toxval_id") %>%
          tidyr::unite(col="study_group", study_group, study_group_addition, sep = ":", na.rm=TRUE) %>%
          # Add dups to report
          dplyr::bind_rows(src.report.out, .)
      }
    }
    # Bind source reports
    if(report.only){
      report.out = report.out %>%
        dplyr::bind_rows(src.report.out)
    }
  }

  # Return report
  if(report.only){
    # Compare to what's currently in ToxVal
    stg = runQuery(paste0("SELECT toxval_id, study_group as study_group_toxval FROM toxval WHERE toxval_id IN (",
                   toString(report.out$toxval_id), ")"),
                   toxval.db)
    report.out %>%
      dplyr::left_join(stg,
                       by="toxval_id") %>%
      return()
  }
}
