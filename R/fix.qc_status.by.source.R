#-------------------------------------------------------------------------------------
#' Fix the qa_status flag
#' @param toxval.db The version of toxval in which the data is altered.
#' @param sourcedb The source database name
#' @param source The source to be fixed
#' @param subsource The subsource to be fixed (NULL default)
#' @param reset If TRUE, reset all values to 'pass' before setting
#' @return The database will be altered
#' @export
#--------------------------------------------------------------------------------------
fix.qc_status.by.source <- function(toxval.db, source.db, source=NULL, subsource=NULL, reset=FALSE){
  printCurrentFunction(paste(toxval.db,":", source, subsource))
  slist = source
  if(is.null(source)) slist = runQuery("select distinct source from toxval",toxval.db)[,1]

  # Handle addition of subsource for queries
  query_addition = ""
  if(!is.null(subsource)) {
    query_addition = paste0(" and subsource='", subsource, "'")
  }

  # Check for qc_status_fail dictionary
  qc_status_fail_dict = tibble::tibble(
    source = character(),
    subsource = character(),
    source_hash = character(),
    failure_reason = character()
  )
  dict_file = paste0(toxval.config()$datapath, "dictionary/qc_status_fail_by_source_hash.xlsx")
  if(file.exists(dict_file)) {
    cat("Reading qc_status updates from dictionary...\n")
    qc_status_fail_dict = readxl::read_xlsx(dict_file)
  }

  for(source in slist) {
    cat(source,"\n")
    if(reset) {
      src_tbl = runQuery(paste0("SELECT distinct source_table FROM toxval WHERE source = '",
                                source, "'", query_addition), toxval.db) %>%
        dplyr::pull(source_table)

      if(any(src_tbl %in% c("direct load", "direct_load"))) {
        runQuery(paste0("update toxval set qc_status='pass' where source like '",source,"'",query_addition) ,toxval.db)
        next
      }

      for(s_tbl in src_tbl){
        cat("...resetting ", source, s_tbl, "\n")
        # Reset to status from source table
        paste0("UPDATE ", toxval.db, ".toxval a ",
               "INNER JOIN ", source.db, ".", s_tbl, " b ",
               "ON (a.source_hash = b.source_hash) ",
               "SET a.qc_status = b.qc_status ",
               "WHERE a.source = '",
               source, "'", query_addition) %>%
          runQuery(., toxval.db)
      }
    }

    # Run through different Failure cases
    # Only append if not already present as a failure flag
    ############################################################################
    ### toxval_numeric cases
    ############################################################################
    runQuery(paste0("UPDATE toxval SET qc_status = CASE ",
                    "WHEN qc_status like '%toxval_numeric<0%' THEN qc_status ",
                    "WHEN qc_status like '%fail%' THEN CONCAT(qc_status, '; toxval_numeric<0') ",
                    "ELSE 'fail:toxval_numeric<0'",
                    "END ",
                    "WHERE toxval_numeric<=0 and source = '",source,"'",query_addition) ,toxval.db)

    runQuery(paste0("UPDATE toxval SET  qc_status = CASE ",
                    "WHEN qc_status like '%toxval_numeric is null%' THEN qc_status ",
                    "WHEN qc_status like '%fail%' THEN CONCAT(qc_status, '; toxval_numeric is null') ",
                    "ELSE 'fail:toxval_numeric is null'",
                    "END ",
                    "WHERE toxval_numeric is null and source = '",source,"'",query_addition) ,toxval.db)
    ############################################################################
    ### toxval_type cases
    ############################################################################
    runQuery(paste0("UPDATE toxval SET  qc_status = CASE ",
                    "WHEN qc_status like '%toxval_type not specified%' THEN qc_status ",
                    "WHEN qc_status like '%fail%' THEN CONCAT(qc_status, '; toxval_type not specified') ",
                    "ELSE 'fail:toxval_type not specified'",
                    "END ",
                    "WHERE toxval_type = '-' and source = '",source,"'",query_addition) ,toxval.db)

    runQuery(paste0("UPDATE toxval SET  qc_status = CASE ",
                    "WHEN qc_status like '%Do not have the relative chemical identifier in the record for toxval_type%' THEN qc_status ",
                    "WHEN qc_status like '%fail%' THEN CONCAT(qc_status, '; Do not have the relative chemical identifier in the record for toxval_type') ",
                    "ELSE 'fail:Do not have the relative chemical identifier in the record for toxval_type'",
                    "END ",
                    "WHERE (toxval_type_original in ('RPF', 'TEF') OR toxval_type in ('RPF', 'TEF')) and source = '",
                    source,"'",query_addition) ,toxval.db)

    runQuery(paste0("UPDATE toxval SET  qc_status = CASE ",
                    "WHEN qc_status like '%toxval_type out of scope for ToxValDB%' THEN qc_status ",
                    "WHEN qc_status like '%fail%' THEN CONCAT(qc_status, '; toxval_type out of scope for ToxValDB') ",
                    "ELSE 'fail:toxval_type out of scope for ToxValDB'",
                    "END ",
                    "WHERE (toxval_type_original in ('soil saturation limit') OR toxval_type in ('soil saturation limit')) and source = '",
                    source,"'",query_addition) ,toxval.db)

    ############################################################################
    ### toxval_units cases
    ############################################################################
    runQuery(paste0("UPDATE toxval SET  qc_status = CASE ",
                    "WHEN qc_status like '%toxval_units out of scope for ToxValDB%' THEN qc_status ",
                    "WHEN qc_status like '%fail%' THEN CONCAT(qc_status, '; toxval_units out of scope for ToxValDB') ",
                    "ELSE 'fail:toxval_units out of scope for ToxValDB'",
                    "END ",
                    "WHERE (toxval_units_original in ('gamma') OR toxval_units in ('gamma')) and source = '",
                    source,"'",query_addition) ,toxval.db)

    runQuery(paste0("UPDATE toxval SET  qc_status = CASE ",
                    "WHEN qc_status like '%Ambiguous toxval_units%' THEN qc_status ",
                    "WHEN qc_status like '%fail%' THEN CONCAT(qc_status, '; Ambiguous toxval_units') ",
                    "ELSE 'fail:Ambiguous toxval_units'",
                    "END ",
                    "WHERE (toxval_units_original in ('g/kg', 'ug/mg/kg') OR toxval_units in ('g/kg', 'ug/mg/kg')) and source = '",
                    source,"'",query_addition) ,toxval.db)

    runQuery(paste0("UPDATE toxval SET  qc_status = CASE ",
                    "WHEN qc_status like '%toxval_units not specified%' THEN qc_status ",
                    "WHEN qc_status like '%fail%' THEN CONCAT(qc_status, '; toxval_units not specified') ",
                    "ELSE 'fail:toxval_units not specified'",
                    "END ",
                    "WHERE toxval_units = '-' and source = '",source,"'",query_addition) ,toxval.db)

    runQuery(paste0("UPDATE toxval SET  qc_status = CASE ",
                    "WHEN qc_status like '%species Unspecified%' THEN qc_status ",
                    "WHEN qc_status like '%fail%' THEN CONCAT(qc_status, '; species Unspecified') ",
                    "ELSE 'fail:species Unspecified'",
                    "END ",
                    # species_id for "Unspecified"
                    "WHERE species_id in (1000000) ",
                    "and source = '",source,"'",query_addition) ,toxval.db)

    ############################################################################
    ### dtxsid cases
    ############################################################################
    runQuery(paste0("UPDATE toxval SET  qc_status = CASE ",
                    "WHEN qc_status like '%dtxsid not specified%' THEN qc_status ",
                    "WHEN qc_status like '%fail%' THEN CONCAT(qc_status, '; dtxsid not specified') ",
                    "ELSE 'fail:dtxsid not specified'",
                    "END ",
                    "WHERE dtxsid = 'NODTXSID' and source = '",source,"'",query_addition) ,toxval.db)

    ############################################################################
    ### human_eco cases
    ############################################################################
    runQuery(paste0("UPDATE toxval SET  qc_status = CASE ",
                    "WHEN qc_status like '%human_eco not specified%' THEN qc_status ",
                    "WHEN qc_status like '%fail%' THEN CONCAT(qc_status, '; human_eco not specified') ",
                    "ELSE 'fail:human_eco not specified'",
                    "END ",
                    "WHERE human_eco in ('-','not specified') and source = '",source,"'",query_addition) ,toxval.db)

    runQuery(paste0("UPDATE toxval SET  qc_status = CASE ",
                    "WHEN qc_status like '%Eco data out of scope for ToxValDB%' THEN qc_status ",
                    "WHEN qc_status like '%fail%' THEN CONCAT(qc_status, '; Eco data out of scope for ToxValDB') ",
                    "ELSE 'fail:Eco data out of scope for ToxValDB'",
                    "END ",
                    "WHERE human_eco = 'eco' and source = '",source,"'",query_addition) ,toxval.db)

    ############################################################################
    ### source specific cases
    ############################################################################
    if(source == "DOE Wildlife Benchmarks"){
      runQuery(paste0("UPDATE toxval SET  qc_status = CASE ",
                      "WHEN qc_status like '%Not an experimental record%' THEN qc_status ",
                      "WHEN qc_status like '%fail%' THEN CONCAT(qc_status, '; Not an experimental record') ",
                      "ELSE 'fail:Not an experimental record'",
                      "END ",
                      "WHERE source = 'DOE Wildlife Benchmarks' AND ",
                      "experimental_record in ('not experimental', 'no', 'No')",
                      query_addition), toxval.db)
    }

    ############################################################################
    ### study_type cases
    ############################################################################
    runQuery(paste0("UPDATE toxval SET  qc_status = CASE ",
                    "WHEN qc_status like '%study_type out of scope%' THEN qc_status ",
                    "WHEN qc_status like '%fail%' THEN CONCAT(qc_status, '; study_type out of scope') ",
                    "ELSE 'fail:study_type out of scope' ",
                    "END ",
                    "WHERE (study_type_original in ('in vitro') or ",
                    "study_type in ('in vitro')) ",
                    "and source = '", source, "'", query_addition),
             toxval.db)

    # runQuery(paste0("update toxval set qc_status='fail:toxval_numeric<0' where toxval_numeric<=0 and source = '",source,"'",query_addition) ,toxval.db)
    # runQuery(paste0("update toxval set qc_status='fail:toxval_numeric is null' where toxval_numeric is null and source = '",source,"'",query_addition) ,toxval.db)
    # runQuery(paste0("update toxval set qc_status='fail:toxval_type not specified' where toxval_type='-' and source = '",source,"'",query_addition) ,toxval.db)
    # runQuery(paste0("update toxval set qc_status='fail:toxval_units not specified' where toxval_units='-' and source = '",source,"'",query_addition),toxval.db)
    # runQuery(paste0("update toxval set qc_status='fail:dtxsid not specified' where dtxsid='NODTXSID' and source = '",source,"'",query_addition),toxval.db)
    # runQuery(paste0("update toxval set qc_status='fail:human_eco not specified' where human_eco in ('-','not specified') and source = '",source,"'",query_addition),toxval.db)
    # runQuery(paste0("update toxval set qc_status='fail:risk_assessment_class not specified' where risk_assessment_class in ('-','not specified') and source = '",source,"'",query_addition),toxval.db)

    ### Handle dictionary source_hash values to fail with reason note
    # Get qc_status for all current source_hashes
    curr_hash_status = runQuery(paste0("SELECT DISTINCT source_hash, qc_status FROM toxval WHERE source='", source, "'",query_addition),
                                toxval.db)

    # Join dictionary entries with toxval entries
    dict_update_queries = qc_status_fail_dict %>%
      dplyr::inner_join(curr_hash_status, by=c("source_hash")) %>%
      dplyr::mutate(
        # Add new qc_status value
        qc_status = dplyr::case_when(
          grepl("\\bfail\\b", qc_status, ignore.case=TRUE) ~ stringr::str_c(qc_status, "; ", failure_reason),
          TRUE ~ stringr::str_c("fail:", failure_reason)
        ),

        # Use qc_status value to generate query
        status_query = stringr::str_c('UPDATE toxval SET qc_status = CASE ',
                                      'WHEN qc_status like "%',failure_reason,'%" THEN qc_status ',
                                      'ELSE "', qc_status,
                                      '" END WHERE source_hash="', source_hash, '"')
      ) %>%
      tidyr::drop_na(status_query) %>%
      # Pull list of new queries
      dplyr::pull(status_query) %>%
      unique()

    # Set qc_status based on dictionary
    for(status_query in dict_update_queries) {
      runQuery(status_query, toxval.db)
    }
  }
}
