#-------------------------------------------------------------------------------------
#' Fix the qa_status flag
#' @param toxval.db The version of toxval in which the data is altered.
#' @param source The source to be fixed
#' @param subsource The subsource to be fixed (NULL default)
#' @param reset If TRUE, reset all values to 'pass' before setting
#' @return The database will be altered
#' @export
#--------------------------------------------------------------------------------------
fix.qc_status.by.source <- function(toxval.db,source=NULL, subsource=NULL, reset=T){
  printCurrentFunction(paste(toxval.db,":", source,subsource))
  slist = source
  if(is.null(source)) slist = runQuery("select distinct source from toxval",toxval.db)[,1]
  cat("  set all to 'not classified'\n")

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
    if(reset) runQuery(paste0("update toxval set qc_status='pass' where source like '",source,"'",query_addition) ,toxval.db)

    runQuery(paste0("update toxval set qc_status='fail:toxval_numeric<0' where toxval_numeric<=0 and source = '",source,"'",query_addition) ,toxval.db)
    runQuery(paste0("update toxval set qc_status='fail:toxval_numeric is null' where toxval_numeric is null and source = '",source,"'",query_addition) ,toxval.db)
    runQuery(paste0("update toxval set qc_status='fail:toxval_type not specified' where toxval_type='-' and source = '",source,"'",query_addition) ,toxval.db)
    runQuery(paste0("update toxval set qc_status='fail:toxval_units not specified' where toxval_units='-' and source = '",source,"'",query_addition),toxval.db)
    #runQuery(paste0("update toxval set qc_status='fail:risk_assessment_class not specified' where risk_assessment_class='other' and source = '",source,"'"),toxval.db)
    runQuery(paste0("update toxval set qc_status='fail:dtxsid not specified' where dtxsid='NODTXSID' and source = '",source,"'",query_addition),toxval.db)
    #if(!is.element(source,"WHO IPCS")) runQuery(paste0("update toxval set qc_status='fail:species not specified' where species_id=1000000 and source = '",source,"'") ,toxval.db)
    runQuery(paste0("update toxval set qc_status='fail:human_eco not specified' where human_eco in ('-','not specified') and source = '",source,"'",query_addition),toxval.db)
    runQuery(paste0("update toxval set qc_status='fail:risk_assessment_class not specified' where risk_assessment_class in ('-','not specified') and source = '",source,"'",query_addition),toxval.db)

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
          TRUE ~ stringr::str_c("fail: ", failure_reason)
        ),

        # Use qc_status value to generate query
        status_query = stringr::str_c("UPDATE toxval SET qc_status='", qc_status,
                                      "' WHERE source_hash='", source_hash, "'")
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
