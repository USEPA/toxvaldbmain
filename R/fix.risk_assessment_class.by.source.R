#-------------------------------------------------------------------------------------
#' Set the risk assessment class of toxval according to an excel dictionary.
#' Values may beset multiple times, so the excel sheet should be ordered so that
#' the last ones to be set are last
#'
#' @param toxval.db The version of toxval in which the data is altered.
#' @param source The source to be updated
#' @param subsource The subsource to be updated (NULL default)
#' @param restart If TRUE, delete all values and start from scratch
#' @param report.only Whether to report or write/export data. Default is FALSE (write/export data)
#' @export
#--------------------------------------------------------------------------------------
fix.risk_assessment_class.by.source <- function(toxval.db, source=NULL, subsource=NULL, restart=TRUE, report.only=FALSE) {
  printCurrentFunction(paste(toxval.db,":", source, subsource))

  # Get list of sources to use
  slist = source
  if(is.null(source)) {
    slist = runQuery("select distinct source from toxval",toxval.db) %>%
      dplyr::pull(source)
  }

  # Collapse into list of sources for query filtering
  source_string = slist %>%
    paste0(collapse="', '")

  # Handle addition of subsource for queries
  query_addition = ""
  if(!is.null(subsource)) {
    query_addition = paste0(query_addition, " and subsource='", subsource, "'")
  }

  # Read in RAC dictionary
  rac_dict = readxl::read_xlsx("Repo/dictionary/RAC_toxval_type_dict.xlsx") %>%
    dplyr::distinct() %>%
    dplyr::mutate(dplyr::across(dplyr::where(is.character), ~tidyr::replace_na(., "-")))

  # Split into subtype and no subtype dictionaries
  rac_no_subtype = rac_dict %>%
    dplyr::filter(toxval_subtype == "-")
  rac_subtype = rac_dict %>%
    dplyr::filter(toxval_subtype != "-")

  # Reset values if specified
  if(restart & !report.only) {
    query = paste0("UPDATE toxval SET risk_assessment_class='-' WHERE source IN ('",source_string,"')",
                   query_addition)
    runQuery(query, toxval.db)
  }

  if(!report.only) {
    # Make changes that are based on subtype
    # There are limits to CASE WHEN conditional statement allowed sizes
    # https://stackoverflow.com/questions/1160459/sql-limit-on-case-number-of-when-then-conditions
    case_block = rac_subtype %>%
      dplyr::mutate(
        component = stringr::str_c(
          "WHEN toxval_type='", toxval_type, "' AND toxval_subtype LIKE '", toxval_subtype, "' ",
          "THEN '", risk_assessment_class, "'"
        )
      ) %>%
      dplyr::pull(component) %>%
      paste0(collapse=" ")

    # Prep update query
    query = paste0("UPDATE toxval SET risk_assessment_class = CASE ",
                   case_block, " ",
                   "ELSE risk_assessment_class END ",
                   "WHERE source IN ('", source_string, "') ",
                   "AND qc_status not like '%fail%' ",
                   query_addition)
    # Push update
    runQuery(query, toxval.db)

    # Pull non-toxval_subtype cases
    # Perform separate Bulk Update query due to limits on CASE WHEN statement sizes
    rac_update = runQuery(paste0("SELECT toxval_id, toxval_type FROM toxval WHERE ",
                                 "risk_assessment_class = '-' AND ",
                                 "source IN ('", source_string, "') ",
                                 query_addition),
                          toxval.db) %>%
      dplyr::left_join(rac_dict %>%
                         dplyr::select(toxval_type, risk_assessment_class),
                       by="toxval_type") %>%
      dplyr::filter(!is.na(risk_assessment_class))

    ##############################################################################
    ### Batch Update
    ##############################################################################
    batch_size <- 50000
    startPosition <- 1
    endPosition <- nrow(rac_update)
    incrementPosition <- batch_size

    while(startPosition <= endPosition){
      if(incrementPosition > endPosition) incrementPosition = endPosition
      message("...Inserting new data in batch: ", batch_size, " startPosition: ", startPosition," : incrementPosition: ", incrementPosition,
              " (",round((incrementPosition/endPosition)*100, 3), "%)", " at: ", Sys.time())

      update_query <- paste0("UPDATE toxval a ",
                             "INNER JOIN z_updated_df b ",
                             "ON (a.toxval_id = b.toxval_id) ",
                             "SET a.risk_assessment_class = b.risk_assessment_class ",
                             "WHERE a.toxval_id in (",toString(rac_update$toxval_id[startPosition:incrementPosition]),")",
                             query_addition %>%
                               # Add query table stems
                               gsub("subsource", "a.subsource", .) %>%
                               gsub("qc_status", "a.qc_status", .) %>%
                               gsub("human_eco", "a.human_eco", .))

      runUpdate(table="toxval",
                updateQuery = update_query,
                updated_df = rac_update[startPosition:incrementPosition,],
                db=toxval.db)

      startPosition <- startPosition + batch_size
      incrementPosition <- startPosition + batch_size - 1
    }

    # Get missing entries
    query = paste0("SELECT * FROM toxval WHERE risk_assessment_class='-' ",
                   "AND source IN ('", source_string, "') ",
                   "AND qc_status not like '%fail%' ",
                   query_addition)
    missing_rac = runQuery(query, toxval.db)

    # Get list of toxval_type with dictionary entries
    missing_check = rac_dict %>%
      dplyr::select(toxval_type) %>%
      dplyr::distinct() %>%
      dplyr::mutate(missing_toxval_type_dict_entry = 0)

    # Filter to those missing RAC and missing a toxval_type RAC dictionary entry
    missing_rac = missing_rac %>%
      dplyr::left_join(missing_check, by=c("toxval_type")) %>%
      dplyr::mutate(missing_toxval_type_dict_entry = missing_toxval_type_dict_entry %>% tidyr::replace_na(1)) %>%
      dplyr::filter(missing_toxval_type_dict_entry == 1)

    for(source in missing_rac %>% dplyr::pull(source) %>% unique()) {
      # Generate filename
      out_file = paste0("dictionary/missing/missing_rac/missing_RAC_", source,
                        " ", subsource,".xlsx") %>%
        gsub(" \\.xlsx", ".xlsx", .)
      # Filter to current source
      curr_missing = missing_rac %>%
        dplyr::filter(source == !!source_string)
      # Export for source
      writexl::write_xlsx(curr_missing, out_file)
    }
  } else {
    message("Need to implement report.only logic")
  }
}
