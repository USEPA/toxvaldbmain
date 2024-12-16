#--------------------------------------------------------------------------------------
#' Generate summary statistics on the toxval database
#'
#' @param toxval.db The version of toxval into which the tables are loaded.
#' @param export Boolean whether to export a file. Default FALSE.
#' @export
#' @return DataFrame of record qc_status summary by source.
#'
#--------------------------------------------------------------------------------------
toxval.summary.stats <- function(toxval.db, export=FALSE) {
  # printCurrentFunction(toxval.db)

  # Get chemical DTXSID (represented by chemical_id) and record count
  res = runQuery(paste0("SELECT source, count(distinct chemical_id) as chemicals, count(*) as `total records` ",
                        "FROM toxval GROUP BY source"),
                 toxval.db)

  # Query summarize all source qc statuses
  query = paste0("SELECT source, qc_status, count(*) as n FROM toxval GROUP BY source, qc_status")
  qc_stats = runQuery(query, toxval.db) %>%
    # Split failure reason lists
    dplyr::mutate(qc_status = qc_status %>%
                    gsub(";", ";fail:", .),
                  # Handle special case for critical_effect categorization failures
                  # https://stackoverflow.com/questions/47296616/remove-everything-after-a-character-but-keep-the-character
                  qc_status = dplyr::case_when(
                    grepl("critical_effect categorization", qc_status) ~
                      sub('(critical_effect categorization).*', '\\1', qc_status),
                    TRUE ~ qc_status
                  )) %>%
    tidyr::separate_rows(qc_status, sep = ";") %>%
    dplyr::mutate(qc_status = qc_status %>%
                    stringr::str_squish() %>%
                    gsub(": ", ":", .)) %>%
    dplyr::group_by(source, qc_status) %>%
    # Summarize split groups
    dplyr::summarise(n = sum(n)) %>%
    dplyr::ungroup() %>%
    # Convert to column headers
    tidyr::pivot_wider(id_cols = c("source"), names_from = qc_status, values_from = n)

  # Combine
  out = res %>%
    dplyr::left_join(qc_stats,
                     by="source") %>%
    # Summarize pass percentage
    dplyr::mutate(dplyr::across(dplyr::everything(), ~replace(., is.na(.), 0)),
                  `pass percent` = round(((pass + `not determined`)/ `total records`) * 100, 3)) %>%
    # Reorder columns
    dplyr::select(order(colnames(.), decreasing = TRUE)) %>%
    dplyr::select(source, chemicals, `total records`, pass,
                  `not determined`, `pass percent`, dplyr::everything())

  if(export){
    file = paste0(toxval.config()$datapath,"export/source_count ",Sys.Date(),".xlsx")
    sty = openxlsx::createStyle(halign="center",valign="center",textRotation=90,textDecoration = "bold")
    openxlsx::write.xlsx(out,file,firstRow=T,headerStyle=sty)
  }

  return(out)
}
