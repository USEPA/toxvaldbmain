#--------------------------------------------------------------------------------------
#' Generate summary statistics on the toxval database
#'
#' @param toxval.db The version of toxval into which the tables are loaded.
#' @param export Boolean whether to export a file. Default FALSE.
#' @param summ_level String of "source" or "supersource" how to group the summary
#' @export
#' @return DataFrame of record qc_status summary by source.
#'
#--------------------------------------------------------------------------------------
toxval.summary.stats <- function(toxval.db, export=FALSE, summ_level = "source") {
  # printCurrentFunction(toxval.db)

  # Query summarize all source qc statuses in the database
  query = paste0("SELECT ", summ_level,", qc_status, count(*) as n FROM toxval GROUP BY ", summ_level, ", qc_status")
  qc_stats_db = runQuery(query, toxval.db)

  # Get qc_status fail logs
  outDir = paste0(toxval.config()$datapath, "export_qc_fail/", toxval.db)
  qc_stats_fail = list.files(outDir, full.names = TRUE)
  qc_stats_fail = lapply(qc_stats_fail, readxl::read_xlsx) %>%
    dplyr::bind_rows() %>%
    dplyr::select(dplyr::any_of(c(summ_level, "chemical_id", "qc_status")))

  # if(summ_level == "supersource"){
  #   supersource_map = runQuery("SELECT distinct supersource, source FROM toxval",
  #                              toxval.db)
  #
  #   # Add supersource column back in
  #   qc_stats_fail = qc_stats_fail %>%
  #     dplyr::left_join(supersource_map,
  #                      by = "source") %>%
  #     dplyr::select(supersource, dplyr::everything())
  # }

  # Combine and sum counts
  res_combined = runQuery(paste0("SELECT ", summ_level,", chemical_id FROM toxval"),
                 toxval.db) %>%
    dplyr::bind_rows(qc_stats_fail %>%
                       dplyr::select(dplyr::any_of(c(summ_level, "chemical_id"))))

  res = res_combined %>%
    dplyr::count(dplyr::across(summ_level), name = "total records") %>%
    dplyr::left_join(res_combined %>%
                       dplyr::distinct() %>%
                       dplyr::count(dplyr::across(summ_level), name = "chemicals"),
                     by = summ_level) %>%
    dplyr::arrange(dplyr::across(summ_level)) %>%
    dplyr::select(dplyr::any_of(c(summ_level, "chemicals", "total records")))

  # Summarize qc_status for fails
  qc_stats_fail = qc_stats_fail %>%
    dplyr::count(dplyr::across(c(summ_level, "qc_status")))

  # Combine and summarize
  qc_stats = qc_stats_db %>%
    dplyr::bind_rows(qc_stats_fail) %>%
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
    dplyr::group_by(dplyr::across(c(summ_level, "qc_status"))) %>%
    # Summarize split groups
    dplyr::summarise(n = sum(n)) %>%
    dplyr::ungroup() %>%
    # Convert to column headers
    tidyr::pivot_wider(id_cols = c(summ_level), names_from = qc_status, values_from = n)

  # Combine
  out = res %>%
    dplyr::left_join(qc_stats,
                     by=summ_level) %>%
    # Summarize pass percentage
    dplyr::mutate(dplyr::across(dplyr::everything(), ~replace(., is.na(.), 0)),
                  `pass percent` = round(((pass + `not determined`)/ `total records`) * 100, 1)) %>%
    # Reorder columns
    dplyr::select(order(colnames(.), decreasing = TRUE)) %>%
    dplyr::select(dplyr::all_of(c(summ_level, "chemicals", "total records", "pass",
                  "not determined", "pass percent")), dplyr::everything())

  if(export){
    file = paste0(toxval.config()$datapath,"export/source_count ",Sys.Date(),".xlsx")
    sty = openxlsx::createStyle(halign="center",valign="center",textRotation=90,textDecoration = "bold")
    openxlsx::write.xlsx(out,file,firstRow=T,headerStyle=sty)
  }

  return(out)
}
