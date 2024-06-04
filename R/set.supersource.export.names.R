#--------------------------------------------------------------------------------------
#' Populate export_source_name and supersource fields in ToxVal
#'
#' @param toxval.db The version of toxval from which to set defaults.
#' @param source The source to be fixed (if NULL then edit all sources)
#' @param version_date The date of the source_info dictionary to be used
#' @export
#--------------------------------------------------------------------------------------
set.supersource.export.names <- function(toxval.db, source=NULL, version_date="2024-05-31"){
  printCurrentFunction()
  # Read source_info dictionary
  file = paste0(toxval.config()$datapath,"dictionary/source_info ", version_date, ".xlsx")
  source_info = readxl::read_xlsx(file)

  # Get list of sources if NULL source provided
  slist = source
  if(is.null(source)) {
    slist = runQuery("select distinct source from toxval", toxval.db)$source
  }

  for(src in slist) {
    # Get relevant data for current source
    src_data = source_info %>%
      dplyr::filter(source == !!src) %>%
      dplyr::select(source, export_source_name, supersource) %>%
      dplyr::distinct()

    # Check for multiple mappings
    if(nrow(src_data) > 1) {
      cat("Multiple mappings exist for", src, "export_source_name or supersource.\n")
      View(src_data)
      browser()
    }

    export_source_name = src_data %>%
      dplyr::pull(export_source_name) %>%
      .[[1]]
    supersource = src_data %>%
      dplyr::pull(supersource) %>%
      .[[1]]
    cat("source:", src, "| export_source_name:", export_source_name, "| supersource:", supersource, "\n")

    # Push export_source_name and supersource values to ToxVal
    query = paste0("UPDATE toxval SET ",
                   "export_source_name='", export_source_name, "', ",
                   "supersource='", supersource, "' ",
                   "WHERE source='", src, "'")
    runQuery(query, toxval.db)
  }
}
