#--------------------------------------------------------------------------------------
#' Function for assigning QC Categories to sources in toxval via the qc_category field
#'
#' @param toxval.db The version of toxval into which the tables are loaded.
#' @param source The source to set a qc_category for
#' @export
#--------------------------------------------------------------------------------------
set.qc.category.by.source <- function(toxval.db, source=NULL){
  printCurrentFunction(toxval.db)
  if(!is.null(source)) {
    slist = source
  } else {
    slist = runQuery("select distinct source from toxval",toxval.db)[,1]
  }
  library(rvest)
  url <- "https://confluence.epa.gov/display/ToxValDB/Source+QC+Tracking+-+Prioritized+by+Chemical+Counts"
  for(source in slist) {
    webpage <- read_html(url)
    text_content <- html_text(webpage)
    tables <- html_table(webpage)

    # df %>%
    #   tidyr::unite(col="qc_category", qc_category, qc_category_new, sep = ", ", na.rm = TRUE) %>%
    #   dplyr::rowwise() %>%
    #   dplyr::mutate(qc_category = toString(unique(unlist(strsplit(qc_category,",\\s+")))))

  }
}
