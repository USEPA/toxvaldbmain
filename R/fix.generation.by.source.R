#-------------------------------------------------------------------------------------
#' Alter the contents of toxval according to an excel dictionary file with field generation
#' @param toxval.db The version of toxval in which the data is altered.
#' @param source The source to be processed
#' @param subsource The subsource to be processed (NULL default)
#' @return The database will be altered
#' @export
#-------------------------------------------------------------------------------------
fix.generation.by.source <- function(toxval.db, source, subsource=NULL) {
  printCurrentFunction(paste(toxval.db,":", source))

  file <- paste0(toxval.config()$datapath,"dictionary/2021_dictionaries/generation_5.xlsx")
  full_dict <- read.xlsx(file)
  full_dict$field <- "generation"
  colnames <- c("term_final","term_original","field")

  # Handle addition of subsource for queries
  query_addition = ""
  if(!is.null(subsource)) {
    query_addition = paste0(" and subsource='", subsource, "'")
  }

  for(i in seq_len(nrow(full_dict))) {
    original <- full_dict[i,2]
    final <- full_dict[i,1]
    field <- full_dict[i,3]
    query <- paste0("update toxval set ",field,"=\"",final,"\" where ",field,"=\"",original,"\" and source like '",source,"'",query_addition)
    runInsert(query,toxval.db,T,F,T)
  }
  query <- paste0("update toxval set ",field,"='-' where ",field," is NULL and source like '",source,"'",query_addition)
  runInsert(query,toxval.db,T,F,T)
}
