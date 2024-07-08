#-------------------------------------------------------------------------------------
#' Fix the priority_id in the toxval table based on source
#' @param toxval.db The version of toxvaldb to use.
#' @param source The source to be fixed, If NULL, set for all sources
#' @param subsource The subsource to be fixed (NULL default)
#' @export
#-------------------------------------------------------------------------------------
fix.priority_id.by.source <- function(toxval.db, source=NULL, subsource=NULL) {
  printCurrentFunction(paste(toxval.db,":", source, subsource))

  slist = source
  if(is.null(source)) slist = runQuery("select distinct source from toxval",toxval.db)[,1]
  source_string = slist %>%
    paste0(., collapse="', '")

  # Handle addition of subsource for queries
  query_addition = ""
  if(!is.null(subsource)) {
    query_addition = paste0(" and subsource='", subsource, "'")
  }

  # Set appropriate priority ID in toxval using source_info table
  cat("Updating priority ID for:", gsub("', '", ", ", source_string), "\n")
  query = paste0("update toxval set priority_id=-1 where source in ('",source_string,"')",query_addition)
  runQuery(query,toxval.db)
  query = paste0("select priority_id from source_info where where source in ('",source_string,"')",query_addition)
  pid = runQuery(query,toxval.db)[1,1]
  query = paste0("update toxval set priority_id=",pid," where source in ('",source_string,"')",query_addition)
  runQuery(query,toxval.db)

  x = runQuery("select distinct source from toxval where priority_id=-1",toxval.db)
  if(nrow(x)>0) print(x)
  cat("Number of sources with missing priority_id:",nrow(x),"\n")
}
