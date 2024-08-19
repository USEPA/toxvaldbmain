#--------------------------------------------------------------------------------------
#' Load Source Info for each source into toxval
#' The information is in the file ~/dictionary/source_info 2023-11-30.xlsx
#' @param toxval.db The version of toxval into which the source info is loaded.
#' @param source The specific source to be loaded, If NULL, load for all sources
#' @export
#--------------------------------------------------------------------------------------
import.source.info.by.source <- function(toxval.db, source=NULL) {
  printCurrentFunction(toxval.db)

  # Read latest source_info dictionary
  file = paste0(toxval.config()$datapath,"dictionary/source_info 2024-05-31.xlsx")
  print(file)
  mat = readxl::read_xlsx(file) %>% # openxlsx::read.xlsx(file)
    dplyr::filter(!retired == 1)
  cols = runQuery("desc source_info",toxval.db)[,1]
  mat = mat[, names(mat) %in% cols]

  slist = source
  if(is.null(source)) {
    slist = runQuery("select distinct source from toxval", toxval.db)$source
  }
  for(source in slist) {
    cat(source,"\n")
    # Output any sources missing source_info data
    if(!source %in% mat$source) {
      cat("source info missing for ",source,"\n")
      cat("add to file:",file,"\n")
      browser()
    }
    # Refresh source_info to be from latest dictionary
    runQuery(paste0("delete from source_info where source='",source,"'"), toxval.db)
    mat1 = mat[mat$source %in% source,]
    runInsertTable(mat1,"source_info",toxval.db,do.halt=T,verbose=T)
  }
}
