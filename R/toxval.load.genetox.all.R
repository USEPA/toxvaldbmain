#-------------------------------------------------------------------------------------
#' Load the Genetox data from Grace
#' @param toxval.db The database to use.
#' @param verbose If TRUE output debug information
#' @param do.read If TRUE, read in the DSSTox file
#' @export
#--------------------------------------------------------------------------------------
toxval.load.genetox.all <- function(toxval.db, source.db,sys.date="2021-09-10", verbose=FALSE) {
  printCurrentFunction(toxval.db)

  runQuery("delete from genetox_details",db)
  file <- paste0(toxval.config()$datapath,"genetox/dataprep/combined_genetox_standard_",sys.date,".xlsx")
  mat = read.xlsx(file)
  x1 = names(mat)
  x2 = runQuery("desc genetox_details",db)[,1]
  x3 = x1[!is.element(x1,x2)]
  print(x3)
  mat = mat[!is.na(mat[,"casrn"]),]
  res = source_chemical.extra(toxval.db,source.db,mat,"Genetox Details")
  res2 = subset(res,select=-c(casrn,name,chemical_index))
  n1 = names(res2)
  n2 = runQuery("desc genetox_details",toxval.db)[,1]
  n3 = n1[is.element(n1,n2)]
  res2 = res2[,n3]
  runInsertTable(res2, "genetox_details", toxval.db,verbose)

  runQuery("delete from genetox_summary",db)
  file <- paste0(toxval.config()$datapath,"genetox/dataprep/genetox_summary_",sys.date,".xlsx")
  mat = read.xlsx(file)
  mat = mat[!is.na(mat[,"casrn"]),]
  res = source_chemical.extra(toxval.db,source.db,mat,"Genetox Summary")
  res2 = subset(res,select=-c(casrn,name,chemical_index))
  runInsertTable(res2, "genetox_summary", toxval.db)
}
