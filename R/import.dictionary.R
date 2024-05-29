#--------------------------------------------------------------------------------------
#'
#' import the toxval and toxval_type dictionaries
#'
#' @param toxval.db The name of the database
#--------------------------------------------------------------------------------------
import.dictionary <- function(toxval.db) {
  printCurrentFunction(toxval.db)
  runInsert("delete from toxval_type_dictionary",toxval.db)

  file <- paste0(toxval.config()$datapath,"dictionary/toxval_type_dictionary_5.xlsx")
  mat <- read.xlsx(file)
  mat[is.na(mat)] <- "-"
  # the following is stupid code to deal with invisible utf8 code
  for(i in 1:dim(mat)[1]) {
    for(j in 2:dim(mat)[2]) {
      #cat(i,j,mat[i,j],"\n")
      Encoding(mat[i,j]) <- "latin1"
    }
  }
  runInsertTable(mat,"toxval_type_dictionary",toxval.db,do.halt=T,verbose=T)

  runInsert("delete from toxval_dictionary",toxval.db)

  file <- paste0(toxval.config()$datapath,"dictionary/toxval_dictionary 2021.xlsx")
  mat <- read.xlsx(file)
  runInsertTable(mat,"toxval_dictionary",toxval.db,do.halt=T,verbose=T)
}
