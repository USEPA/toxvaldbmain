#-------------------------------------------------------------------------------------
#' Trim leading and trailing blanks from all character columns
#' @param toxval.db The version of toxval in which the data is altered.
#' @param source The source to be fixed. If source=NULL, fix all sources
#' @param fill.toxval_fix If TRUE (default) read the dictionaries into the toxval_fix table
#' @return The database will be altered
#' @export
#-------------------------------------------------------------------------------------
fix.trim_spaces <- function(res) {
  # printCurrentFunction()
  # x=sapply(res,class)
  # y=x[x=="character"]
  # clist = names(y)
  # for(col in clist) res[,col] = str_trim(res[,col],side="both")
  # return(res)

  res <- res %>%
    dplyr::mutate(dplyr::across(tidyselect::where(is.character), ~stringr::str_squish(.)))
}
