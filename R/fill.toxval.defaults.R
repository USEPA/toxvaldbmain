#--------------------------------------------------------------------------------------
#' Set Toxval Defaults
#'
#' @param toxval.db The version of toxval from which to set defaults.
#' @param mat An input matrix of data
#' @return The data matrix afer fixing
#' @export
#--------------------------------------------------------------------------------------
fill.toxval.defaults <- function(toxval.db,mat){
  defs = runQuery("desc toxval",toxval.db)
  defs = defs[,c(1,5)]
  for(i in 1:length(mat)){
    if (names(mat)[i] %in% defs$Field){
      mat[is.na(mat[,i]),i] = defs[defs$Field==names(mat)[i],2]
    }
  }
  name.list <- names(mat)
  name.list[is.element(name.list,"species")] <- "species_original"
  names(mat) <- name.list
  return(mat)
}
