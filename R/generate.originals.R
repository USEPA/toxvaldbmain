#--------------------------------------------------------------------------------------
#' Duplicate any columns with '_original'
#' Set Toxval Defaults
#' @param toxval.db The version of toxval from which to set defaults.
#' @param mat THe matrix of data to be altered
#' @return The altered input matrix
#' @export
#--------------------------------------------------------------------------------------
generate.originals <- function(toxval.db,mat){
  defs = runQuery("desc toxval",toxval.db)[,1]
  defs = grep("_original",defs,value=TRUE)
  defs = gsub("_original","",defs)
  targets = which(colnames(mat) %in% defs)
  for(i in targets){
    newname <- paste0(names(mat)[i],"_original")
    if(!is.element(newname,names(mat))) {
      mat[,length(mat)+1] = mat[,i]
      names(mat)[length(mat)] = newname
    }
  }
  return(mat)
}
