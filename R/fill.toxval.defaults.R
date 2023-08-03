#--------------------------------------------------------------------------------------
#' Set Toxval Defaults
#'
#' @param toxval.db The version of toxval from which to set defaults.
#' @param mat An input matrix of data
#' @return The data matrix afer fixing
#' @export
#--------------------------------------------------------------------------------------
fill.toxval.defaults <- function(toxval.db,mat){
  defs = runQuery("desc toxval",toxval.db) %>%
    select(Field, Default)
  # defs = defs[,c(1,5)]
  # for(i in seq_along(names(mat))){
  #   if (names(mat)[i] %in% defs$Field){
  #     mat[is.na(mat[,i]),i] = defs[defs$Field==names(mat)[i],2]
  #   }
  # }
  for(field_name in names(mat)){
    if(field_name %in% defs$Field){
      if(nrow(mat[is.na(mat[[field_name]]), field_name])){
        mat[is.na(mat[[field_name]]), field_name] = defs$Default[defs$Field == field_name]
      }
    }
  }
  # Ensure species is renamed to species_original
  name.list <- names(mat)
  name.list[is.element(name.list,"species")] <- "species_original"
  names(mat) <- name.list
  return(mat)
}
