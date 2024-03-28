#--------------------------------------------------------------------------------------
#'
#' Fix species duplicates - same common name but multiple species_ids
#'
#' @param toxval.db The version of the database to use
#' @export
#--------------------------------------------------------------------------------------
fix.species.duplicates <- function(toxval.db) {
  printCurrentFunction()
  cat("Check for duplicates with same species_original but different species_ids\n")
  query = paste0("select a.species_original, b.species_id,b.common_name from
                   toxval a, species b where
                   a.species_id=b.species_id
                   and a.human_eco='human health'")
  mat = unique(runQuery(query,toxval.db))
  mat = mat[order(mat$species_original),]
  x = mat$species_original
  dups = x[duplicated(x)]
  cat("Number of duplicates:",length(dups),"\n")
  if(length(dups)>0) {
    for(i in 1:length(dups)) {
      spo = dups[i]
      y = mat[is.element(mat$species_original,spo),]
      y = y[order(y$species_id),]
      if(is.element("Not Specified",y$common_name) || is.element("Not specified",y$common_name)) {
        y = y[!is.element(y$common_name,"Not Specified"),]
        y = y[!is.element(y$common_name,"Not specified"),]
      }
      cat(spo,":",nrow(y),"\n")
      query = paste0("update toxval set species_id=",y[1,"species_id"]," where species_original='",spo,"'")
      print(query)
      runQuery(query,toxval.db)
    }
  }
}
