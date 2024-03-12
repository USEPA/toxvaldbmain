#-------------------------------------------------------------------------------------
#' Sets experimental flag by source for records in toxval
#' @param toxval.db The version of toxval into which the tables are loaded.
#' @param source Name of source to set. Default NULL means set experimental record for all sources
#' @return None. SQL update statement is performed
#' @export
#-------------------------------------------------------------------------------------
set_experimental_record_flag_by_source <- function(toxval.db, source=NULL){
  printCurrentFunction(toxval.db)
  if(!is.null(source)) {
    slist = source
  } else {
    slist = runQuery("select distinct source from toxval",toxval.db)[,1]
  }
  file = paste0(toxval.config()$datapath,"dictionary/toxval_source_type_species_20240225.xlsx")
  exp_rec_tag = read.xlsx(file)

  res <- lapply(slist, function(source) {
    dplyr::filter(exp_rec_tag, source == source)
  })
  res <- res[[1]]
  res <- subset(res, select = -c(`X1`, `cancer`, `dev/repro`, `X9`))

  if(nrow(res)){
    # Query to inner join and update toxval with experimental_record tag (temp table added/dropped)
    updateQuery = paste0("UPDATE toxval a INNER JOIN z_updated_df b
                        ON (a.source = b.source AND a.toxval_type = b.toxval_type
                        AND a.species_original = b.species_original AND a.source_url = b.source_url)
                       SET a.experimental_record = b.experiment")
    # Run update query
    runUpdate(table="toxval", updateQuery=updateQuery, updated_df=res, db=toxval.db)
  }
}
