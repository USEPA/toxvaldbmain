#--------------------------------------------------------------------------------------
#
#' Delete a portion of the contents of the toxval database
#' @param toxval.db The version of toxval from which the data is deleted.
#' @param source The data source name
#' @return The database will be altered
#' @export
#-------------------------------------------------------------------------------------
clean.toxval.by.source <- function(toxval.db,source) {
  printCurrentFunction(paste(toxval.db,":",source))

  count = runQuery(paste0("select count(*) from toxval where source='",source,"'"),toxval.db)[1,1]
  if(count>0) {
    runQuery(paste0("delete from toxval_notes where toxval_id in (select toxval_id from toxval where source='",source,"')"),toxval.db)
    runQuery(paste0("delete from toxval_qc_notes where toxval_id in (select toxval_id from toxval where source='",source,"')"),toxval.db)
    runQuery(paste0("delete from record_source where toxval_id in (select toxval_id from toxval where source='",source,"')"),toxval.db)
    runQuery(paste0("delete from toxval_uf where toxval_id in (select toxval_id from toxval where source='",source,"')"),toxval.db)
    runQuery(paste0("delete from toxval_uf where parent_id in (select toxval_id from toxval where source='",source,"')"),toxval.db)
    runQuery(paste0("delete from toxval_relationship where toxval_id_1 in (select toxval_id from toxval where source='",source,"')"),toxval.db)
    runQuery(paste0("delete from toxval_relationship where toxval_id_2 in (select toxval_id from toxval where source='",source,"')"),toxval.db)
    runQuery(paste0("delete from toxval where source='",source,"'"),toxval.db)
  }
  runQuery(paste0("delete from source_chemical where source='",source,"'"),toxval.db)
}
