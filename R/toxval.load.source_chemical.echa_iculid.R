#-------------------------------------------------------------------------------------
#'
#' Perform the DSSTox mapping
#'
#' @param toxval.db The version of toxvaldb to use.
#' @param source.db The source database version
#' @param source The source to update for
#' @param verbose If TRUE, print out extra diagnostic messages
#' @param chem_source The source_table name - this is the soruce in chemical source
#-------------------------------------------------------------------------------------
toxval.load.source_chemical.echa_iuclid <- function(toxval.db,source.db,source="ECHA IUCLID",verbose=T,chem_source) {
  printCurrentFunction(paste(toxval.db,":", source,":",chem_source))

  if(!exists("DSSTOX")) load.dsstox()
  dsstox = DSSTOX

  cat("------------------------------------------------------\n")
  cat(source,chem_source,"\n")
  cat("------------------------------------------------------\n")
  cat("pull data from the source database\n")
  runQuery(paste0("delete from source_chemical where source='",chem_source,"'"),toxval.db)
  chems = runQuery(paste0("select * from source_chemical where source='",chem_source,"'"),source.db)
  runInsertTable(chems, "source_chemical", toxval.db)
  count = runQuery(paste0("select count(*) from source_chemical where (dtxsid is null or (dtxsid in ('-','','NODTXSID'))) and source='",chem_source,"'"),source.db)[1,1]
  clist = chems$casrn
  dtemp = dsstox[clist,]
  clist = dtemp$casrn
  if(count>0) {
    cat("set dtxsid, name from missing chemicals",chem_source,":",count,"\n")
    chems1 = runQuery(paste0("select chemical_id,cleaned_casrn as casrn,cleaned_name as name from source_chemical where dtxsid is null and source='",chem_source,"'"),toxval.db)
    chems2 = runQuery(paste0("select chemical_id,cleaned_casrn as casrn,cleaned_name as name from source_chemical where dtxsid ='NODTXSID' and source='",chem_source,"'"),toxval.db)
    chems3 = runQuery(paste0("select chemical_id,cleaned_casrn as casrn,cleaned_name as name from source_chemical where dtxsid ='-' and source='",chem_source,"'"),toxval.db)
    chems4 = runQuery(paste0("select chemical_id,cleaned_casrn as casrn,cleaned_name as name from source_chemical where dtxsid ='' and source='",chem_source,"'"),toxval.db)
    chems = unique(rbind(chems1,chems2,chems3,chems4))
    names(chems) = c("chemical_id","casrn","name")
    for(i in 1:nrow(chems)) {
      casrn = chems[i,"casrn"]
      name = chems[i,"name"]
      name = stri_escape_unicode(stri_enc_toutf8(name))
      cid = chems[i,"chemical_id"]
      dtxsid = "NODTXSID"
      if(is.element(casrn,clist)) {
        name = dtemp[casrn,"preferred_name"]
        name = str_replace_all(name,"\\'","")
        #cat(name,"\n")
        name = stri_escape_unicode(stri_enc_toutf8(name))
        dtxsid = dtemp[casrn,"dtxsid"]
      }
      query = paste0("update source_chemical set casrn='",casrn,"', name='",name,"', dtxsid='",dtxsid,"' where chemical_id='",cid,"' and source='",chem_source,"'")
      runQuery(query,toxval.db)
      runQuery(query,source.db)
      if(verbose) if(i%%500==0) cat("  (A) chemicals updated:",i," out of ",nrow(chems),"\n")
    }
    #}
  }
  cat("set dtxsid in the toxval table\n")
  runQuery(paste0("update source_chemical set dtxsid='NODTXSID' where dtxsid='-' and source='",chem_source,"'"),toxval.db)
  runQuery(paste0("update toxval set dtxsid='NODTXSID' where source='",chem_source,"'"),toxval.db)
  chems = runQuery(paste0("select chemical_id,dtxsid from source_chemical where source='",chem_source,"'"),toxval.db)
  for(i in 1:nrow(chems)) {
    chemical_id = chems[i,"chemical_id"]
    dtxsid = chems[i,"dtxsid"]
    query = paste0("update toxval set dtxsid='",dtxsid,"' where chemical_id='",chemical_id,"'")
    runQuery(query,toxval.db)
    if(verbose) if(i%%500==0) cat("  (B) chemicals updated:",i," out of ",nrow(chems),"\n")
  }
}
