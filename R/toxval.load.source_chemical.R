#-------------------------------------------------------------------------------------
#'
#' Perform the DSSTox mapping
#'
#' @param toxval.db The version of toxvaldb to use.
#' @param source.db The source database version
#' @param source The source to update for
#' @param verbose If TRUE, print out extra diagnostic messages
#-------------------------------------------------------------------------------------
toxval.load.source_chemical <- function(toxval.db,source.db,source=NULL,verbose=T) {
  printCurrentFunction(paste(toxval.db,":", source))

  if(!exists("DSSTOX")) load.dsstox()
  dsstox = DSSTOX
  slist = runQuery("select distinct source from toxval",toxval.db)[,1]
  #slist = slist[!is.element(slist,c("ECOTOX","ToRefDB","ECHA IUCLID"))]
  slist = slist[!is.element(slist,c("ECOTOX","ToRefDB"))]
  slist = sort(slist)
  if(!is.null(source)) slist = source
  for(source in slist) {
    cat("------------------------------------------------------\n")
    cat(source,"\n")
    cat("------------------------------------------------------\n")
    cat("pull data from the source database\n")
    runQuery(paste0("delete from source_chemical where source='",source,"'"),toxval.db)
    chems = runQuery(paste0("select * from source_chemical where source='",source,"'"),source.db)
    # if(source=="IUCLID_iuclid_developmentaltoxicityteratogenicity") {
    #   chems[is.na(chems$casrn),"dtxsid"] = "NODTXSID"
    #   chems[is.na(chems$casrn),"casrn"] = chems[is.na(chems$casrn),"cleaned_casrn"]
    #   chems[is.na(chems$casrn),"casrn"] = "-"
    # }
    chems = fix.non_ascii.v2(chems,source)
    nlist = names(chems)
    nlist = nlist[!is.element(nlist,c("parent_chemical_id","chemical_id_rehash_orig"))]
    chems = chems[,nlist]
    runInsertTable(chems, "source_chemical", toxval.db)
    #count = runQuery(paste0("select count(*) from source_chemical where (dtxsid is null or (dtxsid in ('-','','NODTXSID'))) and source='",source,"'"),source.db)[1,1]
    count = runQuery(paste0("select count(*) from source_chemical where (dtxsid is null or (dtxsid in ('-','','NODTXSID'))) and source='",source,"'"),toxval.db)[1,1]
    clist = chems$casrn
    dtemp = dsstox[clist,]
    clist = dtemp$casrn
    clist = clist[!is.na(clist)]
    chems0 = chems
    if(count>0) {
      cat("set dtxsid, name from missing chemicals",source,":",count,"\n")
      chems1 = runQuery(paste0("select chemical_id,cleaned_casrn as casrn,cleaned_name as name from source_chemical where dtxsid is null and source='",source,"'"),toxval.db)
      chems2 = runQuery(paste0("select chemical_id,cleaned_casrn as casrn,cleaned_name as name from source_chemical where dtxsid ='NODTXSID' and source='",source,"'"),toxval.db)
      chems3 = runQuery(paste0("select chemical_id,cleaned_casrn as casrn,cleaned_name as name from source_chemical where dtxsid ='-' and source='",source,"'"),toxval.db)
      chems4 = runQuery(paste0("select chemical_id,cleaned_casrn as casrn,cleaned_name as name from source_chemical where dtxsid ='' and source='",source,"'"),toxval.db)
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
        query = paste0("update source_chemical set casrn='",casrn,"', name='",name,"', dtxsid='",dtxsid,"' where chemical_id='",cid,"' and source='",source,"'")
        runQuery(query,toxval.db)
        runQuery(query,source.db)
        if(verbose) if(i%%500==0) cat("  (A) chemicals updated:",i," out of ",nrow(chems),"\n")
      }
    }
    cat("set dtxsid in the toxval table\n")
    runQuery(paste0("update source_chemical set dtxsid='NODTXSID' where dtxsid='-' and source='",source,"'"),toxval.db)
    runQuery(paste0("update toxval set dtxsid='NODTXSID' where source='",source,"'"),toxval.db)
    chems = runQuery(paste0("select chemical_id,dtxsid from source_chemical where source='",source,"'"),toxval.db)
    for(i in 1:nrow(chems)) {
      chemical_id = chems[i,"chemical_id"]
      dtxsid = chems[i,"dtxsid"]
      query = paste0("update toxval set dtxsid='",dtxsid,"' where chemical_id='",chemical_id,"'")
      runQuery(query,toxval.db)
      if(verbose) if(i%%500==0) cat("  (B) chemicals updated:",i," out of ",nrow(chems),"\n")
    }
  }
}
