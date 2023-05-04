#-------------------------------------------------------------------------------------
#' Fix the study_type using manual curation
#' @param toxval.db The version of toxval in which the data is altered.
#' @return The database will be altered
#' @export
#--------------------------------------------------------------------------------------
fix.study_type.manual = function(toxval.db,source=NULL,sys.date="2023-05-04"){
  printCurrentFunction(toxval.db)
  file = paste0(toxval.config()$datapath,"dictionary/study_type/toxval_new_study_type ",toxval.db," ",sys.date,".xlsx")
  print(file)
  mat = read.xlsx(file)
  mat = mat[mat$dtxsid!='NODTXSID',]
  mat = mat[!is.na(mat$dtxsid),]
  mat = fix.trim_spaces(mat)

  slist = sort(unique(mat$source))

  if(!is.null(source)) slist = source
  for(source in slist) {
    temp0 = mat[is.element(mat$source,source),c("dtxsid","source","study_type_corrected","source_hash")]
    temp0 = unique(temp0)
    cat(source,nrow(temp0),"\n")
    temp0$key = paste(temp0$dtxsid,temp0$source,temp0$study_type_corrected,temp0$source_hash)
    temp = unique(temp0[,c("source_hash","study_type_corrected")])
    names(temp) = c("source_hash","study_type")

    temp.old = runQuery(paste0("select source_hash,study_type from toxval where dtxsid!='NODTXSID' and source='",source,
                            "' and toxval_type in (select toxval_type from toxval_type_dictionary where toxval_type_supercategory in ('Point of Departure','Lethality Effect Level','Toxicity Value'))
                            and human_eco='human health'"),toxval.db)

    temp$code = paste(temp$source_hash,temp$study_type)
    temp.old$code = paste(temp.old$source_hash,temp.old$study_type)
    n1 = nrow(temp)
    n2 = nrow(temp.old)
    temp3 = temp[!is.element(temp$code,temp.old$code),]
    n3 = nrow(temp3)
    cat("==============================================\n")
    cat(source,n1,n2,n3," [n1 is new records, n2 is old records, n3 is number of records to be updated]\n")
    cat("==============================================\n")
    for(i in 1:nrow(temp3)) {
      hk = temp3[i,"source_hash"]
      st = temp3[i,"study_type"]
      query = paste0("update toxval set study_type='",st,"' where source_hash='",hk,"'")
      #print(query)
      runQuery(query,toxval.db)
      if(i%%100==0) cat("finished",i,"out of",nrow(temp3),"\n")
    }

    check = runQuery(paste0("select dtxsid,source,study_type,source_hash from toxval where dtxsid!='NODTXSID' and source='",source,
                            "' and toxval_type in (select toxval_type from toxval_type_dictionary where toxval_type_supercategory in ('Point of Departure','Lethality Effect Level','Toxicity Value'))
                            and human_eco='human health'"),toxval.db)
    check = unique(check)
    check$key = paste(check[,1],check[,2],check[,3],check[,4])
    check = check[order(check$source_hash),]
    temp0 = temp0[order(temp0$source_hash),]
    missing.new = check[!is.element(check$key,temp0$key),]
    missing.old = temp0[!is.element(temp0$key,check$key),]
    n1 = nrow(missing.old)
    n2 = nrow(missing.new)
    cat("  check: ",source,n1,n2,"\n")
    #if(n1>0 || n2>0) {
    if(n2>0) {
        # temp0 is read from the input (new study type spreadsheet)
      # check is from the current version of the database
      cat("\n\nStopping here means that the study_type fix process did not work for source:",source,"\n")
      browser()
    }
  }
}
